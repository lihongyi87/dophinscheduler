/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dolphinscheduler.server.master.registry;

import static org.apache.dolphinscheduler.common.constants.Constants.SLEEP_TIME_MILLIS;

import org.apache.dolphinscheduler.common.IStoppable;
import org.apache.dolphinscheduler.common.thread.ThreadUtils;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.common.utils.NetUtils;
import org.apache.dolphinscheduler.meter.metrics.MetricsProvider;
import org.apache.dolphinscheduler.registry.api.RegistryClient;
import org.apache.dolphinscheduler.registry.api.RegistryException;
import org.apache.dolphinscheduler.registry.api.enums.RegistryNodeType;
import org.apache.dolphinscheduler.server.master.config.MasterConfig;
import org.apache.dolphinscheduler.server.master.config.MasterServerLoadProtection;
import org.apache.dolphinscheduler.server.master.engine.MasterCoordinator;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * DolphinScheduler Master注册中心客户端
 * 
 * 这个类是Master节点与注册中心（如Zookeeper或Raft）通信的核心组件。
 * 它负责：
 * 1. 将Master节点注册到集群中，让其他节点知道这个节点的存在
 * 2. 定期发送心跳信号，告知其他节点自己还活着
 * 3. 监听集群中其他节点的状态变化（如Worker节点上下线）
 * 4. 处理注册中心连接异常情况
 * 
 * 可以把Master注册中心客户端想象为一个“联络员”，它负责维持与其他节点的通信。
 */
@Component
@Slf4j
public class MasterRegistryClient implements AutoCloseable {

    /**
     * 注册中心客户端 - 实际与注册中心通信的对象
     * 可能是Zookeeper客户端或者Raft客户端，取决于配置
     */
    @Autowired
    private RegistryClient registryClient;

    /**
     * Master节点配置信息 - 包含端口、心跳间隔等配置
     */
    @Autowired
    private MasterConfig masterConfig;

    /**
     * Master服务器负载保护 - 监控资源使用情况，防止过载
     */
    @Autowired
    private MasterServerLoadProtection masterServerLoadProtection;

    /**
     * 指标提供者 - 用于获取系统资源指标（CPU、内存等）
     */
    @Autowired
    private MetricsProvider metricsProvider;

    /**
     * Master协调器 - 负责Master节点间的协调和选举逻辑
     */
    @Autowired
    private MasterCoordinator masterCoordinator;

    /**
     * Master心跳任务 - 定期向注册中心发送心跳信号的后台线程
     * 心跳信号包含：
     * - 节点的健康状态
     * - 当前的资源使用情况（CPU、内存等）
     * - 节点的工作负载情况
     */
    private MasterHeartBeatTask masterHeartBeatTask;

    /**
     * 启动Master注册中心客户端
     * 
     * 这个方法会执行以下步骤：
     * 1. 创建心跳任务，设置定期向泣册中心发送状态信息
     * 2. 执行注册操作，将当前Master节点的信息登记到注册中心
     * 3. 添加连接状态监听器，处理网络断开等异常情况
     * 
     * 简单理解：就像上班签到一样，告知公司“我来上班了”。
     * 
     * @throws RegistryException 如果注册过程中出现错误
     */
    public void start() {
        try {
            // 创建心跳任务，这个任务会定期向注册中心发送心跳信号
            // 心跳信号包含节点的健康状态、资源使用情况等信息
            this.masterHeartBeatTask = new MasterHeartBeatTask(masterConfig, masterServerLoadProtection,
                    metricsProvider, registryClient, masterCoordinator);
            
            // 执行注册操作，将当前节点信息写入注册中心
            registry();
            
            // 添加连接状态监听器，当与注册中心的连接出现问题时进行处理
            // 例如：连接断开时尝试重新连接，或者触发故障转移逻辑
            registryClient.addConnectionStateListener(new MasterConnectionStateListener(registryClient));
        } catch (Exception e) {
            throw new RegistryException("Master registry client start up error", e);
        }
    }

    /**
     * 设置注册中心的停止回调对象
     * 
     * 当注册中心检测到连接问题时，会调用这个对象的stop方法来停止Master服务
     * 这是一种安全机制，防止“脑裂”问题（即多个Master认为自己是主节点）
     * 
     * 简单理解：就像给电脑设置一个“断网自动关机”功能一样。
     * 
     * @param stoppable 可停止的对象，通常是MasterServer本身
     */
    public void setRegistryStoppable(IStoppable stoppable) {
        registryClient.setStoppable(stoppable);
    }

    /**
     * 关闭注册中心客户端并清理资源
     * 
     * 这个方法会执行以下步骤：
     * 1. 停止心跳任务，不再向注册中心发送心跳信号
     * 2. 检查与注册中心的连接状态
     * 3. 如果连接正常，从注册中心中删除本节点信息
     * 
     * 简单理解：就像下班时的“签退”操作，告知公司“我下班了”。
     */
    @Override
    public void close() {
        // TODO: 需要取消订阅MasterRegistryDataListener，停止监听其他节点状态
        
        // 停止心跳任务，不再向注册中心发送心跳信号
        if (masterHeartBeatTask != null) {
            masterHeartBeatTask.shutdown();
        }
        
        // 如果与注册中心的连接还在，则执行注销操作
        // 从注册中心中删除本 Master 节点的信息
        if (registryClient.isConnected()) {
            deregister();
        }
        
        log.info("Closed MasterRegistryClient");
    }

    /**
     * 将当前Master服务器注册到注册中心
     * 
     * 这是Master节点加入集群的核心步骤，包括：
     * 1. 获取Master节点的注册路径（类似于“工位号”）
     * 2. 先删除旧的注册信息（防止重复注册）
     * 3. 将本节点信息作为临时节点存储到注册中心
     * 4. 验证注册是否成功
     * 5. 启动心跳任务
     * 
     * 注意：使用“临时节点”的好处是，当Master宕机时，
     * 注册中心会自动删除这个节点信息，其他节点就知道这个Master已经不可用了。
     */
    void registry() {
        log.info("Master node : {} registering to registry center", masterConfig.getMasterAddress());
        
        // 获取当前Master节点在注册中心中的路径
        // 路径格式类似：/dolphinscheduler/masters/192.168.1.100:5678
        String masterRegistryPath = masterConfig.getMasterRegistryPath();

        // 先删除旧的注册信息，防止重复注册或者之前的残留数据
        registryClient.remove(masterRegistryPath);
        
        // 将当前节点信息作为“临时节点”存储到注册中心
        // 临时节点的特点：当连接断开时会自动被删除
        // 存储的内容包含：节点地址、端口、资源信息等
        registryClient.persistEphemeral(masterRegistryPath, JSONUtils.toJsonString(masterHeartBeatTask.getHeartBeat()));

        // 验证注册是否成功，循环检查直到确认节点已经存在于注册中心
        while (!registryClient.checkNodeExists(NetUtils.getHost(), RegistryNodeType.MASTER)) {
            log.warn("The current master server node:{} cannot find in registry", NetUtils.getHost());
            // 等待一段时间后再次检查
            ThreadUtils.sleep(SLEEP_TIME_MILLIS);
        }

        // 注册成功后，启动心跳任务，定期向注册中心发送状态更新
        masterHeartBeatTask.start();
        
        log.info("Master node : {} registered to registry center successfully", masterConfig.getMasterAddress());

    }

    public void deregister() {
        try {
            registryClient.remove(masterConfig.getMasterRegistryPath());
            log.info("Master node : {} unRegistry to register center.", masterConfig.getMasterAddress());
            if (masterHeartBeatTask != null) {
                masterHeartBeatTask.shutdown();
            }
            registryClient.close();
        } catch (Exception e) {
            log.error("MasterServer remove registry path exception ", e);
        }
    }

    public boolean isAvailable() {
        return registryClient.isConnected();
    }
}
