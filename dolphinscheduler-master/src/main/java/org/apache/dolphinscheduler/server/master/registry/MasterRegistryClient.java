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
            // 这一步会创建临时节点，让其他节点知道这个Master已经上线
            registry();

            // 添加连接状态监听器，当与注册中心的连接出现问题时进行处理
            // 监听器会在以下情况触发：
            // - 网络连接断开：尝试重新连接
            // - 会话超时：重新创建会话
            // - 注册中心服务器故障：切换到备用服务器
            registryClient.addConnectionStateListener(new MasterConnectionStateListener(registryClient));
        } catch (Exception e) {
            // 如果启动过程中出现任何异常，都包装成RegistryException抛出
            // 这会导致Master服务启动失败，符合快速失败的设计原则
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
        // 将可停止对象设置到注册中心客户端
        // 当检测到连接问题时，注册中心客户端会调用stoppable.stop()方法
        // 这是一种防御性编程，避免Master在失去集群连接后继续工作造成数据不一致
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
        // 心跳停止后，其他节点会在超时后检测到该Master已经离线
        if (masterHeartBeatTask != null) {
            masterHeartBeatTask.shutdown();
        }

        // 如果与注册中心的连接还在，则执行注销操作
        // 主动注销比等待心跳超时更快，能立即通知其他节点
        if (registryClient.isConnected()) {
            // 调用注销方法，从注册中心删除本Master节点的信息
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
        // 这是一种清理机制，确保从干净的状态开始注册
        registryClient.remove(masterRegistryPath);

        // 将当前节点信息作为"临时节点"存储到注册中心
        // 临时节点的特点：当连接断开时会自动被删除
        // 存储的内容包含：节点地址、端口、资源信息等
        // 使用心跳任务的心跳数据作为节点信息
        registryClient.persistEphemeral(masterRegistryPath, JSONUtils.toJsonString(masterHeartBeatTask.getHeartBeat()));

        // 验证注册是否成功，循环检查直到确认节点已经存在于注册中心
        // 这是一个安全检查，确保注册真正生效
        while (!registryClient.checkNodeExists(NetUtils.getHost(), RegistryNodeType.MASTER)) {
            log.warn("The current master server node:{} cannot find in registry", NetUtils.getHost());
            // 等待一段时间后再次检查，避免频繁查询给注册中心造成压力
            ThreadUtils.sleep(SLEEP_TIME_MILLIS);
        }

        // 注册成功后，启动心跳任务，定期向注册中心发送状态更新
        // 心跳任务会定期更新节点的健康状态和资源使用情况
        masterHeartBeatTask.start();

        log.info("Master node : {} registered to registry center successfully", masterConfig.getMasterAddress());

    }

    /**
     * 从注册中心注销当前Master节点
     *
     * 这个方法在Master正常关闭时执行注销操作，与异常断开不同：
     * - 正常关闭：主动调用此方法，从注册中心删除节点信息
     * - 异常断开：注册中心会自动检测并删除临时节点
     *
     * 注销步骤：
     * 1. 从注册中心删除节点路径和数据
     * 2. 停止心跳任务，不再发送状态更新
     * 3. 关闭注册中心客户端连接
     *
     * 为什么要主动注销？
     * 主动注销可以立即通知其他节点该Master已经下线，
     * 而不用等待心跳超时检测，提高集群响应速度。
     */
    public void deregister() {
        try {
            // 从注册中心中删除该Master节点的注册信息
            // 这会触发其他节点的监听器，通知它们该Master已经下线
            // 删除操作是原子性的，确保状态变更的一致性
            registryClient.remove(masterConfig.getMasterRegistryPath());
            log.info("Master node : {} unRegistry to register center.", masterConfig.getMasterAddress());

            // 停止心跳任务，不再向注册中心发送状态更新
            // 这步骤确保不会有残留的心跳数据发送
            if (masterHeartBeatTask != null) {
                masterHeartBeatTask.shutdown();
            }

            // 关闭注册中心客户端连接，释放网络资源
            // 这是最后的清理步骤，确保所有资源都被正确释放
            registryClient.close();
        } catch (Exception e) {
            // 注销过程中的异常不应该阻止Master的关闭
            // 记录错误日志但不抛出异常，采用降级策略
            log.error("MasterServer remove registry path exception ", e);
        }
    }

    /**
     * 检查Master注册中心客户端是否可用
     *
     * 这个方法用于判断当前Master节点是否能够正常与注册中心通信。
     * 在以下场景下特别有用：
     * 1. 健康检查：判断该Master是否还能参与集群协调
     * 2. 故障转移：决定是否需要将任务转移到其他Master
     * 3. 负载均衡：决定是否可以接收新的任务分配
     *
     * 注意：这个方法只检查网络连接状态，不代表Master本身的业务状态。
     * 完整的健康检查还需要结合CPU、内存等资源指标。
     *
     * @return true 如果注册中心客户端连接正常，false 否则
     */
    public boolean isAvailable() {
        // 检查注册中心客户端的连接状态
        // 这个方法返回true表示：
        // 1. 网络连接正常
        // 2. 会话状态有效
        // 3. 可以正常进行读写操作
        // 但不代表Master本身的业务处理能力正常
        return registryClient.isConnected();
    }
}
