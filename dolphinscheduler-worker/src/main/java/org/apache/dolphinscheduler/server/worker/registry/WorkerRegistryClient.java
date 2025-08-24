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

package org.apache.dolphinscheduler.server.worker.registry;

import static org.apache.dolphinscheduler.common.constants.Constants.SLEEP_TIME_MILLIS;

import org.apache.dolphinscheduler.common.IStoppable;
import org.apache.dolphinscheduler.common.model.Server;
import org.apache.dolphinscheduler.common.thread.ThreadUtils;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.extract.base.utils.Host;
import org.apache.dolphinscheduler.meter.metrics.MetricsProvider;
import org.apache.dolphinscheduler.registry.api.RegistryClient;
import org.apache.dolphinscheduler.registry.api.RegistryException;
import org.apache.dolphinscheduler.registry.api.enums.RegistryNodeType;
import org.apache.dolphinscheduler.server.worker.config.WorkerConfig;
import org.apache.dolphinscheduler.server.worker.config.WorkerServerLoadProtection;
import org.apache.dolphinscheduler.server.worker.executor.PhysicalTaskExecutorContainerProvider;
import org.apache.dolphinscheduler.server.worker.task.WorkerHeartBeatTask;

import org.apache.commons.collections4.CollectionUtils;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import javax.annotation.PostConstruct;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * DolphinScheduler Worker注册中心客户端
 * 
 * 这个类是Worker节点与注册中心（如Zookeeper或Raft）通信的核心组件。
 * 它负责：
 * 1. 将Worker节点注册到集群中，让Master节点可以发现并分发任务
 * 2. 定期发送心跳信号，上报资源使用情况和健康状态
 * 3. 监听集群中其他节点的状态变化
 * 4. 处理注册中心连接异常情况
 * 
 * 可以把Worker注册中心客户端想象为一个“招聘网站个人简历”，
 * 它会告知雇主（Master）自己的能力和状态。
 */
@Slf4j
@Service
public class WorkerRegistryClient implements AutoCloseable {

    /**
     * Worker节点配置信息 - 包含端口、组名、权重等配置
     */
    @Autowired
    private WorkerConfig workerConfig;

    /**
     * Worker服务器负载保护 - 监控资源使用情况，防止过载
     * 当CPU、内存或磁盘使用率过高时，会拒绝接收新任务
     */
    @Autowired
    private WorkerServerLoadProtection workerServerLoadProtection;

    /**
     * 物理任务执行器容器提供者 - 管理当前正在执行的任务
     * 用于统计任务执行情况和资源使用情况
     */
    @Autowired
    private PhysicalTaskExecutorContainerProvider physicalTaskExecutorContainerDelegator;

    /**
     * 注册中心客户端 - 实际与注册中心通信的对象
     */
    @Autowired
    private RegistryClient registryClient;

    /**
     * 指标提供者 - 用于获取系统资源指标（CPU、内存、磁盘等）
     */
    @Autowired
    private MetricsProvider metricsProvider;

    /**
     * Worker心跳任务 - 定期向注册中心发送心跳信号的后台线程
     * 心跳信号包含：
     * - 节点的健康状态
     * - 当前的资源使用情况（CPU、内存、磁盘）
     * - 当前正在执行的任务数量
     * - Worker的权重和分组信息
     */
    private WorkerHeartBeatTask workerHeartBeatTask;

    /**
     * 初始化Worker注册表
     * 
     * 这个方法在Spring容器初始化该Bean后自动执行（@PostConstruct注解）。
     * 主要作用是创建心跳任务对象，但还不启动。
     * 
     * 简单理解：就像准备好了简历，但还没有投递给雇主。
     */
    @PostConstruct
    public void initWorkRegistry() {
        // 创建 Worker 心跳任务对象，传入所需的所有组件
        // 这个任务会定期收集Worker节点的状态信息并上报给注册中心
        this.workerHeartBeatTask = new WorkerHeartBeatTask(
                workerConfig,                                                    // Worker配置
                workerServerLoadProtection,                                      // 负载保护组件
                metricsProvider,                                                 // 指标提供者
                registryClient,                                                  // 注册中心客户端
                physicalTaskExecutorContainerDelegator.getExecutorContainer()   // 任务执行器容器
        );
    }

    /**
     * 启动Worker注册中心客户端
     * 
     * 这个方法会执行以下步骤：
     * 1. 执行注册操作，将当前Worker节点信息登记到注册中心
     * 2. 添加连接状态监听器，处理网络断开等异常情况
     * 
     * 简单理解：就像新员工入职时的“报到”流程，
     * 告知公司“我来上班了，可以给我分配工作了”。
     * 
     * @throws RegistryException 如果注册过程中出现错误
     */
    public void start() {
        try {
            // 执行注册操作，将Worker节点信息写入注册中心
            registry();
            
            // 添加连接状态监听器，当与注册中心的连接出现问题时进行处理
            // 例如：连接断开时尝试重新连接，或者停止接收新任务
            registryClient.addConnectionStateListener(new WorkerConnectionStateListener(registryClient));
        } catch (Exception ex) {
            throw new RegistryException("Worker registry client start up error", ex);
        }
    }

    /**
     * 将当前Worker服务器注册到注册中心
     * 
     * 这是Worker节点加入集群的核心步骤，包括：
     * 1. 获取Worker节点的注册路径
     * 2. 先删除旧的注册信息（防止重复注册）
     * 3. 将本节点信息作为临时节点存储到注册中心
     * 4. 验证注册是否成功
     * 5. 启动心跳任务
     * 
     * 注意：使用“临时节点”的好处是，当Worker宕机时，
     * 注册中心会自动删除这个节点信息，Master就知道这个Worker已经不可用了。
     */
    private void registry() {
        // 获取当前Worker节点在注册中心中的路径
        // 路径格式类似：/dolphinscheduler/workers/default/192.168.1.100:1234
        String workerRegistryPath = workerConfig.getWorkerRegistryPath();
        
        // 先删除旧的注册信息，防止重复注册或者之前的残留数据
        registryClient.remove(workerRegistryPath);
        
        // 将当前节点信息作为“临时节点”存储到注册中心
        // 存储的内容包含：节点地址、端口、资源信息、所属组、权重等
        registryClient.persistEphemeral(workerRegistryPath, JSONUtils.toJsonString(workerHeartBeatTask.getHeartBeat()));
        
        log.info("Worker node: {} registry to registry center {} successfully", workerConfig.getWorkerAddress(),
                workerRegistryPath);

        // 验证注册是否成功，循环检查直到确认节点已经存在于注册中心
        while (!registryClient.checkNodeExists(workerConfig.getWorkerAddress(), RegistryNodeType.WORKER)) {
            // 等待一段时间后再次检查
            ThreadUtils.sleep(SLEEP_TIME_MILLIS);
        }

        // 注册成功后，启动心跳任务，定期向注册中心发送状态更新
        workerHeartBeatTask.start();
        
        log.info("Worker node: {} registry finished", workerConfig.getWorkerAddress());
    }

    /**
     * 获取告警服务器地址
     * 
     * Worker节点在某些情况下需要直接向告警服务器发送告警信息，
     * 这个方法从注册中心获取可用的告警服务器地址。
     * 
     * 使用场景：
     * - 任务执行失败时的告警通知
     * - 资源使用超限的告警通知
     * - Worker节点异常的告警通知
     * 
     * @return 告警服务器地址，如果没有可用的服务器则返回Optional.empty()
     */
    public Optional<Host> getAlertServerAddress() {
        // 从注册中心获取所有告警服务器列表
        List<Server> serverList = registryClient.getServerList(RegistryNodeType.ALERT_SERVER);
        
        // 如果没有可用的告警服务器，返回空值
        if (CollectionUtils.isEmpty(serverList)) {
            return Optional.empty();
        }
        
        // 简单策略：取第一个可用的告警服务器
        // TODO: 可以实现负载均衡策略来选择最优的服务器
        Server server = serverList.get(0);
        return Optional.of(new Host(server.getHost(), server.getPort()));
    }

    /**
     * 设置注册中心的停止回调对象
     * 
     * 当注册中心检测到连接问题时，会调用这个对象的stop方法来停止Worker服务
     * 这是一种安全机制，防止Worker节点在失去与集群连接时仍然执行任务
     * 
     * 简单理解：就像给手机设置一个“断网自动停止下载”功能一样。
     * 
     * @param stoppable 可停止的对象，通常是WorkerServer本身
     */
    public void setRegistryStoppable(IStoppable stoppable) {
        registryClient.setStoppable(stoppable);
    }

    @Override
    public void close() throws IOException {
        if (workerHeartBeatTask != null) {
            workerHeartBeatTask.shutdown();
        }
        registryClient.close();
        log.info("Closed WorkerRegistryClient");
    }

    public boolean isAvailable() {
        return registryClient.isConnected();
    }
}
