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

package org.apache.dolphinscheduler.server.worker.task;

import org.apache.dolphinscheduler.common.enums.ServerStatus;
import org.apache.dolphinscheduler.common.lifecycle.ServerLifeCycleManager;
import org.apache.dolphinscheduler.common.model.BaseHeartBeatTask;
import org.apache.dolphinscheduler.common.model.WorkerHeartBeat;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.common.utils.NetUtils;
import org.apache.dolphinscheduler.common.utils.OSUtils;
import org.apache.dolphinscheduler.meter.metrics.MetricsProvider;
import org.apache.dolphinscheduler.meter.metrics.SystemMetrics;
import org.apache.dolphinscheduler.registry.api.RegistryClient;
import org.apache.dolphinscheduler.registry.api.utils.RegistryUtils;
import org.apache.dolphinscheduler.server.worker.config.WorkerConfig;
import org.apache.dolphinscheduler.server.worker.config.WorkerServerLoadProtection;
import org.apache.dolphinscheduler.server.worker.metrics.WorkerServerMetrics;
import org.apache.dolphinscheduler.task.executor.container.ITaskExecutorContainer;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * Worker节点心跳任务
 *
 * 此类负责定期向注册中心发送Worker节点的心跳信息，确保集群中的Master节点能够
 * 及时感知Worker节点的存活状态和负载情况。心跳任务是Worker节点自我注册和健康
 * 检查的重要机制。
 *
 * 主要功能：
 * 1. 定期收集Worker节点的系统资源信息（CPU、内存、磁盘使用率等）
 * 2. 收集JVM相关的运行时信息（堆内存、非堆内存、垃圾回收状态等）
 * 3. 报告Worker节点的任务执行线程池使用情况
 * 4. 根据负载保护策略判断节点状态（正常/繁忙）
 * 5. 将心跳信息以临时节点的形式写入注册中心
 * 6. 检测是否已被故障转移，如发现则自动关闭服务
 *
 * 心跳发送机制：
 * - 发送频率：由workerConfig.getMaxHeartbeatInterval()配置决定
 * - 心跳内容：包含系统指标、JVM指标、线程池状态、Worker配置信息等
 * - 存储方式：在注册中心以临时节点形式存储，节点断开时自动删除
 * - 故障检测：通过检查故障转移节点路径来判断是否已被故障转移
 */
@Slf4j
public class WorkerHeartBeatTask extends BaseHeartBeatTask<WorkerHeartBeat> {

    /** Worker配置信息，包含心跳间隔、监听端口、工作组等配置 */
    private final WorkerConfig workerConfig;
    /** Worker服务器负载保护策略，用于判断服务器是否过载 */
    private final WorkerServerLoadProtection workerServerLoadProtection;
    /** 注册中心客户端，用于向注册中心写入心跳信息 */
    private final RegistryClient registryClient;

    /** 系统指标提供器，用于获取CPU、内存等系统资源使用情况 */
    private final MetricsProvider metricsProvider;

    /** 当前Worker进程的进程ID */
    private final int processId;

    /** 任务执行器容器，用于获取线程池使用情况 */
    private final ITaskExecutorContainer taskExecutorContainer;

    /**
     * 构造Worker心跳任务
     *
     * @param workerConfig Worker配置信息
     * @param workerServerLoadProtection Worker负载保护策略
     * @param metricsProvider 系统指标提供器
     * @param registryClient 注册中心客户端
     * @param taskExecutorContainer 任务执行器容器
     */
    public WorkerHeartBeatTask(@NonNull WorkerConfig workerConfig,
                               @NonNull WorkerServerLoadProtection workerServerLoadProtection,
                               @NonNull MetricsProvider metricsProvider,
                               @NonNull RegistryClient registryClient,
                               @NonNull ITaskExecutorContainer taskExecutorContainer) {
        // 调用父类构造方法，设置心跳任务名称和心跳间隔
        // 心跳间隔由Worker配置中的maxHeartbeatInterval决定
        super("WorkerHeartBeatTask", workerConfig.getMaxHeartbeatInterval().toMillis());
        // 初始化Worker服务器负载保护策略，用于判断服务器是否过载
        this.workerServerLoadProtection = workerServerLoadProtection;
        // 初始化系统指标提供器，用于获取CPU、内存等系统资源信息
        this.metricsProvider = metricsProvider;
        // 保存Worker配置信息，包含端口、工作组、权重等重要配置
        this.workerConfig = workerConfig;
        // 初始化注册中心客户端，用于向注册中心写入心跳信息
        this.registryClient = registryClient;
        // 初始化任务执行器容器，用于获取任务执行线程池的使用情况
        this.taskExecutorContainer = taskExecutorContainer;
        // 获取当前Worker进程的进程ID，用于标识和监控
        this.processId = OSUtils.getProcessID();
    }

    /**
     * 构建并返回Worker心跳信息
     *
     * 此方法收集Worker节点的各种运行时信息，包括：
     * - 系统资源使用情况（CPU、内存、磁盘）
     * - JVM运行状态（堆内存、非堆内存、GC情况）
     * - 任务执行线程池使用率
     * - Worker配置信息（主机权重、工作组、端口等）
     * - 服务器状态（根据负载保护策略判断是否繁忙）
     *
     * @return 包含完整Worker状态信息的心跳对象
     */
    @Override
    public WorkerHeartBeat getHeartBeat() {
        // 从指标提供器获取当前系统的实时性能指标
        // 包括CPU使用率、内存使用率、磁盘使用率、JVM相关指标等
        SystemMetrics systemMetrics = metricsProvider.getSystemMetrics();

        return WorkerHeartBeat.builder()
                // 设置服务启动时间，用于计算服务运行时长
                .startupTime(ServerLifeCycleManager.getServerStartupTime())  // 服务启动时间
                // 设置当前心跳报告的时间戳，用于Master判断心跳的时效性
                .reportTime(System.currentTimeMillis())                      // 心跳报告时间
                // 设置JVM进程的CPU使用率，反映Java应用的CPU消耗情况
                .jvmCpuUsage(systemMetrics.getJvmCpuUsagePercentage())       // JVM CPU使用率
                // 设置整个系统的CPU使用率，反映服务器的整体负载情况
                .cpuUsage(systemMetrics.getSystemCpuUsagePercentage())       // 系统CPU使用率
                // 设置JVM内存使用率，包括堆内存和非堆内存的总体使用情况
                .jvmMemoryUsage(systemMetrics.getJvmMemoryUsedPercentage())  // JVM内存使用率
                // 设置JVM堆内存已使用的字节数，用于监控对象分配情况
                .jvmHeapUsed(systemMetrics.getJvmHeapUsed())                 // JVM堆内存已使用量
                // 设置JVM堆内存的最大可用字节数，用于计算堆内存使用率
                .jvmHeapMax(systemMetrics.getJvmHeapMax())                   // JVM堆内存最大值
                // 设置JVM非堆内存已使用的字节数，包括方法区、代码缓存等
                .jvmNonHeapUsed(systemMetrics.getJvmNonHeapUsed())           // JVM非堆内存已使用量
                // 设置JVM非堆内存的最大可用字节数
                .jvmNonHeapMax(systemMetrics.getJvmNonHeapMax())             // JVM非堆内存最大值
                // 设置系统物理内存的使用率，反映整个服务器的内存负载
                .memoryUsage(systemMetrics.getSystemMemoryUsedPercentage())  // 系统内存使用率
                // 设置磁盘使用率，用于监控存储空间是否充足
                .diskUsage(systemMetrics.getDiskUsedPercentage())            // 磁盘使用率
                // 设置Worker进程的进程ID，用于进程监控和管理
                .processId(processId)                                        // Worker进程ID
                // 设置主机权重，用于Master进行任务分配时的负载均衡
                .workerHostWeight(workerConfig.getHostWeight())              // 主机权重
                // 设置任务执行线程池的使用率，反映Worker的任务处理能力
                .threadPoolUsage(taskExecutorContainer.slotUsage())          // 线程池使用率
                // 根据负载保护策略判断服务器状态，决定是否接受新的任务分配
                .serverStatus(                                               // 服务器状态
                        // 如果系统过载则标记为BUSY，否则标记为NORMAL
                        workerServerLoadProtection.isOverload(systemMetrics) ? ServerStatus.BUSY : ServerStatus.NORMAL)
                // 设置Worker所在主机的IP地址或主机名
                .host(NetUtils.getHost())                                    // 主机地址
                // 设置Worker监听的RPC端口号，用于Master与Worker的通信
                .port(workerConfig.getListenPort())                          // 监听端口
                // 设置Worker所属的工作组，用于任务的分组执行
                .workerGroup(workerConfig.getGroup())                        // Worker工作组
                .build();
    }

    /**
     * 将Worker心跳信息写入注册中心
     *
     * 此方法执行以下操作：
     * 1. 检查是否已存在故障转移节点，如果存在则说明当前Worker已被故障转移，需要自动关闭
     * 2. 将心跳信息序列化为JSON格式
     * 3. 在注册中心创建临时节点存储心跳信息，节点断开时会自动删除
     * 4. 更新心跳发送次数的监控指标
     *
     * 故障转移检测机制：
     * - 通过检查注册中心中是否存在对应的故障转移节点来判断
     * - 如果发现故障转移节点存在，说明Master已经检测到当前Worker故障并进行了故障转移
     * - 此时当前Worker应该主动关闭，避免出现脑裂情况
     *
     * @param workerHeartBeat 要写入的Worker心跳信息
     */
    @Override
    public void writeHeartBeat(final WorkerHeartBeat workerHeartBeat) {
        // 检查是否已被故障转移
        // 构建故障转移节点的路径，用于检查当前Worker是否已被Master标记为故障
        final String failoverNodePath = RegistryUtils.getFailoveredNodePath(workerHeartBeat);
        // 检查注册中心中是否存在故障转移节点
        if (registryClient.exists(failoverNodePath)) {
            // 如果存在故障转移节点，说明Master已经检测到当前Worker故障并进行了故障转移
            log.warn("The worker: {} is under {}, means it has been failover will close myself",
                    workerHeartBeat,
                    failoverNodePath);
            // 主动关闭当前Worker服务，避免出现脑裂情况
            // 通过注册中心客户端的可停止接口来优雅关闭服务
            registryClient
                    .getStoppable()
                    .stop("The worker exist: " + failoverNodePath + ", means it has been failover will close myself");
            return;
        }

        // 将心跳信息序列化并写入注册中心
        // 将心跳对象转换为JSON字符串格式，便于在注册中心中存储和传输
        String workerHeartBeatJson = JSONUtils.toJsonString(workerHeartBeat);
        // 获取Worker在注册中心的注册路径，每个Worker有唯一的注册路径
        String workerRegistryPath = workerConfig.getWorkerRegistryPath();
        // 在注册中心创建临时节点存储心跳信息
        // 使用临时节点的好处是Worker断开连接时节点会自动删除
        registryClient.persistEphemeral(workerRegistryPath, workerHeartBeatJson);

        // 更新监控指标
        // 增加心跳发送次数的计数器，用于监控心跳的发送频率和成功率
        WorkerServerMetrics.incWorkerHeartbeatCount();
        // 记录调试日志，便于问题排查和系统监控
        log.debug(
                "Success write worker group heartBeatInfo into registry, workerRegistryPath: {} workerHeartBeatInfo: {}",
                workerRegistryPath,
                workerHeartBeatJson);
    }

}
