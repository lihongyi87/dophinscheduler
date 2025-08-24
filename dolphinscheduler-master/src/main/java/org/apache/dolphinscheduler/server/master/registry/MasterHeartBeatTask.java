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

import org.apache.dolphinscheduler.common.enums.ServerStatus;
import org.apache.dolphinscheduler.common.lifecycle.ServerLifeCycleManager;
import org.apache.dolphinscheduler.common.model.BaseHeartBeatTask;
import org.apache.dolphinscheduler.common.model.MasterHeartBeat;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.common.utils.NetUtils;
import org.apache.dolphinscheduler.common.utils.OSUtils;
import org.apache.dolphinscheduler.meter.metrics.MetricsProvider;
import org.apache.dolphinscheduler.meter.metrics.SystemMetrics;
import org.apache.dolphinscheduler.registry.api.RegistryClient;
import org.apache.dolphinscheduler.registry.api.utils.RegistryUtils;
import org.apache.dolphinscheduler.server.master.config.MasterConfig;
import org.apache.dolphinscheduler.server.master.config.MasterServerLoadProtection;
import org.apache.dolphinscheduler.server.master.engine.MasterCoordinator;
import org.apache.dolphinscheduler.server.master.metrics.MasterServerMetrics;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * Master心跳任务类
 * 
 * 这个类负责定期向注册中心发送Master节点的心跳信号。
 * 心跳信号包含了节点的各种状态信息，供其他节点和管理组件参考。
 * 
 * 简单理解：就像每天的“打卡上班”，向公司报告自己的工作状态。
 * 
 * 心跳信号内容包括：
 * - 节点的健康状态（正常/繁忙/过载）
 * - 资源使用情况（CPU、内存、磁盘）
 * - 当前处理的工作流数量
 * - 节点的负载情况
 */
@Slf4j
public class MasterHeartBeatTask extends BaseHeartBeatTask<MasterHeartBeat> {

    /**
     * Master配置对象 - 包含心跳间隔、端口等配置
     */
    private final MasterConfig masterConfig;

    /**
     * Master服务器负载保护组件 - 用于判断节点是否过载
     */
    private MasterServerLoadProtection masterServerLoadProtection;

    /**
     * 指标提供者 - 用于获取系统资源指标
     */
    private final MetricsProvider metricsProvider;

    /**
     * 注册中心客户端 - 用于向注册中心发送心跳信号
     */
    private final RegistryClient registryClient;

    /**
     * Master协调器 - 提供集群协调相关信息
     */
    private final MasterCoordinator masterCoordinator;

    /**
     * 心跳信号在注册中心中的存储路径
     */
    private final String heartBeatPath;

    /**
     * 当前进程ID - 用于唯一标识进程
     */
    private final int processId;

    /**
     * Master心跳任务构造函数
     * 
     * @param masterConfig Master配置对象
     * @param masterServerLoadProtection 负载保护组件
     * @param metricsProvider 指标提供者
     * @param registryClient 注册中心客户端
     * @param masterCoordinator Master协调器
     */
    public MasterHeartBeatTask(@NonNull MasterConfig masterConfig,
                               @NonNull MasterServerLoadProtection masterServerLoadProtection,
                               @NonNull MetricsProvider metricsProvider,
                               @NonNull RegistryClient registryClient,
                               @NonNull MasterCoordinator masterCoordinator) {
        // 调用父类构造函数，设置任务名称和心跳间隔
        super("MasterHeartBeatTask", masterConfig.getMaxHeartbeatInterval().toMillis());
        
        // 初始化所有组件引用
        this.masterConfig = masterConfig;
        this.masterServerLoadProtection = masterServerLoadProtection;
        this.metricsProvider = metricsProvider;
        this.registryClient = registryClient;
        this.masterCoordinator = masterCoordinator;
        
        // 获取Master节点在注册中心中的路径
        this.heartBeatPath = masterConfig.getMasterRegistryPath();
        
        // 获取当前进程ID，用于唯一标识进程
        this.processId = OSUtils.getProcessID();
    }

    /**
     * 获取当前的心跳信号数据
     * 
     * 这个方法会收集Master节点当前的所有状态信息，组装成一个心跳对象。
     * 这些信息会被发送到注册中心，供其他节点和管理组件使用。
     * 
     * 简单理解：就像做体检时的“健康报告”，包含身体的各项指标。
     * 
     * @return 包含Master节点所有状态信息的心跳对象
     */
    @Override
    public MasterHeartBeat getHeartBeat() {
        // 获取系统资源指标
        SystemMetrics systemMetrics = metricsProvider.getSystemMetrics();
        
        // 构建心跳信号对象，包含所有重要的状态信息
        return MasterHeartBeat.builder()
                .startupTime(ServerLifeCycleManager.getServerStartupTime())    // 节点启动时间
                .reportTime(System.currentTimeMillis())                        // 当前报告时间
                .jvmCpuUsage(systemMetrics.getJvmCpuUsagePercentage())         // JVM CPU使用率
                .cpuUsage(systemMetrics.getSystemCpuUsagePercentage())         // 系统CPU使用率
                .jvmMemoryUsage(systemMetrics.getJvmMemoryUsedPercentage())    // JVM内存使用率
                .jvmHeapUsed(systemMetrics.getJvmHeapUsed())                   // JVM堆内存使用量
                .jvmHeapMax(systemMetrics.getJvmHeapMax())
                .jvmNonHeapUsed(systemMetrics.getJvmNonHeapUsed())
                .jvmNonHeapMax(systemMetrics.getJvmNonHeapMax())
                .memoryUsage(systemMetrics.getSystemMemoryUsedPercentage())
                .diskUsage(systemMetrics.getDiskUsedPercentage())
                .processId(processId)
                .serverStatus(
                        masterServerLoadProtection.isOverload(systemMetrics) ? ServerStatus.BUSY : ServerStatus.NORMAL)
                .host(NetUtils.getHost())
                .port(masterConfig.getListenPort())
                .isCoordinator(masterCoordinator.isActive())
                .build();
    }

    @Override
    public void writeHeartBeat(final MasterHeartBeat masterHeartBeat) {
        final String failoverNodePath = RegistryUtils.getFailoveredNodePath(masterHeartBeat);
        if (registryClient.exists(failoverNodePath)) {
            log.warn("The master: {} is under {}, means it has been failover will close myself",
                    masterHeartBeat,
                    failoverNodePath);
            registryClient
                    .getStoppable()
                    .stop("The master exist: " + failoverNodePath + ", means it has been failover will close myself");
            return;
        }
        String masterHeartBeatJson = JSONUtils.toJsonString(masterHeartBeat);
        registryClient.persistEphemeral(heartBeatPath, masterHeartBeatJson);
        MasterServerMetrics.incMasterHeartbeatCount();
        log.debug("Success write master heartBeatInfo into registry, masterRegistryPath: {}, heartBeatInfo: {}",
                heartBeatPath,
                masterHeartBeatJson);
    }

}
