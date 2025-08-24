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

package org.apache.dolphinscheduler.server.master;

import org.apache.dolphinscheduler.common.CommonConfiguration;
import org.apache.dolphinscheduler.common.IStoppable;
import org.apache.dolphinscheduler.common.constants.Constants;
import org.apache.dolphinscheduler.common.lifecycle.ServerLifeCycleManager;
import org.apache.dolphinscheduler.common.thread.DefaultUncaughtExceptionHandler;
import org.apache.dolphinscheduler.common.thread.ThreadUtils;
import org.apache.dolphinscheduler.dao.DaoConfiguration;
import org.apache.dolphinscheduler.meter.metrics.MetricsProvider;
import org.apache.dolphinscheduler.meter.metrics.SystemMetrics;
import org.apache.dolphinscheduler.plugin.datasource.api.plugin.DataSourceProcessorProvider;
import org.apache.dolphinscheduler.plugin.storage.api.StorageConfiguration;
import org.apache.dolphinscheduler.plugin.task.api.TaskPluginManager;
import org.apache.dolphinscheduler.registry.api.RegistryConfiguration;
import org.apache.dolphinscheduler.scheduler.api.SchedulerApi;
import org.apache.dolphinscheduler.server.master.cluster.ClusterManager;
import org.apache.dolphinscheduler.server.master.cluster.ClusterStateMonitors;
import org.apache.dolphinscheduler.server.master.engine.MasterCoordinator;
import org.apache.dolphinscheduler.server.master.engine.WorkflowEngine;
import org.apache.dolphinscheduler.server.master.engine.system.SystemEventBus;
import org.apache.dolphinscheduler.server.master.engine.system.SystemEventBusFireWorker;
import org.apache.dolphinscheduler.server.master.engine.system.event.GlobalMasterFailoverEvent;
import org.apache.dolphinscheduler.server.master.engine.task.dispatcher.WorkerGroupDispatcherCoordinator;
import org.apache.dolphinscheduler.server.master.metrics.MasterServerMetrics;
import org.apache.dolphinscheduler.server.master.registry.MasterRegistryClient;
import org.apache.dolphinscheduler.server.master.rpc.MasterRpcServer;
import org.apache.dolphinscheduler.server.master.utils.MasterThreadFactory;
import org.apache.dolphinscheduler.service.ServiceConfiguration;
import org.apache.dolphinscheduler.service.bean.SpringApplicationContext;

import java.util.Date;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;

@Slf4j
@Import({DaoConfiguration.class,
        ServiceConfiguration.class,
        CommonConfiguration.class,
        StorageConfiguration.class,
        RegistryConfiguration.class})
@SpringBootApplication
public class MasterServer implements IStoppable {

    @Autowired
    private SpringApplicationContext springApplicationContext;

    @Autowired
    private MasterRegistryClient masterRegistryClient;

    @Autowired
    private WorkflowEngine workflowEngine;

    @Autowired
    private SchedulerApi schedulerApi;

    @Autowired
    private MasterRpcServer masterRPCServer;

    @Autowired
    private MetricsProvider metricsProvider;

    @Autowired
    private ClusterStateMonitors clusterStateMonitors;

    @Autowired
    private ClusterManager clusterManager;

    @Autowired
    private SystemEventBus systemEventBus;

    @Autowired
    private SystemEventBusFireWorker systemEventBusFireWorker;

    @Autowired
    private MasterCoordinator masterCoordinator;

    @Autowired
    private WorkerGroupDispatcherCoordinator workerGroupDispatcherCoordinator;

    public static void main(String[] args) {
        MasterServerMetrics.registerUncachedException(DefaultUncaughtExceptionHandler::getUncaughtExceptionCount);

        Thread.setDefaultUncaughtExceptionHandler(DefaultUncaughtExceptionHandler.getInstance());
        Thread.currentThread().setName(Constants.THREAD_NAME_MASTER_SERVER);
        SpringApplication.run(MasterServer.class);
    }

    /**
     * Master服务器初始化方法 - 启动所有核心组件
     * 这是Master节点的核心启动流程，按顺序启动各个子系统
     */
    @PostConstruct
    public void initialized() {
        // 将服务器生命周期状态设置为运行中
        ServerLifeCycleManager.toRunning();

        // 第一步：启动RPC服务器 - 用于接收Worker节点和其他Master节点的通信
        this.masterRPCServer.start();

        // 第二步：加载任务插件 - 支持Shell、SQL、Spark等多种任务类型
        TaskPluginManager.loadTaskPlugin();
        // 初始化数据源处理器 - 支持MySQL、PostgreSQL等多种数据源
        DataSourceProcessorProvider.initialize();

        // 第三步：启动注册中心客户端 - 服务发现和集群管理的核心
        this.masterRegistryClient.start();
        // 设置注册中心的停止回调，确保优雅关闭
        this.masterRegistryClient.setRegistryStoppable(this);

        // 第四步：启动Master协调器 - 负责Master节点间的协调和选举
        this.masterCoordinator.start();

        // 第五步：启动集群管理器 - 管理整个集群的状态和拓扑
        this.clusterManager.start();

        // 第六步：启动集群状态监控 - 监控集群健康状况和节点状态
        this.clusterStateMonitors.start();

        // 第七步：启动工作流引擎 - 这是调度系统的核心，处理DAG工作流
        this.workflowEngine.start();

        // 第八步：启动调度器API - 处理定时调度任务
        this.schedulerApi.start();

        // 第九步：发布全局Master故障转移事件 - 通知其他组件Master已就绪
        this.systemEventBus
                .publish(GlobalMasterFailoverEvent.of(new Date(ServerLifeCycleManager.getServerStartupTime())));
        // 启动系统事件总线工作线程 - 处理异步事件
        this.systemEventBusFireWorker.start();

        // 第十步：注册监控指标 - 用于Prometheus等监控系统
        // 注册CPU使用率监控指标
        MasterServerMetrics.registerMasterCpuUsageGauge(() -> {
            SystemMetrics systemMetrics = metricsProvider.getSystemMetrics();
            return systemMetrics.getSystemCpuUsagePercentage();
        });
        // 注册可用内存监控指标（以GB为单位）
        MasterServerMetrics.registerMasterMemoryAvailableGauge(() -> {
            SystemMetrics systemMetrics = metricsProvider.getSystemMetrics();
            return (systemMetrics.getSystemMemoryMax() - systemMetrics.getSystemMemoryUsed()) / 1024.0 / 1024 / 1024;
        });
        // 注册JVM内存使用率监控指标
        MasterServerMetrics.registerMasterMemoryUsageGauge(() -> {
            SystemMetrics systemMetrics = metricsProvider.getSystemMetrics();
            return systemMetrics.getJvmMemoryUsedPercentage();
        });

        // 注册JVM关闭钩子 - 确保进程退出时优雅关闭所有资源
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (!ServerLifeCycleManager.isStopped()) {
                close("MasterServer shutdownHook");
            }
        }));
        
        // 记录启动成功日志，包含启动耗时
        log.info("MasterServer initialized successfully in {} ms",
                System.currentTimeMillis() - ServerLifeCycleManager.getServerStartupTime());
    }

    @PreDestroy
    public void shutdown() {
        close("MasterServer shutdown");
    }

    /**
     * Master服务器优雅关闭方法
     * 按照与启动相反的顺序关闭各个组件，确保资源正确释放
     * 
     * @param cause 关闭原因，用于日志记录和问题诊断
     */
    public void close(String cause) {
        // 设置停止标志，确保关闭操作只执行一次
        // 使用CAS操作保证线程安全
        if (!ServerLifeCycleManager.toStopped()) {
            log.warn("MasterServer is already stopped, current cause: {}", cause);
            return;
        }
        
        // 等待3秒让正在执行的线程安静停止，避免强制中断导致数据不一致
        ThreadUtils.sleep(Constants.SERVER_CLOSE_WAIT_TIME.toMillis());
        
        // 立即关闭Master线程池，停止所有调度任务
        MasterThreadFactory.getDefaultSchedulerThreadExecutor().shutdownNow();
        
        // 使用try-with-resources确保所有组件都被正确关闭
        // 关闭顺序与启动顺序相反，避免依赖问题
        try (
                // 1. 关闭系统事件总线工作线程 - 停止事件处理
                SystemEventBusFireWorker systemEventBusFireWorker1 = systemEventBusFireWorker;
                // 2. 关闭工作流引擎 - 停止工作流调度和执行
                WorkflowEngine workflowEngine1 = workflowEngine;
                // 3. 关闭调度器API - 停止定时任务调度
                SchedulerApi closedSchedulerApi = schedulerApi;
                // 4. 关闭RPC服务器 - 停止接收外部请求
                MasterRpcServer closedRpcServer = masterRPCServer;
                // 5. 关闭Master协调器 - 停止集群协调
                MasterCoordinator closeMasterCoordinator = masterCoordinator;
                // 6. 关闭注册中心客户端 - 从集群中注销本节点
                MasterRegistryClient closedMasterRegistryClient = masterRegistryClient;
                // 7. 关闭Worker组调度协调器 - 停止任务分发
                WorkerGroupDispatcherCoordinator closeWorkerGroupDispatcherCoordinator =
                        workerGroupDispatcherCoordinator;
                // 8. 最后关闭Spring上下文 - 会调用所有带@PreDestroy注解的方法
                // 这会销毁ServerNodeManager、HostManager、TaskResponseService等Bean
                SpringApplicationContext closedSpringContext = springApplicationContext) {

            log.info("MasterServer is stopping, current cause : {}", cause);
        } catch (Exception e) {
            // 记录关闭过程中的异常，但不抛出，避免影响其他清理逻辑
            log.error("MasterServer stop failed, current cause: {}", cause, e);
            return;
        }
        log.info("MasterServer stopped, current cause: {}", cause);
    }

    /**
     * 强制停止Master服务器
     * 先执行优雅关闭，然后强制退出JVM进程
     * 
     * @param cause 停止原因
     */
    @Override
    public void stop(String cause) {
        // 先执行优雅关闭流程
        close(cause);

        // 确保在服务器完全关闭后再退出进程
        // 不在close方法中调用System.exit，避免并发调用时产生死锁
        // 退出码1表示异常退出，便于运维监控识别
        System.exit(1);
    }
}
