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

package org.apache.dolphinscheduler.server.worker;

import org.apache.dolphinscheduler.common.CommonConfiguration;
import org.apache.dolphinscheduler.common.IStoppable;
import org.apache.dolphinscheduler.common.constants.Constants;
import org.apache.dolphinscheduler.common.lifecycle.ServerLifeCycleManager;
import org.apache.dolphinscheduler.common.thread.DefaultUncaughtExceptionHandler;
import org.apache.dolphinscheduler.common.thread.ThreadUtils;
import org.apache.dolphinscheduler.meter.metrics.MetricsProvider;
import org.apache.dolphinscheduler.meter.metrics.SystemMetrics;
import org.apache.dolphinscheduler.plugin.datasource.api.plugin.DataSourceProcessorProvider;
import org.apache.dolphinscheduler.plugin.storage.api.StorageConfiguration;
import org.apache.dolphinscheduler.plugin.task.api.TaskPluginManager;
import org.apache.dolphinscheduler.registry.api.RegistryConfiguration;
import org.apache.dolphinscheduler.server.worker.executor.PhysicalTaskEngineDelegator;
import org.apache.dolphinscheduler.server.worker.metrics.WorkerServerMetrics;
import org.apache.dolphinscheduler.server.worker.registry.WorkerRegistryClient;
import org.apache.dolphinscheduler.server.worker.rpc.WorkerRpcServer;

import javax.annotation.PostConstruct;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;

@Slf4j
@Import({CommonConfiguration.class,
        StorageConfiguration.class,
        RegistryConfiguration.class})
@SpringBootApplication
public class WorkerServer implements IStoppable {

    @Autowired
    private WorkerRegistryClient workerRegistryClient;

    @Autowired
    private WorkerRpcServer workerRpcServer;

    @Autowired
    private MetricsProvider metricsProvider;

    @Autowired
    private PhysicalTaskEngineDelegator physicalTaskEngineDelegator;

    /**
     * worker server startup, not use web service
     *
     * @param args arguments
     */
    public static void main(String[] args) {
        WorkerServerMetrics.registerUncachedException(DefaultUncaughtExceptionHandler::getUncaughtExceptionCount);
        Thread.setDefaultUncaughtExceptionHandler(DefaultUncaughtExceptionHandler.getInstance());
        Thread.currentThread().setName(Constants.THREAD_NAME_WORKER_SERVER);
        SpringApplication.run(WorkerServer.class);
    }

    /**
     * Worker服务器初始化方法 - 启动所有核心组件
     * 这是Worker节点的核心启动流程，负责任务执行和状态上报
     */
    @PostConstruct
    public void run() {
        // 将服务器生命周期状态设置为运行中
        ServerLifeCycleManager.toRunning();

        // 第一步：启动Worker RPC服务器 - 用于接收Master节点的任务分发请求
        this.workerRpcServer.start();

        // 第二步：加载任务插件 - 支持Shell、SQL、Spark、Python等多种任务类型
        TaskPluginManager.loadTaskPlugin();

        // 第三步：初始化数据源处理器 - 支持MySQL、PostgreSQL等多种数据源连接
        DataSourceProcessorProvider.initialize();

        // 第四步：配置注册中心客户端 - 设置停止回调，确保优雅关闭
        this.workerRegistryClient.setRegistryStoppable(this);
        // 启动注册中心客户端 - 向集群注册Worker节点，参与服务发现
        this.workerRegistryClient.start();

        // 第五步：启动物理任务引擎委托器 - 这是任务执行的核心
        // 负责接收Master分发的任务，并在本地执行
        this.physicalTaskEngineDelegator.start();

        // 第六步：注册监控指标 - 用于监控Worker节点的资源使用情况
        // 注册CPU使用率监控指标 - Master会根据此指标判断是否分发任务
        WorkerServerMetrics.registerWorkerCpuUsageGauge(() -> {
            SystemMetrics systemMetrics = metricsProvider.getSystemMetrics();
            return systemMetrics.getSystemCpuUsagePercentage();
        });
        // 注册可用内存监控指标（以GB为单位） - 用于负载均衡策略
        WorkerServerMetrics.registerWorkerMemoryAvailableGauge(() -> {
            SystemMetrics systemMetrics = metricsProvider.getSystemMetrics();
            return (systemMetrics.getSystemMemoryMax() - systemMetrics.getSystemMemoryUsed()) / 1024.0 / 1024 / 1024;
        });
        // 注册JVM内存使用率监控指标 - 监控Worker的JVM健康状况
        WorkerServerMetrics.registerWorkerMemoryUsageGauge(() -> {
            SystemMetrics systemMetrics = metricsProvider.getSystemMetrics();
            return systemMetrics.getJvmMemoryUsedPercentage();
        });

        // 注册JVM关闭钩子 - 确保进程退出时优雅关闭所有资源
        /*
         * 注册关闭钩子，在进程退出前执行清理工作
         * 这对于确保数据一致性和资源正确释放非常重要
         */
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (!ServerLifeCycleManager.isStopped()) {
                close("WorkerServer shutdown hook");
            }
        }));
    }

    /**
     * Worker服务器优雅关闭方法
     * 按照与启动相反的顺序关闭各个组件，确保正在执行的任务能够正常完成
     * 
     * @param cause 关闭原因，用于日志记录和问题诊断
     */
    public void close(String cause) {
        // 设置停止标志，确保关闭操作只执行一次
        // 使用CAS操作保证线程安全
        if (!ServerLifeCycleManager.toStopped()) {
            log.warn("WorkerServer is already stopped, current cause: {}", cause);
            return;
        }
        
        // 等待一段时间让正在执行的任务安静停止
        // 这对于Worker尤其重要，因为需要等待任务执行完成并上报结果
        ThreadUtils.sleep(Constants.SERVER_CLOSE_WAIT_TIME.toMillis());

        // 使用try-with-resources确保所有组件都被正确关闭
        // 关闭顺序与启动顺序相反，避免依赖问题
        try (
                // 1. 关闭物理任务引擎委托器 - 停止新任务接收，等待当前任务执行完成
                final PhysicalTaskEngineDelegator ignore1 = physicalTaskEngineDelegator;
                // 2. 关闭Worker RPC服务器 - 停止接收Master的任务分发请求
                final WorkerRpcServer ignore2 = workerRpcServer;
                // 3. 关闭注册中心客户端 - 从集群中注销Worker节点，停止接收新任务
                final WorkerRegistryClient ignore3 = workerRegistryClient) {
            log.info("Worker server is stopping, current cause : {}", cause);
        } catch (Exception e) {
            // 记录关闭过程中的异常，但不抛出，避免影响其他清理逻辑
            log.error("Worker server stop failed, current cause: {}", cause, e);
            return;
        }
        log.info("Worker server stopped, current cause: {}", cause);
    }

    /**
     * 强制停止Worker服务器
     * 先执行优雅关闭，然后强制退出JVM进程
     * 
     * @param cause 停止原因
     */
    @Override
    public void stop(String cause) {
        // 先执行优雅关闭流程，等待任务执行完成
        close(cause);

        // 确保在服务器完全关闭后再退出进程
        // 不在close方法中调用System.exit，避免并发调用时产生死锁
        // 退出码1表示异常退出，便于运维监控识别
        System.exit(1);
    }

}
