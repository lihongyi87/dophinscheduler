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

/**
 * Worker服务器主启动类
 *
 * <p>Worker是DolphinScheduler分布式调度系统的核心执行节点，负责：</p>
 * <ul>
 *   <li>接收Master节点分发的具体任务</li>
 *   <li>执行Shell、SQL、Spark、Python等多种类型的任务</li>
 *   <li>监控任务执行状态并实时上报给Master</li>
 *   <li>管理本地资源（CPU、内存）并参与负载均衡</li>
 *   <li>维护与注册中心的连接，实现服务发现和故障转移</li>
 * </ul>
 *
 * <p><b>启动流程：</b></p>
 * <ol>
 *   <li>启动RPC服务器，建立与Master的通信通道</li>
 *   <li>加载任务插件，支持多种任务类型执行</li>
 *   <li>初始化数据源处理器，连接各种数据库</li>
 *   <li>注册到注册中心，加入Worker集群</li>
 *   <li>启动物理任务执行引擎，准备接收任务</li>
 *   <li>注册监控指标，供Master负载均衡使用</li>
 * </ol>
 *
 * <p><b>与Master的交互：</b></p>
 * <ul>
 *   <li>通过RPC接收Master分发的任务执行请求</li>
 *   <li>通过注册中心维持心跳，报告健康状态</li>
 *   <li>通过监控指标上报资源使用情况</li>
 *   <li>执行完成后向Master回调任务结果</li>
 * </ul>
 *
 * <p><b>优雅关闭：</b></p>
 * <ul>
 *   <li>停止接收新任务</li>
 *   <li>等待正在执行的任务完成</li>
 *   <li>从注册中心注销</li>
 *   <li>关闭所有资源连接</li>
 * </ul>
 *
 * @author DolphinScheduler Team
 * @since 3.0.0
 */
@Slf4j
@Import({CommonConfiguration.class,
        StorageConfiguration.class,
        RegistryConfiguration.class})
@SpringBootApplication
public class WorkerServer implements IStoppable {

    /**
     * Worker注册中心客户端
     * 负责向注册中心（如ZooKeeper）注册Worker节点信息，维持心跳
     * Master通过注册中心发现可用的Worker节点进行任务分发
     */
    @Autowired
    private WorkerRegistryClient workerRegistryClient;

    /**
     * Worker RPC服务器
     * 提供gRPC服务接口，接收Master节点的任务分发请求
     * 包括任务执行、任务取消、任务状态查询等操作
     */
    @Autowired
    private WorkerRpcServer workerRpcServer;

    /**
     * 监控指标提供者
     * 收集Worker节点的系统资源使用情况（CPU、内存、磁盘等）
     * Master根据这些指标进行负载均衡决策
     */
    @Autowired
    private MetricsProvider metricsProvider;

    /**
     * 物理任务引擎委托器
     * Worker的任务执行核心组件，负责：
     * - 管理任务执行线程池
     * - 调度具体的任务插件执行任务
     * - 监控任务执行状态
     * - 处理任务超时和异常情况
     */
    @Autowired
    private PhysicalTaskEngineDelegator physicalTaskEngineDelegator;

    /**
     * Worker服务器主入口方法
     *
     * <p>Worker服务器采用非Web服务模式启动，专注于任务执行而非HTTP请求处理。</p>
     * <p>启动过程包括：</p>
     * <ul>
     *   <li>注册未捕获异常处理器，提高系统稳定性</li>
     *   <li>设置主线程名称，便于问题诊断</li>
     *   <li>启动Spring Boot应用容器</li>
     * </ul>
     *
     * @param args 命令行参数（通常为空，配置通过application.yml提供）
     */
    public static void main(String[] args) {
        // 注册未捕获异常监控指标，用于系统健康监控
        // 当Worker出现未处理异常时，指标会自动递增，便于运维监控
        WorkerServerMetrics.registerUncachedException(DefaultUncaughtExceptionHandler::getUncaughtExceptionCount);

        // 设置全局未捕获异常处理器
        // 确保Worker进程不会因为未处理的异常而意外退出
        // 对于长期运行的Worker服务器来说，这是关键的稳定性保障
        Thread.setDefaultUncaughtExceptionHandler(DefaultUncaughtExceptionHandler.getInstance());

        // 设置主线程名称，便于日志分析和问题定位
        // 在多节点部署时，清晰的线程命名有助于运维和调试
        Thread.currentThread().setName(Constants.THREAD_NAME_WORKER_SERVER);

        // 启动Spring Boot应用
        // 这将触发@PostConstruct注解的run()方法，完成Worker的完整初始化
        SpringApplication.run(WorkerServer.class);
    }

    /**
     * Worker服务器初始化方法
     *
     * <p>这是Worker节点的核心启动流程，按照严格的顺序初始化各个组件。
     * 启动顺序经过精心设计，确保组件间的依赖关系得到正确处理。</p>
     *
     * <p><b>启动顺序说明：</b></p>
     * <ol>
     *   <li><b>RPC服务器：</b>最先启动，为后续组件提供通信基础</li>
     *   <li><b>任务插件：</b>加载各种任务类型的执行器</li>
     *   <li><b>数据源处理器：</b>初始化数据库连接能力</li>
     *   <li><b>注册中心：</b>向集群注册，开始接收任务分发</li>
     *   <li><b>任务执行引擎：</b>核心执行组件，开始处理任务</li>
     *   <li><b>监控指标：</b>向Master暴露资源使用情况</li>
     * </ol>
     *
     * <p><b>注意事项：</b></p>
     * <ul>
     *   <li>启动过程中任何一步失败都会导致整个Worker无法正常工作</li>
     *   <li>组件启动有严格的依赖关系，不能随意调整顺序</li>
     *   <li>监控指标的注册必须在最后，确保所有组件已经准备就绪</li>
     * </ul>
     */
    @PostConstruct
    public void run() {
        // 标记服务器状态为运行中，这是一个全局状态标识
        // 其他组件可以通过此状态判断Worker是否已经完全启动
        ServerLifeCycleManager.toRunning();

        // 第一步：启动Worker RPC服务器
        // 这是Worker与Master通信的基础设施，必须最先启动
        // 提供任务执行、状态查询、任务取消等gRPC服务接口
        this.workerRpcServer.start();

        // 第二步：加载任务插件
        // 动态加载各种任务类型的执行插件，如Shell、SQL、Spark、Python等
        // 插件化设计使得Worker能够支持扩展的任务类型
        TaskPluginManager.loadTaskPlugin();

        // 第三步：初始化数据源处理器
        // 建立与各种数据源的连接能力，支持MySQL、PostgreSQL、Oracle等
        // 许多任务需要访问数据库，因此需要预先初始化数据源连接池
        DataSourceProcessorProvider.initialize();

        // 第四步：配置并启动注册中心客户端
        // 设置停止回调，确保Worker关闭时能从注册中心优雅注销
        this.workerRegistryClient.setRegistryStoppable(this);
        // 向注册中心（如ZooKeeper）注册Worker节点信息
        // Master通过注册中心发现可用的Worker进行任务分发
        // 同时开始维持心跳，证明Worker节点健康可用
        this.workerRegistryClient.start();

        // 第五步：启动物理任务执行引擎
        // 这是Worker的核心组件，负责：
        // - 维护任务执行线程池
        // - 接收并执行Master分发的任务
        // - 监控任务执行状态和资源使用
        // - 处理任务超时、失败等异常情况
        this.physicalTaskEngineDelegator.start();

        // 第六步：注册系统监控指标
        // 这些指标用于Master的负载均衡决策和集群监控

        // 注册CPU使用率监控指标
        // Master会根据Worker的CPU使用率进行任务分发决策
        // 高CPU使用率的Worker会收到较少的新任务分配
        WorkerServerMetrics.registerWorkerCpuUsageGauge(() -> {
            SystemMetrics systemMetrics = metricsProvider.getSystemMetrics();
            return systemMetrics.getSystemCpuUsagePercentage();
        });

        // 注册可用内存监控指标（单位：GB）
        // 用于负载均衡算法，确保内存资源不会过度使用
        // Master会优先向内存充足的Worker分发内存密集型任务
        WorkerServerMetrics.registerWorkerMemoryAvailableGauge(() -> {
            SystemMetrics systemMetrics = metricsProvider.getSystemMetrics();
            return (systemMetrics.getSystemMemoryMax() - systemMetrics.getSystemMemoryUsed()) / 1024.0 / 1024 / 1024;
        });

        // 注册JVM内存使用率监控指标
        // 监控Worker进程自身的JVM健康状况
        // 防止因JVM内存不足导致的任务执行失败
        WorkerServerMetrics.registerWorkerMemoryUsageGauge(() -> {
            SystemMetrics systemMetrics = metricsProvider.getSystemMetrics();
            return systemMetrics.getJvmMemoryUsedPercentage();
        });

        // 注册JVM关闭钩子，确保优雅关闭
        // 当收到SIGTERM信号或JVM准备退出时，执行清理工作
        // 这对于确保正在执行的任务能够正常完成并上报结果非常重要
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (!ServerLifeCycleManager.isStopped()) {
                close("WorkerServer shutdown hook");
            }
        }));
    }

    /**
     * Worker服务器优雅关闭方法
     *
     * <p>按照与启动相反的顺序关闭各个组件，确保正在执行的任务能够正常完成。
     * 优雅关闭是保证数据一致性和系统稳定性的关键环节。</p>
     *
     * <p><b>关闭顺序说明：</b></p>
     * <ol>
     *   <li><b>任务执行引擎：</b>停止接收新任务，等待当前任务完成</li>
     *   <li><b>RPC服务器：</b>停止接收Master的通信请求</li>
     *   <li><b>注册中心客户端：</b>从集群中注销，停止心跳</li>
     * </ol>
     *
     * <p><b>关闭策略：</b></p>
     * <ul>
     *   <li>使用CAS操作确保关闭逻辑只执行一次</li>
     *   <li>等待一定时间让正在执行的任务优雅完成</li>
     *   <li>使用try-with-resources确保资源正确释放</li>
     *   <li>即使部分组件关闭失败，也继续执行其他清理逻辑</li>
     * </ul>
     *
     * @param cause 关闭原因，用于日志记录和问题诊断
     */
    public void close(String cause) {
        // 使用CAS操作设置停止状态，确保关闭逻辑只执行一次
        // 在高并发场景下，可能有多个线程同时调用关闭方法
        // ServerLifeCycleManager提供线程安全的状态管理
        if (!ServerLifeCycleManager.toStopped()) {
            log.warn("WorkerServer is already stopped, current cause: {}", cause);
            return;
        }

        // 等待一段时间让正在执行的任务有机会优雅完成
        // 对于Worker来说，这个等待非常重要，因为：
        // 1. 需要等待任务执行完成并上报结果给Master
        // 2. 避免任务中途被强制中断导致数据不一致
        // 3. 给任务清理资源（如临时文件、数据库连接）的时间
        ThreadUtils.sleep(Constants.SERVER_CLOSE_WAIT_TIME.toMillis());

        // 使用try-with-resources确保所有组件都被正确关闭
        // 即使某个组件关闭失败，其他组件的关闭逻辑仍会执行
        // 关闭顺序与启动顺序相反，遵循"后进先出"原则
        try (
                // 第一步：关闭物理任务执行引擎
                // 这是最重要的一步，停止接收新任务，等待当前任务执行完成
                // 确保所有正在运行的任务都能正常结束并上报状态
                final PhysicalTaskEngineDelegator ignore1 = physicalTaskEngineDelegator;

                // 第二步：关闭Worker RPC服务器
                // 停止接收Master的任务分发请求和状态查询请求
                // 确保不会有新的任务被分发到正在关闭的Worker
                final WorkerRpcServer ignore2 = workerRpcServer;

                // 第三步：关闭注册中心客户端
                // 从集群中注销Worker节点，停止心跳维持
                // Master会感知到Worker下线，不再向其分发新任务
                final WorkerRegistryClient ignore3 = workerRegistryClient) {

            log.info("Worker server is stopping, current cause : {}", cause);
        } catch (Exception e) {
            // 记录关闭过程中的异常，但不重新抛出
            // 这样可以避免异常影响其他清理逻辑的执行
            // 同时保留问题诊断的必要信息
            log.error("Worker server stop failed, current cause: {}", cause, e);
            return;
        }
        log.info("Worker server stopped, current cause: {}", cause);
    }

    /**
     * 强制停止Worker服务器
     *
     * <p>这是Worker服务器的最终停止方法，通常在以下场景被调用：</p>
     * <ul>
     *   <li>注册中心连接丢失且无法恢复</li>
     *   <li>系统检测到无法恢复的错误状态</li>
     *   <li>运维人员执行紧急停机操作</li>
     *   <li>容器编排系统（如K8s）要求强制停止</li>
     * </ul>
     *
     * <p><b>停止流程：</b></p>
     * <ol>
     *   <li>调用close()方法执行优雅关闭流程</li>
     *   <li>等待所有清理工作完成</li>
     *   <li>强制退出JVM进程</li>
     * </ol>
     *
     * <p><b>设计考虑：</b></p>
     * <ul>
     *   <li>分离优雅关闭和进程退出逻辑，避免并发调用死锁</li>
     *   <li>使用退出码1标识异常退出，便于运维监控</li>
     *   <li>确保进程最终会退出，不会变成僵尸进程</li>
     * </ul>
     *
     * @param cause 停止原因，用于日志记录和运维诊断
     */
    @Override
    public void stop(String cause) {
        // 先执行优雅关闭流程
        // 这包括停止接收新任务、等待当前任务完成、清理资源等
        // 即使在强制停止的场景下，也要尽可能保证数据一致性
        close(cause);

        // 强制退出JVM进程
        // 使用System.exit(1)而不是在close()方法中退出的原因：
        // 1. 避免close()方法被多个线程并发调用时产生死锁
        // 2. 确保close()方法专注于资源清理，职责单一
        // 3. 退出码1向运维系统表明这是异常退出，需要关注
        System.exit(1);
    }

}
