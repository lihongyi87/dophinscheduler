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

package org.apache.dolphinscheduler.server.master.engine.command;

import static java.util.concurrent.CompletableFuture.supplyAsync;

import org.apache.dolphinscheduler.common.constants.Constants;
import org.apache.dolphinscheduler.common.enums.WorkflowExecutionStatus;
import org.apache.dolphinscheduler.common.thread.BaseDaemonThread;
import org.apache.dolphinscheduler.common.thread.ThreadUtils;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.dao.entity.Command;
import org.apache.dolphinscheduler.dao.entity.WorkflowInstance;
import org.apache.dolphinscheduler.meter.metrics.MetricsProvider;
import org.apache.dolphinscheduler.meter.metrics.SystemMetrics;
import org.apache.dolphinscheduler.server.master.config.MasterConfig;
import org.apache.dolphinscheduler.server.master.config.MasterServerLoadProtection;
import org.apache.dolphinscheduler.server.master.engine.IWorkflowRepository;
import org.apache.dolphinscheduler.server.master.engine.WorkflowEventBusCoordinator;
import org.apache.dolphinscheduler.server.master.engine.exceptions.CommandDuplicateHandleException;
import org.apache.dolphinscheduler.server.master.engine.workflow.lifecycle.event.WorkflowStartLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.workflow.runnable.IWorkflowExecutionRunnable;
import org.apache.dolphinscheduler.server.master.engine.workflow.runnable.WorkflowExecutionRunnableFactory;
import org.apache.dolphinscheduler.server.master.metrics.MasterServerMetrics;
import org.apache.dolphinscheduler.service.command.CommandService;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Master调度器线程 - 命令引擎核心类
 *
 * 这是调度系统的心脏，负责：
 * 1. 从数据库持续拉取待执行的命令
 * 2. 将命令转换为工作流实例
 * 3. 触发工作流实例的执行
 *
 * 设计理念：
 * - 采用守护线程模式，持续运行
 * - 支持并发命令处理，提高吞吐量
 * - 具备负载保护机制，防止系统过载
 * - 异常隔离，单个命令失败不影响其他命令
 */
@Service
@Slf4j
public class CommandEngine extends BaseDaemonThread implements AutoCloseable {

    /**
     * 命令获取器 - 负责从数据库拉取待处理的命令
     * 支持多种获取策略：ID_SLOT_BASED（基于槽位）、FETCH_BY_LIMIT（按限制获取）
     */
    @Autowired
    private ICommandFetcher commandFetcher;

    /**
     * 命令服务 - 提供命令的CRUD操作
     */
    @Autowired
    private CommandService commandService;

    /**
     * Master配置 - 包含命令处理相关的配置参数
     */
    @Autowired
    private MasterConfig masterConfig;

    /**
     * Master服务器负载保护器 - 监控系统负载，防止过载
     */
    @Autowired
    private MasterServerLoadProtection masterServerLoadProtection;

    /**
     * 工作流仓库 - 存储正在运行的工作流实例
     */
    @Autowired
    private IWorkflowRepository workflowRepository;

    /**
     * 工作流执行运行器工厂 - 创建工作流执行器
     */
    @Autowired
    private WorkflowExecutionRunnableFactory workflowExecutionRunnableFactory;

    /**
     * 监控指标提供者 - 获取系统性能指标
     */
    @Autowired
    private MetricsProvider metricsProvider;

    /**
     * 工作流事件总线协调器 - 管理工作流的事件处理
     */
    @Autowired
    private WorkflowEventBusCoordinator workflowEventBusCoordinator;

    /**
     * 命令处理线程池 - 并发处理多个命令，提高吞吐量
     */
    private ExecutorService commandHandleThreadPool;

    /**
     * 运行标志 - 控制命令循环的开关
     */
    private boolean flag = false;

    protected CommandEngine() {
        super("MasterCommandLoopThread");
    }

    /**
     * 启动命令引擎
     * 初始化线程池并启动守护线程
     */
    @Override
    public synchronized void start() {
        log.info("MasterSchedulerBootstrap starting..");

        // 创建命令处理线程池，大小为CPU核心数，充分利用多核性能
        // 使用守护线程，确保主程序退出时线程池自动关闭
        this.commandHandleThreadPool = ThreadUtils.newDaemonFixedThreadExecutor("MasterCommandHandleThreadPool",
                Runtime.getRuntime().availableProcessors());

        // 设置运行标志为true，允许命令循环执行
        flag = true;

        // 启动父类的守护线程，开始执行run方法
        super.start();

        log.info("MasterSchedulerBootstrap started...");
    }

    /**
     * 关闭命令引擎
     * 设置停止标志，让运行循环优雅退出
     */
    @Override
    public void close() throws Exception {
        log.info("MasterSchedulerBootstrap stopping...");

        // 设置停止标志，run方法的while循环会检测到并退出
        flag = false;

        log.info("MasterSchedulerBootstrap stopped...");
    }

    /**
     * 命令引擎主循环 - 核心调度逻辑
     * 持续从数据库获取命令并处理，直到收到停止信号
     */
    @Override
    public void run() {
        // 主循环：只要flag为true就持续运行
        while (flag) {
            try {
                // ========== 第一步：负载检查 ==========
                // TODO: 未来需要处理工作流事件队列积压时的反压机制

                // 获取系统当前的性能指标（CPU、内存等）
                SystemMetrics systemMetrics = metricsProvider.getSystemMetrics();

                // 检查系统是否过载（CPU或内存使用率超过阈值）
                if (masterServerLoadProtection.isOverload(systemMetrics)) {
                    // 系统过载时记录警告日志
                    log.warn("The current server is overload, cannot consumes commands.");
                    // 增加过载计数器，用于监控告警
                    MasterServerMetrics.incMasterOverload();
                    // 休眠1秒，避免频繁检查加重系统负担
                    Thread.sleep(Constants.SLEEP_TIME_MILLIS);
                    // 跳过本轮循环，继续下一轮
                    continue;
                }

                // ========== 第二步：获取待处理命令 ==========
                // 从数据库批量获取待处理的命令
                // 根据配置的策略（槽位或限制）获取属于本Master的命令
                List<Command> commands = commandFetcher.fetchCommands();

                // 如果没有待处理的命令
                if (CollectionUtils.isEmpty(commands)) {
                    // 休眠1秒，避免频繁查询数据库
                    Thread.sleep(Constants.SLEEP_TIME_MILLIS);
                    // 继续下一轮循环
                    continue;
                }

                // ========== 第三步：并发处理命令 ==========
                // 创建CompletableFuture列表，用于跟踪所有命令的处理状态
                List<CompletableFuture<Void>> allCompleteFutures = new ArrayList<>();

                // 遍历每个命令，提交到线程池并发处理
                for (Command command : commands) {
                    // 构建异步处理链：
                    // 1. bootstrapCommand: 创建工作流执行器
                    // 2. bootstrapWorkflowExecutionRunnable: 注册并启动工作流
                    // 3. bootstrapSuccess: 记录成功日志和指标
                    // 4. bootstrapError: 异常处理和错误记录
                    CompletableFuture<Void> completableFuture = bootstrapCommand(command)
                            .thenAccept(this::bootstrapWorkflowExecutionRunnable)
                            .thenAccept((unused) -> bootstrapSuccess(command))
                            .exceptionally(throwable -> bootstrapError(command, throwable));

                    // 将Future添加到列表中
                    allCompleteFutures.add(completableFuture);
                }

                // ========== 第四步：等待所有命令处理完成 ==========
                // 使用CompletableFuture.allOf等待所有命令处理完成
                // 这确保了本轮所有命令都处理完毕后才开始下一轮
                CompletableFuture.allOf(allCompleteFutures.toArray(new CompletableFuture[0])).join();

            } catch (InterruptedException interruptedException) {
                // 处理中断异常（通常是关闭信号）
                log.warn("Master schedule bootstrap interrupted, close the loop", interruptedException);
                // 重新设置中断标志，保持中断状态
                Thread.currentThread().interrupt();
                // 退出循环
                break;
            } catch (Exception e) {
                // 捕获所有其他异常，避免线程意外终止
                log.error("Master schedule workflow error", e);
                // 休眠1秒，避免异常（如数据库宕机）导致的异常风暴
                ThreadUtils.sleep(Constants.SLEEP_TIME_MILLIS);
            }
        }
    }

    /**
     * 异步创建工作流执行器
     * 将命令转换为可执行的工作流实例
     *
     * @param command 待处理的命令
     * @return 包含工作流执行器的CompletableFuture
     */
    private CompletableFuture<IWorkflowExecutionRunnable> bootstrapCommand(Command command) {
        // 使用线程池异步执行命令处理
        // 避免阻塞主线程，提高并发处理能力
        return supplyAsync(
                // 调用工厂方法创建工作流执行器
                // 这个过程包括：解析DAG、创建工作流实例、初始化任务列表等
                () -> workflowExecutionRunnableFactory.createWorkflowExecuteRunnable(command),
                // 使用专门的命令处理线程池
                commandHandleThreadPool);
    }

    /**
     * 启动工作流执行器
     * 注册到仓库并发布启动事件
     *
     * @param workflowExecutionRunnable 工作流执行器
     * @return 完成的CompletableFuture
     */
    private CompletableFuture<Void> bootstrapWorkflowExecutionRunnable(IWorkflowExecutionRunnable workflowExecutionRunnable) {
        // 获取工作流实例，检查其状态
        final WorkflowInstance workflowInstance =
                workflowExecutionRunnable.getWorkflowExecuteContext().getWorkflowInstance();

        // ========== 串行等待状态处理 ==========
        // 如果工作流处于串行等待状态（同一工作流定义只允许一个实例运行）
        if (workflowInstance.getState() == WorkflowExecutionStatus.SERIAL_WAIT) {
            // 记录日志，说明该工作流暂时不会被触发
            log.info("The workflow {} state is: {} will not be trigger now",
                    workflowInstance.getName(),
                    workflowInstance.getState());
            // 直接返回，不启动该工作流
            return CompletableFuture.completedFuture(null);
        }

        // ========== 注册工作流到仓库 ==========
        // 将工作流执行器存储到内存仓库中
        // 这样可以通过工作流实例ID快速查找到对应的执行器
        workflowRepository.put(workflowExecutionRunnable);

        // ========== 注册事件总线 ==========
        // 为工作流注册专属的事件总线
        // 用于处理工作流生命周期中的各种事件
        workflowEventBusCoordinator.registerWorkflowEventBus(workflowExecutionRunnable);

        // ========== 发布启动事件 ==========
        // 发布工作流启动生命周期事件
        // 这会触发工作流的实际执行，包括任务调度、状态更新等
        workflowExecutionRunnable.getWorkflowEventBus()
                .publish(WorkflowStartLifecycleEvent.of(workflowExecutionRunnable));

        // 返回完成的Future
        return CompletableFuture.completedFuture(null);
    }

    /**
     * 命令处理成功回调
     * 记录成功日志和监控指标
     *
     * @param command 成功处理的命令
     * @return 完成的CompletableFuture
     */
    private CompletableFuture<Void> bootstrapSuccess(Command command) {
        // 记录成功处理的命令信息
        // 使用JSON格式便于日志分析和问题追踪
        log.info("Success bootstrap command {}", JSONUtils.toPrettyJsonString(command));

        // 增加命令消费计数器
        // 用于监控系统的命令处理吞吐量
        MasterServerMetrics.incMasterConsumeCommand(1);

        // 返回完成的Future
        return CompletableFuture.completedFuture(null);
    }

    /**
     * 命令处理异常回调
     * 处理各类异常情况，确保系统稳定性
     *
     * @param command 处理失败的命令
     * @param throwable 抛出的异常
     * @return null（CompletableFuture要求的返回值）
     */
    private Void bootstrapError(Command command, Throwable throwable) {
        // ========== 处理重复处理异常 ==========
        // 在多Master环境中，可能出现命令被多个Master同时处理的情况
        if (throwable instanceof CommandDuplicateHandleException) {
            // 记录警告日志，这不是错误，只是正常的竞争情况
            log.warn("Handle command failed, the command: {} has been handled by other master",
                    command,
                    throwable);
            // 直接返回，不需要进一步处理
            return null;
        }

        // ========== 处理其他异常 ==========
        // 记录错误日志，包含命令详情和完整的异常堆栈
        log.error("Failed bootstrap command {} ", JSONUtils.toPrettyJsonString(command), throwable);

        // 将失败的命令移动到错误命令表
        // 包含错误信息，便于后续人工处理或重试
        commandService.moveToErrorCommand(command, ExceptionUtils.getStackTrace(throwable));

        return null;
    }

}
