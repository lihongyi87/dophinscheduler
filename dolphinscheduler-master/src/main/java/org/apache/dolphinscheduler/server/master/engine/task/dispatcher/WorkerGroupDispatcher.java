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

package org.apache.dolphinscheduler.server.master.engine.task.dispatcher;

import org.apache.dolphinscheduler.common.thread.BaseDaemonThread;
import org.apache.dolphinscheduler.plugin.task.api.utils.LogUtils;
import org.apache.dolphinscheduler.server.master.engine.task.client.ITaskExecutorClient;
import org.apache.dolphinscheduler.server.master.engine.task.dispatcher.event.TaskDispatchableEvent;
import org.apache.dolphinscheduler.server.master.engine.task.runnable.ITaskExecutionRunnable;
import org.apache.dolphinscheduler.task.executor.log.TaskExecutorMDCUtils;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import lombok.extern.slf4j.Slf4j;

/**
 * 工作组任务分发器
 *
 * 负责从任务队列中分发任务到特定工作组的Worker节点。这是Master节点任务调度系统的核心组件，
 * 专门处理单个工作组内的任务分发逻辑。
 *
 * 核心职责：
 * 1. 任务队列管理：持续从 {@link TaskDispatchableEvent} 队列中获取待分发任务
 * 2. 任务分发执行：将任务通过TaskExecutorClient发送到对应的Worker节点
 * 3. 失败重试机制：对分发失败的任务实施指数退避重试策略
 * 4. 状态安全管理：确保任务处理过程中的线程安全和状态转换正确性
 * 5. 生命周期控制：支持分发器的启动、运行和优雅关闭
 *
 * 设计特点：
 * - 异步处理：基于事件驱动的异步任务分发机制
 * - 延迟队列：支持任务的延迟分发，用于重试和定时调度
 * - 线程安全：使用ConcurrentHashMap和原子操作确保并发安全
 * - 故障恢复：内置重试机制，支持任务分发失败后的自动重试
 * - 资源隔离：每个工作组独立的分发器，避免相互影响
 *
 * 工作流程：
 * 1. 启动阶段：初始化分发器并启动后台处理线程
 * 2. 任务接收：通过dispatchTask方法接收需要分发的任务
 * 3. 队列缓存：将任务包装成事件并加入延迟队列
 * 4. 循环处理：后台线程持续从队列中取出到期任务
 * 5. 分发执行：调用TaskExecutorClient将任务发送到Worker
 * 6. 异常处理：分发失败时实施指数退避重试策略
 * 7. 状态维护：维护等待分发任务的ID集合，支持任务取消
 *
 * 重试策略：
 * - 指数退避：重试间隔递增，避免系统过载
 * - 最大间隔：重试间隔最大不超过60秒
 * - 自动重入：失败任务自动重新入队等待下次重试
 *
 * 类比理解：
 * 就像快递配送站的分拣员，专门负责某个区域的快递配送：
 * - 接收快递：从总调度中心接收分配给本区域的快递任务
 * - 排序等待：按照配送时间要求对快递进行排序和等待
 * - 配送执行：按时将快递配送到指定地址
 * - 失败重试：配送失败时按策略进行重新配送
 * - 状态跟踪：记录哪些快递正在配送过程中
 */
@Slf4j
public class WorkerGroupDispatcher extends BaseDaemonThread {

    /**
     * 任务执行器客户端
     *
     * 用于与Worker节点通信，负责将任务分发到具体的Worker执行器。
     * 这是分发器与底层执行环境的桥梁，封装了网络通信和协议处理的复杂性。
     */
    private final ITaskExecutorClient taskExecutorClient;

    /**
     * 工作组事件总线
     *
     * 管理当前工作组的任务分发事件队列，支持延迟和优先级处理。
     * 所有待分发的任务都会先进入这个事件总线，然后按时间和优先级顺序被处理。
     *
     * 特性：
     * - 延迟处理：支持任务的延迟分发，用于重试机制
     * - 优先级排序：任务按照优先级和时间顺序进行排队
     * - 线程安全：支持多线程并发访问
     */
    private final TaskDispatchableEventBus<TaskDispatchableEvent<ITaskExecutionRunnable>, ITaskExecutionRunnable> workerGroupEventBus;

    /**
     * 等待分发任务ID集合
     *
     * 记录所有正在等待分发的任务ID，用于任务状态跟踪和重复检查。
     * 主要用途：
     * - 去重检查：避免同一任务被重复分发
     * - 状态跟踪：跟踪任务的分发状态
     * - 取消支持：支持任务的取消操作
     * - 调试诊断：提供任务分发状态的可视化信息
     *
     * 使用ConcurrentHashMap.newKeySet()确保线程安全的集合操作。
     */
    private final Set<Integer> waitingDispatchTaskIds;

    /**
     * 运行状态标志
     *
     * 使用原子布尔值控制分发器的运行状态，确保线程安全的状态管理。
     * - true：分发器正在运行，接受和处理任务
     * - false：分发器已停止，不再处理新任务
     *
     * 采用AtomicBoolean保证状态变更的原子性，避免并发问题。
     */
    private final AtomicBoolean runningFlag = new AtomicBoolean(false);

    /**
     * 构造工作组任务分发器
     *
     * 初始化指定工作组的任务分发器，创建必要的组件和数据结构。
     *
     * @param workerGroupName 工作组名称，用于标识分发器和日志记录
     * @param taskExecutorClient 任务执行器客户端，用于与Worker节点通信
     */
    public WorkerGroupDispatcher(String workerGroupName, ITaskExecutorClient taskExecutorClient) {
        super("WorkerGroupTaskDispatcher-" + workerGroupName);
        this.taskExecutorClient = taskExecutorClient;
        this.workerGroupEventBus = new TaskDispatchableEventBus<>();
        this.waitingDispatchTaskIds = ConcurrentHashMap.newKeySet();
        log.info("Initialize WorkerGroupDispatcher: {}", this.getName());
    }

    /**
     * 启动工作组分发器
     *
     * 以线程安全的方式启动分发器，确保不会重复启动。
     * 使用原子性CAS操作保证状态变更的安全性。
     *
     * 启动流程：
     * 1. 检查当前运行状态，如果已启动则拒绝重复启动
     * 2. 原子性地设置运行标志为true
     * 3. 调用父类启动方法，创建并启动后台处理线程
     * 4. 记录启动成功日志
     *
     * 线程安全：使用synchronized关键字和CAS操作确保线程安全
     */
    @Override
    public synchronized void start() {
        if (runningFlag.compareAndSet(false, true)) {
            log.info("The {} starting...", this.getName());
            super.start();
            log.info("The {}  started", this.getName());
        } else {
            log.error("The {} status is {}, will not start again", this.getName(), runningFlag.get());
        }
    }

    /**
     * 主处理循环 - 分发器的核心工作方法
     *
     * 后台线程持续运行的主循环，负责从事件队列中获取任务并进行分发。
     * 这是分发器的心脏，持续不断地处理任务分发请求。
     *
     * 处理流程：
     * 1. 循环检查运行状态，只要分发器在运行就持续处理
     * 2. 从事件总线中阻塞式获取到期的任务事件
     * 3. 提取任务执行对象，准备进行分发
     * 4. 设置MDC日志上下文，便于问题追踪和诊断
     * 5. 调用doDispatchTask执行具体的任务分发逻辑
     * 6. 清理MDC上下文，避免内存泄漏
     *
     * 异常处理：
     * - 使用try-with-resources确保MDC资源正确释放
     * - 分发过程中的异常在doDispatchTask方法中处理
     *
     * 日志追踪：
     * - 设置任务ID和工作流实例ID到MDC，方便日志关联
     * - 支持分布式环境下的日志追踪和问题定位
     *
     * 线程模型：
     * - 运行在独立的后台守护线程中
     * - 阻塞式处理模式，确保资源利用率和响应性的平衡
     */
    @Override
    public void run() {
        while (runningFlag.get()) {
            TaskDispatchableEvent<ITaskExecutionRunnable> taskEntry = workerGroupEventBus.take();
            ITaskExecutionRunnable taskExecutionRunnable = taskEntry.getData();
            try (
                    TaskExecutorMDCUtils.MDCAutoClosable ignore =
                            TaskExecutorMDCUtils.logWithMDC(taskExecutionRunnable.getId())) {
                LogUtils.setWorkflowInstanceIdMDC(taskExecutionRunnable.getTaskInstance().getWorkflowInstanceId());
                doDispatchTask(taskExecutionRunnable);
            } finally {
                LogUtils.removeWorkflowInstanceIdMDC();
            }
        }
    }

    /**
     * 执行任务分发的核心逻辑
     *
     * 这是任务分发的核心实现方法，处理单个任务的分发过程，包括状态检查、
     * 实际分发和失败重试等逻辑。
     *
     * 分发流程：
     * 1. 状态验证：检查任务是否仍在等待分发队列中
     * 2. 状态清理：从等待队列中移除任务ID，标记开始分发
     * 3. 执行分发：通过TaskExecutorClient将任务发送到Worker
     * 4. 异常处理：分发失败时触发重试机制
     *
     * 状态检查逻辑：
     * - 如果任务不在waitingDispatchTaskIds中，说明任务可能已被暂停或取消
     * - 这种情况下跳过分发，避免分发已经无效的任务
     * - 防止并发场景下的重复分发问题
     *
     * 重试策略：
     * - 指数退避算法：重试间隔 = 失败次数 × 1000毫秒
     * - 最大限制：重试间隔不超过60秒，避免过度延迟
     * - 自动重入：失败的任务会重新进入分发队列
     * - 次数累积：每次失败都会增加重试计数器
     *
     * 异常处理：
     * - 捕获所有分发异常，确保分发器不会因单个任务失败而崩溃
     * - 记录详细的错误日志，包括任务ID、重试时间等信息
     * - 自动触发重试机制，提高系统的容错能力
     *
     * @param taskExecutionRunnable 待分发的任务执行对象
     */
    private void doDispatchTask(ITaskExecutionRunnable taskExecutionRunnable) {
        try {
            if (!waitingDispatchTaskIds.remove(taskExecutionRunnable.getId())) {
                log.info(
                        "The task: {} doesn't exist in waitingDispatchTaskIds(it might be paused or killed), will skip dispatch",
                        taskExecutionRunnable.getId());
                return;
            }
            taskExecutorClient.dispatch(taskExecutionRunnable);
        } catch (Exception e) {
            // 如果分发失败，将任务重新放回队列
            // 任务将在等待时间后重新分发
            // 等待时间会递增，但不会超过60秒
            long waitingTimeMills = Math.min(
                    taskExecutionRunnable.getTaskExecutionContext().increaseDispatchFailTimes() * 1_000L, 60_000L);
            dispatchTask(taskExecutionRunnable, waitingTimeMills);
            log.error("Dispatch Task: {} failed will retry after: {}/ms", taskExecutionRunnable.getId(),
                    waitingTimeMills, e);
        }
    }

    /**
     * 将任务添加到工作组分发队列
     *
     * 这是任务分发的入口方法，将任务包装成可调度事件并加入到延迟处理队列中。
     * 支持即时分发和延迟分发两种模式。
     *
     * 处理流程：
     * 1. 状态管理：将任务ID添加到等待分发集合中
     * 2. 事件包装：创建带有延迟时间的任务调度事件
     * 3. 队列入队：将事件加入到事件总线的延迟队列中
     * 4. 等待分发：事件在队列中等待到指定时间后被处理
     *
     * 使用场景：
     * - 即时分发：delayTimeMills=0，任务立即进入分发流程
     * - 延迟分发：用于定时任务或特定时间开始的任务
     * - 重试分发：失败任务的延迟重试，通过delayTimeMills控制重试间隔
     * - 负载均衡：分散任务执行时间，避免系统负载峰值
     *
     * 线程安全：
     * - waitingDispatchTaskIds使用线程安全的ConcurrentHashMap
     * - workerGroupEventBus内部实现了线程安全的队列操作
     *
     * 性能优化：
     * - 异步处理：此方法不会阻塞，快速返回
     * - 延迟排序：队列内部按时间自动排序，保证按时分发
     *
     * @param taskExecutionRunnable 要分发的任务执行对象，实现 {@link ITaskExecutionRunnable} 接口
     * @param delayTimeMills 延迟时间（毫秒），0表示立即分发
     */
    public void dispatchTask(final ITaskExecutionRunnable taskExecutionRunnable, final long delayTimeMills) {
        waitingDispatchTaskIds.add(taskExecutionRunnable.getId());
        workerGroupEventBus.add(new TaskDispatchableEvent<>(delayTimeMills, taskExecutionRunnable));
    }

    /**
     * 从分发队列中移除任务
     *
     * 尝试从等待分发的任务集合中移除指定任务。这个方法主要用于任务取消、
     * 暂停或中止等场景。
     *
     * 重要说明：
     * 这个方法只能移除还在等待队列中的任务，如果任务已经被取出并开始分发，
     * 则无法移除。这是因为任务一旦进入doDispatchTask方法，就会从
     * waitingDispatchTaskIds中被移除。
     *
     * 返回值含义：
     * - true：成功移除，任务仍在等待队列中，已取消分发
     * - false：移除失败，任务不在等待队列中（可能已被取出或从未添加）
     *
     * 使用场景：
     * - 任务取消：用户手动取消任务执行
     * - 流程中止：工作流执行过程中被停止
     * - 优先级调整：高优先级任务需要插队
     * - 异常恢复：系统异常时清理待处理任务
     *
     * 注意事项：
     * - 此方法不会从事件总线的延迟队列中移除事件
     * - 即使移除成功，对应的事件仍可能会被从队列中取出
     * - 但由于doDispatchTask中有ID检查，所以不会被重复处理
     *
     * @param taskExecutionRunnable 要移除的任务执行对象
     * @return true-成功移除, false-任务不在等待队列中
     */
    public boolean removeTask(ITaskExecutionRunnable taskExecutionRunnable) {
        return waitingDispatchTaskIds.remove(taskExecutionRunnable.getId());
    }

    /**
     * 检查任务是否存在于等待分发队列中
     *
     * 查询指定任务是否还在等待分发的状态。这是一个只读操作，
     * 不会改变任务的状态。
     *
     * 返回值含义：
     * - true：任务存在于等待分发队列中，尚未被分发
     * - false：任务不在等待队列中（可能已分发、已取消或从未添加）
     *
     * 使用场景：
     * - 状态查询：检查任务当前的分发状态
     * - 重复检查：避免重复添加同一任务
     * - 调试诊断：排查任务分发问题时的状态检查
     * - 条件判断：在执行某些操作前检查任务状态
     * - 监控统计：统计待分发任务数量等信息
     *
     * 性能考虑：
     * - 快速查询：ConcurrentHashMap的contains操作时间复杂度为O(1)
     * - 线程安全：可在多线程环境下并发调用
     * - 实时数据：返回的是调用时刻的实时状态
     *
     * 注意事项：
     * - 返回结果只反映调用时刻的状态，可能立即发生变化
     * - 不能依赖返回结果进行并发控制，应使用原子操作
     *
     * @param taskExecutionRunnable 要检查的任务执行对象
     * @return true-任务存在于等待队列中, false-任务不在等待队列中
     */
    public boolean existTask(ITaskExecutionRunnable taskExecutionRunnable) {
        return waitingDispatchTaskIds.contains(taskExecutionRunnable.getId());
    }

    /**
     * 关闭工作组分发器
     *
     * 以线程安全的方式关闭分发器，停止任务分发并清理相关资源。
     * 这是分发器生命周期的最后阶段。
     *
     * 关闭流程：
     * 1. 状态检查：检查当前运行状态，确保只关闭一次
     * 2. 原子切换：使用CAS操作将运行标志设置为false
     * 3. 线程停止：主循环线程检测到状态变化后退出
     * 4. 资源清理：系统自动清理相关线程和内存资源
     *
     * 设计特点：
     * - 幂等性：多次调用不会产生副作用，安全可靠
     * - 原子性：使用CAS操作保证状态变更的原子性
     * - 线程安全：使用synchronized确保并发调用的安全性
     * - 优雅关闭：不强制中断线程，等待当前任务处理完成
     *
     * 使用场景：
     * - 工作组删除：工作组被移除时关闭对应的分发器
     * - 系统关闭：Master节点关闭时清理所有分发器
     * - 维护操作：系统维护期间的资源清理
     * - 异常恢复：异常情况下的分发器重启
     *
     * TODO: 注意事项
     * 在工作组被删除后，需要关闭WorkerGroupTaskDispatcher线程。
     * 目前的实现只是设置了状态标志，可能需要额外的线程清理逻辑。
     *
     * 日志级别：
     * - INFO：正常关闭，记录关闭事件
     * - WARN：尝试关闭未启动的分发器，记录警告信息
     */
    public synchronized void close() {
        // TODO: 在工作组被删除后，需要关闭WorkerGroupTaskDispatcher线程
        if (runningFlag.compareAndSet(true, false)) {
            log.info("WorkerGroupDispatcher {} closed", this.getName());
        } else {
            log.warn("The WorkerGroupDispatcher: {} doesn't started", this.getName());
        }
    }

    /**
     * 获取当前队列中等待分发的任务数量
     *
     * 返回当前事件总线中等待处理的任务事件数量。
     * 这是一个快照值，主要用于监控和调试。
     *
     * 使用场景：
     * - 性能监控：监控分发器的负载情况
     * - 容量规划：评估系统容量和性能需求
     * - 调试诊断：排查任务积压和分发延迟问题
     * - 资源优化：根据队列大小调整资源分配
     * - 告警系统：队列积压过多时触发告警
     *
     * 数据特性：
     * - 实时性：返回调用时刻的实时数值
     * - 可变性：队列大小随时可能发生变化
     * - 近似值：在高并发环境下可能不完全精确
     *
     * 性能考虑：
     * - 快速查询：操作时间复杂度为O(1)
     * - 线程安全：底层队列保证线程安全
     * - 低开销：不会影响分发器的正常工作
     *
     * 注意事项：
     * - 包级可见性：方法没有公开修饰符，仅供包内使用
     * - 快照数据：返回值只代表调用时刻的状态
     * - 非实时统计：不应用于精确的实时统计
     *
     * @return 当前队列中等待处理的任务事件数量
     */
    int queueSize() {
        return this.workerGroupEventBus.size();
    }
}
