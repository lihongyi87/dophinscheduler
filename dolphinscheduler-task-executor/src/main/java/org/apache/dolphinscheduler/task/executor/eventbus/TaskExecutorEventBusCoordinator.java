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

package org.apache.dolphinscheduler.task.executor.eventbus;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.dolphinscheduler.common.thread.ThreadUtils;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.plugin.task.api.log.TaskLogMarkers;
import org.apache.dolphinscheduler.task.executor.ITaskExecutor;
import org.apache.dolphinscheduler.task.executor.ITaskExecutorRepository;
import org.apache.dolphinscheduler.task.executor.events.AbstractTaskExecutorLifecycleEvent;
import org.apache.dolphinscheduler.task.executor.events.ITaskExecutorLifecycleEvent;
import org.apache.dolphinscheduler.task.executor.events.TaskExecutorDispatchedLifecycleEvent;
import org.apache.dolphinscheduler.task.executor.events.TaskExecutorFailedLifecycleEvent;
import org.apache.dolphinscheduler.task.executor.events.TaskExecutorFinalizeLifecycleEvent;
import org.apache.dolphinscheduler.task.executor.events.TaskExecutorKillLifecycleEvent;
import org.apache.dolphinscheduler.task.executor.events.TaskExecutorKilledLifecycleEvent;
import org.apache.dolphinscheduler.task.executor.events.TaskExecutorPauseLifecycleEvent;
import org.apache.dolphinscheduler.task.executor.events.TaskExecutorPausedLifecycleEvent;
import org.apache.dolphinscheduler.task.executor.events.TaskExecutorRuntimeContextChangedLifecycleEvent;
import org.apache.dolphinscheduler.task.executor.events.TaskExecutorStartedLifecycleEvent;
import org.apache.dolphinscheduler.task.executor.events.TaskExecutorSuccessLifecycleEvent;
import org.apache.dolphinscheduler.task.executor.listener.ITaskExecutorLifecycleEventListener;
import org.apache.dolphinscheduler.task.executor.log.TaskExecutorMDCUtils;

import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;

/**
 * 任务执行器事件总线协调器实现
 *
 * <p>该类实现了{@link ITaskExecutorEventBusCoordinator}接口，
 * 负责管理和协调任务执行器的生命周期事件。
 *
 * <p>核心功能：
 * <ul>
 *   <li>使用定时任务扫描所有任务执行器的事件队列</li>
 *   <li>异步处理事件，避免阻塞主线程</li>
 *   <li>将事件分发给所有注册的监听器</li>
 *   <li>防止重复处理同一任务的事件</li>
 * </ul>
 */
@Slf4j
public class TaskExecutorEventBusCoordinator implements ITaskExecutorEventBusCoordinator {

    /**
     * 协调器名称
     * 用于标识不同的协调器实例，通常包含节点类型（如Master或Worker）
     */
    private final String coordinatorName;

    /**
     * 任务执行器仓库
     * 用于管理和获取所有的任务执行器实例
     */
    private final ITaskExecutorRepository taskExecutorRepository;

    /**
     * 任务执行器生命周期事件监听器列表
     * 存储所有注册的事件监听器，事件发生时会通知这些监听器
     */
    private final List<ITaskExecutorLifecycleEventListener> taskExecutorLifecycleEventListeners;

    /**
     * 默认工作线程数
     * 设置为CPU核心数，以充分利用系统资源
     */
    private static final int DEFAULT_WORKER_SIZE = Runtime.getRuntime().availableProcessors();

    /**
     * 默认触发间隔（毫秒）
     * 每50毫秒扫描一次所有任务执行器的事件队列
     */
    private static final long DEFAULT_FIRE_INTERVAL = 50;

    /**
     * 正在处理中的任务执行器ID集合
     * 用于防止同一任务执行器的事件被多个线程同时处理
     * 使用ConcurrentHashMap.newKeySet()保证线程安全
     */
    private static final Set<Integer> firingTaskExecutorIds = ConcurrentHashMap.newKeySet();

    /**
     * 主线程池
     * 用于定时扫描任务执行器事件队列
     */
    private ScheduledExecutorService mainExecutorThreadPool;

    /**
     * 工作线程池
     * 用于异步处理各个任务执行器的事件
     */
    private ThreadPoolExecutor workerExecutorThreadPool;

    public TaskExecutorEventBusCoordinator(final String coordinatorName,
                                           final ITaskExecutorRepository taskExecutorRepository) {
        this.coordinatorName = coordinatorName;
        this.taskExecutorRepository = taskExecutorRepository;
        this.taskExecutorLifecycleEventListeners = new ArrayList<>();
    }

    /**
     * 启动协调器
     *
     * <p>初始化线程池并启动定时任务：
     * <ul>
     *   <li>创建主线程池，定时扫描事件队列</li>
     *   <li>创建工作线程池，异步处理事件</li>
     * </ul>
     */
    public void start() {
        // 创建主线程池，只有一个线程，负责定时扫描
        mainExecutorThreadPool = ThreadUtils.newDaemonScheduledExecutorService(
                coordinatorName + "-eventbus-coordinator-main-%d", 1);
        // todo: 使用事件分发器根据条件控制事件触发
        // 定时任务：立即开始，每50毫秒执行一次
        mainExecutorThreadPool.scheduleWithFixedDelay(
                this::fireTaskExecutorEventBus,  // 执行方法
                0,  // 初始延迟为0，立即开始
                DEFAULT_FIRE_INTERVAL,  // 间隔时间50ms
                TimeUnit.MILLISECONDS);

        // 创建工作线程池，用于异步处理各任务执行器的事件
        // 线程数设置为CPU核心数，充分利用多核性能
        workerExecutorThreadPool = ThreadUtils.newDaemonFixedThreadExecutor(
                coordinatorName + "-eventbus-coordinator-worker-%d", DEFAULT_WORKER_SIZE);
        log.info("{} started, worker size: {}", coordinatorName, DEFAULT_WORKER_SIZE);
    }

    @Override
    public void registerTaskExecutorLifecycleEventListener(final ITaskExecutorLifecycleEventListener taskExecutorLifecycleEventListener) {
        // 检查参数不为null，为null会抛出NullPointerException
        checkNotNull(taskExecutorLifecycleEventListener);
        // 将监听器添加到列表中
        // 注意：这里没有同步保护，假设注册只在启动阶段进行
        taskExecutorLifecycleEventListeners.add(taskExecutorLifecycleEventListener);
    }

    @Override
    public void close() {
        // 立即关闭主线程池，停止定时扫描
        // shutdownNow()会尝试中断正在执行的任务
        mainExecutorThreadPool.shutdownNow();
        // 注意：这里没有关闭workerExecutorThreadPool
        // 可能需要等待工作线程完成当前任务
        log.info("{} closed", coordinatorName);
    }

    /**
     * 触发任务执行器事件总线
     *
     * <p>该方法由定时任务调用，扫描所有任务执行器并处理其事件：
     * <ul>
     *   <li>获取所有任务执行器</li>
     *   <li>跳过正在处理中的任务执行器</li>
     *   <li>异步处理每个任务执行器的事件</li>
     *   <li>处理完成后从处理中集合移除</li>
     * </ul>
     */
    private void fireTaskExecutorEventBus() {
        try {
            // 获取所有正在运行的任务执行器
            final Collection<ITaskExecutor> taskExecutors = taskExecutorRepository.getAll();
            if (CollectionUtils.isEmpty(taskExecutors)) {
                // 没有任务执行器，直接返回
                return;
            }
            // 遍历每个任务执行器
            for (final ITaskExecutor taskExecutor : taskExecutors) {
                // 检查该任务执行器是否正在被处理
                if (isFiring(taskExecutor)) {
                    // 正在处理中，跳过避免重复处理
                    continue;
                }
                final Integer taskExecutorId = taskExecutor.getId();
                // 使用CompletableFuture异步处理
                CompletableFuture
                        // 第一步：将ID加入正在处理集合，标记为处理中
                        .runAsync(() -> firingTaskExecutorIds.add(taskExecutorId), workerExecutorThreadPool)
                        // 第二步：执行实际的事件处理
                        .thenAccept(v -> doFireTaskExecutorEventBus(taskExecutor))
                        // 最后：无论成功或失败，都从处理中集合移除
                        .whenComplete((v, e) -> firingTaskExecutorIds.remove(taskExecutorId));
            }
        } catch (Throwable throwable) {
            // 捕获所有异常，避免定时任务被中断
            log.error("Fire TaskExecutorEventBus error", throwable);
        }
    }

    /**
     * 处理单个任务执行器的事件
     *
     * <p>从任务执行器的事件总线中获取事件，并根据事件类型
     * 分发给相应的监听器处理。
     *
     * <p>支持的事件类型包括：
     * <ul>
     *   <li>DISPATCHED - 任务被分发</li>
     *   <li>RUNNING - 任务开始运行</li>
     *   <li>PAUSE/PAUSED - 任务暂停</li>
     *   <li>KILL/KILLED - 任务被杀死</li>
     *   <li>SUCCESS - 任务成功完成</li>
     *   <li>FAILED - 任务执行失败</li>
     *   <li>FINALIZE - 任务最终化</li>
     * </ul>
     *
     * @param taskExecutor 要处理事件的任务执行器
     */
    private void doFireTaskExecutorEventBus(final ITaskExecutor taskExecutor) {
        // 使用try-with-resources自动管理MDC上下文
        // MDC用于日志追踪，保证日志中包含任务信息
        try (final TaskExecutorMDCUtils.MDCAutoClosable ignored = TaskExecutorMDCUtils.logWithMDC(taskExecutor)) {
            // 获取任务执行器的事件总线
            final TaskExecutorEventBus taskExecutorEventBus = taskExecutor.getTaskExecutorEventBus();
            if (taskExecutorEventBus.isEmpty()) {
                // 事件队列为空，无需处理
                return;
            }
            // 非阻塞获取队列头部的事件
            Optional<AbstractTaskExecutorLifecycleEvent> headEventOptional = taskExecutorEventBus.poll();
            if (!headEventOptional.isPresent()) {
                // 没有可处理的事件（可能事件还未到期）
                return;
            }
            // 获取到需要处理的事件
            final ITaskExecutorLifecycleEvent taskExecutorLifecycleEvent = headEventOptional.get();
            try {
                // 通知所有注册的监听器
                for (final ITaskExecutorLifecycleEventListener taskExecutorLifecycleEventListener : taskExecutorLifecycleEventListeners) {
                    // 根据事件类型调用相应的监听器方法
                    switch (taskExecutorLifecycleEvent.getType()) {
                        case DISPATCHED:
                            // 任务被分发事件
                            taskExecutorLifecycleEventListener.onTaskExecutorDispatchedLifecycleEvent(
                                    ((TaskExecutorDispatchedLifecycleEvent) taskExecutorLifecycleEvent));
                            break;
                        case RUNNING:
                            // 任务开始运行事件
                            taskExecutorLifecycleEventListener.onTaskExecutorStartedLifecycleEvent(
                                    ((TaskExecutorStartedLifecycleEvent) taskExecutorLifecycleEvent));
                            break;
                        case RUNTIME_CONTEXT_CHANGE:
                            taskExecutorLifecycleEventListener.onTaskExecutorRuntimeContextChangedEvent(
                                    ((TaskExecutorRuntimeContextChangedLifecycleEvent) taskExecutorLifecycleEvent));
                            break;
                        case PAUSE:
                            taskExecutorLifecycleEventListener.onTaskExecutorPauseLifecycleEvent(
                                    ((TaskExecutorPauseLifecycleEvent) taskExecutorLifecycleEvent));
                            break;
                        case PAUSED:
                            taskExecutorLifecycleEventListener.onTaskExecutorPausedLifecycleEvent(
                                    ((TaskExecutorPausedLifecycleEvent) taskExecutorLifecycleEvent));
                            break;
                        case KILL:
                            taskExecutorLifecycleEventListener.onTaskExecutorKillLifecycleEvent(
                                    ((TaskExecutorKillLifecycleEvent) taskExecutorLifecycleEvent));
                            break;
                        case KILLED:
                            taskExecutorLifecycleEventListener.onTaskExecutorKilledLifecycleEvent(
                                    ((TaskExecutorKilledLifecycleEvent) taskExecutorLifecycleEvent));
                            break;
                        case SUCCESS:
                            taskExecutorLifecycleEventListener.onTaskExecutorSuccessLifecycleEvent(
                                    ((TaskExecutorSuccessLifecycleEvent) taskExecutorLifecycleEvent));
                            break;
                        case FAILED:
                            taskExecutorLifecycleEventListener.onTaskExecutorFailLifecycleEvent(
                                    ((TaskExecutorFailedLifecycleEvent) taskExecutorLifecycleEvent));
                            break;
                        case FINALIZE:
                            taskExecutorLifecycleEventListener.onTaskExecutorFinalizeLifecycleEvent(
                                    ((TaskExecutorFinalizeLifecycleEvent) taskExecutorLifecycleEvent));
                            break;
                        default:
                            throw new IllegalArgumentException(
                                    "Unsupported TaskExecutorLifecycleEvent: " + taskExecutorLifecycleEvent);
                    }
                }
                log.info(TaskLogMarkers.excludeInTaskLog(), "Success fire {}: {} ",
                        taskExecutorLifecycleEvent.getClass().getSimpleName(),
                        JSONUtils.toJsonString(taskExecutorLifecycleEvent));
            } catch (Exception e) {
                log.error("Fire TaskExecutorLifecycleEvent: {} error", taskExecutorLifecycleEvent, e);
            }
        }
    }

    /**
     * 检查任务执行器是否正在处理中
     *
     * <p>通过检查firingTaskExecutorIds集合，判断指定的任务执行器
     * 是否正在被其他线程处理，避免重复处理。
     *
     * @param taskExecutor 要检查的任务执行器
     * @return 如果正在处理中返回true，否则返回false
     */
    private boolean isFiring(final ITaskExecutor taskExecutor) {
        // 检查任务执行器ID是否在处理中集合里
        // firingTaskExecutorIds是ConcurrentHashMap.newKeySet()创建的，线程安全
        return firingTaskExecutorIds.contains(taskExecutor.getId());
    }

}
