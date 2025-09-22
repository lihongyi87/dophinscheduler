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

import org.apache.dolphinscheduler.common.exception.BaseException;
import org.apache.dolphinscheduler.common.thread.BaseDaemonThread;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.plugin.task.api.log.TaskLogMarkers;
import org.apache.dolphinscheduler.task.executor.ITaskExecutor;
import org.apache.dolphinscheduler.task.executor.ITaskExecutorRepository;
import org.apache.dolphinscheduler.task.executor.events.IReportableTaskExecutorLifecycleEvent;
import org.apache.dolphinscheduler.task.executor.events.TaskExecutorFinalizeLifecycleEvent;
import org.apache.dolphinscheduler.task.executor.events.TaskExecutorLifecycleEventType;
import org.apache.dolphinscheduler.task.executor.log.TaskExecutorMDCUtils;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import com.google.common.annotations.VisibleForTesting;

/**
 * 任务执行器生命周期事件远程报告器实现
 *
 * <p>该类实现了{@link ITaskExecutorLifecycleEventReporter}接口，
 * 负责将任务执行器的生命周期事件可靠地报告给Master节点。
 *
 * <p>主要特性：
 * <ul>
 *   <li>异步事件报告：事件先放入队列，后台线程负责发送</li>
 *   <li>重试机制：未收到ACK的事件会定期重试</li>
 *   <li>事件通道：每个任务实例有独立的事件通道</li>
 *   <li>故障转移支持：当Master变更时可重置事件状态</li>
 * </ul>
 */
@Slf4j
public class TaskExecutorLifecycleEventRemoteReporter extends BaseDaemonThread
        implements
            ITaskExecutorLifecycleEventReporter {

    /**
     * 默认事件重试间隔（毫秒）
     * 设置为3分钟，未收到ACK的事件将在3分钟后重试
     */
    private static final Long DEFAULT_TASK_EXECUTOR_EVENT_RETRY_INTERVAL = TimeUnit.MINUTES.toMillis(3);

    /**
     * 报告器名称
     */
    private final String reporterName;

    /**
     * 事件通道集合
     * key: 任务实例ID，value: 对应的事件通道
     * 使用ConcurrentHashMap保证线程安全
     */
    private final Map<Integer, ReportableTaskExecutorLifecycleEventChannel> eventChannels = new ConcurrentHashMap<>();

    /**
     * 远程报告客户端
     * 用于实际发送事件到Master
     */
    private final ITaskExecutorEventRemoteReporterClient taskExecutorEventRemoteReporterClient;

    /**
     * 运行状态标志
     */
    private volatile boolean runningFlag;

    /**
     * 事件通道锁
     * 用于保护eventChannels的并发访问
     */
    private final Lock eventChannelsLock = new ReentrantLock();

    /**
     * 空事件条件变量
     * 当所有事件通道为空时，线程在此条件上等待
     */
    private final Condition taskExecutionEventEmptyCondition = eventChannelsLock.newCondition();

    /**
     * 任务执行器仓库
     */
    private final ITaskExecutorRepository taskExecutorRepository;

    public TaskExecutorLifecycleEventRemoteReporter(final String reporterName,
                                                    final ITaskExecutorEventRemoteReporterClient taskExecutorEventRemoteReporterClient,
                                                    final ITaskExecutorRepository taskExecutorRepository) {
        super(reporterName);
        this.reporterName = reporterName;
        this.taskExecutorEventRemoteReporterClient = taskExecutorEventRemoteReporterClient;
        this.taskExecutorRepository = taskExecutorRepository;
    }

    @Override
    public void start() {
        // start a thread to send the events
        this.runningFlag = true;
        super.start();
        log.info("{} started", reporterName);
    }

    /**
     * 主循环
     *
     * <p>后台线程的主处理逻辑：
     * <ul>
     *   <li>遍历所有事件通道，处理待发送的事件</li>
     *   <li>如果所有通道为空，在条件变量上等待</li>
     *   <li>根据重试间隔控制发送频率</li>
     * </ul>
     */
    @Override
    public void run() {
        while (runningFlag) {
            try {
                // 处理所有非空事件通道
                for (final ReportableTaskExecutorLifecycleEventChannel eventChannel : eventChannels.values()) {
                    if (eventChannel.isEmpty()) {
                        continue;
                    }
                    handleTaskExecutionEventChannel(eventChannel);
                }
                // 所有通道为空时等待新事件
                tryToWaitIfAllTaskExecutionEventChannelEmpty();
                // 根据重试间隔控制发送频率
                waitIfAnyTaskExecutionEventChannelRetryIntervalPassed();
            } catch (InterruptedException e) {
                log.info("{} interrupted", reporterName);
                Thread.currentThread().interrupt();
                break;
            } catch (Exception ex) {
                log.error("Fire ReportableTaskExecutorLifecycleEventChannel error", ex);
            }
        }
        log.info("{} break loop", reporterName);
    }

    @Override
    public void reportTaskExecutorLifecycleEvent(final IReportableTaskExecutorLifecycleEvent reportableTaskExecutorLifecycleEvent) {
        // 获取锁，保证对eventChannels的并发访问安全
        eventChannelsLock.lock();
        try {
            // 记录日志，使用excludeInTaskLog标记避免混入任务日志
            log.info(TaskLogMarkers.excludeInTaskLog(), "Report : {}",
                    JSONUtils.toPrettyJsonString(reportableTaskExecutorLifecycleEvent));
            // 获取任务实例ID
            int taskInstanceId = reportableTaskExecutorLifecycleEvent.getTaskInstanceId();
            // 使用computeIfAbsent确保每个任务实例只有一个通道
            // 如果通道不存在，则创建新的
            eventChannels.computeIfAbsent(
                    taskInstanceId,
                    k -> new ReportableTaskExecutorLifecycleEventChannel(taskInstanceId))
                    .addTaskExecutionEvent(reportableTaskExecutorLifecycleEvent);
            // 唤醒所有等待事件的线程
            // 通知主循环有新事件需要处理
            taskExecutionEventEmptyCondition.signalAll();
        } finally {
            // 确保锁被释放
            eventChannelsLock.unlock();
        }

    }

    @Override
    public void receiveTaskExecutorLifecycleEventACK(final TaskExecutorLifecycleEventAck eventAck) {
        // 获取ACK对应的任务执行器ID
        final int taskExecutorId = eventAck.getTaskExecutorId();
        // 加锁保护eventChannels
        eventChannelsLock.lock();
        try {
            // 查找对应的事件通道
            final ReportableTaskExecutorLifecycleEventChannel eventChannel = eventChannels.get(taskExecutorId);
            if (eventChannel == null) {
                // 通道不存在，可能已经被清理
                return;
            }
            // 根据事件类型从通道中移除对应的事件
            final IReportableTaskExecutorLifecycleEvent removed =
                    eventChannel.remove(eventAck.getTaskExecutorLifecycleEventType());
            if (removed != null) {
                // 成功移除事件
                log.info("Success removed {} by ack: {}", removed, eventAck);
            } else {
                // 未找到对应的事件，可能已经被处理
                log.info("Failed removed ReportableTaskExecutorLifecycleEvent by ack: {}", eventAck);
            }
            // 检查通道是否为空
            if (eventChannel.isEmpty()) {
                // 延长任务执行器的生命周期，覆盖整个任务处理周期
                // 在关联通道被移除后，才能最终化任务执行器
                if (removed != null && removed.getType().isFinished()) {
                    // 任务已经结束，执行最终化
                    finalizeTaskExecutor(removed.getTaskInstanceId());
                }
                // 移除空通道
                eventChannels.remove(taskExecutorId);
                log.debug("Removed ReportableTaskExecutorLifecycleEventChannel: {}", taskExecutorId);
            }
            // 唤醒等待线程
            taskExecutionEventEmptyCondition.signalAll();
        } finally {
            // 释放锁
            eventChannelsLock.unlock();
        }
    }

    @Override
    public void onWorkflowInstanceHostChanged(int taskInstanceId) {
        eventChannelsLock.lock();
        try {
            final ReportableTaskExecutorLifecycleEventChannel eventChannel = eventChannels.get(taskInstanceId);
            if (eventChannel != null) {
                eventChannel.taskExecutionEventsQueue.forEach(event -> event.setLatestReportTime(null));
                taskExecutionEventEmptyCondition.signalAll();
            }
        } finally {
            eventChannelsLock.unlock();
        }
    }

    @Override
    public void close() {
        // shutdown the thread
        runningFlag = false;
        log.info("{} closed", reporterName);
    }

    @VisibleForTesting
    public Map<Integer, ReportableTaskExecutorLifecycleEventChannel> getEventChannels() {
        return eventChannels;
    }

    private void finalizeTaskExecutor(final Integer taskExecutorId) {
        final Optional<ITaskExecutor> taskExecutorOptional = taskExecutorRepository.get(taskExecutorId);
        if (taskExecutorOptional.isPresent()) {
            taskExecutorOptional.get().getTaskExecutorEventBus()
                    .publish(TaskExecutorFinalizeLifecycleEvent.of(taskExecutorOptional.get()));
        } else {
            log.warn("TaskExecutor is not exists: {}", taskExecutorId);
        }
    }

    /**
     * 处理单个事件通道
     *
     * <p>从通道中获取事件并发送到Master：
     * <ul>
     *   <li>检查事件是否需要发送（首次或超过重试间隔）</li>
     *   <li>获取Master地址并发送事件</li>
     *   <li>出错时记录日志并等待重试</li>
     * </ul>
     *
     * @param reportableTaskExecutorLifecycleEventChannel 要处理的事件通道
     */
    private void handleTaskExecutionEventChannel(final ReportableTaskExecutorLifecycleEventChannel reportableTaskExecutorLifecycleEventChannel) {
        if (reportableTaskExecutorLifecycleEventChannel.isEmpty()) {
            return;
        }
        while (!reportableTaskExecutorLifecycleEventChannel.isEmpty()) {
            final IReportableTaskExecutorLifecycleEvent headEvent = reportableTaskExecutorLifecycleEventChannel.peek();
            try (
                    final TaskExecutorMDCUtils.MDCAutoClosable ignore =
                            TaskExecutorMDCUtils.logWithMDC(headEvent.getTaskInstanceId())) {
                try {
                    if (isTaskExecutorEventNeverSent(headEvent) || isRetryIntervalExceeded(headEvent)) {
                        final Optional<ITaskExecutor> taskExecutorOptional =
                                taskExecutorRepository.get(headEvent.getTaskInstanceId());
                        if (!taskExecutorOptional.isPresent()) {
                            throw new BaseException(String.format("The TaskExecutor id %d is not exist.",
                                    headEvent.getTaskInstanceId()));
                        }
                        final String masterAddress =
                                taskExecutorOptional.get().getTaskExecutionContext().getWorkflowInstanceHost();
                        taskExecutorEventRemoteReporterClient.reportTaskExecutionEventToMaster(masterAddress,
                                headEvent);
                        continue;
                    }
                    if (log.isDebugEnabled()) {
                        log.debug(
                                "The ReportableTaskExecutorLifecycleEvent: {} latest send time: {} doesn't exceeded retry interval",
                                headEvent,
                                headEvent.getLatestReportTime());
                    }
                    break;
                } catch (Exception ex) {
                    log.error("Send TaskExecutionEvent: {} to master error will retry after {} mills",
                            headEvent,
                            DEFAULT_TASK_EXECUTOR_EVENT_RETRY_INTERVAL,
                            ex);
                    break;

                }
            }
        }
    }

    private boolean isAllTaskExecutorEventChannelEmpty() {
        return eventChannels
                .values()
                .stream()
                .allMatch(ReportableTaskExecutorLifecycleEventChannel::isEmpty);
    }

    private long getOldestReportTime() {
        return eventChannels.values()
                .stream()
                .filter(ReportableTaskExecutorLifecycleEventChannel::isNotEmpty)
                .map(ReportableTaskExecutorLifecycleEventChannel::peek)
                .filter(event -> !isTaskExecutorEventNeverSent(event))
                .map(IReportableTaskExecutorLifecycleEvent::getLatestReportTime)
                .min(Long::compareTo)
                .orElse(0L);
    }

    /**
     * 检查事件是否从未发送过
     *
     * @param headEvent 要检查的事件
     * @return 如果从未发送过返回true
     */
    private boolean isTaskExecutorEventNeverSent(final IReportableTaskExecutorLifecycleEvent headEvent) {
        // LatestReportTime为null表示从未发送
        return headEvent.getLatestReportTime() == null;
    }

    /**
     * 检查是否超过重试间隔
     *
     * @param reportableTaskExecutorLifecycleEvent 要检查的事件
     * @return 如果超过重试间隔返回true
     */
    private boolean isRetryIntervalExceeded(final IReportableTaskExecutorLifecycleEvent reportableTaskExecutorLifecycleEvent) {
        if (isTaskExecutorEventNeverSent(reportableTaskExecutorLifecycleEvent)) {
            // 从未发送过的事件立即发送
            return true;
        }
        long currentTime = System.currentTimeMillis();
        // 检查距离上次发送是否超过3分钟
        return currentTime - reportableTaskExecutorLifecycleEvent
                .getLatestReportTime() > DEFAULT_TASK_EXECUTOR_EVENT_RETRY_INTERVAL;
    }

    /**
     * 如果所有事件通道为空，则等待
     *
     * @throws InterruptedException 如果线程被中断
     */
    private void tryToWaitIfAllTaskExecutionEventChannelEmpty() throws InterruptedException {
        eventChannelsLock.lock();
        try {
            // 当所有通道都为空时，在条件变量上等待
            while (isAllTaskExecutorEventChannelEmpty()) {
                // await()会释放锁并等待，被唤醒后重新获取锁
                taskExecutionEventEmptyCondition.await();
            }
        } finally {
            eventChannelsLock.unlock();
        }
    }

    /**
     * 根据重试间隔控制发送频率
     *
     * @throws InterruptedException 如果线程被中断
     */
    private void waitIfAnyTaskExecutionEventChannelRetryIntervalPassed() throws InterruptedException {
        eventChannelsLock.lock();
        try {
            // 计算距离下次重试需要等待的时间
            // 公式: (最早报告时间 + 重试间隔) - 当前时间
            final long waitInterval =
                    (getOldestReportTime() + DEFAULT_TASK_EXECUTOR_EVENT_RETRY_INTERVAL) - System.currentTimeMillis();
            if (waitInterval <= 0) {
                // 不需要等待，立即返回
                return;
            }
            // 在指定时间内等待，或被唤醒
            taskExecutionEventEmptyCondition.await(waitInterval, TimeUnit.MILLISECONDS);
        } finally {
            eventChannelsLock.unlock();
        }
    }

    /**
     * 可报告任务执行器生命周期事件通道
     *
     * <p>为每个任务实例维护一个独立的事件队列，
     * 管理该任务所有待报告的生命周期事件。
     */
    public static class ReportableTaskExecutorLifecycleEventChannel {

        /**
         * 任务执行器ID
         */
        @Getter
        private final int taskExecutorId;

        /**
         * 任务执行事件队列
         * 使用链表阻塞队列，保证事件顺序
         */
        private final LinkedBlockingQueue<IReportableTaskExecutorLifecycleEvent> taskExecutionEventsQueue;

        /**
         * 构造函数
         *
         * @param taskExecutorId 任务执行器ID
         */
        // todo: 从Ta skExecutor获取Master地址，而不是在channel中存储
        public ReportableTaskExecutorLifecycleEventChannel(int taskExecutorId) {
            this.taskExecutorId = taskExecutorId;
            this.taskExecutionEventsQueue = new LinkedBlockingQueue<>();
        }

        /**
         * 添加事件到队列
         *
         * @param reportableTaskExecutorLifecycleEvent 可报告的任务执行器生命周期事件
         */
        public void addTaskExecutionEvent(final IReportableTaskExecutorLifecycleEvent reportableTaskExecutorLifecycleEvent) {
            taskExecutionEventsQueue.add(reportableTaskExecutorLifecycleEvent);
        }

        public IReportableTaskExecutorLifecycleEvent peek() {
            return taskExecutionEventsQueue.peek();
        }

        /**
         * 根据类型移除事件
         *
         * <p>从队列中移除指定类型的事件，
         * 通常在收到Master的ACK后调用。
         *
         * @param type 要移除的事件类型
         * @return 被移除的事件，如果不存在返回null
         */
        public IReportableTaskExecutorLifecycleEvent remove(TaskExecutorLifecycleEventType type) {
            final AtomicReference<IReportableTaskExecutorLifecycleEvent> removed = new AtomicReference<>();
            taskExecutionEventsQueue.removeIf(event -> {
                if (event.getType() == type) {
                    removed.set(event);
                    return true;
                }
                return false;
            });
            return removed.get();
        }

        public boolean isEmpty() {
            return taskExecutionEventsQueue.isEmpty();
        }

        public boolean isNotEmpty() {
            return !isEmpty();
        }

    }
}
