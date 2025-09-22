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

package org.apache.dolphinscheduler.server.master.engine.task.statemachine;

import org.apache.dolphinscheduler.common.utils.DateUtils;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.dao.repository.TaskInstanceDao;
import org.apache.dolphinscheduler.plugin.task.api.enums.TaskExecutionStatus;
import org.apache.dolphinscheduler.server.master.engine.task.dispatcher.WorkerGroupDispatcherCoordinator;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event.TaskDispatchLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event.TaskDispatchedLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event.TaskFailedLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event.TaskFailoverLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event.TaskKillLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event.TaskKilledLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event.TaskPauseLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event.TaskPausedLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event.TaskRetryLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event.TaskRunningLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event.TaskStartLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event.TaskSuccessLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.task.runnable.ITaskExecutionRunnable;
import org.apache.dolphinscheduler.server.master.engine.workflow.runnable.IWorkflowExecutionRunnable;

import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * 任务已提交状态动作处理器
 *
 * 处理任务处于SUBMITTED_SUCCESS状态时的各种生命周期事件。
 * 这是任务状态机中的初始活跃状态，任务在这个状态下等待被分发到执行器。
 *
 * 状态特征：
 * - 任务已成功提交到调度系统
 * - 等待获取执行资源（如任务组槽位）
 * - 等待被分发到合适的执行器
 * - 可以被暂停或取消
 * - 支持延迟执行逻辑
 *
 * 核心职责：
 * 1. 任务启动处理：检查工作流状态，决定是否继续执行
 * 2. 任务分发：处理任务分发事件，支持延迟执行
 * 3. 状态检查：确保各种事件在合适的状态下处理
 * 4. 异常处理：对不合适的事件记录警告日志
 * 5. 暂停/终止：支持在分发前暂停或终止任务
 *
 * 事件处理策略：
 * - onStartEvent: 检查工作流状态，尝试分发任务
 * - onDispatchEvent: 处理延迟执行逻辑，初始化上下文，执行分发
 * - onPauseEvent: 尝试从分发队列移除，失败则延迟暂停
 * - onKillEvent: 尝试从分发队列移除，失败则延迟终止
 * - 其他事件: 记录警告日志，表示状态不匹配
 *
 * 延迟执行机制：
 * 1. 计算剩余延迟时间（基于首次提交时间和配置的延迟时间）
 * 2. 如果需要延迟，更新状态为DELAY_EXECUTION
 * 3. 将任务加入延迟执行队列
 * 4. 延迟时间到达后自动继续分发流程
 *
 * 分发协调流程：
 * 1. 初始化任务执行上下文
 * 2. 通过工作组分发协调器分发任务
 * 3. 处理分发成功/失败的反馈
 * 4. 更新任务状态和执行器信息
 *
 * 暂停/终止处理：
 * - 优先尝试从分发队列中直接移除任务
 * - 如果任务已经分发但未开始执行，延迟处理
 * - 通过延迟机制给执行器时间响应暂停/终止指令
 *
 * 工作流状态检查：
 * - 工作流准备暂停：直接发布任务暂停事件
 * - 工作流准备停止：直接发布任务终止事件
 * - 正常状态：继续正常的任务分发流程
 *
 * 类比理解：
 * 就像工厂的生产订单处理中心：
 * - 接收生产订单（任务提交）
 * - 检查生产线状态（工作流状态）
 * - 分配生产资源（任务组槽位）
 * - 安排到具体生产线（分发到执行器）
 * - 处理订单变更（暂停/取消）
 * - 支持延期生产（延迟执行）
 */
@Slf4j
@Component
public class TaskSubmittedStateAction extends AbstractTaskStateAction {

    @Autowired
    private WorkerGroupDispatcherCoordinator workerGroupDispatcherCoordinator;

    @Autowired
    private TaskInstanceDao taskInstanceDao;

    @Override
    public void onStartEvent(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                             final ITaskExecutionRunnable taskExecutionRunnable,
                             final TaskStartLifecycleEvent taskStartEvent) {
        throwExceptionIfStateIsNotMatch(taskExecutionRunnable);

        if (workflowExecutionRunnable.isWorkflowReadyPause()) {
            workflowExecutionRunnable.getWorkflowEventBus().publish(TaskPausedLifecycleEvent.of(taskExecutionRunnable));
            return;
        }

        if (workflowExecutionRunnable.isWorkflowReadyStop()) {
            workflowExecutionRunnable.getWorkflowEventBus().publish(TaskKilledLifecycleEvent.of(taskExecutionRunnable));
            return;
        }

        tryToDispatchTask(taskExecutionRunnable);
    }

    @Override
    public void onStartedEvent(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                               final ITaskExecutionRunnable taskExecutionRunnable,
                               final TaskRunningLifecycleEvent taskRunningEvent) {
        throwExceptionIfStateIsNotMatch(taskExecutionRunnable);
        logWarningIfCannotDoAction(taskExecutionRunnable, taskRunningEvent);
    }

    @Override
    public void onRetryEvent(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                             final ITaskExecutionRunnable taskExecutionRunnable,
                             final TaskRetryLifecycleEvent taskRetryEvent) {
        throwExceptionIfStateIsNotMatch(taskExecutionRunnable);
        logWarningIfCannotDoAction(taskExecutionRunnable, taskRetryEvent);
    }

    @Override
    public void onDispatchEvent(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                                final ITaskExecutionRunnable taskExecutionRunnable,
                                final TaskDispatchLifecycleEvent taskDispatchEvent) {
        throwExceptionIfStateIsNotMatch(taskExecutionRunnable);
        final TaskInstance taskInstance = taskExecutionRunnable.getTaskInstance();
        long remainTimeMills = DateUtils.getRemainTime(
                taskInstance.getFirstSubmitTime(),
                taskInstance.getDelayTime() * 60L) * 1_000;
        if (remainTimeMills > 0) {
            taskInstance.setState(TaskExecutionStatus.DELAY_EXECUTION);
            taskInstanceDao.updateById(taskInstance);
            log.info("Current taskInstance: {} is choose delay execution, delay time: {}/min, remainTime: {}/ms",
                    taskInstance.getName(),
                    taskInstance.getDelayTime(),
                    remainTimeMills);
        }
        taskExecutionRunnable.initializeTaskExecutionContext();
        workerGroupDispatcherCoordinator.dispatchTask(taskExecutionRunnable, remainTimeMills);
    }

    @Override
    public void onDispatchedEvent(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                                  final ITaskExecutionRunnable taskExecutionRunnable,
                                  final TaskDispatchedLifecycleEvent taskDispatchedEvent) {
        throwExceptionIfStateIsNotMatch(taskExecutionRunnable);
        super.onDispatchedEvent(workflowExecutionRunnable, taskExecutionRunnable, taskDispatchedEvent);
    }

    @Override
    public void onPauseEvent(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                             final ITaskExecutionRunnable taskExecutionRunnable,
                             final TaskPauseLifecycleEvent taskPauseEvent) {
        throwExceptionIfStateIsNotMatch(taskExecutionRunnable);
        if (workerGroupDispatcherCoordinator.removeTask(taskExecutionRunnable)) {
            log.info("Success pause task: {} before dispatch", taskExecutionRunnable.getName());
            taskExecutionRunnable.getWorkflowEventBus().publish(TaskPausedLifecycleEvent.of(taskExecutionRunnable));
            return;
        }
        log.info("The task[id={}] is submitted and already dispatched, cannot pause, will try to pause it after 5s",
                taskExecutionRunnable.getId());
        taskExecutionRunnable.getWorkflowEventBus()
                .publish(TaskPauseLifecycleEvent.of(taskExecutionRunnable, TimeUnit.SECONDS.toMillis(5)));
    }

    @Override
    public void onPausedEvent(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                              final ITaskExecutionRunnable taskExecutionRunnable,
                              final TaskPausedLifecycleEvent taskPausedEvent) {
        throwExceptionIfStateIsNotMatch(taskExecutionRunnable);
        super.onPausedEvent(workflowExecutionRunnable, taskExecutionRunnable, taskPausedEvent);
    }

    @Override
    public void onKillEvent(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                            final ITaskExecutionRunnable taskExecutionRunnable,
                            final TaskKillLifecycleEvent taskKillEvent) {
        throwExceptionIfStateIsNotMatch(taskExecutionRunnable);
        if (workerGroupDispatcherCoordinator.removeTask(taskExecutionRunnable)) {
            log.info("Success kill task[id={}] before dispatch", taskExecutionRunnable.getId());
            taskExecutionRunnable.getWorkflowEventBus().publish(TaskKilledLifecycleEvent.of(taskExecutionRunnable));
            return;
        }
        log.info("The task[id={}] is submitted and already dispatched, cannot kill, will kill it after 5s",
                taskExecutionRunnable.getId());
        taskExecutionRunnable.getWorkflowEventBus()
                .publish(TaskKillLifecycleEvent.of(taskExecutionRunnable, TimeUnit.SECONDS.toMillis(5)));
    }

    @Override
    public void onKilledEvent(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                              final ITaskExecutionRunnable taskExecutionRunnable,
                              final TaskKilledLifecycleEvent taskKilledEvent) {
        throwExceptionIfStateIsNotMatch(taskExecutionRunnable);
        super.onKilledEvent(workflowExecutionRunnable, taskExecutionRunnable, taskKilledEvent);
    }

    @Override
    public void onFailedEvent(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                              final ITaskExecutionRunnable taskExecutionRunnable,
                              final TaskFailedLifecycleEvent taskFailedEvent) {
        throwExceptionIfStateIsNotMatch(taskExecutionRunnable);
        super.onFailedEvent(workflowExecutionRunnable, taskExecutionRunnable, taskFailedEvent);
    }

    @Override
    public void onSucceedEvent(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                               final ITaskExecutionRunnable taskExecutionRunnable,
                               final TaskSuccessLifecycleEvent taskSuccessEvent) {
        throwExceptionIfStateIsNotMatch(taskExecutionRunnable);
        logWarningIfCannotDoAction(taskExecutionRunnable, taskSuccessEvent);
    }

    @Override
    public void onFailoverEvent(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                                final ITaskExecutionRunnable taskExecutionRunnable,
                                final TaskFailoverLifecycleEvent taskFailoverEvent) {
        throwExceptionIfStateIsNotMatch(taskExecutionRunnable);
        logWarningIfCannotDoAction(taskExecutionRunnable, taskFailoverEvent);
    }

    @Override
    public TaskExecutionStatus matchState() {
        return TaskExecutionStatus.SUBMITTED_SUCCESS;
    }

}
