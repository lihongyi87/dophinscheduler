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

package org.apache.dolphinscheduler.server.master.rpc;

import org.apache.dolphinscheduler.extract.master.ITaskExecutorEventListener;
import org.apache.dolphinscheduler.server.master.engine.IWorkflowRepository;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event.TaskDispatchedLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event.TaskFailedLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event.TaskKilledLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event.TaskPausedLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event.TaskRunningLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event.TaskRuntimeContextChangedEvent;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event.TaskSuccessLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.task.runnable.ITaskExecutionRunnable;
import org.apache.dolphinscheduler.server.master.engine.workflow.runnable.IWorkflowExecutionRunnable;
import org.apache.dolphinscheduler.task.executor.events.IReportableTaskExecutorLifecycleEvent;
import org.apache.dolphinscheduler.task.executor.events.TaskExecutorDispatchedLifecycleEvent;
import org.apache.dolphinscheduler.task.executor.events.TaskExecutorFailedLifecycleEvent;
import org.apache.dolphinscheduler.task.executor.events.TaskExecutorKilledLifecycleEvent;
import org.apache.dolphinscheduler.task.executor.events.TaskExecutorPausedLifecycleEvent;
import org.apache.dolphinscheduler.task.executor.events.TaskExecutorRuntimeContextChangedLifecycleEvent;
import org.apache.dolphinscheduler.task.executor.events.TaskExecutorStartedLifecycleEvent;
import org.apache.dolphinscheduler.task.executor.events.TaskExecutorSuccessLifecycleEvent;

import java.util.Date;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * 任务执行器事件监听器实现类
 *
 * <p>该类是Master节点中负责监听和处理任务执行器生命周期事件的核心组件。
 * 通过RPC接口接收来自TaskExecutor的各种生命周期事件，并将其转换为
 * Master内部的任务生命周期事件，然后发布到工作流事件总线中。</p>
 *
 * <p>该类扮演了事件适配器的角色，负责：</p>
 * <ul>
 *   <li>接收来自远程TaskExecutor的生命周期事件</li>
 *   <li>将外部事件转换为内部事件格式</li>
 *   <li>通过事件总线发布到工作流执行引擎</li>
 *   <li>维护任务执行状态的一致性</li>
 * </ul>
 *
 * <p>支持的任务生命周期事件包括：</p>
 * <ul>
 *   <li>任务分发事件 - 任务已被分发到执行器</li>
 *   <li>任务运行事件 - 任务开始执行</li>
 *   <li>任务运行时上下文变更事件 - 任务运行时信息发生变化</li>
 *   <li>任务成功事件 - 任务执行成功</li>
 *   <li>任务失败事件 - 任务执行失败</li>
 *   <li>任务终止事件 - 任务被强制终止</li>
 *   <li>任务暂停事件 - 任务被暂停</li>
 * </ul>
 *
 * @author DolphinScheduler Community
 * @see ITaskExecutorEventListener
 * @see IWorkflowRepository
 */
@Slf4j
@Service
public class TaskExecutorEventListenerImpl implements ITaskExecutorEventListener {

    /**
     * 工作流仓库
     * 用于根据工作流实例ID获取工作流执行运行时对象
     */
    @Autowired
    private IWorkflowRepository workflowRepository;

    /**
     * 处理任务执行器分发事件
     *
     * <p>当任务成功分发到TaskExecutor时触发该事件。
     * 该方法将外部的TaskExecutorDispatchedLifecycleEvent转换为内部的
     * TaskDispatchedLifecycleEvent，并通过工作流事件总线发布。</p>
     *
     * <p>处理流程：</p>
     * <ol>
     *   <li>根据事件信息获取任务执行运行时对象</li>
     *   <li>构建内部的任务分发生命周期事件</li>
     *   <li>设置执行器主机信息</li>
     *   <li>通过工作流事件总线发布事件</li>
     * </ol>
     *
     * @param taskExecutorDispatchedLifecycleEvent 任务执行器分发生命周期事件
     */
    @Override
    public void onTaskExecutorDispatched(final TaskExecutorDispatchedLifecycleEvent taskExecutorDispatchedLifecycleEvent) {
        final ITaskExecutionRunnable taskExecutionRunnable =
                getTaskExecutionRunnable(taskExecutorDispatchedLifecycleEvent);
        final TaskDispatchedLifecycleEvent taskDispatchedLifecycleEvent = TaskDispatchedLifecycleEvent.builder()
                .taskExecutionRunnable(taskExecutionRunnable)
                .executorHost(taskExecutorDispatchedLifecycleEvent.getTaskInstanceHost())
                .build();

        taskExecutionRunnable.getWorkflowEventBus().publish(taskDispatchedLifecycleEvent);
    }

    /**
     * 处理任务执行器运行事件
     *
     * <p>当任务在TaskExecutor中开始执行时触发该事件。
     * 该方法将外部的TaskExecutorStartedLifecycleEvent转换为内部的
     * TaskRunningLifecycleEvent，并记录任务的开始时间和日志路径。</p>
     *
     * <p>处理流程：</p>
     * <ol>
     *   <li>根据事件信息获取任务执行运行时对象</li>
     *   <li>构建内部的任务运行生命周期事件</li>
     *   <li>设置任务开始时间和日志路径</li>
     *   <li>通过工作流事件总线发布事件</li>
     * </ol>
     *
     * @param taskExecutorStartedLifecycleEvent 任务执行器开始生命周期事件
     */
    @Override
    public void onTaskExecutorRunning(final TaskExecutorStartedLifecycleEvent taskExecutorStartedLifecycleEvent) {
        final ITaskExecutionRunnable taskExecutionRunnable =
                getTaskExecutionRunnable(taskExecutorStartedLifecycleEvent);
        final TaskRunningLifecycleEvent taskRunningEvent = TaskRunningLifecycleEvent.builder()
                .taskExecutionRunnable(taskExecutionRunnable)
                .startTime(new Date(taskExecutorStartedLifecycleEvent.getStartTime()))
                .logPath(taskExecutorStartedLifecycleEvent.getLogPath())
                .build();

        taskExecutionRunnable.getWorkflowEventBus().publish(taskRunningEvent);
    }

    /**
     * 处理任务执行器运行时上下文变更事件
     *
     * <p>当任务在执行过程中运行时上下文信息发生变化时触发该事件。
     * 运行时上下文包括但不限于应用ID、进程信息等。</p>
     *
     * <p>该事件对于跟踪任务的执行状态和资源使用情况非常重要，
     * 可以帮助Master监控任务的实时执行状态。</p>
     *
     * <p>处理流程：</p>
     * <ol>
     *   <li>根据事件信息获取任务执行运行时对象</li>
     *   <li>构建内部的任务运行时上下文变更事件</li>
     *   <li>设置新的运行时上下文信息</li>
     *   <li>通过工作流事件总线发布事件</li>
     * </ol>
     *
     * @param taskExecutorRuntimeContextChangedLifecycleEventr 任务执行器运行时上下文变更生命周期事件
     */
    @Override
    public void onTaskExecutorRuntimeContextChanged(final TaskExecutorRuntimeContextChangedLifecycleEvent taskExecutorRuntimeContextChangedLifecycleEventr) {
        final ITaskExecutionRunnable taskExecutionRunnable =
                getTaskExecutionRunnable(taskExecutorRuntimeContextChangedLifecycleEventr);

        final TaskRuntimeContextChangedEvent taskRuntimeContextChangedEvent = TaskRuntimeContextChangedEvent.builder()
                .taskExecutionRunnable(taskExecutionRunnable)
                .runtimeContext(taskExecutorRuntimeContextChangedLifecycleEventr.getAppIds())
                .build();

        taskExecutionRunnable.getWorkflowEventBus().publish(taskRuntimeContextChangedEvent);
    }

    /**
     * 处理任务执行器成功事件
     *
     * <p>当任务在TaskExecutor中成功执行完成时触发该事件。
     * 该方法将外部的TaskExecutorSuccessLifecycleEvent转换为内部的
     * TaskSuccessLifecycleEvent，并记录任务的结束时间和变量池。</p>
     *
     * <p>处理流程：</p>
     * <ol>
     *   <li>根据事件信息获取任务执行运行时对象</li>
     *   <li>构建内部的任务成功生命周期事件</li>
     *   <li>设置任务结束时间和变量池</li>
     *   <li>通过工作流事件总线发布事件</li>
     * </ol>
     *
     * @param taskExecutorSuccessLifecycleEvent 任务执行器成功生命周期事件
     */
    @Override
    public void onTaskExecutorSuccess(final TaskExecutorSuccessLifecycleEvent taskExecutorSuccessLifecycleEvent) {
        final ITaskExecutionRunnable taskExecutionRunnable =
                getTaskExecutionRunnable(taskExecutorSuccessLifecycleEvent);
        final TaskSuccessLifecycleEvent taskSuccessEvent = TaskSuccessLifecycleEvent.builder()
                .taskExecutionRunnable(taskExecutionRunnable)
                .endTime(new Date(taskExecutorSuccessLifecycleEvent.getEndTime()))
                .varPool(taskExecutorSuccessLifecycleEvent.getVarPool())
                .build();
        taskExecutionRunnable.getWorkflowEventBus().publish(taskSuccessEvent);
    }

    /**
     * 处理任务执行器失败事件
     *
     * <p>当任务在TaskExecutor中执行失败时触发该事件。
     * 该方法将外部的TaskExecutorFailedLifecycleEvent转换为内部的
     * TaskFailedLifecycleEvent，并记录任务的结束时间。</p>
     *
     * <p>处理流程：</p>
     * <ol>
     *   <li>根据事件信息获取任务执行运行时对象</li>
     *   <li>构建内部的任务失败生命周期事件</li>
     *   <li>设置任务结束时间</li>
     *   <li>通过工作流事件总线发布事件</li>
     * </ol>
     *
     * @param taskExecutorFailedLifecycleEvent 任务执行器失败生命周期事件
     */
    @Override
    public void onTaskExecutorFailed(final TaskExecutorFailedLifecycleEvent taskExecutorFailedLifecycleEvent) {
        final ITaskExecutionRunnable taskExecutionRunnable = getTaskExecutionRunnable(taskExecutorFailedLifecycleEvent);
        final TaskFailedLifecycleEvent taskFailedEvent = TaskFailedLifecycleEvent.builder()
                .taskExecutionRunnable(taskExecutionRunnable)
                .endTime(new Date(taskExecutorFailedLifecycleEvent.getEndTime()))
                .build();
        taskExecutionRunnable.getWorkflowEventBus().publish(taskFailedEvent);
    }

    /**
     * 处理任务执行器终止事件
     *
     * <p>当任务在TaskExecutor中被强制终止时触发该事件。
     * 该方法将外部的TaskExecutorKilledLifecycleEvent转换为内部的
     * TaskKilledLifecycleEvent，并记录任务的终止时间。</p>
     *
     * <p>任务被终止的原因可能包括：</p>
     * <ul>
     *   <li>用户主动取消任务</li>
     *   <li>任务执行超时</li>
     *   <li>系统资源不足</li>
     *   <li>工作流被终止</li>
     * </ul>
     *
     * <p>处理流程：</p>
     * <ol>
     *   <li>根据事件信息获取任务执行运行时对象</li>
     *   <li>构建内部的任务终止生命周期事件</li>
     *   <li>设置任务终止时间</li>
     *   <li>通过工作流事件总线发布事件</li>
     * </ol>
     *
     * @param taskExecutorKilledLifecycleEvent 任务执行器终止生命周期事件
     */
    @Override
    public void onTaskExecutorKilled(final TaskExecutorKilledLifecycleEvent taskExecutorKilledLifecycleEvent) {
        final ITaskExecutionRunnable taskExecutionRunnable = getTaskExecutionRunnable(taskExecutorKilledLifecycleEvent);
        final TaskKilledLifecycleEvent taskKilledEvent = TaskKilledLifecycleEvent.builder()
                .taskExecutionRunnable(taskExecutionRunnable)
                .endTime(new Date(taskExecutorKilledLifecycleEvent.getEndTime()))
                .build();
        taskExecutionRunnable.getWorkflowEventBus().publish(taskKilledEvent);
    }

    /**
     * 处理任务执行器暂停事件
     *
     * <p>当任务在TaskExecutor中被暂停时触发该事件。
     * 该方法将外部的TaskExecutorPausedLifecycleEvent转换为内部的
     * TaskPausedLifecycleEvent。</p>
     *
     * <p>与终止不同，暂停操作是可逆的，任务可以从暂停状态恢复执行。
     * 这对于长时间运行的任务或需要人工干预的任务非常有用。</p>
     *
     * <p>处理流程：</p>
     * <ol>
     *   <li>根据事件信息获取任务执行运行时对象</li>
     *   <li>构建内部的任务暂停生命周期事件</li>
     *   <li>通过工作流事件总线发布事件</li>
     * </ol>
     *
     * @param taskExecutorPausedLifecycleEvent 任务执行器暂停生命周期事件
     */
    @Override
    public void onTaskExecutorPaused(final TaskExecutorPausedLifecycleEvent taskExecutorPausedLifecycleEvent) {
        final ITaskExecutionRunnable taskExecutionRunnable = getTaskExecutionRunnable(taskExecutorPausedLifecycleEvent);
        final TaskPausedLifecycleEvent taskPausedEvent = TaskPausedLifecycleEvent.of(taskExecutionRunnable);
        taskExecutionRunnable.getWorkflowEventBus().publish(taskPausedEvent);
    }

    /**
     * 根据事件获取任务执行运行时对象
     *
     * <p>该方法是一个工具方法，用于从任务执行器生命周期事件中
     * 提取必要的标识信息，并查找对应的任务执行运行时对象。</p>
     *
     * <p>查找流程：</p>
     * <ol>
     *   <li>从事件中提取工作流实例ID和任务实例ID</li>
     *   <li>通过工作流仓库查找工作流执行运行时对象</li>
     *   <li>从工作流执行图中查找任务执行运行时对象</li>
     *   <li>返回找到的任务执行运行时对象</li>
     * </ol>
     *
     * @param reportableTaskExecutorLifecycleEvent 可报告的任务执行器生命周期事件
     * @return ITaskExecutionRunnable 任务执行运行时对象
     * @throws IllegalArgumentException 当找不到对应的工作流或任务时抛出
     */
    private ITaskExecutionRunnable getTaskExecutionRunnable(final IReportableTaskExecutorLifecycleEvent reportableTaskExecutorLifecycleEvent) {
        final int workflowInstanceId = reportableTaskExecutorLifecycleEvent.getWorkflowInstanceId();
        final int taskInstanceId = reportableTaskExecutorLifecycleEvent.getTaskInstanceId();

        final IWorkflowExecutionRunnable workflowExecutionRunnable = workflowRepository.get(workflowInstanceId);
        if (workflowExecutionRunnable == null) {
            throw new IllegalArgumentException("Cannot find the WorkflowExecuteRunnable: " + workflowInstanceId);
        }
        final ITaskExecutionRunnable taskExecutionRunnable = workflowExecutionRunnable.getWorkflowExecuteContext()
                .getWorkflowExecutionGraph()
                .getTaskExecutionRunnableById(taskInstanceId);
        if (taskExecutionRunnable == null) {
            throw new IllegalArgumentException("Cannot find the TaskExecuteRunnable: " + taskInstanceId);
        }
        return taskExecutionRunnable;
    }

}
