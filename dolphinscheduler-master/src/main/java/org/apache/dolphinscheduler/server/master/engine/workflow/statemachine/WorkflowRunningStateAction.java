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

package org.apache.dolphinscheduler.server.master.engine.workflow.statemachine;

import org.apache.dolphinscheduler.common.enums.WorkflowExecutionStatus;
import org.apache.dolphinscheduler.server.master.engine.WorkflowEventBus;
import org.apache.dolphinscheduler.server.master.engine.graph.IWorkflowExecutionGraph;
import org.apache.dolphinscheduler.server.master.engine.workflow.lifecycle.event.WorkflowFailedLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.workflow.lifecycle.event.WorkflowFinalizeLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.workflow.lifecycle.event.WorkflowPauseLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.workflow.lifecycle.event.WorkflowPausedLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.workflow.lifecycle.event.WorkflowStartLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.workflow.lifecycle.event.WorkflowStopLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.workflow.lifecycle.event.WorkflowStoppedLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.workflow.lifecycle.event.WorkflowSucceedLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.workflow.lifecycle.event.WorkflowTopologyLogicalTransitionWithTaskFinishLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.workflow.runnable.IWorkflowExecutionRunnable;

import lombok.extern.slf4j.Slf4j;

import org.springframework.stereotype.Component;

/**
 * 工作流“正在运行”状态操作类
 * 
 * 这个类处理工作流处于“正在运行”(RUNNING)状态时的各种操作和状态转换。
 * 
 * 状态说明：
 * - RUNNING状态表示工作流正在正常执行中，任务正在按照DAG的拓扑顺序执行
 * - 这是工作流的正常执行状态，大部分生命周期事件都在这个状态下处理
 * 
 * 允许的状态转换：
 * - RUNNING → SUCCESS：所有任务成功完成
 * - RUNNING → FAILED：关键任务失败
 * - RUNNING → READY_PAUSE：用户请求暂停
 * - RUNNING → READY_STOP：用户请求停止
 * 
 * 简单理解：就像一个“正在进行中的项目”，可以继续执行、暂停或停止。
 */
@Slf4j
@Component
public class WorkflowRunningStateAction extends AbstractWorkflowStateAction {

    /**
     * 处理工作流启动事件
     * 
     * 在RUNNING状态下接收到启动事件意味着工作流正式开始执行。
     * 系统会触发DAG中所有初始节点（没有前置依赖的任务）的执行。
     * 
     * @param workflowExecutionRunnable 工作流执行对象
     * @param workflowStartEvent 工作流启动事件
     */
    @Override
    public void onStartEvent(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                             final WorkflowStartLifecycleEvent workflowStartEvent) {
        // 检查状态是否匹配
        throwExceptionIfStateIsNotMatch(workflowExecutionRunnable);
        
        // 获取工作流执行图（DAG拓扑结构）
        final IWorkflowExecutionGraph workflowExecutionGraph =
                workflowExecutionRunnable.getWorkflowExecuteContext().getWorkflowExecutionGraph();
        
        // 触发所有起始节点的执行（没有前置依赖的任务）
        triggerTasks(workflowExecutionRunnable, workflowExecutionGraph.getStartNodes());
    }

    /**
     * 处理工作流拓扑逻辑转换事件
     * 
     * 当某个任务完成时，需要检查并触发其后继任务的执行。
     * 这是DAG工作流执行的核心逻辑，确保任务按照正确的依赖关系执行。
     * 
     * @param workflowExecutionRunnable 工作流执行对象
     * @param workflowTopologyLogicalTransitionWithTaskFinishEvent 任务完成转换事件
     */
    @Override
    public void onTopologyLogicalTransitionEvent(
                                                 final IWorkflowExecutionRunnable workflowExecutionRunnable,
                                                 final WorkflowTopologyLogicalTransitionWithTaskFinishLifecycleEvent workflowTopologyLogicalTransitionWithTaskFinishEvent) {
        throwExceptionIfStateIsNotMatch(workflowExecutionRunnable);
        
        // 尝试触发已完成任务的后继任务
        // 检查后继任务的所有前置依赖是否都已完成
        super.tryToTriggerSuccessorsAfterTaskFinish(workflowExecutionRunnable,
                workflowTopologyLogicalTransitionWithTaskFinishEvent.getTaskExecutionRunnable());
    }

    /**
     * 处理工作流暂停事件
     * 
     * 当用户请求暂停正在运行的工作流时，系统会：
     * 1. 将工作流状态转换为READY_PAUSE（准备暂停）
     * 2. 暂停所有正在执行的任务
     * 3. 等待任务暂停完成后转为PAUSED状态
     * 
     * @param workflowExecutionRunnable 工作流执行对象
     * @param workflowPauseEvent 工作流暂停事件
     */
    @Override
    public void onPauseEvent(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                             final WorkflowPauseLifecycleEvent workflowPauseEvent) {
        throwExceptionIfStateIsNotMatch(workflowExecutionRunnable);
        
        // 将工作流状态转换为“准备暂停”
        super.transformWorkflowInstanceState(workflowExecutionRunnable, WorkflowExecutionStatus.READY_PAUSE);
        
        // 暂停所有正在执行的活跃任务
        super.pauseActiveTask(workflowExecutionRunnable);
    }

    @Override
    public void onPausedEvent(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                              final WorkflowPausedLifecycleEvent workflowPausedEvent) {
        throwExceptionIfStateIsNotMatch(workflowExecutionRunnable);
        logWarningIfCannotDoAction(workflowExecutionRunnable, workflowPausedEvent);
    }

    /**
     * 处理工作流停止事件
     * 
     * 当用户请求停止正在运行的工作流时，系统会：
     * 1. 将工作流状态转换为READY_STOP（准备停止）
     * 2. 杀死所有正在执行的任务
     * 3. 等待任务停止完成后转为STOPPED状态
     * 
     * @param workflowExecutionRunnable 工作流执行对象
     * @param workflowStopEvent 工作流停止事件
     */
    @Override
    public void onStopEvent(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                            final WorkflowStopLifecycleEvent workflowStopEvent) {
        throwExceptionIfStateIsNotMatch(workflowExecutionRunnable);
        
        // 将工作流状态转换为“准备停止”
        super.transformWorkflowInstanceState(workflowExecutionRunnable, WorkflowExecutionStatus.READY_STOP);
        
        // 杀死所有正在执行的活跃任务
        super.killActiveTask(workflowExecutionRunnable);
    }

    @Override
    public void onStoppedEvent(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                               final WorkflowStoppedLifecycleEvent workflowStoppedEvent) {
        throwExceptionIfStateIsNotMatch(workflowExecutionRunnable);
        // [Fix-17354]
        if (!workflowExecutionRunnable.getWorkflowExecutionGraph().isExistKilledTaskExecutionRunnableChain()) {
            throw new IllegalStateException(
                    "The workflow: " + workflowExecutionRunnable.getName()
                            + " does not exist tasks chain which is killed");
        }
        super.workflowFinish(workflowExecutionRunnable, WorkflowExecutionStatus.STOP);
    }

    @Override
    public void onSucceedEvent(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                               final WorkflowSucceedLifecycleEvent workflowSucceedEvent) {
        throwExceptionIfStateIsNotMatch(workflowExecutionRunnable);
        final IWorkflowExecutionGraph workflowExecutionGraph = workflowExecutionRunnable.getWorkflowExecutionGraph();
        if (!workflowExecutionGraph.isAllTaskExecutionRunnableChainSuccess()) {
            throw new IllegalStateException(
                    "The workflow: " + workflowExecutionRunnable.getName() + "exist tasks chain which is not success");
        }
        workflowFinish(workflowExecutionRunnable, WorkflowExecutionStatus.SUCCESS);
    }

    @Override
    public void onFailedEvent(IWorkflowExecutionRunnable workflowExecutionRunnable,
                              WorkflowFailedLifecycleEvent workflowFailedEvent) {
        throwExceptionIfStateIsNotMatch(workflowExecutionRunnable);
        final IWorkflowExecutionGraph workflowExecutionGraph = workflowExecutionRunnable.getWorkflowExecutionGraph();
        if (!workflowExecutionGraph.isExistFailureTaskExecutionRunnableChain()) {
            throw new IllegalStateException(
                    "The workflow: " + workflowExecutionRunnable.getName()
                            + " does not exist tasks chain which is failed");
        }
        workflowFinish(workflowExecutionRunnable, WorkflowExecutionStatus.FAILURE);
    }

    @Override
    public void onFinalizeEvent(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                                final WorkflowFinalizeLifecycleEvent workflowFinalizeEvent) {
        throwExceptionIfStateIsNotMatch(workflowExecutionRunnable);
        logWarningIfCannotDoAction(workflowExecutionRunnable, workflowFinalizeEvent);
    }

    @Override
    public WorkflowExecutionStatus matchState() {
        return WorkflowExecutionStatus.RUNNING_EXECUTION;
    }

    /**
     * The running state can only finish with success/failure.
     */
    @Override
    protected void emitWorkflowFinishedEventIfApplicable(IWorkflowExecutionRunnable workflowExecutionRunnable) {
        final IWorkflowExecutionGraph workflowExecutionGraph =
                workflowExecutionRunnable.getWorkflowExecuteContext().getWorkflowExecutionGraph();
        if (!workflowExecutionGraph.isAllTaskExecutionRunnableChainFinish()) {
            log.debug("There exist task which is not finish, don't need to emit workflow finished event");
            return;
        }

        final WorkflowEventBus workflowEventBus = workflowExecutionRunnable.getWorkflowEventBus();
        if (workflowExecutionGraph.isExistFailureTaskExecutionRunnableChain()) {
            workflowEventBus.publish(WorkflowFailedLifecycleEvent.of(workflowExecutionRunnable));
            return;
        }

        // [Fix-17354]
        // If there exist tasks which has set timeout failed, then will publish a kill event to kill the task.
        // So there might exist task which is killed, and the workflow instance state is running.
        // This is a special case, the workflow instance can transform from running to stop state.
        // Is there better way to handle this case?
        if (workflowExecutionGraph.isExistKilledTaskExecutionRunnableChain()) {
            workflowEventBus.publish(WorkflowStoppedLifecycleEvent.of(workflowExecutionRunnable));
            return;
        }

        if (workflowExecutionGraph.isAllTaskExecutionRunnableChainSuccess()) {
            workflowEventBus.publish(WorkflowSucceedLifecycleEvent.of(workflowExecutionRunnable));
            return;
        }

        throw new IllegalStateException("The workflow: " + workflowExecutionRunnable.getName() +
                " state is " + workflowExecutionRunnable.getState()
                + " can only finish with task success/failed/killed but exist task which state is not success、failure、killed");
    }
}
