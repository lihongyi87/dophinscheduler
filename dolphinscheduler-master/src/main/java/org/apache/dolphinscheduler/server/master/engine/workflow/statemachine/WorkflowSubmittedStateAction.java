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
 * 工作流“已提交”状态操作类
 * 
 * 这个类处理工作流处于“已提交”(SUBMITTED)状态时的各种操作和状态转换。
 * 
 * 状态说明：
 * - SUBMITTED状态表示工作流已经被提交到系统中，但还没有开始执行
 * - 这是工作流的初始状态，等待调度器分配资源并开始执行
 * 
 * 允许的状态转换：
 * - SUBMITTED → RUNNING：开始执行工作流
 * - SUBMITTED → STOP：用户取消工作流
 * 
 * 简单理解：就像一个“已提交但还没开始处理的工单”，
 * 只能等待开始处理或者被取消。
 */
@Slf4j
@Component
public class WorkflowSubmittedStateAction extends AbstractWorkflowStateAction {

    /**
     * 处理工作流启动事件
     * 
     * 在SUBMITTED状态下，工作流不应该又接收到启动事件。
     * 这通常表示系统出现了重复处理或者状态不一致的问题。
     * 
     * @param workflowExecutionRunnable 工作流执行对象
     * @param workflowStartEvent 工作流启动事件
     */
    @Override
    public void onStartEvent(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                             final WorkflowStartLifecycleEvent workflowStartEvent) {
        // 检查状态是否匹配，不匹配则抛出异常
        throwExceptionIfStateIsNotMatch(workflowExecutionRunnable);
        // 记录警告日志，提示这个操作不应该在当前状态下执行
        logWarningIfCannotDoAction(workflowExecutionRunnable, workflowStartEvent);
    }

    /**
     * 处理工作流拓扑逻辑转换事件
     * 
     * 在SUBMITTED状态下，工作流还没有开始执行，不应该有任务完成事件。
     * 这通常表示系统状态出现了问题。
     * 
     * @param workflowExecutionRunnable 工作流执行对象
     * @param workflowTopologyLogicalTransitionWithTaskFinishEvent 任务完成转换事件
     */
    @Override
    public void onTopologyLogicalTransitionEvent(
                                                 final IWorkflowExecutionRunnable workflowExecutionRunnable,
                                                 final WorkflowTopologyLogicalTransitionWithTaskFinishLifecycleEvent workflowTopologyLogicalTransitionWithTaskFinishEvent) {
        throwExceptionIfStateIsNotMatch(workflowExecutionRunnable);
        logWarningIfCannotDoAction(workflowExecutionRunnable, workflowTopologyLogicalTransitionWithTaskFinishEvent);
    }

    /**
     * 处理工作流暂停事件
     * 
     * 在SUBMITTED状态下，工作流还没有开始执行，不需要暂停操作。
     * 用户可以直接取消工作流而不需要暂停。
     * 
     * @param workflowExecutionRunnable 工作流执行对象
     * @param workflowPauseEvent 工作流暂停事件
     */
    @Override
    public void onPauseEvent(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                             final WorkflowPauseLifecycleEvent workflowPauseEvent) {
        throwExceptionIfStateIsNotMatch(workflowExecutionRunnable);
        logWarningIfCannotDoAction(workflowExecutionRunnable, workflowPauseEvent);
    }

    @Override
    public void onPausedEvent(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                              final WorkflowPausedLifecycleEvent workflowPausedEvent) {
        throwExceptionIfStateIsNotMatch(workflowExecutionRunnable);
        logWarningIfCannotDoAction(workflowExecutionRunnable, workflowPausedEvent);
    }

    @Override
    public void onStopEvent(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                            final WorkflowStopLifecycleEvent workflowStopEvent) {
        throwExceptionIfStateIsNotMatch(workflowExecutionRunnable);
        logWarningIfCannotDoAction(workflowExecutionRunnable, workflowStopEvent);
    }

    @Override
    public void onStoppedEvent(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                               final WorkflowStoppedLifecycleEvent workflowStoppedEvent) {
        throwExceptionIfStateIsNotMatch(workflowExecutionRunnable);
        logWarningIfCannotDoAction(workflowExecutionRunnable, workflowStoppedEvent);
    }

    @Override
    public void onSucceedEvent(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                               final WorkflowSucceedLifecycleEvent workflowSucceedEvent) {
        throwExceptionIfStateIsNotMatch(workflowExecutionRunnable);
        logWarningIfCannotDoAction(workflowExecutionRunnable, workflowSucceedEvent);
    }

    @Override
    public void onFailedEvent(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                              final WorkflowFailedLifecycleEvent workflowFailedEvent) {
        throwExceptionIfStateIsNotMatch(workflowExecutionRunnable);
        logWarningIfCannotDoAction(workflowExecutionRunnable, workflowFailedEvent);
    }

    @Override
    public void onFinalizeEvent(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                                final WorkflowFinalizeLifecycleEvent workflowFinalizeEvent) {
        throwExceptionIfStateIsNotMatch(workflowExecutionRunnable);
        logWarningIfCannotDoAction(workflowExecutionRunnable, workflowFinalizeEvent);
    }

    @Override
    public WorkflowExecutionStatus matchState() {
        return WorkflowExecutionStatus.SUBMITTED_SUCCESS;
    }

    /**
     * The running state can only finish with success/failure.
     */
    @Override
    protected void emitWorkflowFinishedEventIfApplicable(IWorkflowExecutionRunnable workflowExecutionRunnable) {
        throw new IllegalStateException(
                "The workflow " + workflowExecutionRunnable.getName() +
                        "is submitted, shouldn't emit workflow finished event");
    }
}
