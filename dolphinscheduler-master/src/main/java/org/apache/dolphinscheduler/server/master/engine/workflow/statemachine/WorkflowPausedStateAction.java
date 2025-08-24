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
 * 工作流"已暂停"状态操作类
 * 
 * 这个类处理工作流处于"已暂停"(PAUSED)状态时的各种操作和状态转换。
 * 
 * 状态说明：
 * - PAUSED状态表示工作流已经成功暂停，所有运行中的任务都已停止
 * - 这是一种中间状态，工作流可以从此状态恢复执行
 * - 在此状态下，大部分操作都不允许执行，只能等待恢复或停止
 * 
 * 允许的状态转换：
 * - PAUSED → RUNNING：恢复工作流执行（用户手动恢复）
 * - PAUSED → STOP：停止暂停的工作流
 * - PAUSED → FINALIZED：执行最终清理
 * 
 * 不允许的操作：
 * - 不能再次暂停、直接成功或失败
 * 
 * 简单理解：就像一个"暂停的项目"，所有工作都临时停止，
 * 可以选择继续执行或彻底停止。
 */
@Slf4j
@Component
public class WorkflowPausedStateAction extends AbstractWorkflowStateAction {

    /**
     * 处理工作流启动事件
     * 
     * 在PAUSED状态下，工作流不应该接收新的启动事件。
     * 如果要恢复执行，应该通过恢复操作而不是启动操作。
     * 
     * @param workflowExecutionRunnable 工作流执行对象
     * @param workflowStartEvent 工作流启动事件
     */
    @Override
    public void onStartEvent(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                             final WorkflowStartLifecycleEvent workflowStartEvent) {
        throwExceptionIfStateIsNotMatch(workflowExecutionRunnable);
        logWarningIfCannotDoAction(workflowExecutionRunnable, workflowStartEvent);
    }

    /**
     * 处理工作流拓扑逻辑转换事件
     * 
     * 在PAUSED状态下，工作流已暂停，不应该有任务完成事件。
     * 所有任务都应该已经停止。
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
     * 在PAUSED状态下，工作流已经暂停，不能再次暂停。
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

    /**
     * 处理工作流停止事件
     * 
     * 在PAUSED状态下，工作流已经暂停，不需要执行停止操作。
     * 用户可以直接取消暂停的工作流。
     * 
     * @param workflowExecutionRunnable 工作流执行对象
     * @param workflowStopEvent 工作流停止事件
     */
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

    /**
     * 处理工作流最终化事件
     * 
     * 在PAUSED状态下，可以执行最终化操作，清理暂停的工作流。
     * 这通常发生在用户决定不再恢复执行，直接清理资源时。
     * 
     * @param workflowExecutionRunnable 工作流执行对象
     * @param workflowFinalizeEvent 工作流最终化事件
     */
    @Override
    public void onFinalizeEvent(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                                final WorkflowFinalizeLifecycleEvent workflowFinalizeEvent) {
        throwExceptionIfStateIsNotMatch(workflowExecutionRunnable);
        // 执行最终化清理操作
        super.finalizeEventAction(workflowExecutionRunnable);
    }

    /**
     * 返回该状态操作类匹配的工作流状态
     * 
     * @return 工作流暂停状态
     */
    @Override
    public WorkflowExecutionStatus matchState() {
        return WorkflowExecutionStatus.PAUSE;
    }

    /**
     * 暂停状态不能发出工作流完成事件
     * 
     * 因为工作流处于暂停状态，还没有最终完成，
     * 不应该发出完成事件。
     */
    @Override
    protected void emitWorkflowFinishedEventIfApplicable(IWorkflowExecutionRunnable workflowExecutionRunnable) {
        throw new IllegalStateException(
                "The workflow " + workflowExecutionRunnable.getName() +
                        "is paused, shouldn't emit workflow finished event");
    }
}
