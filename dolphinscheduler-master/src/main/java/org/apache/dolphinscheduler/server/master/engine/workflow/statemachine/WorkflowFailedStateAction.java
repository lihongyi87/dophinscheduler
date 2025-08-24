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
 * 工作流"已失败"状态操作类
 * 
 * 这个类处理工作流处于"已失败"(FAILED)状态时的各种操作和状态转换。
 * 
 * 状态说明：
 * - FAILED状态表示工作流因关键任务失败而导致整个工作流失败
 * - 这是工作流的一种终态，表示执行已结束且结果为失败
 * - 在此状态下，大部分操作都不再允许执行
 * 
 * 允许的操作：
 * - FAILED → FINALIZED：执行最终清理和资源释放
 * 
 * 不允许的操作：
 * - 不能重新启动、暂停、停止或转换为成功状态
 * 
 * 简单理解：就像一个"已经失败的项目"，只能做最后的清理工作，
 * 不能再进行其他操作。
 */
@Slf4j
@Component
public class WorkflowFailedStateAction extends AbstractWorkflowStateAction {

    /**
     * 处理工作流启动事件
     * 
     * 在FAILED状态下，工作流不应该接收启动事件。
     * 失败的工作流不能重新启动，需要重新提交一个新的工作流实例。
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
     * 在FAILED状态下，工作流已经失败，不应该再有任务完成事件。
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
     * 在FAILED状态下，工作流已经失败，不能执行暂停操作。
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
     * 在FAILED状态下，工作流已经失败，不需要再执行停止操作。
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
     * 在FAILED状态下，可以执行最终化操作，进行资源清理和数据清理。
     * 这是失败工作流的最后一步操作。
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
     * @return 工作流失败状态
     */
    @Override
    public WorkflowExecutionStatus matchState() {
        return WorkflowExecutionStatus.FAILURE;
    }

    /**
     * 失败状态不能发出工作流完成事件
     * 
     * 因为工作流已经处于失败状态，不应该再发出完成事件。
     */
    @Override
    protected void emitWorkflowFinishedEventIfApplicable(IWorkflowExecutionRunnable workflowExecutionRunnable) {
        throw new IllegalStateException(
                "The workflow " + workflowExecutionRunnable.getName() +
                        "is failed, shouldn't emit workflow finished event");
    }
}
