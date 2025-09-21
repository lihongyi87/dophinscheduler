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

/**
 * 工作流状态动作接口
 *
 * 表示当工作流处于特定状态并接收到目标事件时要执行的动作。
 * 这是工作流状态机模式的核心接口，定义了状态-事件-动作的映射关系。
 *
 * <p>设计理念：
 * - 状态机模式：每个工作流状态对应一个具体的状态动作实现
 * - 事件驱动：通过不同的生命周期事件触发相应的状态转换和动作执行
 * - 责任分离：每个状态的处理逻辑独立，便于维护和扩展
 *
 * <p>状态转换规则：
 * - 每个 {@link WorkflowExecutionStatus} 都应该有对应的 {@link IWorkflowStateAction} 实现
 * - 状态转换必须遵循预定义的有限状态机规则
 * - 非法的状态转换会抛出异常，确保工作流状态的一致性
 *
 * <p>生命周期事件处理：
 * - 启动事件：工作流开始执行，触发初始任务
 * - 拓扑转换事件：任务完成后触发后继任务的检查和执行
 * - 暂停/恢复事件：支持工作流的暂停和恢复操作
 * - 停止事件：支持工作流的强制停止操作
 * - 成功/失败事件：工作流执行结果的最终状态设置
 * - 终结事件：工作流生命周期结束，清理资源
 *
 * <p>简单理解：就像一个"智能客服系统"，根据当前状态（如：咨询中、处理中、已完成）
 * 和接收到的事件（如：新消息、超时、用户满意度反馈）来决定下一步要做什么。
 *
 * @see WorkflowSubmittedStateAction 已提交状态动作
 * @see WorkflowRunningStateAction 运行中状态动作
 * @see WorkflowReadyPauseStateAction 准备暂停状态动作
 * @see WorkflowPausedStateAction 已暂停状态动作
 * @see WorkflowReadyStopStateAction 准备停止状态动作
 * @see WorkflowStoppedStateAction 已停止状态动作
 * @see WorkflowSerialWaitStateAction 串行等待状态动作
 * @see WorkflowFailedStateAction 失败状态动作
 * @see WorkflowSuccessStateAction 成功状态动作
 * @see WorkflowFailoverStateAction 故障转移状态动作
 */
public interface IWorkflowStateAction {

    /**
     * Perform the necessary actions when the workflow in a certain state receive a {@link WorkflowStartLifecycleEvent}.
     */
    void onStartEvent(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                      final WorkflowStartLifecycleEvent workflowStartEvent);

    /**
     * Perform the necessary actions when the workflow in a certain state receive a {@link WorkflowTopologyLogicalTransitionWithTaskFinishLifecycleEvent}.
     */
    void onTopologyLogicalTransitionEvent(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                                          final WorkflowTopologyLogicalTransitionWithTaskFinishLifecycleEvent workflowTopologyLogicalTransitionWithTaskFinishEvent);

    /**
     * Perform the necessary actions when the workflow in a certain state receive a {@link WorkflowPauseLifecycleEvent}.
     */
    void onPauseEvent(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                      final WorkflowPauseLifecycleEvent workflowPauseEvent);

    /**
     * Perform the necessary actions when the workflow in a certain state receive a {@link WorkflowPausedLifecycleEvent}.
     */
    void onPausedEvent(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                       final WorkflowPausedLifecycleEvent workflowPausedEvent);

    /**
     * Perform the necessary actions when the workflow in a certain state receive a {@link WorkflowStopLifecycleEvent}.
     */
    void onStopEvent(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                     final WorkflowStopLifecycleEvent workflowStopEvent);

    /**
     * Perform the necessary actions when the workflow in a certain state receive a {@link WorkflowStoppedLifecycleEvent}.
     */
    void onStoppedEvent(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                        final WorkflowStoppedLifecycleEvent workflowStoppedEvent);

    /**
     * Perform the necessary actions when the workflow in a certain state receive a {@link WorkflowSucceedLifecycleEvent}.
     */
    void onSucceedEvent(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                        final WorkflowSucceedLifecycleEvent workflowSucceedEvent);

    /**
     * Perform the necessary actions when the workflow in a certain state receive a {@link WorkflowFailedLifecycleEvent}.
     */
    void onFailedEvent(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                       final WorkflowFailedLifecycleEvent workflowFailedEvent);

    /**
     * Perform the necessary actions when the workflow in a certain state receive a {@link WorkflowFinalizeLifecycleEvent}.
     */
    void onFinalizeEvent(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                         final WorkflowFinalizeLifecycleEvent workflowFinalizeEvent);

    /**
     * Get the {@link WorkflowExecutionStatus} that this action match.
     */
    WorkflowExecutionStatus matchState();
}
