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

package org.apache.dolphinscheduler.server.master.engine.workflow.lifecycle.handler;

import org.apache.dolphinscheduler.server.master.engine.ILifecycleEventType;
import org.apache.dolphinscheduler.server.master.engine.workflow.lifecycle.WorkflowLifecycleEventType;
import org.apache.dolphinscheduler.server.master.engine.workflow.lifecycle.event.WorkflowFailedLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.workflow.runnable.IWorkflowExecutionRunnable;
import org.apache.dolphinscheduler.server.master.engine.workflow.statemachine.IWorkflowStateAction;

import lombok.extern.slf4j.Slf4j;

import org.springframework.stereotype.Component;

/**
 * 工作流失败生命周期事件处理器
 * 
 * 这个类专门处理工作流失败事件。当工作流中的关键任务失败导致整个工作流失败时，
 * 会发送一个失败事件，由这个处理器负责失败处理和收尾工作。
 * 
 * 主要功能：
 * 1. 接收工作流失败事件
 * 2. 更新工作流状态为FAILED
 * 3. 停止所有正在运行的任务
 * 4. 记录失败原因和错误信息
 * 5. 触发失败告警和通知
 * 6. 清理相关资源和临时数据
 * 
 * 简单理解：就像一个专门处理"项目失败"的应急处理员，
 * 当项目出现重大问题时，负责紧急叫停并做善后工作。
 */
@Slf4j
@Component
public class WorkflowFailedLifecycleEventHandler
        extends
            AbstractWorkflowLifecycleEventHandler<WorkflowFailedLifecycleEvent> {

    /**
     * 处理工作流失败事件
     *
     * 当工作流因关键任务失败而失败时，会触发这个方法。
     * 处理器会执行失败处理逻辑，如停止运行任务、更新状态、发送告警等。
     *
     * @param workflowStateAction 工作流状态操作对象
     * @param workflowExecutionRunnable 工作流执行对象
     * @param workflowFailedEvent 工作流失败事件对象
     */
    @Override
    public void handle(final IWorkflowStateAction workflowStateAction,
                       final IWorkflowExecutionRunnable workflowExecutionRunnable,
                       final WorkflowFailedLifecycleEvent workflowFailedEvent) {

        // =========================================================================
        // 工作流失败事件处理的核心逻辑
        // =========================================================================

        // 将失败事件委托给当前工作流状态对应的状态操作类进行处理
        //
        // 处理流程说明：
        // 1. 工作流失败的触发条件：
        //    - 关键路径上的任务执行失败，且没有配置失败重试或重试次数已用完
        //    - 任务失败策略为"失败即停止"（FAILURE_FAILURE_POLICY）
        //    - 工作流执行超时（如果配置了超时时间）
        //    - 系统异常导致工作流无法继续执行
        //
        // 2. 失败应急处理：
        //    - 立即停止所有正在运行的任务（发送KILL信号）
        //    - 取消所有等待执行的任务（状态设置为NEED_FAULT_TOLERANCE）
        //    - 将工作流状态从RUNNING转换为FAILURE
        //    - 设置工作流结束时间和失败原因
        //
        // 3. 失败信息记录：
        //    - 收集并记录详细的失败原因和错误堆栈
        //    - 记录失败任务的日志文件路径
        //    - 保存工作流执行的上下文信息，便于问题排查
        //    - 更新工作流实例在数据库中的失败状态
        //
        // 4. 告警和通知：
        //    - 根据告警规则发送失败通知（邮件、短信、钉钉、企业微信等）
        //    - 触发监控系统的失败告警
        //    - 记录运维事件，便于后续问题追踪
        //    - 如果配置了自动重启，可能触发工作流重新调度
        //
        // 5. 资源清理：
        //    - 强制停止工作流相关的所有运行中任务
        //    - 清理任务执行过程中产生的临时文件
        //    - 释放工作流占用的计算资源和内存
        //    - 清理相关的缓存和临时数据
        //
        // 6. 故障转移处理：
        //    - 如果配置了故障转移策略，可能尝试在其他节点重新执行
        //    - 检查是否需要触发备用工作流
        //    - 处理依赖当前工作流的下游工作流状态
        //
        // 简单理解：就像一个项目的紧急叫停，需要：
        // - 立即停止所有正在进行的工作
        // - 分析失败原因，记录详细信息
        // - 通知相关人员项目失败
        // - 清理项目资源，避免资源浪费
        // - 评估是否需要启动应急预案
        workflowStateAction.onFailedEvent(workflowExecutionRunnable, workflowFailedEvent);
    }

    /**
     * 返回该处理器匹配的事件类型
     * 
     * 返回FAILED事件类型，系统会将所有的工作流失败事件
     * 路由到这个处理器进行处理。
     * 
     * @return 工作流失败事件类型
     */
    @Override
    public ILifecycleEventType matchEventType() {
        return WorkflowLifecycleEventType.FAILED;
    }
}
