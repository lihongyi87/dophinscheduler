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
import org.apache.dolphinscheduler.server.master.engine.workflow.lifecycle.event.WorkflowSucceedLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.workflow.runnable.IWorkflowExecutionRunnable;
import org.apache.dolphinscheduler.server.master.engine.workflow.statemachine.IWorkflowStateAction;

import lombok.extern.slf4j.Slf4j;

import org.springframework.stereotype.Component;

/**
 * 工作流成功完成生命周期事件处理器
 * 
 * 这个类专门处理工作流成功完成的事件。当工作流中所有任务都成功完成时，
 * 会发送一个成功事件，由这个处理器负责后续的收尾工作。
 * 
 * 主要功能：
 * 1. 接收工作流成功事件
 * 2. 更新工作流状态为SUCCESS
 * 3. 记录成功日志和统计信息
 * 4. 触发后续的清理和通知操作
 * 
 * 简单理解：就像一个专门处理“项目成功结项”的后勤人员。
 */
@Slf4j
@Component
public class WorkflowSucceedLifecycleEventHandler
        extends
            AbstractWorkflowLifecycleEventHandler<WorkflowSucceedLifecycleEvent> {

    /**
     * 处理工作流成功事件
     *
     * 当工作流中所有必需任务都成功完成后，会触发这个事件。
     * 处理器会执行最终的收尾工作，如更新状态、记录日志、发送通知等。
     *
     * @param workflowStateAction 工作流状态操作对象
     * @param workflowExecutionRunnable 工作流执行对象
     * @param workflowSucceedEvent 工作流成功事件对象
     */
    @Override
    public void handle(final IWorkflowStateAction workflowStateAction,
                       final IWorkflowExecutionRunnable workflowExecutionRunnable,
                       final WorkflowSucceedLifecycleEvent workflowSucceedEvent) {

        // =========================================================================
        // 工作流成功完成事件处理的核心逻辑
        // =========================================================================

        // 将成功事件委托给当前工作流状态对应的状态操作类进行处理
        //
        // 处理流程说明：
        // 1. 工作流成功的触发条件：
        //    - 所有必须执行的任务（非SKIP状态）都已成功完成
        //    - 没有运行中或等待中的任务
        //    - 工作流DAG的执行路径已全部完成
        //
        // 2. 状态转换处理：
        //    - 将工作流状态从RUNNING转换为SUCCESS
        //    - 设置工作流结束时间（endTime）
        //    - 更新工作流实例的执行结果和统计信息
        //
        // 3. 成功后的收尾工作：
        //    - 计算工作流总执行时长
        //    - 更新工作流实例在数据库中的状态
        //    - 记录成功完成的审计日志
        //    - 清理工作流相关的临时资源
        //
        // 4. 后续处理触发：
        //    - 发送工作流成功完成的通知（邮件、短信、钉钉等）
        //    - 触发工作流完成后的钩子函数（如果配置了）
        //    - 更新相关的监控指标和统计数据
        //    - 处理依赖当前工作流的下游工作流（如工作流链）
        //
        // 5. 资源清理：
        //    - 清理工作流执行过程中的临时文件
        //    - 释放工作流占用的资源配额
        //    - 清理过期的任务实例缓存
        //
        // 简单理解：就像一个项目的成功验收，需要：
        // - 确认所有交付物都已完成且符合质量要求
        // - 更新项目状态为"已完成"
        // - 计算项目耗时，整理项目文档
        // - 通知相关人员项目成功完成
        // - 释放项目资源，为下一个项目做准备
        workflowStateAction.onSucceedEvent(workflowExecutionRunnable, workflowSucceedEvent);
    }

    /**
     * 返回该处理器匹配的事件类型
     * 
     * 返回WORKFLOW_SUCCEED事件类型，系统会将所有的工作流成功事件
     * 路由到这个处理器进行处理。
     * 
     * @return 工作流成功事件类型
     */
    @Override
    public ILifecycleEventType matchEventType() {
        return WorkflowLifecycleEventType.SUCCEED;
    }
}
