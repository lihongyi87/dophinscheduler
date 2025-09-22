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

package org.apache.dolphinscheduler.server.master.engine.workflow.lifecycle.event;

import org.apache.dolphinscheduler.server.master.engine.ILifecycleEventType;
import org.apache.dolphinscheduler.server.master.engine.workflow.lifecycle.AbstractWorkflowLifecycleLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.workflow.lifecycle.WorkflowLifecycleEventType;
import org.apache.dolphinscheduler.server.master.engine.workflow.runnable.IWorkflowExecutionRunnable;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * 工作流成功生命周期事件
 *
 * 当工作流中的所有任务都成功完成时触发的事件。
 * 这是工作流正常结束的标志，表示所有业务逻辑都按预期完成。
 *
 * 事件特点：
 * 1. 是一个正向的终态事件，表示工作流成功完成
 * 2. 所有必须执行的任务都已成功结束
 * 3. 工作流达到了预期的业务目标
 * 4. 可以触发成功相关的后续处理
 *
 * 使用场景：
 * - 所有任务都执行成功
 * - 条件任务的所有分支都正常结束
 * - 循环任务在满足条件后正常退出
 *
 * 处理结果：
 * - 工作流状态更新为成功（SUCCESS）
 * - 触发成功相关的通知和日志
 * - 更新统计信息和执行历史
 * - 执行最终化操作
 *
 * @author DolphinScheduler
 */
@Getter
@AllArgsConstructor
public class WorkflowSucceedLifecycleEvent extends AbstractWorkflowLifecycleLifecycleEvent {

    /**
     * 工作流执行对象
     *
     * 成功完成的工作流实例。
     */
    private final IWorkflowExecutionRunnable workflowExecutionRunnable;

    /**
     * 创建工作流成功事件
     *
     * 使用静态工厂方法创建事件实例。
     *
     * @param workflowExecutionRunnable 成功完成的工作流执行对象
     * @return 工作流成功事件实例
     */
    public static WorkflowSucceedLifecycleEvent of(final IWorkflowExecutionRunnable workflowExecutionRunnable) {
        return new WorkflowSucceedLifecycleEvent(workflowExecutionRunnable);
    }

    /**
     * 获取事件类型
     *
     * @return 工作流成功事件类型
     */
    @Override
    public ILifecycleEventType getEventType() {
        return WorkflowLifecycleEventType.SUCCEED;
    }

    /**
     * 事件的字符串表示
     *
     * 用于日志记录和调试，显示成功完成的工作流信息。
     *
     * @return 包含工作流名称的字符串描述
     */
    @Override
    public String toString() {
        return "WorkflowSucceedLifecycleEvent{" +
                "workflow=" + workflowExecutionRunnable.getName() +
                '}';
    }
}
