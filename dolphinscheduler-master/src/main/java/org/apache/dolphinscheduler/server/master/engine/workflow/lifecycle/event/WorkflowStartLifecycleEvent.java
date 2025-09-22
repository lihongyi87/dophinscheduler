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

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * 工作流启动生命周期事件
 *
 * 当需要启动一个工作流实例时触发的事件。
 * 这是工作流生命周期的起始事件，标志着工作流开始执行。
 *
 * 使用场景：
 * 1. 用户手动触发工作流执行
 * 2. 定时调度器触发工作流启动
 * 3. 工作流重启或重试时的启动
 *
 * 处理结果：
 * - 工作流状态从初始状态转换为运行状态
 * - 初始化工作流执行环境
 * - 开始执行工作流中的起始任务
 *
 * @author DolphinScheduler
 */
@Getter
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class WorkflowStartLifecycleEvent extends AbstractWorkflowLifecycleLifecycleEvent {

    /**
     * 工作流执行对象
     *
     * 包含了工作流的完整执行信息，如工作流定义、实例信息、
     * 执行上下文、任务列表等。
     */
    private IWorkflowExecutionRunnable workflowExecutionRunnable;

    /**
     * 创建工作流启动事件
     *
     * 使用静态工厂方法创建事件实例，提供更好的代码可读性。
     *
     * @param workflowExecutionRunnable 要启动的工作流执行对象
     * @return 工作流启动事件实例
     */
    public static WorkflowStartLifecycleEvent of(IWorkflowExecutionRunnable workflowExecutionRunnable) {
        return new WorkflowStartLifecycleEvent(workflowExecutionRunnable);
    }

    /**
     * 获取事件类型
     *
     * @return 工作流启动事件类型
     */
    @Override
    public ILifecycleEventType getEventType() {
        return WorkflowLifecycleEventType.START;
    }

    /**
     * 事件的字符串表示
     *
     * 用于日志记录和调试，显示相关的工作流信息。
     *
     * @return 包含工作流名称的字符串描述
     */
    @Override
    public String toString() {
        return "WorkflowStartLifecycleEvent{" +
                "workflow=" + workflowExecutionRunnable.getWorkflowExecuteContext().getWorkflowInstance().getName() +
                '}';
    }
}
