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
 * 工作流停止生命周期事件
 *
 * 当用户请求停止工作流时触发的事件。
 * 停止操作比暂停更加强制，会尝试中断正在执行的任务并结束整个工作流。
 *
 * 与暂停的区别：
 * - 暂停：优雅停止，等待任务完成，可恢复执行
 * - 停止：强制停止，中断任务，不可恢复执行
 *
 * 使用场景：
 * 1. 用户手动停止工作流执行
 * 2. 系统异常时紧急停止
 * 3. 资源不足时强制终止
 * 4. 工作流执行超时时强制停止
 *
 * 处理结果：
 * - 工作流状态转换为停止中（STOPPING）
 * - 中断所有正在执行的任务
 * - 取消所有等待执行的任务
 * - 最终转换为已停止（STOPPED）状态
 *
 * @author DolphinScheduler
 */
@Getter
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class WorkflowStopLifecycleEvent extends AbstractWorkflowLifecycleLifecycleEvent {

    /**
     * 工作流执行对象
     *
     * 要停止的工作流实例，包含工作流的完整执行信息。
     */
    private final IWorkflowExecutionRunnable workflowExecutionRunnable;

    /**
     * 创建工作流停止事件
     *
     * 使用静态工厂方法创建事件实例。
     *
     * @param workflowExecutionRunnable 要停止的工作流执行对象
     * @return 工作流停止事件实例
     */
    public static WorkflowStopLifecycleEvent of(final IWorkflowExecutionRunnable workflowExecutionRunnable) {
        return new WorkflowStopLifecycleEvent(workflowExecutionRunnable);
    }

    /**
     * 获取事件类型
     *
     * @return 工作流停止事件类型
     */
    @Override
    public ILifecycleEventType getEventType() {
        return WorkflowLifecycleEventType.STOP;
    }

    /**
     * 事件的字符串表示
     *
     * 用于日志记录和调试，显示要停止的工作流信息。
     *
     * @return 包含工作流名称的字符串描述
     */
    @Override
    public String toString() {
        return "WorkflowStopLifecycleEvent{" +
                "workflow=" + workflowExecutionRunnable.getName() +
                '}';
    }
}
