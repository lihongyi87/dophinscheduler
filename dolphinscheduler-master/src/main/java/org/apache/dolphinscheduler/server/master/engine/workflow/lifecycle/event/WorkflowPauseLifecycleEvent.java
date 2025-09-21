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
 * 工作流暂停生命周期事件
 *
 * 当用户请求暂停工作流时触发的事件。
 * 暂停操作会停止新任务的调度，但不会中断正在执行的任务。
 *
 * 使用场景：
 * 1. 用户手动暂停工作流执行
 * 2. 系统资源不足时自动暂停
 * 3. 维护期间临时暂停工作流
 *
 * 处理结果：
 * - 工作流状态转换为暂停中（PAUSING）
 * - 停止调度新的任务
 * - 等待正在执行的任务完成或超时
 * - 最终转换为已暂停（PAUSED）状态
 *
 * 注意：暂停是一个优雅的停止过程，不会强制中断正在运行的任务。
 *
 * @author DolphinScheduler
 */
@Getter
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class WorkflowPauseLifecycleEvent extends AbstractWorkflowLifecycleLifecycleEvent {

    /**
     * 工作流执行对象
     *
     * 要暂停的工作流实例，包含工作流的完整执行信息。
     */
    private final IWorkflowExecutionRunnable workflowExecutionRunnable;

    /**
     * 创建工作流暂停事件
     *
     * 使用静态工厂方法创建事件实例。
     *
     * @param workflowExecutionRunnable 要暂停的工作流执行对象
     * @return 工作流暂停事件实例
     */
    public static WorkflowPauseLifecycleEvent of(final IWorkflowExecutionRunnable workflowExecutionRunnable) {
        return new WorkflowPauseLifecycleEvent(workflowExecutionRunnable);
    }

    /**
     * 获取事件类型
     *
     * @return 工作流暂停事件类型
     */
    @Override
    public ILifecycleEventType getEventType() {
        return WorkflowLifecycleEventType.PAUSE;
    }

    /**
     * 事件的字符串表示
     *
     * 用于日志记录和调试，显示要暂停的工作流信息。
     *
     * @return 包含工作流名称的字符串描述
     */
    @Override
    public String toString() {
        return "WorkflowPauseLifecycleEvent{" +
                "workflow=" + workflowExecutionRunnable.getName() +
                '}';
    }
}
