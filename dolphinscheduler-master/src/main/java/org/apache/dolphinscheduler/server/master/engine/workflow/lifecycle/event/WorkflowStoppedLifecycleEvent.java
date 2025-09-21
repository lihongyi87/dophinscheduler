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
 * 工作流已停止生命周期事件
 *
 * 当工作流成功停止后触发的事件。
 * 这是停止操作的结果事件，表示工作流已经成功进入停止状态。
 *
 * 事件特点：
 * 1. 是一个终态事件，表示工作流生命周期结束
 * 2. 所有任务已被中断或取消
 * 3. 工作流不能再次启动，只能重新运行
 * 4. 所有相关资源已经释放
 *
 * 使用场景：
 * - 停止操作完成后的状态通知
 * - 触发停止相关的后续处理（如清理资源、通知用户）
 * - 更新工作流的最终状态和统计信息
 *
 * @author DolphinScheduler
 */
@Getter
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class WorkflowStoppedLifecycleEvent extends AbstractWorkflowLifecycleLifecycleEvent {

    /**
     * 工作流执行对象
     *
     * 已成功停止的工作流实例。
     */
    private final IWorkflowExecutionRunnable workflowExecutionRunnable;

    /**
     * 创建工作流已停止事件
     *
     * 使用静态工厂方法创建事件实例。
     *
     * @param workflowExecutionRunnable 已停止的工作流执行对象
     * @return 工作流已停止事件实例
     */
    public static WorkflowStoppedLifecycleEvent of(final IWorkflowExecutionRunnable workflowExecutionRunnable) {
        return new WorkflowStoppedLifecycleEvent(workflowExecutionRunnable);
    }

    /**
     * 获取事件类型
     *
     * @return 工作流已停止事件类型
     */
    @Override
    public ILifecycleEventType getEventType() {
        return WorkflowLifecycleEventType.STOPPED;
    }

    /**
     * 事件的字符串表示
     *
     * 用于日志记录和调试，显示已停止的工作流信息。
     *
     * @return 包含工作流信息的字符串描述
     */
    @Override
    public String toString() {
        return "WorkflowStoppedLifecycleEvent{" +
                "workflow=" + workflowExecutionRunnable +
                '}';
    }
}
