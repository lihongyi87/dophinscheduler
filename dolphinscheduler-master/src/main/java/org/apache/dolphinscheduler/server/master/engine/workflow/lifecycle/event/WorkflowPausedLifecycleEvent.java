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

import com.google.common.base.Preconditions;

/**
 * 工作流已暂停生命周期事件
 *
 * 当工作流成功暂停后触发的事件。
 * 这是暂停操作的结果事件，表示工作流已经成功进入暂停状态。
 *
 * 事件特点：
 * 1. 是一个状态确认事件，表示暂停操作已完成
 * 2. 此时工作流不会调度新的任务
 * 3. 正在执行的任务已经完成或被中断
 * 4. 工作流可以被重新启动或停止
 *
 * 使用场景：
 * - 暂停操作完成后的状态通知
 * - 触发暂停相关的后续处理（如通知、日志记录）
 * - 更新工作流的最终状态
 *
 * @author DolphinScheduler
 */
@Getter
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class WorkflowPausedLifecycleEvent extends AbstractWorkflowLifecycleLifecycleEvent {

    /**
     * 工作流执行对象
     *
     * 已成功暂停的工作流实例。
     */
    private final IWorkflowExecutionRunnable workflowExecutionRunnable;

    /**
     * 创建工作流已暂停事件
     *
     * 使用静态工厂方法创建事件实例，并进行参数校验。
     *
     * @param workflowExecutionRunnable 已暂停的工作流执行对象，不能为null
     * @return 工作流已暂停事件实例
     * @throws NullPointerException 如果参数为null
     */
    public static WorkflowPausedLifecycleEvent of(final IWorkflowExecutionRunnable workflowExecutionRunnable) {
        // 参数校验，确保工作流对象不为空
        Preconditions.checkNotNull(workflowExecutionRunnable, "workflowExecutionRunnable is null");
        return new WorkflowPausedLifecycleEvent(workflowExecutionRunnable);
    }

    /**
     * 获取事件类型
     *
     * @return 工作流已暂停事件类型
     */
    @Override
    public ILifecycleEventType getEventType() {
        return WorkflowLifecycleEventType.PAUSED;
    }

    /**
     * 事件的字符串表示
     *
     * 用于日志记录和调试，显示已暂停的工作流信息。
     *
     * @return 包含工作流名称的字符串描述
     */
    @Override
    public String toString() {
        return "WorkflowPausedLifecycleEvent{" +
                "workflow=" + workflowExecutionRunnable.getName() +
                '}';
    }
}
