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
 * 工作流失败生命周期事件
 *
 * 当工作流中有任务失败且不可恢复时触发的事件。
 * 这是工作流异常结束的标志，表示工作流无法继续执行。
 *
 * 事件特点：
 * 1. 是一个负向的终态事件，表示工作流执行失败
 * 2. 至少有一个关键任务执行失败
 * 3. 工作流无法达到预期的业务目标
 * 4. 需要人工干预或重新启动
 *
 * 失败原因可能包括：
 * - 任务执行超时
 * - 任务返回错误码
 * - 系统资源不足
 * - 依赖的外部系统不可用
 * - 数据校验失败
 *
 * 处理结果：
 * - 工作流状态更新为失败（FAILED）
 * - 触发失败相关的通知和告警
 * - 记录详细的错误信息和堆栈
 * - 执行最终化操作和资源清理
 *
 * @author DolphinScheduler
 */
@Getter
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class WorkflowFailedLifecycleEvent extends AbstractWorkflowLifecycleLifecycleEvent {

    /**
     * 工作流执行对象
     *
     * 执行失败的工作流实例。
     */
    private final IWorkflowExecutionRunnable workflowExecutionRunnable;

    /**
     * 创建工作流失败事件
     *
     * 使用静态工厂方法创建事件实例。
     *
     * @param workflowExecutionRunnable 执行失败的工作流执行对象
     * @return 工作流失败事件实例
     */
    public static WorkflowFailedLifecycleEvent of(final IWorkflowExecutionRunnable workflowExecutionRunnable) {
        return new WorkflowFailedLifecycleEvent(workflowExecutionRunnable);
    }

    /**
     * 获取事件类型
     *
     * @return 工作流失败事件类型
     */
    @Override
    public ILifecycleEventType getEventType() {
        return WorkflowLifecycleEventType.FAILED;
    }

    /**
     * 事件的字符串表示
     *
     * 用于日志记录和调试，显示失败的工作流信息。
     *
     * @return 包含工作流名称的字符串描述
     */
    @Override
    public String toString() {
        return "WorkflowFailedLifecycleEvent{" +
                "workflow=" + workflowExecutionRunnable.getName() +
                '}';
    }
}
