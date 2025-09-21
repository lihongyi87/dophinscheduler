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

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.server.master.engine.ILifecycleEventType;
import org.apache.dolphinscheduler.server.master.engine.task.runnable.ITaskExecutionRunnable;
import org.apache.dolphinscheduler.server.master.engine.workflow.lifecycle.AbstractWorkflowLifecycleLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.workflow.lifecycle.WorkflowLifecycleEventType;
import org.apache.dolphinscheduler.server.master.engine.workflow.runnable.IWorkflowExecutionRunnable;

import java.util.Optional;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * 工作流拓扑逻辑转换事件（任务完成触发）
 *
 * 当工作流中的某个任务执行完成时触发的事件，用于处理工作流的拓扑逻辑转换。
 * 这是工作流调度的核心事件，负责根据DAG（有向无环图）的拓扑结构，
 * 判断是否需要触发下游任务的执行。
 *
 * 主要功能：
 * 1. 检查任务完成状态（成功/失败）
 * 2. 分析下游任务的依赖关系
 * 3. 判断是否满足下游任务的启动条件
 * 4. 触发符合条件的下游任务开始执行
 * 5. 更新工作流的整体执行状态
 *
 * 使用场景：
 * - 任务成功完成后，触发后续任务
 * - 任务失败后，根据失败策略决定后续流程
 * - 条件任务根据结果选择不同的执行分支
 *
 * 简单理解：就像多米诺骨牌，一个任务倒下（完成）会引发连锁反应。
 *
 * @author DolphinScheduler
 */
@Getter
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class WorkflowTopologyLogicalTransitionWithTaskFinishLifecycleEvent
        extends
            AbstractWorkflowLifecycleLifecycleEvent {

    /**
     * 工作流执行对象
     *
     * 包含工作流的完整执行信息和上下文环境。
     */
    private final IWorkflowExecutionRunnable workflowExecutionRunnable;

    /**
     * 已完成的任务执行对象
     *
     * 触发此事件的任务对象，包含任务的执行状态和结果信息。
     * 系统会根据这个任务的完成情况来决定下游任务的执行策略。
     */
    private final ITaskExecutionRunnable taskExecutionRunnable;

    /**
     * 创建工作流拓扑逻辑转换事件
     *
     * 使用静态工厂方法创建事件实例，并进行参数校验。
     *
     * @param workflowExecutionRunnable 工作流执行对象，不能为null
     * @param taskExecutionRunnable 已完成的任务执行对象，不能为null
     * @return 工作流拓扑逻辑转换事件实例
     * @throws NullPointerException 如果任一参数为null
     */
    public static WorkflowTopologyLogicalTransitionWithTaskFinishLifecycleEvent of(
                                                                                   final IWorkflowExecutionRunnable workflowExecutionRunnable,
                                                                                   final ITaskExecutionRunnable taskExecutionRunnable) {
        // 参数校验，确保关键对象不为空
        checkNotNull(workflowExecutionRunnable, "workflowExecutionRunnable is null");
        checkNotNull(taskExecutionRunnable, "taskExecutionRunnable is null");
        return new WorkflowTopologyLogicalTransitionWithTaskFinishLifecycleEvent(
                workflowExecutionRunnable,
                taskExecutionRunnable);
    }

    /**
     * 获取事件类型
     *
     * @return 工作流拓扑逻辑转换事件类型
     */
    @Override
    public ILifecycleEventType getEventType() {
        return WorkflowLifecycleEventType.TOPOLOGY_LOGICAL_TRANSACTION_WITH_TASK_FINISH;
    }

    /**
     * 事件的字符串表示
     *
     * 用于日志记录和调试，显示触发事件的任务信息和状态。
     *
     * @return 包含任务名称和状态的字符串描述
     */
    @Override
    public String toString() {
        return "WorkflowTopologyLogicalTransitionWithTaskFinishLifecycleEvent{"
                + "task=" + taskExecutionRunnable.getName()
                + "taskState="
                + Optional.ofNullable(taskExecutionRunnable.getTaskInstance()).map(TaskInstance::getState)
                        .map(Enum::name).orElse(null)
                + '}';
    }
}
