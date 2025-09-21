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
import org.apache.dolphinscheduler.server.master.engine.workflow.lifecycle.event.WorkflowTopologyLogicalTransitionWithTaskFinishLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.workflow.runnable.IWorkflowExecutionRunnable;
import org.apache.dolphinscheduler.server.master.engine.workflow.statemachine.IWorkflowStateAction;

import org.springframework.stereotype.Component;

/**
 * 工作流拓扑逻辑转换事件处理器（任务完成触发）
 *
 * 这是工作流调度的核心处理器，负责处理任务完成后的拓扑逻辑转换。
 * 当工作流中的任务执行完成时，会触发这个处理器来判断和触发后续的任务。
 *
 * 主要功能：
 * 1. 接收任务完成事件
 * 2. 分析DAG拓扑结构，找到下游任务
 * 3. 检查下游任务的依赖关系是否满足
 * 4. 触发符合条件的下游任务开始执行
 * 5. 检查工作流是否全部完成
 *
 * 处理逻辑：
 * - 获取已完成任务的所有下游任务
 * - 逐个检查下游任务的依赖条件
 * - 对于依赖满足的任务，创建任务实例并提交执行
 * - 如果所有必须任务都已完成，则标记工作流成功
 *
 * 简单理解：就像一个流水线的调度员，一个工人完成了工作，
 * 立即安排下一个环节的工人开始工作。
 *
 * @author DolphinScheduler
 */
@Component
public class WorkflowTopologyLogicalTransitionWithTaskFinishLifecycleEventHandler
        extends
            AbstractWorkflowLifecycleEventHandler<WorkflowTopologyLogicalTransitionWithTaskFinishLifecycleEvent> {

    /**
     * 处理工作流拓扑逻辑转换事件
     *
     * 这是工作流调度的核心方法。当任务完成时，会调用这个方法
     * 来检查是否需要触发后续任务的执行。
     *
     * @param workflowStateAction 工作流状态操作对象，包含具体的处理逻辑
     * @param workflowExecutionRunnable 工作流执行对象，包含工作流的所有信息
     * @param workflowTopologyLogicalTransitionWithTaskFinishEvent 拓扑逻辑转换事件对象
     */
    @Override
    public void handle(final IWorkflowStateAction workflowStateAction,
                       final IWorkflowExecutionRunnable workflowExecutionRunnable,
                       final WorkflowTopologyLogicalTransitionWithTaskFinishLifecycleEvent workflowTopologyLogicalTransitionWithTaskFinishEvent) {

        // =========================================================================
        // 工作流拓扑逻辑转换事件处理的核心逻辑
        // =========================================================================

        // 将拓扑逻辑转换事件委托给当前工作流状态对应的状态操作类进行处理
        //
        // 这是工作流调度引擎的核心部分，负责DAG图的执行流程控制
        //
        // 处理流程详细说明：
        //
        // 1. 事件触发条件：
        //    - 当工作流中的某个任务完成（成功、失败、跳过）时触发
        //    - 任务状态变更为终态（SUCCESS、FAILURE、KILL、SKIPPED等）
        //    - 需要根据完成任务的结果，决定后续任务的执行策略
        //
        // 2. DAG拓扑分析：
        //    - 获取已完成任务在DAG中的位置和角色
        //    - 分析已完成任务的所有下游任务（后继任务）
        //    - 检查每个下游任务的所有上游依赖关系
        //    - 考虑任务之间的依赖类型（普通依赖、条件依赖、弱依赖等）
        //
        // 3. 依赖关系检查：
        //    - 遍历已完成任务的所有下游任务
        //    - 对每个下游任务，检查其所有上游依赖是否已满足：
        //      * 成功依赖：上游任务必须成功完成
        //      * 失败依赖：上游任务必须失败
        //      * 完成依赖：上游任务完成即可（不论成功失败）
        //      * 条件依赖：根据上游任务的输出结果判断
        //    - 计算任务的依赖满足度，确定是否可以开始执行
        //
        // 4. 任务执行决策：
        //    对于依赖条件满足的下游任务：
        //    - 创建新的TaskInstance实例
        //    - 设置任务执行上下文和参数
        //    - 将任务提交到任务调度队列
        //    - 更新任务状态为SUBMITTED或RUNNING_EXECUTION
        //
        // 5. 工作流状态评估：
        //    - 检查工作流是否还有未完成的任务
        //    - 判断是否所有必须路径都已完成
        //    - 评估工作流是否应该成功结束、失败结束或继续执行：
        //      * 如果所有任务都已完成且符合成功条件 → 触发SUCCESS事件
        //      * 如果关键任务失败且无法恢复 → 触发FAILED事件
        //      * 如果还有任务可以执行 → 继续等待
        //
        // 6. 条件分支处理：
        //    - 处理条件任务的分支逻辑
        //    - 根据条件任务的执行结果，激活相应的执行分支
        //    - 跳过不满足条件的任务分支
        //
        // 7. 并行度控制：
        //    - 检查工作流和任务的并行度限制
        //    - 确保不超过系统资源配额
        //    - 合理调度任务执行顺序
        //
        // 8. 异常情况处理：
        //    - 处理循环依赖检测
        //    - 处理任务执行超时
        //    - 处理资源不足的情况
        //    - 处理任务调度失败的重试逻辑
        //
        // 简单理解：就像一个智能的流水线调度员：
        // - 当一个工人完成了工作，立即检查下一个环节是否可以开始
        // - 确认所有前置条件都满足后，安排下一批工人开始工作
        // - 如果发现某个环节出现问题，及时调整整个流水线的执行策略
        // - 当所有工作都完成时，宣布整个项目成功
        workflowStateAction.onTopologyLogicalTransitionEvent(
                workflowExecutionRunnable,
                workflowTopologyLogicalTransitionWithTaskFinishEvent);
    }

    /**
     * 返回该处理器匹配的事件类型
     *
     * 返回拓扑逻辑转换事件类型，系统会将所有的任务完成
     * 触发的拓扑转换事件路由到这个处理器进行处理。
     *
     * @return 工作流拓扑逻辑转换事件类型
     */
    @Override
    public ILifecycleEventType matchEventType() {
        return WorkflowLifecycleEventType.TOPOLOGY_LOGICAL_TRANSACTION_WITH_TASK_FINISH;
    }
}
