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

package org.apache.dolphinscheduler.server.master.engine.graph;

import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.plugin.task.api.model.SwitchResultVo;
import org.apache.dolphinscheduler.plugin.task.api.parameters.ConditionsParameters;
import org.apache.dolphinscheduler.plugin.task.api.parameters.SwitchParameters;
import org.apache.dolphinscheduler.plugin.task.api.utils.TaskTypeUtils;
import org.apache.dolphinscheduler.server.master.engine.executor.plugin.condition.ConditionLogicTask;
import org.apache.dolphinscheduler.server.master.engine.executor.plugin.switchtask.SwitchLogicTask;
import org.apache.dolphinscheduler.server.master.engine.task.runnable.ITaskExecutionRunnable;

import org.apache.commons.collections4.CollectionUtils;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import lombok.extern.slf4j.Slf4j;

import org.springframework.stereotype.Component;

/**
 * 后继任务流程调整器
 *
 * 负责根据任务执行结果动态调整工作流的执行路径，处理条件分支和任务跳过逻辑。
 * 这是工作流引擎中实现条件执行和动态路径选择的核心组件。
 *
 * 类比：就像项目管理中的路径调整专家，根据当前工序的完成情况和结果，
 * 决定接下来应该执行哪些工序，跳过哪些不必要的工序。
 *
 * 核心职责：
 * 1. 条件分支处理：根据条件任务的执行结果选择不同的执行分支
 * 2. 开关逻辑处理：根据开关任务的执行结果动态调整后续任务流
 * 3. 任务跳过逻辑：处理被跳过任务的后续影响传播
 * 4. 禁用任务处理：处理被禁用任务的流程调整
 * 5. 状态传播管理：确保状态变化正确传播到依赖任务
 *
 * 支持的任务类型：
 * - {@link ConditionLogicTask}：条件任务，根据条件表达式结果选择分支
 * - {@link SwitchLogicTask}：开关任务，根据开关逻辑选择执行路径
 * - 普通任务：处理跳过和禁用状态的传播
 *
 * 调整策略：
 * - 禁用任务：不进行任何后续流程调整，保持原有依赖关系
 * - 跳过任务：检查后继任务是否所有前驱都已跳过，如是则同样跳过
 * - 条件任务：根据条件结果跳过不满足条件的分支
 * - 开关任务：根据开关结果跳过未选中的分支
 *
 * 跳过传播机制：
 * - 被跳过的任务视为"成功完成"，可以触发后继任务
 * - 如果后继任务的所有前驱都被跳过，则该后继任务也会被跳过
 * - 跳过状态会沿着依赖链条向下传播
 *
 * 条件任务处理流程：
 * 1. 解析任务参数中的条件执行结果
 * 2. 根据条件成功或失败确定需要跳过的分支
 * 3. 将不满足条件的分支中的所有任务标记为跳过
 *
 * 开关任务处理流程：
 * 1. 解析任务参数中的开关执行结果
 * 2. 确定被选中的下一个执行分支
 * 3. 将所有未被选中的分支中的任务标记为跳过
 *
 * 使用场景：
 * - 条件工作流：根据数据或业务规则选择不同的处理路径
 * - 故障恢复：在任务失败时跳过非关键的后续任务
 * - 资源优化：在资源不足时跳过可选的任务
 * - 分支合并：在多分支执行后的合并点处理
 *
 * 设计模式：
 * - 策略模式：根据不同的任务类型采用不同的调整策略
 * - 责任链模式：状态变化沿着依赖链条传播
 * - 观察者模式：任务状态变化触发后续调整
 *
 * 线程安全性：
 * - 调整操作是幂等的，可以安全地并发执行
 * - 使用工作流执行图的线程安全方法进行状态更新
 * - 避免并发修改可能导致的状态不一致
 */
@Slf4j
@Component
public class SuccessorFlowAdjuster {

    /**
     * 控制任务的后继流程
     *
     * 根据任务的执行状态和类型，动态调整工作流的后续执行路径。
     * 这是工作流引擎中实现智能路径选择的核心方法。
     *
     * 调整策略：
     * 1. 禁用任务：不进行任何后继流程调整，保持原有依赖关系
     * 2. 跳过任务：检查后继任务的所有前驱是否都已跳过，如是则标记后继任务也跳过
     * 3. 条件任务：根据条件执行结果调整流程，跳过不满足条件的分支
     * 4. 开关任务：根据开关执行结果调整流程，跳过未选中的分支
     *
     * 执行流程：
     * 1. 首先检查任务是否被跳过，如是则处理跳过传播逻辑
     * 2. 然后检查任务是否被禁用，如是则直接返回不进行调整
     * 3. 最后根据任务类型进行相应的流程调整
     *
     * 跳过传播机制：
     * - 被跳过的任务视为“成功完成”，可以触发后继任务
     * - 如果后继任务的所有前驱都被跳过，则该后继任务也会被跳过
     * - 跳过状态会沿着依赖链条向下传播
     *
     * 支持的任务类型：
     * - {@link ConditionLogicTask}：条件任务，根据条件表达式结果选择分支
     * - {@link SwitchLogicTask}：开关任务，根据开关逻辑选择执行路径
     * - 普通任务：仅处理跳过和禁用状态的传播
     *
     * @param taskExecutionRunnable 需要调整后继流程的任务对象
     *
     * 类比：项目经理根据工序完成情况和结果，决定后续的施工安排：
     * - 跳过的工序会影响后续依赖工序的调度
     * - 条件工序根据检查结果决定是否需要进行补修工作
     * - 开关工序根据选择结果决定进入哪个施工分支
     */
    public void adjustSuccessorFlow(final ITaskExecutionRunnable taskExecutionRunnable) {
        final IWorkflowExecutionGraph workflowExecutionGraph = taskExecutionRunnable.getWorkflowExecutionGraph();

        if (workflowExecutionGraph.isTaskExecutionRunnableSkipped(taskExecutionRunnable)) {
            // If the successor flow's all parent is skipped, then mark the successor skipped.
            for (ITaskExecutionRunnable successor : workflowExecutionGraph.getSuccessors(taskExecutionRunnable)) {
                if (workflowExecutionGraph.isAllPredecessorsSkipped(successor)) {
                    workflowExecutionGraph.markTaskSkipped(successor);
                }
            }
            return;
        }

        if (workflowExecutionGraph.isTaskExecutionRunnableForbidden(taskExecutionRunnable)) {
            return;
        }

        final String taskType = taskExecutionRunnable.getTaskInstance().getTaskType();
        if (TaskTypeUtils.isConditionTask(taskType)) {
            adjustConditionTaskSuccessorFlow(taskExecutionRunnable);
            return;
        }

        if (TaskTypeUtils.isSwitchTask(taskType)) {
            adjustSwitchTaskSuccessorFlow(taskExecutionRunnable);
            return;
        }
    }

    /**
     * 调整条件任务的后继流程
     *
     * 根据条件任务的执行结果，决定哪些分支需要被跳过。
     * 条件任务具有成功和失败两个分支，根据条件表达式的计算结果选择执行其中一个。
     *
     * 执行流程：
     * 1. 从任务参数中解析条件参数对象
     * 2. 获取条件表达式的执行结果
     * 3. 根据条件是否成功决定需要跳过的分支
     * 4. 将需要跳过的分支中的所有任务标记为跳过
     *
     * 分支选择逻辑：
     * - 条件成功：跳过失败分支（failedNode）中的所有任务
     * - 条件失败：跳过成功分支（successNode）中的所有任务
     * - 被跳过的分支中的任务不会被执行，但视为已完成
     *
     * 参数验证：
     * - 条件参数必须不为空，否则抛出IllegalArgumentException
     * - 条件结果必须不为空，否则抛出IllegalArgumentException
     * - 任务参数必须包含合法的JSON格式数据
     *
     * @param taskExecutionRunnable 条件任务的可执行对象
     * @throws IllegalArgumentException 当任务参数无效或条件结果为空时
     *
     * 类比：项目质量检查工序，根据检查结果决定是进入整改流程还是直接验收流程。
     */
    private void adjustConditionTaskSuccessorFlow(final ITaskExecutionRunnable taskExecutionRunnable) {
        final String taskParams = taskExecutionRunnable.getTaskInstance().getTaskParams();
        final ConditionsParameters conditionsParameters = JSONUtils.parseObject(taskParams, ConditionsParameters.class);
        if (conditionsParameters == null) {
            throw new IllegalArgumentException("Condition task params: " + taskParams + " is invalid.");
        }
        final ConditionsParameters.ConditionResult conditionResult = conditionsParameters.getConditionResult();
        if (conditionResult == null) {
            throw new IllegalArgumentException("ConditionResult: is null in taskParam: " + taskParams);
        }
        final List<Long> needSkippedBranch;
        if (conditionResult.isConditionSuccess()) {
            needSkippedBranch = conditionResult.getFailedNode();
        } else {
            needSkippedBranch = conditionResult.getSuccessNode();
        }
        markTaskSkipped(taskExecutionRunnable, needSkippedBranch);
    }

    /**
     * 调整开关任务的后继流程
     *
     * 根据开关任务的执行结果，决定哪些分支需要被跳过。
     * 开关任务具有多个可能的执行路径，根据开关逻辑的计算结果选择其中一个或多个。
     *
     * 执行流程：
     * 1. 从任务参数中解析开关参数对象
     * 2. 获取开关逻辑的执行结果
     * 3. 收集所有可能的分支路径（包括主分支和依赖分支）
     * 4. 从所有分支中移除被选中的下一个分支
     * 5. 将未被选中的分支中的所有任务标记为跳过
     *
     * 分支收集逻辑：
     * - 收集主分支：switchResult.getNextNode()
     * - 收集依赖分支：switchResult.getDependTaskList()中的所有nextNode
     * - 移除被选中的分支：switchParameters.getNextBranch()
     * - 剩余的分支就是需要跳过的分支
     *
     * 参数验证：
     * - 开关参数必须不为空，否则抛出IllegalArgumentException
     * - 开关结果必须不为空，否则抛出IllegalArgumentException
     * - 任务参数必须包含合法的JSON格式数据
     *
     * 空值处理：
     * - 如果主分支为null，则不会被添加到需要跳过的列表中
     * - 如果依赖分支列表为空，则不会处理依赖分支
     *
     * @param taskExecutionRunnable 开关任务的可执行对象
     * @throws IllegalArgumentException 当任务参数无效或开关结果为空时
     *
     * 类比：项目决策点，根据决策结果选择一个或多个执行方案，
     * 放弃其他不适用的方案。
     */
    private void adjustSwitchTaskSuccessorFlow(final ITaskExecutionRunnable taskExecutionRunnable) {
        final String taskParams = taskExecutionRunnable.getTaskInstance().getTaskParams();
        final SwitchParameters switchParameters = JSONUtils.parseObject(taskParams, SwitchParameters.class);
        if (switchParameters == null) {
            throw new IllegalArgumentException("Switch task params: " + taskParams + " is invalid.");
        }
        final SwitchParameters.SwitchResult switchResult = switchParameters.getSwitchResult();
        if (switchResult == null) {
            throw new IllegalArgumentException("ConditionResult: is null in taskParam: " + taskParams);
        }
        final Set<Long> needSkippedBranch = new HashSet<>();
        if (switchResult.getNextNode() != null) {
            needSkippedBranch.add(switchResult.getNextNode());
        }
        if (CollectionUtils.isNotEmpty(switchResult.getDependTaskList())) {
            for (SwitchResultVo switchResultVo : switchResult.getDependTaskList()) {
                needSkippedBranch.add(switchResultVo.getNextNode());
            }
        }
        needSkippedBranch.remove(switchParameters.getNextBranch());
        markTaskSkipped(taskExecutionRunnable, needSkippedBranch);
    }

    private void markTaskSkipped(final ITaskExecutionRunnable taskExecutionRunnable,
                                 final Collection<Long> needSkippedTaskCodes) {
        if (CollectionUtils.isEmpty(needSkippedTaskCodes)) {
            return;
        }
        final IWorkflowExecutionGraph workflowExecutionGraph = taskExecutionRunnable.getWorkflowExecutionGraph();
        for (Long taskCode : needSkippedTaskCodes) {
            final ITaskExecutionRunnable branch = workflowExecutionGraph.getTaskExecutionRunnableByTaskCode(taskCode);
            if (branch == null) {
                log.info("Branch(taskCode={}) is not found in the workflow: {}.", taskCode,
                        taskExecutionRunnable.getWorkflowInstance().getName());
                continue;
            }
            workflowExecutionGraph.markTaskSkipped(branch);
        }
    }

}
