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

package org.apache.dolphinscheduler.server.master.engine.executor.plugin.condition;

import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.dao.repository.TaskInstanceDao;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.enums.DependResult;
import org.apache.dolphinscheduler.plugin.task.api.model.ConditionDependentItem;
import org.apache.dolphinscheduler.plugin.task.api.parameters.ConditionsParameters;
import org.apache.dolphinscheduler.plugin.task.api.utils.DependentUtils;
import org.apache.dolphinscheduler.server.master.engine.executor.plugin.AbstractLogicTask;
import org.apache.dolphinscheduler.server.master.engine.executor.plugin.ITaskParameterDeserializer;
import org.apache.dolphinscheduler.server.master.engine.workflow.runnable.IWorkflowExecutionRunnable;
import org.apache.dolphinscheduler.server.master.exception.MasterTaskExecuteException;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

import com.fasterxml.jackson.core.type.TypeReference;

/**
 * 条件逻辑任务实现
 * 根据上游任务的执行状态进行条件判断，决定工作流的后续执行路径
 *
 * 条件任务的核心功能：
 * 1. 检查指定的上游任务实例的执行状态
 * 2. 根据预定义的条件规则计算依赖结果
 * 3. 支持复杂的逻辑关系组合（AND/OR）
 * 4. 将条件计算结果存储到任务参数中，供下游任务使用
 *
 * 使用场景：
 * - 根据上游任务成功/失败状态决定后续执行分支
 * - 实现复杂的工作流分支控制逻辑
 * - 支持多任务状态的组合条件判断
 *
 * 注意：条件任务本身在Master节点执行，不需要提交到Worker节点
 *
 * @author DolphinScheduler Team
 */
@Slf4j
public class ConditionLogicTask extends AbstractLogicTask<ConditionsParameters> {

    /**
     * 任务实例数据访问对象
     * 用于查询工作流中其他任务实例的状态信息
     */
    private final TaskInstanceDao taskInstanceDao;

    /**
     * 当前任务实例
     * 存储条件计算结果的目标任务实例
     */
    private final TaskInstance taskInstance;

    /**
     * 构造函数 - 初始化条件逻辑任务
     *
     * @param workflowExecutionRunnable 工作流执行运行时对象，提供工作流上下文信息
     * @param taskExecutionContext 任务执行上下文，包含任务配置和环境信息
     * @param taskInstanceDao 任务实例数据访问对象，用于查询任务状态
     */
    public ConditionLogicTask(IWorkflowExecutionRunnable workflowExecutionRunnable,
                              TaskExecutionContext taskExecutionContext,
                              TaskInstanceDao taskInstanceDao) {
        super(taskExecutionContext);
        // 从工作流执行图中获取当前任务实例
        this.taskInstance = workflowExecutionRunnable
                .getWorkflowExecuteContext()
                .getWorkflowExecutionGraph()
                .getTaskExecutionRunnableById(taskExecutionContext.getTaskInstanceId())
                .getTaskInstance();
        this.taskInstanceDao = taskInstanceDao;
        // 将任务状态设置为运行中
        onTaskRunning();
    }

    /**
     * 启动条件任务执行
     * 这是条件任务的核心逻辑，计算条件依赖结果并更新任务状态
     */
    @Override
    public void start() {
        // 计算条件依赖的结果
        DependResult conditionResult = calculateConditionResult();
        log.info("The condition result is {}", conditionResult);

        // 将计算结果设置到任务参数中，供后续任务使用
        taskParameters.getConditionResult().setConditionSuccess(conditionResult == DependResult.SUCCESS);

        // 将更新后的参数保存到任务实例中
        taskInstance.setTaskParams(JSONUtils.toJsonString(taskParameters));

        // 条件任务总是成功完成（条件结果通过参数传递）
        onTaskSuccess();
    }

    /**
     * 计算条件依赖结果
     * 根据配置的依赖条件，检查相关任务状态并计算最终结果
     *
     * @return 条件计算的最终结果（SUCCESS/FAILED）
     */
    private DependResult calculateConditionResult() {
        // 查询当前工作流实例中的所有有效任务实例
        final List<TaskInstance> taskInstances = taskInstanceDao.queryValidTaskListByWorkflowInstanceId(
                taskExecutionContext.getWorkflowInstanceId());
        // 构建任务代码到任务实例的映射，便于快速查找
        final Map<Long, TaskInstance> taskInstanceMap = taskInstances.stream()
                .collect(Collectors.toMap(TaskInstance::getTaskCode, Function.identity()));

        // 获取条件依赖配置
        ConditionsParameters.ConditionDependency dependence = taskParameters.getDependence();

        // 计算每个依赖任务组的结果
        List<DependResult> dependResults = dependence.getDependTaskList()
                .stream()
                .map(dependentTaskModel -> DependentUtils.getDependResultForRelation(
                        dependentTaskModel.getRelation(), // 任务组内的逻辑关系（AND/OR）
                        dependentTaskModel.getDependItemList()
                                .stream()
                                .map(dependentItem -> getDependResultForItem((ConditionDependentItem) dependentItem,
                                        taskInstanceMap))
                                .collect(Collectors.toList())))
                .collect(Collectors.toList());

        // 根据顶层逻辑关系计算最终结果
        return DependentUtils.getDependResultForRelation(dependence.getRelation(), dependResults);
    }

    /**
     * 计算单个依赖项的结果
     * 检查指定任务的状态是否满足预期条件
     *
     * @param item 条件依赖项，包含期望的任务代码和状态
     * @param taskInstanceMap 任务实例映射，用于快速查找任务状态
     * @return 该依赖项的计算结果
     */
    private DependResult getDependResultForItem(ConditionDependentItem item, Map<Long, TaskInstance> taskInstanceMap) {
        // 根据任务代码查找对应的任务实例
        TaskInstance taskInstance = taskInstanceMap.get(item.getDepTaskCode());
        if (taskInstance == null) {
            // 如果找不到任务实例，说明任务还未执行或不存在
            log.info("The depend item: {} has not completed yet", DependResult.FAILED);
            log.info("The dependent result will be {}", DependResult.FAILED);
            return DependResult.FAILED;
        }

        // 比较期望状态与实际状态
        DependResult dependResult = Objects.equals(item.getStatus(), taskInstance.getState())
                ? DependResult.SUCCESS
                : DependResult.FAILED;

        log.info("The depend item: {}", item);
        log.info("Expect status: {}", item.getStatus());
        log.info("Actual status: {}", taskInstance.getState());
        log.info("The dependent result will be: {}", dependResult);
        return dependResult;
    }

    /**
     * 暂停操作
     * 条件任务不支持暂停操作，因为它是瞬时计算完成的
     */
    @Override
    public void pause() throws MasterTaskExecuteException {
        log.info("The ConditionTask does not support pause operation");
    }

    /**
     * 终止操作
     * 条件任务不支持终止操作，因为它是瞬时计算完成的
     */
    @Override
    public void kill() throws MasterTaskExecuteException {
        log.info("The ConditionTask does not support kill operation");
    }

    /**
     * 获取任务参数反序列化器
     * 用于将JSON格式的任务参数转换为ConditionsParameters对象
     *
     * @return 条件任务参数的反序列化器
     */
    @Override
    public ITaskParameterDeserializer<ConditionsParameters> getTaskParameterDeserializer() {
        return taskParamsJson -> JSONUtils.parseObject(taskParamsJson, new TypeReference<ConditionsParameters>() {
        });
    }

}
