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

package org.apache.dolphinscheduler.server.master.utils;

import static org.apache.dolphinscheduler.plugin.task.api.parameters.DependentParameters.DependentFailurePolicyEnum.DEPENDENT_FAILURE_WAITING;

import org.apache.dolphinscheduler.common.constants.Constants;
import org.apache.dolphinscheduler.common.enums.Flag;
import org.apache.dolphinscheduler.common.enums.TaskExecuteType;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.dao.entity.TaskDefinition;
import org.apache.dolphinscheduler.dao.entity.TaskDefinitionLog;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.dao.entity.WorkflowInstance;
import org.apache.dolphinscheduler.dao.entity.WorkflowTaskRelation;
import org.apache.dolphinscheduler.dao.repository.TaskDefinitionDao;
import org.apache.dolphinscheduler.dao.repository.TaskDefinitionLogDao;
import org.apache.dolphinscheduler.dao.repository.TaskInstanceDao;
import org.apache.dolphinscheduler.dao.repository.WorkflowInstanceDao;
import org.apache.dolphinscheduler.plugin.task.api.enums.DependResult;
import org.apache.dolphinscheduler.plugin.task.api.enums.DependentRelation;
import org.apache.dolphinscheduler.plugin.task.api.enums.Direct;
import org.apache.dolphinscheduler.plugin.task.api.enums.TaskExecutionStatus;
import org.apache.dolphinscheduler.plugin.task.api.model.DateInterval;
import org.apache.dolphinscheduler.plugin.task.api.model.DependentItem;
import org.apache.dolphinscheduler.plugin.task.api.model.Property;
import org.apache.dolphinscheduler.plugin.task.api.parameters.DependentParameters;
import org.apache.dolphinscheduler.plugin.task.api.utils.DependentUtils;
import org.apache.dolphinscheduler.service.bean.SpringApplicationContext;
import org.apache.dolphinscheduler.service.process.ProcessService;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * 依赖任务执行器
 *
 * 该类负责处理任务依赖关系的检查和执行逻辑，主要用于判断依赖的工作流或任务是否满足执行条件。
 * 支持多种依赖类型：
 * 1. 依赖整个工作流 (depend_workflow)
 * 2. 依赖工作流中所有任务 (depend_all)
 * 3. 依赖特定任务 (depend_task)
 *
 * @author DolphinScheduler
 * @since 3.2.0
 */
@Slf4j
public class DependentExecute {

    private final WorkflowInstanceDao workflowInstanceDao = SpringApplicationContext.getBean(WorkflowInstanceDao.class);

    private final TaskInstanceDao taskInstanceDao = SpringApplicationContext.getBean(TaskInstanceDao.class);

    /**
     * 依赖项列表
     * 包含需要检查的所有依赖条件
     */
    private List<DependentItem> dependItemList;

    /**
     * 依赖关系类型
     * 定义多个依赖项之间的逻辑关系（AND或OR）
     */
    private DependentRelation relation;

    private WorkflowInstance workflowInstance;

    private TaskInstance taskInstance;

    /**
     * 依赖结果映射
     * 存储每个依赖项的检查结果，键为依赖项标识，值为检查结果
     */
    @Getter
    private Map<String, DependResult> dependResultMap = new HashMap<>();

    /**
     * 流程服务
     * 提供工作流相关的数据访问和业务逻辑
     */
    private final ProcessService processService = SpringApplicationContext.getBean(ProcessService.class);

    /**
     * 任务定义日志数据访问对象
     * 用于查询任务定义的历史版本信息
     */
    private final TaskDefinitionLogDao taskDefinitionLogDao =
            SpringApplicationContext.getBean(TaskDefinitionLogDao.class);

    /**
     * 任务定义数据访问对象
     * 用于查询任务定义的基本信息
     */
    private final TaskDefinitionDao taskDefinitionDao = SpringApplicationContext.getBean(TaskDefinitionDao.class);

    @Getter
    private Map<String, Property> dependTaskVarPoolPropertyMap = new HashMap<>();

    @Getter
    private Map<String, Long> dependTaskVarPoolEndTimeMap = new HashMap<>();

    private Map<String, Property> dependItemVarPoolPropertyMap = new HashMap<>();

    private Map<String, Long> dependItemVarPoolEndTimeMap = new HashMap<>();

    /**
     * 构造函数
     *
     * @param itemList 依赖项列表，包含所有需要检查的依赖条件
     * @param relation 依赖关系类型，定义多个依赖项之间的逻辑关系
     * @param workflowInstance 当前工作流实例
     * @param taskInstance 当前任务实例
     */
    public DependentExecute(List<DependentItem> itemList, DependentRelation relation, WorkflowInstance workflowInstance,
                            TaskInstance taskInstance) {
        this.dependItemList = itemList;
        this.relation = relation;
        this.workflowInstance = workflowInstance;
        this.taskInstance = taskInstance;
    }

    /**
     * 获取单个依赖项的检查结果
     *
     * @param dependentItem 依赖项，包含依赖的目标工作流和任务信息
     * @param currentTime 当前时间，用于计算时间区间
     * @return 依赖检查结果（SUCCESS/FAILED/WAITING）
     */
    private DependResult getDependentResultForItem(DependentItem dependentItem, Date currentTime) {
        List<DateInterval> dateIntervals =
                DependentUtils.getDateIntervalList(currentTime, dependentItem.getDateValue());
        return calculateResultForTasks(dependentItem, dateIntervals);
    }

    /**
     * 计算单个依赖项的结果
     *
     * 根据指定的时间区间，查找并检查相应的工作流实例和任务实例状态。
     * 对于多个时间区间，只有当所有区间的依赖都满足时才返回成功。
     *
     * @param dependentItem 依赖项，包含目标工作流和任务的代码
     * @param dateIntervals 日期时间区间列表，用于查找匹配的工作流实例
     * @return 依赖结果（SUCCESS/FAILED/WAITING）
     */
    private DependResult calculateResultForTasks(DependentItem dependentItem,
                                                 List<DateInterval> dateIntervals) {

        DependResult result = DependResult.FAILED;
        for (DateInterval dateInterval : dateIntervals) {
            WorkflowInstance workflowInstance =
                    findDependentWorkflowCandidate(dependentItem.getDefinitionCode(), dependentItem.getDepTaskCode(),
                            dateInterval);
            if (workflowInstance == null) {
                return DependResult.WAITING;
            }
            // need to check workflow for updates, so get all task and check the task state
            if (dependentItem.getDepTaskCode() == Constants.DEPENDENT_WORKFLOW_CODE) {
                result = dependResultByWorkflowInstance(workflowInstance);
            } else if (dependentItem.getDepTaskCode() == Constants.DEPENDENT_ALL_TASK_CODE) {
                result = dependResultByAllTaskOfWorkflowInstance(workflowInstance);
            } else {
                result = dependResultBySingleTaskInstance(workflowInstance, dependentItem.getDepTaskCode());
            }
            if (result != DependResult.SUCCESS) {
                break;
            }
        }
        return result;
    }

    /**
     * 依赖类型为“依赖整个工作流”的结果检查
     *
     * 当依赖类型为 DEPENDENT_WORKFLOW_CODE 时，检查目标工作流的整体执行状态。
     * 只有当工作流执行完成且成功时，才返回成功结果。
     *
     * @param workflowInstance 目标工作流实例
     * @return 依赖检查结果（SUCCESS/FAILED/WAITING）
     */
    private DependResult dependResultByWorkflowInstance(WorkflowInstance workflowInstance) {
        if (!workflowInstance.getState().isFinished()) {
            return DependResult.WAITING;
        }
        if (workflowInstance.getState().isSuccess()) {
            addItemVarPool(workflowInstance.getVarPool(), workflowInstance.getEndTime().getTime());
            return DependResult.SUCCESS;
        }
        log.warn(
                "The dependent workflow did not execute successfully, so return depend failed. workflowDefinitionCode: {}, workflowInstanceName: {}",
                workflowInstance.getWorkflowDefinitionCode(), workflowInstance.getName());
        return DependResult.FAILED;
    }

    /**
     * 依赖类型为“依赖所有任务”的结果检查
     *
     * 当依赖类型为 DEPENDENT_ALL_TASK_CODE 时，检查目标工作流中所有任务的执行状态。
     * 只有当工作流中的所有任务都执行成功时，才返回成功结果。
     * 注意：流式任务（STREAM类型）不会被纳入检查范围。
     *
     * @param workflowInstance 目标工作流实例
     * @return 依赖检查结果（SUCCESS/FAILED/WAITING）
     */
    private DependResult dependResultByAllTaskOfWorkflowInstance(WorkflowInstance workflowInstance) {
        if (!workflowInstance.getState().isFinished()) {
            log.info(
                    "Wait for the dependent workflow to complete, workflowDefinitionCode: {}, pworkflowInstanceId: {}.",
                    workflowInstance.getWorkflowDefinitionCode(), workflowInstance.getId());
            return DependResult.WAITING;
        }
        if (workflowInstance.getState().isSuccess()) {
            List<WorkflowTaskRelation> workflowTaskRelations =
                    processService.findRelationByCode(workflowInstance.getWorkflowDefinitionCode(),
                            workflowInstance.getWorkflowDefinitionVersion());
            List<TaskDefinitionLog> taskDefinitionLogs =
                    taskDefinitionLogDao.queryTaskDefineLogList(workflowTaskRelations);
            Map<Long, String> taskDefinitionCodeMap =
                    taskDefinitionLogs.stream().filter(taskDefinitionLog -> taskDefinitionLog.getFlag() == Flag.YES)
                            .collect(Collectors.toMap(TaskDefinitionLog::getCode, TaskDefinitionLog::getName));

            List<TaskInstance> taskInstanceList =
                    taskInstanceDao.queryLastTaskInstanceListIntervalInWorkflowInstance(workflowInstance.getId(),
                            taskDefinitionCodeMap.keySet());
            Map<Long, TaskExecutionStatus> taskExecutionStatusMap =
                    taskInstanceList.stream()
                            .filter(taskInstance -> taskInstance.getTaskExecuteType() != TaskExecuteType.STREAM)
                            .collect(Collectors.toMap(TaskInstance::getTaskCode, TaskInstance::getState));

            for (Long taskCode : taskDefinitionCodeMap.keySet()) {
                if (!taskExecutionStatusMap.containsKey(taskCode)) {
                    log.warn(
                            "The task of the workflow is not being executed, taskCode: {}, workflowInstanceId: {}, workflowInstanceName: {}.",
                            taskCode, workflowInstance.getWorkflowDefinitionCode(), workflowInstance.getName());
                    return DependResult.FAILED;
                } else {
                    if (!taskExecutionStatusMap.get(taskCode).isSuccess()) {
                        log.warn(
                                "The task of the workflow is not being executed successfully, taskCode: {}, workflowInstanceId: {}, workflowInstanceName: {}.",
                                taskCode, workflowInstance.getWorkflowDefinitionCode(), workflowInstance.getName());
                        return DependResult.FAILED;
                    }
                }
            }
            addItemVarPool(workflowInstance.getVarPool(), workflowInstance.getEndTime().getTime());
            return DependResult.SUCCESS;
        }
        return DependResult.FAILED;
    }

    /**
     * 依赖类型为“依赖特定任务”的结果检查
     *
     * 当依赖类型为特定任务代码时，检查目标工作流中指定任务的执行状态。
     * 支持以下特殊情况的处理：
     * 1. 任务定义不存在：返回失败
     * 2. 任务被禁用（Flag.NO）：返回成功
     * 3. 流式任务：返回成功
     * 4. 任务实例不存在：根据工作流状态决定结果
     *
     * @param workflowInstance 日期区间内的最后一个工作流实例
     * @param depTaskCode 依赖的任务代码
     * @return 依赖检查结果（SUCCESS/FAILED/WAITING）
     */
    private DependResult dependResultBySingleTaskInstance(WorkflowInstance workflowInstance, long depTaskCode) {
        TaskInstance taskInstance =
                taskInstanceDao.queryLastTaskInstanceIntervalInWorkflowInstance(workflowInstance.getId(),
                        depTaskCode);

        if (taskInstance == null) {
            TaskDefinition taskDefinition = taskDefinitionDao.queryByCode(depTaskCode);

            if (taskDefinition == null) {
                log.error("The dependent task definition can not be find, so return depend failed, taskCode: {}",
                        depTaskCode);
                return DependResult.FAILED;
            }

            if (taskDefinition.getFlag() == Flag.NO) {
                log.info(
                        "The dependent task is a forbidden task, so return depend success. Task code: {}, task name: {}",
                        taskDefinition.getCode(), taskDefinition.getName());
                return DependResult.SUCCESS;
            }

            if (!workflowInstance.getState().isFinished()) {
                log.info(
                        "Wait for the dependent workflow to complete, workflowDefinitionCode: {}, workflowInstanceId: {}.",
                        workflowInstance.getWorkflowDefinitionCode(), workflowInstance.getId());
                return DependResult.WAITING;
            }

            return DependResult.FAILED;
        } else {
            if (TaskExecuteType.STREAM == taskInstance.getTaskExecuteType()) {
                log.info(
                        "The dependent task is a streaming task, so return depend success. Task code: {}, task name: {}.",
                        taskInstance.getTaskCode(), taskInstance.getName());
                addItemVarPool(taskInstance.getVarPool(), taskInstance.getEndTime().getTime());
                return DependResult.SUCCESS;
            }
            return getDependResultOfTask(workflowInstance, taskInstance);
        }
    }

    /**
     * 将变量池添加到依赖项变量池映射中
     *
     * 解析传入的变量池字符串，提取其中的输出变量（Direct.OUT），
     * 并将其添加到依赖项的变量池中，用于后续的参数传递。
     *
     * @param varPoolStr 变量池的JSON字符串表示
     * @param endTime 任务结束时间，用于记录变量的生成时间
     */
    private void addItemVarPool(String varPoolStr, Long endTime) {
        List<Property> varPool = new ArrayList<>(JSONUtils.toList(varPoolStr, Property.class));
        if (!varPool.isEmpty()) {
            Map<String, Property> varPoolPropertyMap = varPool.stream().filter(p -> p.getDirect().equals(Direct.OUT))
                    .collect(Collectors.toMap(Property::getProp, Function.identity()));
            Map<String, Long> varPoolEndTimeMap = varPool.stream().filter(p -> p.getDirect().equals(Direct.OUT))
                    .collect(Collectors.toMap(Property::getProp, d -> endTime));
            dependItemVarPoolPropertyMap.putAll(varPoolPropertyMap);
            dependItemVarPoolEndTimeMap.putAll(varPoolEndTimeMap);
        }
    }

    /**
     * 查找符合条件的最后一个工作流实例
     *
     * 按照以下优先级查找工作流实例：
     * 1. 日期区间内正在运行的工作流实例（最高优先级）
     * 2. 手动运行且在区间内完成的工作流实例
     * 3. 定时运行且调度时间在区间内的工作流实例
     *
     * 如果同时存在手动和定时工作流实例，则选择ID较大的（较新的）实例。
     *
     * @param definitionCode 工作流定义代码
     * @param taskCode 任务代码，用于过滤相关的工作流实例
     * @param dateInterval 日期时间区间，定义查找范围
     * @return 符合条件的工作流实例，没有找到则返回null
     */
    private WorkflowInstance findDependentWorkflowCandidate(Long definitionCode, Long taskCode,
                                                            DateInterval dateInterval) {
        WorkflowInstance runningWorkflow =
                workflowInstanceDao.queryLastRunningWorkflowInterval(definitionCode, dateInterval);
        if (runningWorkflow != null) {
            return runningWorkflow;
        }

        WorkflowInstance lastSchedulerWorkflowInstance =
                workflowInstanceDao.queryLastSchedulerWorkflowInterval(definitionCode, taskCode, dateInterval);

        WorkflowInstance lastManualWorkflowInstance =
                workflowInstanceDao.queryLastManualWorkflowInterval(definitionCode, taskCode, dateInterval);

        if (lastManualWorkflowInstance == null) {
            return lastSchedulerWorkflowInstance;
        }
        if (lastSchedulerWorkflowInstance == null) {
            return lastManualWorkflowInstance;
        }

        // In the time range, there are both manual and scheduled workflow instances, return the last workflow instance
        return lastManualWorkflowInstance.getId() > lastSchedulerWorkflowInstance.getId() ? lastManualWorkflowInstance
                : lastSchedulerWorkflowInstance;
    }

    /**
     * 根据任务/工作流实例获取依赖结果
     *
     * 检查任务实例的执行状态，并根据以下规则返回结果：
     * 1. 任务未完成：返回WAITING
     * 2. 任务成功：返回SUCCESS
     * 3. 任务失败但在重试范围内且工作流还在运行：返回WAITING
     * 4. 其他情况：返回FAILED
     *
     * @param workflowInstance 工作流实例，用于检查工作流状态
     * @param taskInstance 任务实例，用于检查任务状态和重试信息
     * @return 依赖检查结果（SUCCESS/FAILED/WAITING）
     */
    private DependResult getDependResultOfTask(WorkflowInstance workflowInstance, TaskInstance taskInstance) {

        TaskExecutionStatus state = taskInstance.getState();
        if (!state.isFinished()) {
            return DependResult.WAITING;
        } else if (state.isSuccess()) {
            return DependResult.SUCCESS;
        } else {
            if (workflowInstance.getState().isRunning()
                    && taskInstance.getRetryTimes() < taskInstance.getMaxRetryTimes()) {
                log.info("taskDefinitionCode: {}, taskDefinitionName: {}, retryTimes: {}, maxRetryTimes: {}",
                        taskInstance.getTaskCode(), taskInstance.getName(), taskInstance.getRetryTimes(),
                        taskInstance.getMaxRetryTimes());
                return DependResult.WAITING;
            }
            log.warn(
                    "The dependent task were not executed successfully, so return depend failed. Task code: {}, task name: {}.",
                    taskInstance.getTaskCode(), taskInstance.getName());
            return DependResult.FAILED;
        }
    }

    /**
     * 判断依赖项是否已完成检查
     *
     * 根据当前时间和失败策略来判断依赖检查是否完成。
     * 对于失败策略为DEPENDENT_FAILURE_WAITING的情况，
     * 会等待指定时间后才认为检查完成。
     *
     * @param currentTime 当前时间，用于计算等待时间
     * @param failurePolicy 失败处理策略，决定失败时的行为
     * @param failureWaitingTime 失败时的等待时间（分钟）
     * @return true表示依赖检查已完成，false表示还需要继续等待
     */
    public boolean finish(Date currentTime, DependentParameters.DependentFailurePolicyEnum failurePolicy,
                          Integer failureWaitingTime) {
        DependResult modelDependResult = getModelDependResult(currentTime);
        if (modelDependResult == DependResult.WAITING) {
            return false;
        } else if (modelDependResult == DependResult.FAILED && DEPENDENT_FAILURE_WAITING == failurePolicy
                && failureWaitingTime != null) {
            return Duration.between(currentTime.toInstant(), Instant.now())
                    .compareTo(Duration.ofMinutes(failureWaitingTime)) > 0;
        }
        return true;
    }

    /**
     * 获取模型依赖结果
     *
     * 遵循以下流程计算最终的依赖结果：
     * 1. 逐个检查每个依赖项的状态
     * 2. 对于自依赖且是首次运行的情况，默认为成功
     * 3. 对于需要参数传递的依赖项，收集其输出变量
     * 4. 根据依赖关系（AND/OR）计算最终结果
     *
     * @param currentTime 当前时间，用于依赖检查
     * @return 最终的依赖结果（SUCCESS/FAILED/WAITING）
     */
    public DependResult getModelDependResult(Date currentTime) {

        List<DependResult> dependResultList = new ArrayList<>();

        for (DependentItem dependentItem : dependItemList) {
            if (isSelfDependent(dependentItem) && isFirstWorkflowInstance(dependentItem)) {
                // if self-dependent, default success at first time
                dependResultMap.put(dependentItem.getKey(), DependResult.SUCCESS);
                dependResultList.add(DependResult.SUCCESS);
                log.info(
                        "This dependent item is self-dependent and run at first time, default success, workflowDefinitionCode:{}, depTaskCode:{}",
                        dependentItem.getDefinitionCode(), dependentItem.getDepTaskCode());
                continue;
            }
            DependResult dependResult = getDependResultForItem(dependentItem, currentTime);
            if (dependResult != DependResult.WAITING) {
                dependResultMap.put(dependentItem.getKey(), dependResult);
                if (dependentItem.getParameterPassing() && !dependItemVarPoolPropertyMap.isEmpty()) {
                    DependentUtils.addTaskVarPool(dependItemVarPoolPropertyMap, dependItemVarPoolEndTimeMap,
                            dependTaskVarPoolPropertyMap, dependTaskVarPoolEndTimeMap);
                }
            }
            dependItemVarPoolPropertyMap.clear();
            dependItemVarPoolEndTimeMap.clear();
            dependResultList.add(dependResult);
        }
        return DependentUtils.getDependResultForRelation(this.relation, dependResultList);
    }

    /**
     * 获取依赖项结果
     *
     * 先从缓存中查找已计算的结果，如果不存在则重新计算。
     * 这种缓存机制可以避免重复计算相同的依赖项。
     *
     * @param item 依赖项对象，包含依赖的目标信息
     * @param currentTime 当前时间，用于依赖检查
     * @return 依赖检查结果（SUCCESS/FAILED/WAITING）
     */
    private DependResult getDependResultForItem(DependentItem item, Date currentTime) {
        String key = item.getKey();
        if (dependResultMap.containsKey(key)) {
            return dependResultMap.get(key);
        }
        return getDependentResultForItem(item, currentTime);
    }

    /**
     * 检查是否为自依赖
     *
     * 判断依赖项是否指向当前正在执行的工作流或任务。
     * 自依赖的判断条件：
     * 1. 依赖的工作流定义代码与当前工作流相同
     * 2. 依赖所有任务（DEPENDENT_ALL_TASK_CODE）或依赖当前任务
     *
     * @param dependentItem 依赖项对象
     * @return true表示是自依赖，false表示不是自依赖
     */
    public boolean isSelfDependent(DependentItem dependentItem) {
        if (workflowInstance.getWorkflowDefinitionCode().equals(dependentItem.getDefinitionCode())) {
            if (dependentItem.getDepTaskCode() == Constants.DEPENDENT_ALL_TASK_CODE) {
                return true;
            }
            if (dependentItem.getDepTaskCode() == taskInstance.getTaskCode()) {
                return true;
            }
        }
        return false;
    }

    /**
     * 检查是否为首次运行
     *
     * 通过查询指定工作流定义的第一个工作流实例来判断当前实例是否为首次运行。
     * 查询优先级：
     * 1. 先按调度时间（scheduleTime）查询最早的实例
     * 2. 如果没有调度时间，则按开始时间（startTime）查询
     *
     * @param dependentItem 依赖项对象，包含目标工作流定义代码
     * @return true表示当前实例是首次运行，false表示不是首次运行
     */
    public boolean isFirstWorkflowInstance(DependentItem dependentItem) {
        WorkflowInstance firstWorkflowInstance =
                workflowInstanceDao.queryFirstScheduleWorkflowInstance(dependentItem.getDefinitionCode());
        if (firstWorkflowInstance == null) {
            firstWorkflowInstance =
                    workflowInstanceDao.queryFirstStartWorkflowInstance(dependentItem.getDefinitionCode());
            if (firstWorkflowInstance == null) {
                log.warn("First workflow instance is null, workflowDefinitionCode: {}",
                        dependentItem.getDefinitionCode());
                return false;
            }
        }
        return Objects.equals(firstWorkflowInstance.getId(), workflowInstance.getId());
    }
}
