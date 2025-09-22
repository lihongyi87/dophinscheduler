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

package org.apache.dolphinscheduler.api.service.impl;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.dolphinscheduler.common.constants.CommandKeyConstants.CMD_PARAM_COMPLEMENT_DATA_END_DATE;
import static org.apache.dolphinscheduler.common.constants.CommandKeyConstants.CMD_PARAM_COMPLEMENT_DATA_SCHEDULE_DATE_LIST;
import static org.apache.dolphinscheduler.common.constants.CommandKeyConstants.CMD_PARAM_COMPLEMENT_DATA_START_DATE;
import static org.apache.dolphinscheduler.common.constants.CommandKeyConstants.CMD_PARAM_RECOVER_WORKFLOW_ID_STRING;
import static org.apache.dolphinscheduler.common.constants.CommandKeyConstants.CMD_PARAM_START_NODES;
import static org.apache.dolphinscheduler.common.constants.CommandKeyConstants.CMD_PARAM_SUB_WORKFLOW_DEFINITION_CODE;
import static org.apache.dolphinscheduler.common.constants.Constants.COMMA;

import org.apache.dolphinscheduler.api.constants.ApiFuncIdentificationConstant;
import org.apache.dolphinscheduler.api.dto.workflow.WorkflowBackFillRequest;
import org.apache.dolphinscheduler.api.dto.workflow.WorkflowTriggerRequest;
import org.apache.dolphinscheduler.api.dto.workflowInstance.WorkflowExecuteResponse;
import org.apache.dolphinscheduler.api.enums.ExecuteType;
import org.apache.dolphinscheduler.api.enums.Status;
import org.apache.dolphinscheduler.api.exceptions.ServiceException;
import org.apache.dolphinscheduler.api.executor.workflow.ExecutorClient;
import org.apache.dolphinscheduler.api.service.ExecutorService;
import org.apache.dolphinscheduler.api.service.ProjectService;
import org.apache.dolphinscheduler.api.service.WorkerGroupService;
import org.apache.dolphinscheduler.api.service.WorkflowLineageService;
import org.apache.dolphinscheduler.api.validator.workflow.BackfillWorkflowDTO;
import org.apache.dolphinscheduler.api.validator.workflow.BackfillWorkflowDTOValidator;
import org.apache.dolphinscheduler.api.validator.workflow.BackfillWorkflowRequestTransformer;
import org.apache.dolphinscheduler.api.validator.workflow.TriggerWorkflowDTO;
import org.apache.dolphinscheduler.api.validator.workflow.TriggerWorkflowDTOValidator;
import org.apache.dolphinscheduler.api.validator.workflow.TriggerWorkflowRequestTransformer;
import org.apache.dolphinscheduler.common.constants.Constants;
import org.apache.dolphinscheduler.common.enums.CommandType;
import org.apache.dolphinscheduler.common.enums.ComplementDependentMode;
import org.apache.dolphinscheduler.common.enums.CycleEnum;
import org.apache.dolphinscheduler.common.enums.ExecutionOrder;
import org.apache.dolphinscheduler.common.enums.Flag;
import org.apache.dolphinscheduler.common.enums.ReleaseState;
import org.apache.dolphinscheduler.common.enums.RunMode;
import org.apache.dolphinscheduler.common.enums.TaskDependType;
import org.apache.dolphinscheduler.common.utils.DateUtils;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.dao.entity.Command;
import org.apache.dolphinscheduler.dao.entity.DependentWorkflowDefinition;
import org.apache.dolphinscheduler.dao.entity.Schedule;
import org.apache.dolphinscheduler.dao.entity.TaskDefinition;
import org.apache.dolphinscheduler.dao.entity.TaskGroupQueue;
import org.apache.dolphinscheduler.dao.entity.Tenant;
import org.apache.dolphinscheduler.dao.entity.User;
import org.apache.dolphinscheduler.dao.entity.WorkflowDefinition;
import org.apache.dolphinscheduler.dao.entity.WorkflowInstance;
import org.apache.dolphinscheduler.dao.entity.WorkflowTaskRelation;
import org.apache.dolphinscheduler.dao.mapper.TaskDefinitionLogMapper;
import org.apache.dolphinscheduler.dao.mapper.TaskDefinitionMapper;
import org.apache.dolphinscheduler.dao.mapper.TaskGroupQueueMapper;
import org.apache.dolphinscheduler.dao.mapper.TenantMapper;
import org.apache.dolphinscheduler.dao.mapper.WorkflowDefinitionMapper;
import org.apache.dolphinscheduler.dao.mapper.WorkflowTaskRelationMapper;
import org.apache.dolphinscheduler.dao.repository.WorkflowInstanceDao;
import org.apache.dolphinscheduler.plugin.task.api.utils.TaskTypeUtils;
import org.apache.dolphinscheduler.service.command.CommandService;
import org.apache.dolphinscheduler.service.cron.CronUtils;
import org.apache.dolphinscheduler.service.exceptions.CronParseException;
import org.apache.dolphinscheduler.service.process.ProcessService;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.google.common.base.Splitter;

/**
 * 工作流执行服务实现类
 *
 * <p>该类实现了工作流实例的执行管理功能，是DolphinScheduler调度系统的
 * 核心组件。处理工作流的启动、停止、暂停、恢复、重跑、回填等所有
 * 执行相关操作。</p>
 *
 * <p>主要功能：</p>
 * <ul>
 *   <li>触发工作流执行 - 启动工作流实例</li>
 *   <li>回填执行 - 批量补跑历史数据</li>
 *   <li>执行控制 - 暂停、恢复、停止工作流实例</li>
 *   <li>重跑失败 - 从失败节点重新执行</li>
 *   <li>任务操作 - 单独执行或跳过某个任务</li>
 *   <li>参数传递 - 处理启动参数和全局参数</li>
 * </ul>
 *
 * <p>该类通过Command模式将执行请求转换为命令，由Master节点消费并执行。</p>
 */
@Service
@Slf4j
public class ExecutorServiceImpl extends BaseServiceImpl implements ExecutorService {

    @Autowired
    private ProjectService projectService;

    @Autowired
    private WorkflowDefinitionMapper workflowDefinitionMapper;

    @Lazy()
    @Autowired
    private ProcessService processService;

    @Autowired
    private WorkflowInstanceDao workflowInstanceDao;

    @Autowired
    private CommandService commandService;

    @Autowired
    private TaskDefinitionLogMapper taskDefinitionLogMapper;

    @Autowired
    private TaskDefinitionMapper taskDefinitionMapper;

    @Autowired
    private WorkflowTaskRelationMapper workflowTaskRelationMapper;

    @Autowired
    private TaskGroupQueueMapper taskGroupQueueMapper;

    @Autowired
    private WorkerGroupService workerGroupService;

    @Autowired
    private TenantMapper tenantMapper;

    @Autowired
    private WorkflowLineageService workflowLineageService;

    @Autowired
    private TriggerWorkflowRequestTransformer triggerWorkflowRequestTransformer;

    @Autowired
    private TriggerWorkflowDTOValidator triggerWorkflowDTOValidator;

    @Autowired
    private BackfillWorkflowRequestTransformer backfillWorkflowRequestTransformer;

    @Autowired
    private BackfillWorkflowDTOValidator backfillWorkflowDTOValidator;

    @Autowired
    private ExecutorClient executorClient;

    /**
     * 触发工作流执行
     */
    @Override
    @Transactional
    public Integer triggerWorkflowDefinition(final WorkflowTriggerRequest triggerRequest) {
        // 将触发请求转换为内部DTO对象
        final TriggerWorkflowDTO triggerWorkflowDTO = triggerWorkflowRequestTransformer.transform(triggerRequest);

        // 验证触发请求的合法性，包括工作流状态、参数等
        triggerWorkflowDTOValidator.validate(triggerWorkflowDTO);

        // 通过执行器客户端触发工作流，返回工作流实例ID
        return executorClient.triggerWorkflowDefinition().execute(triggerWorkflowDTO);
    }

    /**
     * 回填执行工作流（批量补跑历史数据）
     */
    @Override
    @Transactional
    public List<Integer> backfillWorkflowDefinition(final WorkflowBackFillRequest workflowBackFillRequest) {
        // 将回填请求转换为内部DTO对象
        final BackfillWorkflowDTO backfillWorkflowDTO =
                backfillWorkflowRequestTransformer.transform(workflowBackFillRequest);

        // 验证回填请求的合法性，包括时间范围、实例数量等
        backfillWorkflowDTOValidator.validate(backfillWorkflowDTO);

        // 执行回填操作，返回所有创建的工作流实例ID列表
        return executorClient.backfillWorkflowDefinition().execute(backfillWorkflowDTO);
    }

    /**
     * 检查工作流定义是否可以执行
     *
     * @param projectCode       项目编码
     * @param workflowDefinition 工作流定义
     * @param workflowDefinitionCode 工作流定义编码
     * @param version           工作流定义版本
     */
    @Override
    public void checkWorkflowDefinitionValid(long projectCode, WorkflowDefinition workflowDefinition,
                                             long workflowDefinitionCode, Integer version) {
        // 检查工作流定义是否存在：验证项目编码是否匹配
        // 防止跨项目访问工作流定义，确保资源隔离
        if (projectCode != workflowDefinition.getProjectCode()) {
            throw new ServiceException(Status.WORKFLOW_DEFINITION_NOT_EXIST, workflowDefinition.getCode());
        }

        // 检查工作流定义是否已上线：只有上线状态的工作流才能执行
        // 离线状态的工作流可能存在配置错误或正在修改中
        if (workflowDefinition.getReleaseState() != ReleaseState.ONLINE) {
            throw new ServiceException(Status.WORKFLOW_DEFINITION_NOT_RELEASE, workflowDefinition.getCode(),
                    workflowDefinition.getVersion());
        }

        // 检查子工作流定义是否都已上线：确保所有依赖的子工作流也处于可执行状态
        // 递归检查所有子工作流，防止执行过程中出现依赖问题
        if (!checkSubWorkflowDefinitionValid(workflowDefinition)) {
            throw new ServiceException(Status.SUB_WORKFLOW_DEFINITION_NOT_RELEASE);
        }
    }

    /**
     * 检查当前工作流是否包含子工作流并验证所有子工作流的有效性
     *
     * @param workflowDefinition 工作流定义
     * @return 检查结果，true表示所有子工作流都有效
     */
    @Override
    public boolean checkSubWorkflowDefinitionValid(WorkflowDefinition workflowDefinition) {
        // 查询当前工作流下的所有任务关系
        // 通过任务关系可以获取到工作流中包含的所有任务
        List<WorkflowTaskRelation> workflowTaskRelations =
                workflowTaskRelationMapper.queryDownstreamByWorkflowDefinitionCode(workflowDefinition.getCode());
        // 如果没有任务关系，说明是空工作流，直接返回有效
        if (workflowTaskRelations.isEmpty()) {
            return true;
        }
        // 从任务关系中提取所有后置任务的编码
        Set<Long> relationCodes =
                workflowTaskRelations.stream().map(WorkflowTaskRelation::getPostTaskCode).collect(Collectors.toSet());
        // 根据任务编码查询任务定义
        List<TaskDefinition> taskDefinitions = taskDefinitionMapper.queryByCodeList(relationCodes);

        // 找出所有子工作流任务的工作流定义编码
        Set<Long> workflowDefinitionCodeSet = new HashSet<>();
        // 遍历所有任务定义，筛选出子工作流类型的任务
        taskDefinitions.stream()
                .filter(task -> TaskTypeUtils.isSubWorkflowTask(task.getTaskType())).forEach(
                        taskDefinition -> {
                            // 从任务参数中解析出子工作流定义编码
                            String subWorkflowCode = JSONUtils.getNodeString(taskDefinition.getTaskParams(),
                                    CMD_PARAM_SUB_WORKFLOW_DEFINITION_CODE);
                            if (subWorkflowCode != null) {
                                workflowDefinitionCodeSet.add(Long.valueOf(subWorkflowCode));
                            }
                        });
        // 如果没有子工作流任务，返回有效
        if (workflowDefinitionCodeSet.isEmpty()) {
            return true;
        }

        // 检查所有子工作流的发布状态
        List<WorkflowDefinition> workflowDefinitions = workflowDefinitionMapper.queryByCodes(workflowDefinitionCodeSet);
        // 如果存在任何离线状态的子工作流，则返回false
        // 使用stream过滤出所有离线状态的工作流，如果集合为空说明都是在线状态
        return workflowDefinitions.stream()
                .filter(definition -> definition.getReleaseState().equals(ReleaseState.OFFLINE))
                .collect(Collectors.toSet())
                .isEmpty();
    }

    /**
     * 检查租户是否有效
     *
     * @param tenantCode 租户编码
     */
    private void checkValidTenant(String tenantCode) {
        // 如果不是默认租户，需要验证租户是否存在
        // 默认租户是系统内置的，无需验证
        if (!Constants.DEFAULT.equals(tenantCode)) {
            // 根据租户编码查询租户信息
            Tenant tenant = tenantMapper.queryByTenantCode(tenantCode);
            // 如果租户不存在，抛出异常
            if (tenant == null) {
                throw new ServiceException(Status.TENANT_NOT_EXIST, tenantCode);
            }
        }
    }

    /**
     * 控制工作流实例的执行状态
     *
     * @param loginUser          登录用户
     * @param workflowInstanceId 工作流实例ID
     * @param executeType        执行类型（重跑、暂停、停止等）
     */
    @Override
    public void controlWorkflowInstance(User loginUser, Integer workflowInstanceId, ExecuteType executeType) {
        // 参数非空检查，确保输入参数的有效性
        checkNotNull(workflowInstanceId, "workflowInstanceId cannot be null");
        checkNotNull(executeType, "executeType cannot be null");

        // 查询工作流实例，如果不存在则抛出异常
        WorkflowInstance workflowInstance = workflowInstanceDao
                .queryOptionalById(workflowInstanceId)
                .orElseThrow(() -> new ServiceException(Status.WORKFLOW_INSTANCE_NOT_EXIST, workflowInstanceId));

        // 检查用户对项目的访问权限
        // 根据不同的执行类型检查相应的权限
        projectService.checkProjectAndAuthThrowException(
                loginUser,
                workflowInstance.getProjectCode(),
                ApiFuncIdentificationConstant.map.get(executeType));

        // 根据执行类型分发到不同的处理逻辑
        switch (executeType) {
            case REPEAT_RUNNING:
                // 重跑工作流实例：从头开始重新执行整个工作流
                executorClient
                        .repeatRunningWorkflowInstance()
                        .onWorkflowInstance(workflowInstance)
                        .byUser(loginUser)
                        .execute();
                return;
            case START_FAILURE_TASK_PROCESS:
                // 从失败任务开始恢复：只重跑失败的任务节点
                executorClient.recoverFailureTaskInstance()
                        .onWorkflowInstance(workflowInstance)
                        .byUser(loginUser)
                        .execute();
                return;
            case RECOVER_SUSPENDED_PROCESS:
                // 恢复暂停的工作流：继续执行被暂停的工作流实例
                executorClient.recoverSuspendedWorkflowInstanceOperation()
                        .onWorkflowInstance(workflowInstance)
                        .byUser(loginUser)
                        .execute();
                return;
            case PAUSE:
                // 暂停工作流实例：停止后续任务的执行，但保持当前状态
                executorClient.pauseWorkflowInstance()
                        .onWorkflowInstance(workflowInstance)
                        .byUser(loginUser)
                        .execute();
                return;
            case STOP:
                // 停止工作流实例：强制终止工作流的执行
                executorClient.stopWorkflowInstance()
                        .onWorkflowInstance(workflowInstance)
                        .byUser(loginUser)
                        .execute();
                return;
            default:
                // 不支持的执行类型，抛出异常
                throw new ServiceException("Unsupported executeType: " + executeType);
        }
    }

    /**
     * 在工作流实例中执行指定任务
     *
     * @param loginUser         登录用户
     * @param projectCode       项目编码
     * @param workflowInstanceId 工作流实例ID
     * @param startNodeList     起始节点列表
     * @param taskDependType    任务依赖类型
     * @return 执行结果
     */
    @Override
    public WorkflowExecuteResponse executeTask(User loginUser,
                                               long projectCode,
                                               Integer workflowInstanceId,
                                               String startNodeList,
                                               TaskDependType taskDependType) {

        // 初始化响应对象
        WorkflowExecuteResponse response = new WorkflowExecuteResponse();

        // 检查用户对项目的访问权限，确保用户有执行任务的权限
        projectService.checkProjectAndAuthThrowException(loginUser, projectCode,
                ApiFuncIdentificationConstant.map.get(ExecuteType.EXECUTE_TASK));

        // 查询工作流实例详细信息，如果实例不存在则抛出异常
        WorkflowInstance workflowInstance = processService.findWorkflowInstanceDetailById(workflowInstanceId)
                .orElseThrow(() -> new ServiceException(Status.WORKFLOW_INSTANCE_NOT_EXIST, workflowInstanceId));

        // 检查工作流实例是否已经完成，只有完成的工作流才能执行单独的任务
        // 这是为了避免在运行中的工作流中执行额外任务造成状态混乱
        if (!workflowInstance.getState().isFinished()) {
            log.error("无法在未完成的工作流实例中执行任务，工作流实例ID: {}", workflowInstanceId);
            putMsg(response, Status.WORKFLOW_INSTANCE_IS_NOT_FINISHED);
            return response;
        }

        // 获取工作流定义信息
        WorkflowDefinition workflowDefinition =
                processService.findWorkflowDefinition(workflowInstance.getWorkflowDefinitionCode(),
                        workflowInstance.getWorkflowDefinitionVersion());
        // 设置为在线状态以通过后续验证
        workflowDefinition.setReleaseState(ReleaseState.ONLINE);
        // 验证工作流定义的有效性
        this.checkWorkflowDefinitionValid(projectCode, workflowDefinition, workflowInstance.getWorkflowDefinitionCode(),
                workflowInstance.getWorkflowDefinitionVersion());

        // 解析起始节点列表，转换为长整型任务编码
        long startNodeListLong;
        try {
            startNodeListLong = Long.parseLong(startNodeList);
        } catch (NumberFormatException e) {
            log.error("起始节点列表不是有效的数字格式: {}", startNodeList);
            putMsg(response, Status.REQUEST_PARAMS_NOT_VALID_ERROR, startNodeList);
            return response;
        }

        // 验证任务定义是否存在，确保要执行的任务是已定义的
        if (taskDefinitionLogMapper.queryMaxVersionForDefinition(startNodeListLong) == null) {
            log.error("要执行的任务未定义，任务编码: {}", startNodeListLong);
            putMsg(response, Status.EXECUTE_NOT_DEFINE_TASK);
            return response;
        }

        // 构建命令参数，用于传递给Master进行任务执行
        Map<String, Object> cmdParam = new HashMap<>();
        // 设置要恢复的工作流实例ID
        cmdParam.put(CMD_PARAM_RECOVER_WORKFLOW_ID_STRING, workflowInstanceId);
        // 设置起始节点列表
        cmdParam.put(CMD_PARAM_START_NODES, startNodeList);

        // 创建执行命令对象
        Command command = new Command();
        // 设置命令类型为执行任务
        command.setCommandType(CommandType.EXECUTE_TASK);
        // 设置工作流定义编码
        command.setWorkflowDefinitionCode(workflowDefinition.getCode());
        // 将参数转换为JSON字符串
        command.setCommandParam(JSONUtils.toJsonString(cmdParam));
        // 设置执行者ID
        command.setExecutorId(loginUser.getId());
        // 设置工作流定义版本
        command.setWorkflowDefinitionVersion(workflowDefinition.getVersion());
        // 设置工作流实例ID
        command.setWorkflowInstanceId(workflowInstanceId);
        // 设置任务依赖类型
        command.setTaskDependType(taskDependType);

        // 验证是否需要创建命令，防止重复创建相同的命令
        if (!commandService.verifyIsNeedCreateCommand(command)) {
            log.warn("工作流实例正在执行命令中，工作流定义编码: {}, 版本: {}, 实例ID: {}",
                    workflowDefinition.getCode(), workflowDefinition.getVersion(), workflowInstanceId);
            putMsg(response, Status.WORKFLOW_INSTANCE_EXECUTING_COMMAND,
                    String.valueOf(workflowDefinition.getCode()));
            return response;
        }

        // 创建命令并记录日志
        log.info("正在创建执行命令，命令信息: {}", command);
        int create = commandService.createCommand(command);

        // 检查命令创建结果
        if (create > 0) {
            log.info("创建 {} 命令成功，工作流定义编码: {}, 版本: {}",
                    command.getCommandType().getDescp(), command.getWorkflowDefinitionCode(),
                    workflowDefinition.getVersion());
            putMsg(response, Status.SUCCESS);
        } else {
            log.error("执行工作流实例失败，创建 {} 命令时出错，工作流定义编码: {}, 版本: {}, 实例ID: {}",
                    command.getCommandType().getDescp(), command.getWorkflowDefinitionCode(),
                    workflowDefinition.getVersion(), workflowInstanceId);
            putMsg(response, Status.EXECUTE_WORKFLOW_INSTANCE_ERROR);
        }

        return response;
    }

    /**
     * 强制启动任务实例
     * 用于任务组队列中的任务强制启动，跳过队列等待
     *
     * @param loginUser 登录用户
     * @param queueId   队列ID
     * @return 执行结果
     */
    @Override
    public Map<String, Object> forceStartTaskInstance(User loginUser, int queueId) {
        // 初始化结果映射
        Map<String, Object> result = new HashMap<>();

        // 根据队列ID查询任务组队列信息
        TaskGroupQueue taskGroupQueue = taskGroupQueueMapper.selectById(queueId);

        // 检查工作流实例是否存在，确保任务组队列对应的工作流实例有效
        workflowInstanceDao.queryOptionalById(taskGroupQueue.getWorkflowInstanceId())
                .orElseThrow(() -> new ServiceException(Status.WORKFLOW_INSTANCE_NOT_EXIST,
                        taskGroupQueue.getWorkflowInstanceId()));

        // 检查任务是否还在队列中，如果已经不在队列中则无法强制启动
        if (taskGroupQueue.getInQueue() == Flag.NO.getCode()) {
            throw new ServiceException(Status.TASK_GROUP_QUEUE_ALREADY_START);
        }

        // 设置强制启动标志，标记该任务需要立即执行
        taskGroupQueue.setForceStart(Flag.YES.getCode());
        // 更新最后修改时间
        taskGroupQueue.setUpdateTime(new Date());
        // 保存更新到数据库
        taskGroupQueueMapper.updateById(taskGroupQueue);

        // 设置成功状态并返回结果
        result.put(Constants.STATUS, Status.SUCCESS);
        return result;
    }

    /**
     * 创建补数据命令
     *
     * @param triggerCode              触发编码
     * @param command                  命令对象
     * @param cmdParam                 命令参数
     * @param dateTimeList             日期时间列表
     * @param schedules                调度列表
     * @param complementDependentMode  补数据依赖模式
     * @param allLevelDependent        是否全级别依赖
     * @return 创建的命令数量
     */
    private int createComplementCommand(Long triggerCode, Command command, Map<String, String> cmdParam,
                                        List<ZonedDateTime> dateTimeList, List<Schedule> schedules,
                                        ComplementDependentMode complementDependentMode, boolean allLevelDependent) {

        // 将日期时间列表转换为逗号分隔的字符串
        // 用于传递给Master节点进行批量处理
        String dateTimeListStr = dateTimeList.stream()
                .map(item -> DateUtils.dateToString(item))
                .collect(Collectors.joining(COMMA));

        // 将日期列表添加到命令参数中
        cmdParam.put(CMD_PARAM_COMPLEMENT_DATA_SCHEDULE_DATE_LIST, dateTimeListStr);
        // 将参数对象序列化为JSON字符串
        command.setCommandParam(JSONUtils.toJsonString(cmdParam));

        // 记录创建命令的日志
        log.info("正在创建补数据命令，命令信息: {}", command);
        // 调用命令服务创建命令
        int createCount = commandService.createCommand(command);

        // 检查命令创建结果并记录日志
        if (createCount > 0) {
            log.info("创建 {} 命令成功，工作流定义编码: {}",
                    command.getCommandType().getDescp(), command.getWorkflowDefinitionCode());
        } else {
            log.error("创建 {} 命令失败，工作流定义编码: {}",
                    command.getCommandType().getDescp(), command.getWorkflowDefinitionCode());
        }

        // 根据补数据依赖模式决定是否创建依赖命令
        if (schedules.isEmpty() || complementDependentMode == ComplementDependentMode.OFF_MODE) {
            // 关闭依赖模式或没有调度信息，跳过依赖命令创建
            log.info("补数据依赖模式为关闭或调度为空，跳过创建补数据依赖命令，工作流定义编码: {}",
                    command.getWorkflowDefinitionCode());
        } else {
            // 开启依赖模式且有调度信息，需要创建依赖命令
            log.info("补数据依赖模式为全依赖且调度不为空，需要创建补数据依赖命令，工作流定义编码: {}",
                    command.getWorkflowDefinitionCode());
            // 创建补数据依赖命令
            createComplementDependentCommand(schedules, command, allLevelDependent);
        }

        return createCount;
    }

    /**
     * 创建补数据命令列表
     * 根据不同的运行模式（串行/并行）创建补数据命令
     *
     * @param triggerCode              触发编码
     * @param scheduleTimeParam        调度时间参数
     * @param runMode                  运行模式
     * @param command                  命令对象
     * @param expectedParallelismNumber 期望的并行数
     * @param complementDependentMode  补数据依赖模式
     * @param allLevelDependent        是否全级别依赖
     * @param executionOrder           执行顺序
     * @return 创建的命令数量
     */
    protected int createComplementCommandList(Long triggerCode, String scheduleTimeParam, RunMode runMode,
                                              Command command,
                                              Integer expectedParallelismNumber,
                                              ComplementDependentMode complementDependentMode,
                                              boolean allLevelDependent,
                                              ExecutionOrder executionOrder) throws CronParseException {
        // 初始化创建计数器
        int createCount = 0;
        int dependentWorkflowDefinitionCreateCount = 0;

        // 如果运行模式为空，默认使用串行模式
        runMode = (runMode == null) ? RunMode.RUN_MODE_SERIAL : runMode;

        // 解析命令参数和调度参数
        Map<String, String> cmdParam = JSONUtils.toMap(command.getCommandParam());
        Map<String, String> scheduleParam = JSONUtils.toMap(scheduleTimeParam);

        // 如果执行顺序为空，默认使用降序
        if (Objects.isNull(executionOrder)) {
            executionOrder = ExecutionOrder.DESC_ORDER;
        }

        // 查询工作流的调度配置信息
        List<Schedule> schedules = processService.queryReleaseSchedulerListByWorkflowDefinitionCode(
                command.getWorkflowDefinitionCode());

        // 初始化日期列表
        List<ZonedDateTime> listDate = new ArrayList<>();

        // 处理日期范围模式：通过开始和结束日期生成日期列表
        if (scheduleParam.containsKey(CMD_PARAM_COMPLEMENT_DATA_START_DATE) && scheduleParam.containsKey(
                CMD_PARAM_COMPLEMENT_DATA_END_DATE)) {
            String startDate = scheduleParam.get(CMD_PARAM_COMPLEMENT_DATA_START_DATE);
            String endDate = scheduleParam.get(CMD_PARAM_COMPLEMENT_DATA_END_DATE);
            if (startDate != null && endDate != null) {
                // 根据cron表达式和日期范围生成执行日期列表
                listDate = CronUtils.getSelfFireDateList(
                        DateUtils.stringToZoneDateTime(startDate),
                        DateUtils.stringToZoneDateTime(endDate),
                        schedules);
            }
        }

        // 处理日期列表模式：直接指定具体的日期列表
        if (scheduleParam.containsKey(CMD_PARAM_COMPLEMENT_DATA_SCHEDULE_DATE_LIST)) {
            String dateList = scheduleParam.get(CMD_PARAM_COMPLEMENT_DATA_SCHEDULE_DATE_LIST);

            if (StringUtils.isNotBlank(dateList)) {
                // 解析逗号分隔的日期字符串，转换为日期对象列表
                listDate = Splitter.on(COMMA).splitToStream(dateList)
                        .map(item -> DateUtils.stringToZoneDateTime(item.trim()))
                        .distinct() // 去重
                        .collect(Collectors.toList());
            }
        }

        // 检查日期列表是否为空，为空则抛出异常
        if (CollectionUtils.isEmpty(listDate)) {
            throw new ServiceException(Status.TASK_COMPLEMENT_DATA_DATE_ERROR);
        }

        // 根据指定的执行顺序对日期列表进行排序
        if (executionOrder.equals(ExecutionOrder.DESC_ORDER)) {
            // 降序排列：从最新日期开始执行
            Collections.sort(listDate, Collections.reverseOrder());
        } else {
            // 升序排列：从最早日期开始执行
            Collections.sort(listDate);
        }

        // 根据运行模式采用不同的处理策略
        switch (runMode) {
            case RUN_MODE_SERIAL: {
                // 串行模式：所有日期在一个命令中顺序执行
                log.info("{} 命令的运行模式为串行，工作流定义编码: {}",
                        command.getCommandType().getDescp(), command.getWorkflowDefinitionCode());
                createCount = createComplementCommand(triggerCode, command, cmdParam, listDate, schedules,
                        complementDependentMode, allLevelDependent);
                break;
            }
            case RUN_MODE_PARALLEL: {
                // 并行模式：将日期列表分配到多个队列中并行执行
                log.info("{} 命令的运行模式为并行，工作流定义编码: {}",
                        command.getCommandType().getDescp(), command.getWorkflowDefinitionCode());

                int queueNum = 0;
                if (CollectionUtils.isNotEmpty(listDate)) {
                    // 计算队列数量：默认等于日期数量
                    queueNum = listDate.size();
                    // 如果指定了期望的并行数，则使用较小值防止过度并行
                    if (expectedParallelismNumber != null && expectedParallelismNumber != 0) {
                        queueNum = Math.min(queueNum, expectedParallelismNumber);
                    }
                    log.info("补数据命令在并行模式下运行，当前期望并行数: {}", queueNum);

                    // 创建队列数组用于分配日期
                    List[] queues = new List[queueNum];

                    // 使用轮询算法将日期分配到不同的队列中
                    for (int i = 0; i < listDate.size(); i++) {
                        if (Objects.isNull(queues[i % queueNum])) {
                            queues[i % queueNum] = new ArrayList();
                        }
                        queues[i % queueNum].add(listDate.get(i));
                    }

                    // 为每个队列创建一个补数据命令，实现并行执行
                    for (List queue : queues) {
                        createCount = createComplementCommand(triggerCode, command, cmdParam, queue, schedules,
                                complementDependentMode, allLevelDependent);
                    }
                }
                break;
            }
            default:
                break;
        }

        // 记录创建结果的统计信息
        log.info("创建补数据命令数量: {}, 创建依赖补数据命令数量: {}", createCount,
                dependentWorkflowDefinitionCreateCount);
        return createCount;
    }

    /**
     * 创建补数据依赖命令
     * 为依赖当前工作流的其他工作流创建补数据命令
     *
     * @param schedules         调度列表
     * @param command           原始命令
     * @param allLevelDependent 是否全级别依赖
     * @return 创建的依赖命令数量
     */
    public int createComplementDependentCommand(List<Schedule> schedules, Command command, boolean allLevelDependent) {
        // 初始化依赖工作流定义创建计数器
        int dependentWorkflowDefinitionCreateCount = 0;
        Command dependentCommand;

        try {
            // 克隆原始命令作为依赖命令的模板
            dependentCommand = (Command) BeanUtils.cloneBean(command);
        } catch (Exception e) {
            log.error("复制依赖命令出错", e);
            return dependentWorkflowDefinitionCreateCount;
        }

        // 获取补数据依赖定义列表
        // 根据工作流的调度周期查找所有依赖该工作流的其他工作流
        List<DependentWorkflowDefinition> dependentWorkflowDefinitionList =
                getComplementDependentDefinitionList(dependentCommand.getWorkflowDefinitionCode(),
                        CronUtils.getMaxCycle(schedules.get(0).getCrontab()), dependentCommand.getWorkerGroup(),
                        allLevelDependent);

        // 设置依赖任务类型为后置依赖，表示在主工作流执行后执行
        dependentCommand.setTaskDependType(TaskDependType.TASK_POST);

        // 遍历所有依赖的工作流定义，为每个创建依赖命令
        for (DependentWorkflowDefinition dependentWorkflowDefinition : dependentWorkflowDefinitionList) {
            // 清空ID以防止MyBatis-Plus自增主键冲突
            // 在克隆对象时，如果ID是整数类型，会获取到自增ID导致重复
            dependentCommand.setId(null);
            // 设置依赖工作流的定义编码
            dependentCommand.setWorkflowDefinitionCode(dependentWorkflowDefinition.getWorkflowDefinitionCode());
            // 设置依赖工作流的定义版本
            dependentCommand.setWorkflowDefinitionVersion(dependentWorkflowDefinition.getWorkflowDefinitionVersion());
            // 设置工作组
            dependentCommand.setWorkerGroup(dependentWorkflowDefinition.getWorkerGroup());

            // 更新命令参数，指定从哪个任务节点开始执行
            Map<String, String> cmdParam = JSONUtils.toMap(dependentCommand.getCommandParam());
            cmdParam.put(CMD_PARAM_START_NODES, String.valueOf(dependentWorkflowDefinition.getTaskDefinitionCode()));
            dependentCommand.setCommandParam(JSONUtils.toJsonString(cmdParam));

            // 记录创建依赖命令的日志
            log.info("正在创建补数据依赖命令，命令信息: {}", command);
            // 创建依赖命令并累加计数
            dependentWorkflowDefinitionCreateCount += commandService.createCommand(dependentCommand);
        }

        return dependentWorkflowDefinitionCreateCount;
    }

    /**
     * get complement dependent online workflow definition list
     */
    private List<DependentWorkflowDefinition> getComplementDependentDefinitionList(long workflowDefinitionCode,
                                                                                   CycleEnum workflowDefinitionCycle,
                                                                                   String workerGroup,
                                                                                   boolean allLevelDependent) {
        List<DependentWorkflowDefinition> dependentWorkflowDefinitionList =
                checkDependentWorkflowDefinitionValid(
                        workflowLineageService.queryDownstreamDependentWorkflowDefinitions(workflowDefinitionCode),
                        workflowDefinitionCycle, workerGroup,
                        workflowDefinitionCode);

        if (dependentWorkflowDefinitionList.isEmpty()) {
            return dependentWorkflowDefinitionList;
        }

        if (allLevelDependent) {
            List<DependentWorkflowDefinition> childList = new ArrayList<>(dependentWorkflowDefinitionList);
            while (true) {
                List<DependentWorkflowDefinition> childDependentList = childList
                        .stream()
                        .flatMap(dependentWorkflowDefinition -> checkDependentWorkflowDefinitionValid(
                                workflowLineageService.queryDownstreamDependentWorkflowDefinitions(
                                        dependentWorkflowDefinition.getWorkflowDefinitionCode()),
                                workflowDefinitionCycle,
                                workerGroup,
                                dependentWorkflowDefinition.getWorkflowDefinitionCode()).stream())
                        .collect(Collectors.toList());
                if (childDependentList.isEmpty()) {
                    break;
                }
                dependentWorkflowDefinitionList.addAll(childDependentList);
                childList = new ArrayList<>(childDependentList);
            }
        }
        return dependentWorkflowDefinitionList;
    }

    /**
     * Check whether the dependency cycle of the dependent node is consistent with the schedule cycle of
     * the dependent workflow definition and if there is no worker group in the schedule, use the complement selection's
     * worker group
     */
    private List<DependentWorkflowDefinition> checkDependentWorkflowDefinitionValid(
                                                                                    List<DependentWorkflowDefinition> dependentWorkflowDefinitionList,
                                                                                    CycleEnum workflowDefinitionCycle,
                                                                                    String workerGroup,
                                                                                    long upstreamWorkflowDefinitionCode) {
        List<DependentWorkflowDefinition> validDependentWorkflowDefinitionList = new ArrayList<>();

        List<Long> workflowDefinitionCodeList =
                dependentWorkflowDefinitionList.stream().map(DependentWorkflowDefinition::getWorkflowDefinitionCode)
                        .collect(Collectors.toList());

        Map<Long, String> workflowDefinitionWorkerGroupMap =
                workerGroupService.queryWorkerGroupByWorkflowDefinitionCodes(workflowDefinitionCodeList);

        for (DependentWorkflowDefinition dependentWorkflowDefinition : dependentWorkflowDefinitionList) {
            if (dependentWorkflowDefinition
                    .getDependentCycle(upstreamWorkflowDefinitionCode) == workflowDefinitionCycle) {
                if (workflowDefinitionWorkerGroupMap
                        .get(dependentWorkflowDefinition.getWorkflowDefinitionCode()) == null) {
                    dependentWorkflowDefinition.setWorkerGroup(workerGroup);
                }

                validDependentWorkflowDefinitionList.add(dependentWorkflowDefinition);
            }
        }

        return validDependentWorkflowDefinitionList;
    }

    /**
     * @param schedule
     * @return check error return 0, otherwise 1
     */
    private boolean isValidateScheduleTime(String schedule) {
        Map<String, String> scheduleResult = JSONUtils.toMap(schedule);
        if (scheduleResult == null) {
            return false;
        }
        if (scheduleResult.containsKey(CMD_PARAM_COMPLEMENT_DATA_SCHEDULE_DATE_LIST)) {
            if (scheduleResult.get(CMD_PARAM_COMPLEMENT_DATA_SCHEDULE_DATE_LIST) == null) {
                return false;
            }
        }
        if (scheduleResult.containsKey(CMD_PARAM_COMPLEMENT_DATA_START_DATE)) {
            String startDate = scheduleResult.get(CMD_PARAM_COMPLEMENT_DATA_START_DATE);
            String endDate = scheduleResult.get(CMD_PARAM_COMPLEMENT_DATA_END_DATE);
            if (startDate == null || endDate == null) {
                return false;
            }
            try {
                ZonedDateTime start = DateUtils.stringToZoneDateTime(startDate);
                ZonedDateTime end = DateUtils.stringToZoneDateTime(endDate);
                if (start == null || end == null) {
                    return false;
                }
                if (start.isAfter(end)) {
                    log.error(
                            "Complement data parameter error, start time should be before end time, startDate:{}, endDate:{}.",
                            start, end);
                    return false;
                }
            } catch (Exception ex) {
                log.warn("Parse schedule time error, startDate:{}, endDate:{}.", startDate, endDate);
                return false;
            }
        }
        return true;
    }

    /**
     * 执行流式任务实例
     * 注意：当前版本不支持此功能
     *
     * @param loginUser            登录用户
     * @param projectCode          项目编码
     * @param taskDefinitionCode   任务定义编码
     * @param taskDefinitionVersion 任务定义版本
     * @param warningGroupId       告警组ID
     * @param workerGroup          工作组
     * @param tenantCode           租户编码
     * @param environmentCode      环境编码
     * @param startParams          启动参数
     * @param dryRun               是否为干跑模式
     */
    @Override
    public void execStreamTaskInstance(User loginUser,
                                       long projectCode,
                                       long taskDefinitionCode,
                                       int taskDefinitionVersion,
                                       int warningGroupId,
                                       String workerGroup,
                                       String tenantCode,
                                       Long environmentCode,
                                       Map<String, String> startParams,
                                       int dryRun) {
        // 流式任务执行功能尚未实现，抛出不支持异常
        throw new ServiceException("Not supported");
    }
}
