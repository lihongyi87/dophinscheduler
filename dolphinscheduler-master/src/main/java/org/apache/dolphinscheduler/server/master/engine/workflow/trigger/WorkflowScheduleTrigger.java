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

package org.apache.dolphinscheduler.server.master.engine.workflow.trigger;

import org.apache.dolphinscheduler.common.enums.CommandType;
import org.apache.dolphinscheduler.common.enums.Flag;
import org.apache.dolphinscheduler.common.enums.WarningType;
import org.apache.dolphinscheduler.common.enums.WorkflowExecutionStatus;
import org.apache.dolphinscheduler.common.utils.DateUtils;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.dao.entity.Command;
import org.apache.dolphinscheduler.dao.entity.WorkflowDefinition;
import org.apache.dolphinscheduler.dao.entity.WorkflowInstance;
import org.apache.dolphinscheduler.dao.utils.EnvironmentUtils;
import org.apache.dolphinscheduler.dao.utils.WorkerGroupUtils;
import org.apache.dolphinscheduler.extract.master.command.ScheduleWorkflowCommandParam;
import org.apache.dolphinscheduler.extract.master.transportor.workflow.WorkflowScheduleTriggerRequest;
import org.apache.dolphinscheduler.extract.master.transportor.workflow.WorkflowScheduleTriggerResponse;

import org.apache.commons.lang3.ObjectUtils;

import java.util.Date;

import org.springframework.stereotype.Component;

/**
 * 工作流调度触发器
 * 
 * 这个类专门处理定时调度触发的工作流启动。当调度器按照预设的时间规则
 * 触发工作流执行时，会通过这个触发器创建工作流实例并生成执行命令。
 * 
 * 主要功能：
 * 1. 处理定时调度的工作流触发请求
 * 2. 基于调度配置构建工作流实例
 * 3. 设置调度特有的属性（如调度时间、时区等）
 * 4. 生成SCHEDULER类型的执行命令
 * 5. 返回触发结果给调度器
 * 
 * 与手动触发的区别：
 * - 命令类型为SCHEDULER而非START_PROCESS
 * - 包含调度时间和时区信息
 * - 由调度系统自动触发，不是用户手动操作
 * 
 * 简单理解：就像一个"定时启动工作流"的自动化装置，
 * 按照设定的时间表自动启动对应的工作流。
 */
@Component
public class WorkflowScheduleTrigger
        extends
            AbstractWorkflowTrigger<WorkflowScheduleTriggerRequest, WorkflowScheduleTriggerResponse> {

    /**
     * 构建工作流实例
     * 
     * 基于调度触发请求创建一个新的工作流实例，设置调度相关的属性。
     * 
     * @param scheduleTriggerRequest 调度触发请求，包含调度配置信息
     * @return 构建完成的工作流实例
     */
    @Override
    protected WorkflowInstance constructWorkflowInstance(WorkflowScheduleTriggerRequest scheduleTriggerRequest) {
        final CommandType commandType = CommandType.SCHEDULER;
        final Long workflowCode = scheduleTriggerRequest.getWorkflowCode();
        final Integer workflowVersion = scheduleTriggerRequest.getWorkflowVersion();
        final WorkflowDefinition workflowDefinition = getProcessDefinition(workflowCode, workflowVersion);

        final WorkflowInstance workflowInstance = new WorkflowInstance();
        // 设置工作流定义相关信息
        workflowInstance.setWorkflowDefinitionCode(workflowDefinition.getCode());
        workflowInstance.setWorkflowDefinitionVersion(workflowDefinition.getVersion());
        workflowInstance.setProjectCode(workflowDefinition.getProjectCode());
        
        // 设置命令类型和初始状态
        workflowInstance.setCommandType(commandType);
        workflowInstance.setStateWithDesc(WorkflowExecutionStatus.SUBMITTED_SUCCESS, commandType.name());
        workflowInstance.setRecovery(Flag.NO);
        
        // 设置调度特有的时间信息
        workflowInstance.setScheduleTime(scheduleTriggerRequest.getScheduleTIme());  // 调度时间
        workflowInstance.setStartTime(new Date());  // 实际启动时间
        workflowInstance.setRestartTime(workflowInstance.getStartTime());
        workflowInstance.setRunTimes(1);
        
        // 生成工作流实例名称（包含时间戳）
        workflowInstance.setName(String.join("-", workflowDefinition.getName(), DateUtils.getCurrentTimeStamp()));
        
        // 设置执行策略和告警配置
        workflowInstance.setTaskDependType(scheduleTriggerRequest.getTaskDependType());
        workflowInstance.setFailureStrategy(scheduleTriggerRequest.getFailureStrategy());
        workflowInstance
                .setWarningType(ObjectUtils.defaultIfNull(scheduleTriggerRequest.getWarningType(), WarningType.NONE));
        workflowInstance.setWarningGroupId(scheduleTriggerRequest.getWarningGroupId());
        
        // 设置执行用户信息
        workflowInstance.setExecutorId(scheduleTriggerRequest.getUserId());
        workflowInstance.setExecutorName(getExecutorUser(scheduleTriggerRequest.getUserId()).getUserName());
        workflowInstance.setTenantCode(scheduleTriggerRequest.getTenantCode());
        
        // 设置其他属性
        workflowInstance.setIsSubWorkflow(Flag.NO);
        workflowInstance.addHistoryCmd(commandType);
        workflowInstance.setWorkflowInstancePriority(scheduleTriggerRequest.getWorkflowInstancePriority());
        workflowInstance
                .setWorkerGroup(WorkerGroupUtils.getWorkerGroupOrDefault(scheduleTriggerRequest.getWorkerGroup()));
        workflowInstance.setEnvironmentCode(
                EnvironmentUtils.getEnvironmentCodeOrDefault(scheduleTriggerRequest.getEnvironmentCode()));
        workflowInstance.setTimeout(workflowDefinition.getTimeout());
        workflowInstance.setDryRun(scheduleTriggerRequest.getDryRun().getCode());
        
        return workflowInstance;
    }

    /**
     * 构建触发命令
     * 
     * 为调度触发创建相应的执行命令，包含调度特有的参数。
     * 
     * @param scheduleTriggerRequest 调度触发请求
     * @param workflowInstance 已创建的工作流实例
     * @return 构建完成的执行命令
     */
    @Override
    protected Command constructTriggerCommand(final WorkflowScheduleTriggerRequest scheduleTriggerRequest,
                                              final WorkflowInstance workflowInstance) {
        // 构建调度命令参数，包含时区信息
        final ScheduleWorkflowCommandParam scheduleWorkflowCommandParam = ScheduleWorkflowCommandParam.builder()
                .timeZone(scheduleTriggerRequest.getTimezoneId())
                .build();
                
        // 创建SCHEDULER类型的命令
        return Command.builder()
                .commandType(CommandType.SCHEDULER)
                .workflowDefinitionCode(scheduleTriggerRequest.getWorkflowCode())
                .workflowDefinitionVersion(scheduleTriggerRequest.getWorkflowVersion())
                .workflowInstanceId(workflowInstance.getId())
                .workflowInstancePriority(workflowInstance.getWorkflowInstancePriority())
                .commandParam(JSONUtils.toJsonString(scheduleWorkflowCommandParam))
                .build();
    }

    /**
     * 触发成功后的处理
     * 
     * 当工作流成功触发后，返回成功响应给调度器。
     * 
     * @param workflowInstance 已触发的工作流实例
     * @return 调度触发成功响应
     */
    @Override
    protected WorkflowScheduleTriggerResponse onTriggerSuccess(WorkflowInstance workflowInstance) {
        return WorkflowScheduleTriggerResponse.success(workflowInstance.getId());
    }
}
