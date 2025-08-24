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
import org.apache.dolphinscheduler.extract.master.command.RunWorkflowCommandParam;
import org.apache.dolphinscheduler.extract.master.transportor.workflow.WorkflowManualTriggerRequest;
import org.apache.dolphinscheduler.extract.master.transportor.workflow.WorkflowManualTriggerResponse;

import org.apache.commons.lang3.ObjectUtils;

import java.util.Date;

import org.springframework.stereotype.Component;

/**
 * 工作流手动触发器
 * 
 * 这个类专门处理用户手动触发的工作流启动。当用户通过Web界面或API
 * 手动启动工作流时，会通过这个触发器创建工作流实例并生成执行命令。
 * 
 * 主要功能：
 * 1. 处理用户手动触发的工作流启动请求
 * 2. 基于用户输入构建工作流实例
 * 3. 支持用户自定义参数和启动节点
 * 4. 生成START_PROCESS类型的执行命令
 * 5. 返回触发结果给用户界面
 * 
 * 与调度触发的区别：
 * - 命令类型为START_PROCESS而非SCHEDULER
 * - 支持用户自定义启动参数和指定启动节点
 * - 由用户主动触发，不是系统自动调度
 * - 包含用户当前的时区信息
 * 
 * 简单理解：就像一个"手动启动工作流"的操作按钮，
 * 用户点击后立即启动对应的工作流。
 */
@Component
public class WorkflowManualTrigger
        extends
            AbstractWorkflowTrigger<WorkflowManualTriggerRequest, WorkflowManualTriggerResponse> {

    /**
     * 构建工作流实例
     * 
     * 基于用户的手动触发请求创建一个新的工作流实例，设置用户指定的属性。
     * 
     * @param workflowManualTriggerRequest 手动触发请求，包含用户配置信息
     * @return 构建完成的工作流实例
     */
    @Override
    protected WorkflowInstance constructWorkflowInstance(final WorkflowManualTriggerRequest workflowManualTriggerRequest) {
        final CommandType commandType = CommandType.START_PROCESS;
        final Long workflowCode = workflowManualTriggerRequest.getWorkflowDefinitionCode();
        final Integer workflowVersion = workflowManualTriggerRequest.getWorkflowDefinitionVersion();
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
        
        // 设置启动时间（手动触发立即执行）
        workflowInstance.setStartTime(new Date());
        workflowInstance.setRestartTime(workflowInstance.getStartTime());
        workflowInstance.setRunTimes(1);
        
        // 生成工作流实例名称（包含时间戳）
        workflowInstance.setName(String.join("-", workflowDefinition.getName(), DateUtils.getCurrentTimeStamp()));
        
        // 设置用户指定的执行策略和告警配置
        workflowInstance.setTaskDependType(workflowManualTriggerRequest.getTaskDependType());
        workflowInstance.setFailureStrategy(workflowManualTriggerRequest.getFailureStrategy());
        workflowInstance.setWarningType(
                ObjectUtils.defaultIfNull(workflowManualTriggerRequest.getWarningType(), WarningType.NONE));
        workflowInstance.setWarningGroupId(workflowManualTriggerRequest.getWarningGroupId());
        
        // 设置执行用户信息
        workflowInstance.setExecutorId(workflowManualTriggerRequest.getUserId());
        workflowInstance.setExecutorName(getExecutorUser(workflowManualTriggerRequest.getUserId()).getUserName());
        workflowInstance.setTenantCode(workflowManualTriggerRequest.getTenantCode());
        
        // 设置其他属性
        workflowInstance.setIsSubWorkflow(Flag.NO);
        workflowInstance.addHistoryCmd(commandType);
        workflowInstance.setWorkflowInstancePriority(workflowManualTriggerRequest.getWorkflowInstancePriority());
        workflowInstance.setWorkerGroup(
                WorkerGroupUtils.getWorkerGroupOrDefault(workflowManualTriggerRequest.getWorkerGroup()));
        workflowInstance.setEnvironmentCode(
                EnvironmentUtils.getEnvironmentCodeOrDefault(workflowManualTriggerRequest.getEnvironmentCode()));
        workflowInstance.setTimeout(workflowDefinition.getTimeout());
        workflowInstance.setDryRun(workflowManualTriggerRequest.getDryRun().getCode());
        
        return workflowInstance;
    }

    /**
     * 构建触发命令
     * 
     * 为手动触发创建相应的执行命令，包含用户指定的参数和启动节点。
     * 
     * @param workflowManualTriggerRequest 手动触发请求
     * @param workflowInstance 已创建的工作流实例
     * @return 构建完成的执行命令
     */
    @Override
    protected Command constructTriggerCommand(final WorkflowManualTriggerRequest workflowManualTriggerRequest,
                                              final WorkflowInstance workflowInstance) {
        // 构建运行命令参数，包含用户自定义参数和启动节点
        final RunWorkflowCommandParam runWorkflowCommandParam = RunWorkflowCommandParam.builder()
                .commandParams(workflowManualTriggerRequest.getStartParamList())  // 用户自定义参数
                .startNodes(workflowManualTriggerRequest.getStartNodes())  // 指定的启动节点
                .timeZone(DateUtils.getTimezone())  // 当前时区
                .build();
                
        // 创建START_PROCESS类型的命令
        return Command.builder()
                .commandType(CommandType.START_PROCESS)
                .workflowDefinitionCode(workflowManualTriggerRequest.getWorkflowDefinitionCode())
                .workflowDefinitionVersion(workflowManualTriggerRequest.getWorkflowDefinitionVersion())
                .workflowInstanceId(workflowInstance.getId())
                .workflowInstancePriority(workflowInstance.getWorkflowInstancePriority())
                .commandParam(JSONUtils.toJsonString(runWorkflowCommandParam))
                .build();
    }

    /**
     * 触发成功后的处理
     * 
     * 当工作流成功触发后，返回成功响应给用户界面。
     * 
     * @param workflowInstance 已触发的工作流实例
     * @return 手动触发成功响应
     */
    @Override
    protected WorkflowManualTriggerResponse onTriggerSuccess(WorkflowInstance workflowInstance) {
        return WorkflowManualTriggerResponse.success(workflowInstance.getId());
    }

}
