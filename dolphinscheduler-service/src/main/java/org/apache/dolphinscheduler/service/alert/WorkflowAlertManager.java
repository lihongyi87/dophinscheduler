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

package org.apache.dolphinscheduler.service.alert;

import org.apache.dolphinscheduler.common.enums.AlertType;
import org.apache.dolphinscheduler.common.enums.CommandType;
import org.apache.dolphinscheduler.common.enums.Flag;
import org.apache.dolphinscheduler.common.enums.WarningType;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.dao.AlertDao;
import org.apache.dolphinscheduler.dao.entity.Alert;
import org.apache.dolphinscheduler.dao.entity.Project;
import org.apache.dolphinscheduler.dao.entity.ProjectUser;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.dao.entity.User;
import org.apache.dolphinscheduler.dao.entity.WorkflowAlertContent;
import org.apache.dolphinscheduler.dao.entity.WorkflowDefinitionLog;
import org.apache.dolphinscheduler.dao.entity.WorkflowInstance;
import org.apache.dolphinscheduler.dao.repository.ProjectDao;
import org.apache.dolphinscheduler.dao.repository.UserDao;
import org.apache.dolphinscheduler.dao.repository.WorkflowDefinitionLogDao;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * 工作流告警管理器
 *
 * 负责管理工作流和任务执行过程中的告警信息生成和发送。
 * 根据工作流和任务的执行状态，生成相应的告警内容并发送给指定用户。
 *
 * 告警类型：
 * - 工作流启动告警：工作流开始执行时发送
 * - 工作流成功告警：工作流成功完成时发送
 * - 工作流失败告警：工作流执行失败时发送
 * - 任务失败告警：单个任务执行失败时发送
 * - 任务超时告警：任务执行超时时发送
 * - 阻塞告警：工作流被阻塞时发送
 *
 * 主要功能：
 * - 生成告警内容：根据工作流/任务信息生成详细的告警内容
 * - 确定告警接收人：根据配置确定谁应该接收告警
 * - 发送告警：将告警信息保存到数据库，等待告警服务处理
 * - 告警策略：根据告警类型和用户配置决定是否发送
 *
 * 类比：就像系统的监控告警中心，监控各种事件，
 *      当发生异常或重要事件时，及时通知相关负责人。
 */
@Component
@Slf4j
public class WorkflowAlertManager {

    @Autowired
    private AlertDao alertDao;

    @Autowired
    private WorkflowDefinitionLogDao workflowDefinitionLogDao;

    @Autowired
    private UserDao userDao;

    @Autowired
    private ProjectDao projectDao;

    /**
     * 转换命令类型为可读名称
     *
     * 将命令类型枚举转换为人类可读的中文名称，
     * 用于告警内容的展示
     *
     * @param commandType 命令类型
     * @return 命令的中文名称
     */
    private String getCommandCnName(CommandType commandType) {
        switch (commandType) {
            case RECOVER_TOLERANCE_FAULT_PROCESS:
                return "recover fault tolerance workflow";
            case RECOVER_SUSPENDED_PROCESS:
                return "recover suspended workflow";
            case START_CURRENT_TASK_PROCESS:
                return "start current task workflow";
            case START_FAILURE_TASK_PROCESS:
                return "start failure task workflow";
            case START_PROCESS:
                return "start workflow";
            case REPEAT_RUNNING:
                return "repeat running";
            case SCHEDULER:
                return "scheduler";
            case COMPLEMENT_DATA:
                return "complement data";
            case PAUSE:
                return "pause";
            case STOP:
                return "stop";
            default:
                return "unknown type";
        }
    }

    /**
     * get workflow instance content
     *
     * @param workflowInstance workflow instance
     * @return workflow instance format content
     */
    public String getContentWorkflowInstance(WorkflowInstance workflowInstance,
                                             Project project) {

        String res;
        WorkflowDefinitionLog workflowDefinitionLog = workflowDefinitionLogDao
                .queryByDefinitionCodeAndVersion(workflowInstance.getWorkflowDefinitionCode(),
                        workflowInstance.getWorkflowDefinitionVersion());

        String modifyBy = "";
        if (workflowDefinitionLog != null) {
            User operator = userDao.queryById(workflowDefinitionLog.getOperator());
            modifyBy = operator == null ? "" : operator.getUserName();
        }

        List<WorkflowAlertContent> successTaskList = new ArrayList<>(1);
        WorkflowAlertContent workflowAlertContent = WorkflowAlertContent.builder()
                .projectCode(project.getCode())
                .projectName(project.getName())
                .owner(project.getUserName())
                .workflowInstanceId(workflowInstance.getId())
                .workflowDefinitionCode(workflowInstance.getWorkflowDefinitionCode())
                .workflowInstanceName(workflowInstance.getName())
                .commandType(workflowInstance.getCommandType())
                .workflowExecutionStatus(workflowInstance.getState())
                .modifyBy(modifyBy)
                .recovery(workflowInstance.getRecovery())
                .runTimes(workflowInstance.getRunTimes())
                .workflowStartTime(workflowInstance.getStartTime())
                .workflowEndTime(workflowInstance.getEndTime())
                .workflowHost(workflowInstance.getHost())
                .build();
        successTaskList.add(workflowAlertContent);
        res = JSONUtils.toJsonString(successTaskList);

        return res;
    }

    /**
     * getting worker fault tolerant content
     *
     * @param workflowInstance workflow instance
     * @param toleranceTaskList tolerance task list
     * @return worker tolerance content
     */
    private String getWorkerToleranceContent(WorkflowInstance workflowInstance, List<TaskInstance> toleranceTaskList) {

        List<WorkflowAlertContent> toleranceTaskInstanceList = new ArrayList<>();

        WorkflowDefinitionLog workflowDefinitionLog = workflowDefinitionLogDao
                .queryByDefinitionCodeAndVersion(workflowInstance.getWorkflowDefinitionCode(),
                        workflowInstance.getWorkflowDefinitionVersion());
        String modifyBy = "";
        if (workflowDefinitionLog != null) {
            User operator = userDao.queryById(workflowDefinitionLog.getOperator());
            modifyBy = operator == null ? "" : operator.getUserName();
        }

        for (TaskInstance taskInstance : toleranceTaskList) {
            WorkflowAlertContent workflowAlertContent = WorkflowAlertContent.builder()
                    .workflowInstanceId(workflowInstance.getId())
                    .workflowDefinitionCode(workflowInstance.getWorkflowDefinitionCode())
                    .workflowInstanceName(workflowInstance.getName())
                    .modifyBy(modifyBy)
                    .taskCode(taskInstance.getTaskCode())
                    .taskName(taskInstance.getName())
                    .taskHost(taskInstance.getHost())
                    .taskPriority(taskInstance.getTaskInstancePriority().getDescp())
                    .retryTimes(taskInstance.getRetryTimes())
                    .build();
            toleranceTaskInstanceList.add(workflowAlertContent);
        }
        return JSONUtils.toJsonString(toleranceTaskInstanceList);
    }

    /**
     * send worker alert fault tolerance
     *
     * @param workflowInstance workflow instance
     * @param toleranceTaskList tolerance task list
     */
    public void sendAlertWorkerToleranceFault(WorkflowInstance workflowInstance, List<TaskInstance> toleranceTaskList) {
        try {
            Alert alert = new Alert();
            alert.setTitle("worker fault tolerance");
            String content = getWorkerToleranceContent(workflowInstance, toleranceTaskList);
            alert.setContent(content);
            alert.setWarningType(WarningType.FAILURE);
            alert.setCreateTime(new Date());
            alert.setAlertGroupId(
                    workflowInstance.getWarningGroupId() == null ? 1 : workflowInstance.getWarningGroupId());
            alert.setAlertType(AlertType.FAULT_TOLERANCE_WARNING);
            alertDao.addAlert(alert);

        } catch (Exception e) {
            log.error("send alert failed:{} ", e.getMessage());
        }

    }

    /**
     * send workflow instance alert
     *
     * @param workflowInstance workflow instance
     */
    public void sendAlertWorkflowInstance(WorkflowInstance workflowInstance) {
        if (!isNeedToSendWarning(workflowInstance)) {
            return;
        }
        Project project = projectDao.queryByCode(workflowInstance.getProjectCode());

        Alert alert = new Alert();
        String cmdName = getCommandCnName(workflowInstance.getCommandType());
        String success = workflowInstance.getState().isSuccess() ? "success" : "failed";
        alert.setTitle(cmdName + " " + success);
        alert.setWarningType(workflowInstance.getState().isSuccess() ? WarningType.SUCCESS : WarningType.FAILURE);
        String content = getContentWorkflowInstance(workflowInstance, project);
        alert.setContent(content);
        alert.setAlertGroupId(workflowInstance.getWarningGroupId());
        alert.setCreateTime(new Date());
        alert.setProjectCode(workflowInstance.getProjectCode());
        alert.setWorkflowDefinitionCode(workflowInstance.getWorkflowDefinitionCode());
        alert.setWorkflowInstanceId(workflowInstance.getId());
        alert.setAlertType(workflowInstance.getState().isSuccess() ? AlertType.WORKFLOW_INSTANCE_SUCCESS
                : AlertType.WORKFLOW_INSTANCE_FAILURE);
        alertDao.addAlert(alert);
    }

    /**
     * check if need to be sent warning
     *
     * @param workflowInstance
     * @return
     */
    public boolean isNeedToSendWarning(WorkflowInstance workflowInstance) {
        if (Flag.YES == workflowInstance.getIsSubWorkflow()) {
            return false;
        }
        boolean sendWarning = false;
        WarningType warningType = workflowInstance.getWarningType();
        switch (warningType) {
            case ALL:
                if (workflowInstance.getState().isFinished()) {
                    sendWarning = true;
                }
                break;
            case SUCCESS:
                if (workflowInstance.getState().isSuccess()) {
                    sendWarning = true;
                }
                break;
            case FAILURE:
                if (workflowInstance.getState().isFailure()) {
                    sendWarning = true;
                }
                break;
            default:
        }
        return sendWarning;
    }

    public void sendTaskTimeoutAlert(WorkflowInstance workflowInstance,
                                     TaskInstance taskInstance,
                                     ProjectUser projectUser) {
        alertDao.sendTaskTimeoutAlert(workflowInstance, taskInstance, projectUser);
    }
}
