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

package org.apache.dolphinscheduler.server.master.engine.workflow.listener;

import org.apache.dolphinscheduler.common.enums.CommandType;
import org.apache.dolphinscheduler.common.enums.Flag;
import org.apache.dolphinscheduler.common.utils.DateUtils;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.dao.entity.WorkflowInstance;
import org.apache.dolphinscheduler.dao.repository.CommandDao;
import org.apache.dolphinscheduler.extract.master.command.BackfillWorkflowCommandParam;
import org.apache.dolphinscheduler.extract.master.command.ICommandParam;
import org.apache.dolphinscheduler.extract.master.transportor.workflow.WorkflowBackfillTriggerRequest;
import org.apache.dolphinscheduler.extract.master.transportor.workflow.WorkflowBackfillTriggerResponse;
import org.apache.dolphinscheduler.server.master.engine.workflow.lifecycle.AbstractWorkflowLifecycleLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.workflow.lifecycle.WorkflowLifecycleEventType;
import org.apache.dolphinscheduler.server.master.engine.workflow.runnable.IWorkflowExecutionRunnable;
import org.apache.dolphinscheduler.server.master.engine.workflow.trigger.WorkflowBackfillTrigger;

import org.apache.commons.collections4.CollectionUtils;

import java.util.List;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * 工作流成功生命周期监听器
 * <p>
 * 监听工作流成功事件，主要用于处理补数（backfill）场景下的后续工作流实例生成。
 * 当工作流成功完成时，如果是补数类型的命令，会自动生成下一个时间段的补数工作流实例。
 * </p>
 * <p>
 * 主要功能：
 * 1. 监听工作流成功事件
 * 2. 检查是否为补数类型的工作流
 * 3. 生成下一个补数时间段的工作流实例
 * 4. 过滤子工作流（子工作流不需要生成补数命令）
 * </p>
 *
 * @author DolphinScheduler
 */
@Slf4j
@Component
public class WorkflowSuccessLifecycleListener implements IWorkflowLifecycleListener {

    /**
     * 工作流补数触发器
     * 用于触发新的补数工作流实例
     */
    @Autowired
    private WorkflowBackfillTrigger workflowBackfillTrigger;

    /**
     * 命令数据访问对象
     * 用于操作工作流命令相关的数据库操作
     */
    @Autowired
    private CommandDao commandDao;

    /**
     * 处理工作流生命周期事件通知
     * <p>
     * 当工作流成功完成时，检查是否需要生成下一个补数工作流实例。
     * 处理逻辑：
     * 1. 检查是否为子工作流（子工作流跳过处理）
     * 2. 解析命令参数，验证是否为补数类型
     * 3. 如果是补数类型，生成下一个时间段的补数命令
     * </p>
     *
     * @param workflowExecutionRunnable 工作流执行运行时对象
     * @param lifecycleEvent 工作流生命周期事件
     */
    public void notifyWorkflowLifecycleEvent(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                                             final AbstractWorkflowLifecycleLifecycleEvent lifecycleEvent) {
        // 获取工作流实例
        final WorkflowInstance workflowInstance = workflowExecutionRunnable.getWorkflowInstance();
        if (Flag.YES == workflowInstance.getIsSubWorkflow()) {
            // 子工作流不需要生成补数命令
            // 因为父工作流会触发任务来生成子工作流实例
            return;
        }

        // 解析工作流命令参数
        final ICommandParam commandParam =
                JSONUtils.parseObject(workflowInstance.getCommandParam(), ICommandParam.class);
        if (commandParam == null) {
            log.warn("Command param: {} is invalid for workflow: {}", workflowInstance.getCommandParam(),
                    workflowInstance.getName());
            return;
        }
        // 只处理补数类型的命令
        if (commandParam.getCommandType() != CommandType.COMPLEMENT_DATA) {
            return;
        }
        // 生成下一个补数命令
        generateNextBackfillCommand((BackfillWorkflowCommandParam) commandParam, workflowInstance);
    }

    /**
     * 生成下一个补数命令
     * <p>
     * 从补数时间列表中移除当前已完成的时间，如果还有剩余时间需要补数，
     * 则创建新的补数工作流触发请求。
     * </p>
     *
     * @param commandParam 补数工作流命令参数
     * @param workflowInstance 当前工作流实例
     */
    private void generateNextBackfillCommand(final BackfillWorkflowCommandParam commandParam,
                                             final WorkflowInstance workflowInstance) {
        // 生成下一个补数命令
        final List<String> backfillTimeList = commandParam.getBackfillTimeList();
        // 从补数时间列表中移除当前已完成的调度时间
        backfillTimeList.remove(DateUtils.dateToString(workflowInstance.getScheduleTime()));
        // 如果没有剩余的补数时间，则结束处理
        if (CollectionUtils.isEmpty(backfillTimeList)) {
            return;
        }
        // 构建补数工作流触发请求，复用当前工作流实例的配置参数
        final WorkflowBackfillTriggerRequest backfillTriggerRequest = WorkflowBackfillTriggerRequest.builder()
                .userId(workflowInstance.getExecutorId())                                    // 执行用户ID
                .backfillTimeList(backfillTimeList)                                         // 剩余的补数时间列表
                .workflowCode(workflowInstance.getWorkflowDefinitionCode())                  // 工作流定义代码
                .workflowVersion(workflowInstance.getWorkflowDefinitionVersion())            // 工作流定义版本
                .startNodes(commandParam.getStartNodes())                                   // 起始节点列表
                .failureStrategy(workflowInstance.getFailureStrategy())                     // 失败策略
                .taskDependType(workflowInstance.getTaskDependType())                       // 任务依赖类型
                .warningType(workflowInstance.getWarningType())                             // 告警类型
                .warningGroupId(workflowInstance.getWarningGroupId())                       // 告警组ID
                .workflowInstancePriority(workflowInstance.getWorkflowInstancePriority())   // 工作流实例优先级
                .workerGroup(workflowInstance.getWorkerGroup())                             // Worker组
                .tenantCode(workflowInstance.getTenantCode())                               // 租户代码
                .environmentCode(workflowInstance.getEnvironmentCode())                     // 环境代码
                .startParamList(commandParam.getCommandParams())                           // 启动参数列表
                .dryRun(Flag.of(workflowInstance.getDryRun()))                              // 是否为试运行
                .build();
        // 触发补数工作流
        final WorkflowBackfillTriggerResponse backfillTriggerResponse =
                workflowBackfillTrigger.triggerWorkflow(backfillTriggerRequest);
        // 检查触发结果，记录失败信息
        if (!backfillTriggerResponse.isSuccess()) {
            log.warn("Backfill workflow failed: {}", backfillTriggerResponse.getMessage());
        }
    }

    /**
     * 判断是否匹配工作流生命周期事件
     * <p>
     * 此监听器只处理工作流成功事件，用于在工作流成功完成后
     * 生成后续的补数工作流实例。
     * </p>
     *
     * @param event 工作流生命周期事件
     * @return true-匹配成功事件；false-不匹配其他事件
     */
    @Override
    public boolean match(AbstractWorkflowLifecycleLifecycleEvent event) {
        // 只匹配工作流成功事件
        return event.getEventType() == WorkflowLifecycleEventType.SUCCEED;
    }

}
