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

import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.dao.entity.WorkflowInstance;
import org.apache.dolphinscheduler.server.master.engine.WorkflowEventBus;
import org.apache.dolphinscheduler.server.master.engine.graph.IWorkflowExecutionGraph;
import org.apache.dolphinscheduler.server.master.engine.graph.IWorkflowGraph;
import org.apache.dolphinscheduler.server.master.engine.task.runnable.ITaskExecutionRunnable;
import org.apache.dolphinscheduler.server.master.engine.workflow.runnable.IWorkflowExecutionRunnable;
import org.apache.dolphinscheduler.server.master.runner.IWorkflowExecuteContext;

import java.util.List;
import java.util.stream.Collectors;

import lombok.experimental.UtilityClass;

import com.google.common.base.Strings;

/**
 * 工作流实例工具类
 *
 * 提供工作流实例和任务实例的日志格式化和信息显示工具方法。
 * 主要用于生成统一格式的详细信息日志，方便调试和问题排查。
 * 支持的功能包括：
 * 1. 工作流实例的详细信息日志输出
 * 2. 任务实例的详细信息日志输出
 * 3. 格式化的表格式显示，提高可读性
 *
 * @author DolphinScheduler
 * @since 3.2.0
 */
@UtilityClass
public class WorkflowInstanceUtils {

    /**
     * 记录工作流实例的详细信息
     *
     * 生成一个格式化的工作流实例详细信息日志，包含所有关键的工作流属性和状态。
     * 这个日志对于调试工作流执行问题和系统监控非常有用。
     *
     * 输出信息包括：
     * - 工作流实例名称和基本信息
     * - 命令类型和执行状态
     * - 起始节点和任务总数
     * - 执行主机和环境信息
     * - 时间信息（调度、开始、结束时间等）
     * - 事件总线状态等
     *
     * @param workflowExecutionRunnable 工作流执行可运行对象，包含工作流的所有执行信息
     * @return 格式化的工作流详细信息字符串，包含表格边框和排版格式
     */
    public static String logWorkflowInstanceInDetails(IWorkflowExecutionRunnable workflowExecutionRunnable) {
        final IWorkflowExecuteContext workflowExecuteContext = workflowExecutionRunnable.getWorkflowExecuteContext();
        final IWorkflowExecutionGraph workflowExecutionGraph = workflowExecuteContext.getWorkflowExecutionGraph();
        final IWorkflowGraph workflowGraph = workflowExecuteContext.getWorkflowGraph();
        final WorkflowInstance workflowInstance = workflowExecuteContext.getWorkflowInstance();
        final WorkflowEventBus workflowEventBus = workflowExecuteContext.getWorkflowEventBus();

        final List<String> startNodes = workflowExecutionGraph
                .getStartNodes()
                .stream()
                .map(ITaskExecutionRunnable::getName)
                .collect(Collectors.toList());

        final StringBuilder logBuilder = new StringBuilder();
        // set the length for '*'
        final int horizontalLineLength = 80;
        // Append the title and the centered "Workflow Instance Detail"
        final int titleLength = 40;
        final int leftSpaces = (horizontalLineLength - titleLength) / 2;
        final String centeredTitle = String.format("%" + leftSpaces + "s%s", "", "Workflow Instance Detail");
        logBuilder.append("\n").append(Strings.repeat("*", horizontalLineLength)).append("\n")
                .append(centeredTitle).append("\n")
                .append(Strings.repeat("*", horizontalLineLength)).append("\n")
                .append("Workflow Instance Name:  ").append(workflowInstance.getName()).append("\n")
                .append("Command Type:            ").append(workflowInstance.getCommandType()).append("\n")
                .append("State:                   ").append(workflowInstance.getState().name()).append("\n")
                .append("StartNodes:              ").append(startNodes).append("\n")
                .append("TotalTasks:              ")
                .append(workflowExecutionRunnable.getWorkflowExecuteContext().getWorkflowExecutionGraph()
                        .getAllTaskExecutionRunnable().size())
                .append("\n")
                .append("Host:                    ").append(workflowInstance.getHost()).append("\n")
                .append("Is SubWorkflow:          ").append(workflowInstance.getIsSubWorkflow().name()).append("\n")
                .append("Run Times:               ").append(workflowInstance.getRunTimes()).append("\n")
                .append("Tenant:                  ").append(workflowInstance.getTenantCode()).append("\n")
                .append("Work Group:              ").append(workflowInstance.getWorkerGroup()).append("\n")
                .append("EventBusSummary:         ").append(workflowEventBus.getWorkflowEventBusSummary()).append("\n")
                .append("Schedule Time:           ").append(workflowInstance.getScheduleTime()).append("\n")
                .append("Start Time:              ").append(workflowInstance.getStartTime()).append("\n")
                .append("Restart Time:            ").append(workflowInstance.getRestartTime()).append("\n")
                .append("End Time:                ").append(workflowInstance.getEndTime());
        return logBuilder.toString();
    }

    /**
     * 记录任务实例的详细信息
     *
     * 生成一个格式化的任务实例详细信息日志，包含任务的所有关键属性和执行信息。
     * 这个日志对于调试任务执行问题和性能分析非常有用。
     *
     * 输出信息包括：
     * - 任务名称和所属工作流
     * - 任务执行类型和状态
     * - 执行主机和任务类型
     * - 优先级和租户信息
     * - 时间信息（提交、开始、结束时间等）
     *
     * @param taskInstance 任务实例对象，包含任务的所有执行信息
     * @return 格式化的任务详细信息字符串，包含表格边框和排版格式
     */
    public String logTaskInstanceInDetail(TaskInstance taskInstance) {
        final StringBuilder logBuilder = new StringBuilder();
        // set the length for '*'
        final int horizontalLineLength = 80;
        // Append the title and the centered "Task Instance Detail"
        final int titleLength = 40;
        final int leftSpaces = (horizontalLineLength - titleLength) / 2;
        final String centeredTitle = String.format("%" + leftSpaces + "s%s", "", "Task Instance Detail");
        logBuilder.append("\n").append(Strings.repeat("*", horizontalLineLength)).append("\n")
                .append(centeredTitle).append("\n")
                .append(Strings.repeat("*", horizontalLineLength)).append("\n")
                .append("Task Name:              ").append(taskInstance.getName()).append("\n")
                .append("Workflow Instance Name: ").append(taskInstance.getWorkflowInstance().getName()).append("\n")
                .append("Task Execute Type:      ").append(taskInstance.getTaskExecuteType().getDesc()).append("\n")
                .append("Execute State:          ").append(taskInstance.getState().getDesc()).append("\n")
                .append("Host:                   ").append(taskInstance.getHost()).append("\n")
                .append("Task Type:              ").append(taskInstance.getTaskType()).append("\n")
                .append("Priority:               ").append(taskInstance.getTaskInstancePriority().getDescp())
                .append("\n")
                .append("Tenant:                 ").append(taskInstance.getWorkflowInstance().getTenantCode())
                .append("\n")
                .append("First Submit Time:      ").append(taskInstance.getFirstSubmitTime()).append("\n")
                .append("Submit Time:            ").append(taskInstance.getSubmitTime()).append("\n")
                .append("Start Time:             ").append(taskInstance.getStartTime()).append("\n")
                .append("End Time:               ").append(taskInstance.getEndTime()).append("\n");
        return logBuilder.toString();
    }
}
