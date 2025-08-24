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

package org.apache.dolphinscheduler.server.master.engine.command.handler;

import org.apache.dolphinscheduler.common.enums.CommandType;
import org.apache.dolphinscheduler.common.enums.WorkflowExecutionStatus;
import org.apache.dolphinscheduler.dao.entity.Command;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.dao.entity.WorkflowInstance;
import org.apache.dolphinscheduler.dao.repository.TaskInstanceDao;
import org.apache.dolphinscheduler.dao.repository.WorkflowInstanceDao;
import org.apache.dolphinscheduler.plugin.task.api.enums.TaskExecutionStatus;
import org.apache.dolphinscheduler.server.master.config.MasterConfig;
import org.apache.dolphinscheduler.server.master.engine.graph.IWorkflowGraph;
import org.apache.dolphinscheduler.server.master.engine.graph.WorkflowExecutionGraph;
import org.apache.dolphinscheduler.server.master.engine.graph.WorkflowGraphTopologyLogicalVisitor;
import org.apache.dolphinscheduler.server.master.engine.task.runnable.TaskExecutionRunnable;
import org.apache.dolphinscheduler.server.master.engine.task.runnable.TaskExecutionRunnableBuilder;
import org.apache.dolphinscheduler.server.master.engine.task.runnable.TaskInstanceFactories;
import org.apache.dolphinscheduler.server.master.runner.WorkflowExecuteContext.WorkflowExecuteContextBuilder;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import com.google.common.collect.Lists;

/**
 * 失败任务恢复命令处理器
 * 
 * 这是专门处理START_FAILURE_TASK_PROCESS命令的处理器，负责恢复执行失败、暂停、
 * 被杀死的任务，以及那些因前置任务失败而未被触发的后续任务。
 * 类比：就像一个"故障修复专家"，专门处理生产线故障后的恢复工作。
 * 
 * 核心职责：
 * 1. 故障诊断：分析工作流中哪些任务需要恢复执行
 * 2. 实例恢复：重置工作流实例状态，准备重新执行
 * 3. 任务筛选：识别需要重新执行的失败任务和被阻塞的后续任务
 * 4. 图重构：基于任务状态重新构建执行图
 * 5. 依赖恢复：恢复任务间的依赖关系和执行顺序
 * 
 * 恢复策略：
 * - 失败任务：重置为待执行状态，重新运行
 * - 暂停任务：从暂停状态恢复到运行状态
 * - 被杀任务：重置状态并重新调度执行
 * - 阻塞任务：解除阻塞，允许正常执行
 * - 依赖任务：重新评估依赖关系，恢复执行链
 * 
 * 适用场景：
 * - 故障恢复：系统故障导致的任务执行失败
 * - 手动恢复：用户手动终止后需要恢复执行
 * - 资源恢复：资源不足导致失败后的重新执行
 * - 依赖修复：修复依赖问题后的任务恢复
 * - 部分重跑：只恢复特定失败任务及其后续任务
 * 
 * 处理特点：
 * - 智能恢复：只处理需要恢复的任务，不影响已成功的任务
 * - 依赖感知：考虑任务间的依赖关系进行恢复
 * - 状态保持：保留工作流的原有配置和参数
 * - 增量执行：从失败点继续，而不是全部重新执行
 * 
 * 类比理解：
 * 就像工厂生产线故障修复：
 * - 诊断哪个工序出了问题（识别失败任务）
 * - 修复故障工序（恢复失败任务）
 * - 重启后续工序（恢复被阻塞的任务）
 * - 保持其他工序不变（不影响成功任务）
 * - 确保产品质量（维护数据一致性）
 */
@Component
public class RecoverFailureTaskCommandHandler extends AbstractCommandHandler {

    @Autowired
    private WorkflowInstanceDao workflowInstanceDao;

    @Autowired
    private TaskInstanceDao taskInstanceDao;

    @Autowired
    private ApplicationContext applicationContext;

    @Autowired
    private TaskInstanceFactories taskInstanceFactories;

    @Autowired
    private MasterConfig masterConfig;

    /**
     * Generate the recover workflow instance.
     * <p> Will use the origin workflow instance, but will update the following fields. Need to note we cannot not
     * update the command params here, since this will make the origin command params lost.
     * <ul>
     *     <li>state</li>
     *     <li>command type</li>
     *     <li>start time</li>
     *     <li>restart time</li>
     *     <li>end time</li>
     *     <li>run times</li>
     * </ul>
     */
    @Override
    protected void assembleWorkflowInstance(
                                            final WorkflowExecuteContextBuilder workflowExecuteContextBuilder) {
        final Command command = workflowExecuteContextBuilder.getCommand();
        final int workflowInstanceId = command.getWorkflowInstanceId();
        final WorkflowInstance workflowInstance = workflowInstanceDao.queryOptionalById(workflowInstanceId)
                .orElseThrow(() -> new IllegalArgumentException("Cannot find WorkflowInstance:" + workflowInstanceId));
        workflowInstance.setVarPool(null);
        workflowInstance.setStateWithDesc(WorkflowExecutionStatus.RUNNING_EXECUTION, command.getCommandType().name());
        workflowInstance.setCommandType(command.getCommandType());
        workflowInstance.setHost(masterConfig.getMasterAddress());
        workflowInstanceDao.updateById(workflowInstance);

        workflowExecuteContextBuilder.setWorkflowInstance(workflowInstance);
    }

    /**
     * Generate the workflow execution graph.
     * <p> Will clear the history failure/killed task.
     * <p> If the task's predecessors exist failure/killed, will also mark the task as failure/killed.
     */
    @Override
    protected void assembleWorkflowExecutionGraph(final WorkflowExecuteContextBuilder workflowExecuteContextBuilder) {
        final Map<String, TaskInstance> taskInstanceMap = dealWithHistoryTaskInstances(workflowExecuteContextBuilder)
                .stream()
                .collect(Collectors.toMap(TaskInstance::getName, Function.identity()));

        final IWorkflowGraph workflowGraph = workflowExecuteContextBuilder.getWorkflowGraph();
        final WorkflowExecutionGraph workflowExecutionGraph = new WorkflowExecutionGraph();

        final BiConsumer<String, Set<String>> taskExecutionRunnableCreator = (task, successors) -> {
            final TaskExecutionRunnableBuilder taskExecutionRunnableBuilder =
                    TaskExecutionRunnableBuilder
                            .builder()
                            .workflowExecutionGraph(workflowExecutionGraph)
                            .workflowDefinition(workflowExecuteContextBuilder.getWorkflowDefinition())
                            .project(workflowExecuteContextBuilder.getProject())
                            .workflowInstance(workflowExecuteContextBuilder.getWorkflowInstance())
                            .taskDefinition(workflowGraph.getTaskNodeByName(task))
                            .taskInstance(taskInstanceMap.get(task))
                            .workflowEventBus(workflowExecuteContextBuilder.getWorkflowEventBus())
                            .applicationContext(applicationContext)
                            .build();
            workflowExecutionGraph.addNode(new TaskExecutionRunnable(taskExecutionRunnableBuilder));
            workflowExecutionGraph.addEdge(task, successors);
        };

        final WorkflowGraphTopologyLogicalVisitor workflowGraphTopologyLogicalVisitor =
                WorkflowGraphTopologyLogicalVisitor.builder()
                        .taskDependType(workflowExecuteContextBuilder.getWorkflowInstance().getTaskDependType())
                        .onWorkflowGraph(workflowGraph)
                        .fromTask(parseStartNodesFromWorkflowInstance(workflowExecuteContextBuilder))
                        .doVisitFunction(taskExecutionRunnableCreator)
                        .build();
        workflowGraphTopologyLogicalVisitor.visit();

        workflowExecuteContextBuilder.setWorkflowExecutionGraph(workflowExecutionGraph);
    }

    /**
     * Return the valid task instance which should not be recovered.
     * <p> Will mark the failure/killed task instance as invalid.
     */
    private List<TaskInstance> dealWithHistoryTaskInstances(
                                                            final WorkflowExecuteContextBuilder workflowExecuteContextBuilder) {
        final WorkflowInstance workflowInstance = workflowExecuteContextBuilder.getWorkflowInstance();
        final Map<String, TaskInstance> taskInstanceMap = super.getValidTaskInstance(workflowInstance)
                .stream()
                .collect(Collectors.toMap(TaskInstance::getName, Function.identity()));

        final IWorkflowGraph workflowGraph = workflowExecuteContextBuilder.getWorkflowGraph();

        final Set<String> needRecoverTasks = new HashSet<>();
        final Set<String> markInvalidTasks = new HashSet<>();
        final BiConsumer<String, Set<String>> historyTaskInstanceMarker = (task, successors) -> {
            // If the parent is need recover
            // Then the task should mark as invalid, and it's child should be mark as invalidated.
            if (markInvalidTasks.contains(task)) {
                if (taskInstanceMap.containsKey(task)) {
                    taskInstanceDao.markTaskInstanceInvalid(Lists.newArrayList(taskInstanceMap.get(task)));
                    taskInstanceMap.remove(task);
                }
                markInvalidTasks.addAll(successors);
                return;
            }

            final TaskInstance taskInstance = taskInstanceMap.get(task);
            if (taskInstance == null) {
                return;
            }

            if (isTaskNeedRecreate(taskInstance) || isTaskCanRecover(taskInstance)) {
                needRecoverTasks.add(task);
                markInvalidTasks.addAll(successors);
            }
        };

        final WorkflowGraphTopologyLogicalVisitor workflowGraphTopologyLogicalVisitor =
                WorkflowGraphTopologyLogicalVisitor.builder()
                        .onWorkflowGraph(workflowGraph)
                        .taskDependType(workflowInstance.getTaskDependType())
                        .fromTask(parseStartNodesFromWorkflowInstance(workflowExecuteContextBuilder))
                        .doVisitFunction(historyTaskInstanceMarker)
                        .build();
        workflowGraphTopologyLogicalVisitor.visit();

        for (String task : needRecoverTasks) {
            final TaskInstance taskInstance = taskInstanceMap.get(task);
            if (isTaskCanRecover(taskInstance)) {
                taskInstanceMap.put(task, createRecoverTaskInstance(taskInstance));
                continue;
            }
            if (isTaskNeedRecreate(taskInstance)) {
                taskInstanceMap.put(task, createRecreatedTaskInstance(taskInstance));
            }
        }
        return new ArrayList<>(taskInstanceMap.values());
    }

    /**
     * Whether the task need to be recreated.
     * <p> If the task state is FAILURE and KILL, then will mark the task invalid and recreate the task.
     */
    private boolean isTaskNeedRecreate(final TaskInstance taskInstance) {
        if (taskInstance == null) {
            return false;
        }
        return taskInstance.getState() == TaskExecutionStatus.FAILURE
                || taskInstance.getState() == TaskExecutionStatus.KILL;
    }

    private TaskInstance createRecreatedTaskInstance(final TaskInstance taskInstance) {
        return taskInstanceFactories.failedRecoverTaskInstanceFactory()
                .builder()
                .withTaskInstance(taskInstance)
                .build();
    }

    private boolean isTaskCanRecover(final TaskInstance taskInstance) {
        if (taskInstance == null) {
            return false;
        }
        return taskInstance.getState() == TaskExecutionStatus.PAUSE;
    }

    private TaskInstance createRecoverTaskInstance(final TaskInstance taskInstance) {
        return taskInstanceFactories.pauseRecoverTaskInstanceFactory()
                .builder()
                .withTaskInstance(taskInstance)
                .build();
    }

    @Override
    public CommandType commandType() {
        return CommandType.START_FAILURE_TASK_PROCESS;
    }

}
