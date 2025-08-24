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
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.dao.entity.Command;
import org.apache.dolphinscheduler.dao.entity.WorkflowDefinition;
import org.apache.dolphinscheduler.dao.entity.WorkflowInstance;
import org.apache.dolphinscheduler.dao.repository.TaskInstanceDao;
import org.apache.dolphinscheduler.dao.repository.WorkflowInstanceDao;
import org.apache.dolphinscheduler.extract.master.command.ICommandParam;
import org.apache.dolphinscheduler.extract.master.command.RunWorkflowCommandParam;
import org.apache.dolphinscheduler.plugin.task.api.model.Property;
import org.apache.dolphinscheduler.server.master.config.MasterConfig;
import org.apache.dolphinscheduler.server.master.engine.graph.IWorkflowGraph;
import org.apache.dolphinscheduler.server.master.engine.graph.WorkflowExecutionGraph;
import org.apache.dolphinscheduler.server.master.engine.graph.WorkflowGraphTopologyLogicalVisitor;
import org.apache.dolphinscheduler.server.master.engine.task.runnable.TaskExecutionRunnable;
import org.apache.dolphinscheduler.server.master.engine.task.runnable.TaskExecutionRunnableBuilder;
import org.apache.dolphinscheduler.server.master.runner.WorkflowExecuteContext.WorkflowExecuteContextBuilder;
import org.apache.dolphinscheduler.service.expand.CuringParamsService;

import org.apache.commons.collections4.CollectionUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

/**
 * 运行工作流命令处理器
 * 
 * 这个类是工作流启动的核心处理器，负责处理START_PROCESS类型的命令，
 * 将工作流定义转换为可执行的工作流实例，并构建完整的执行图。
 * 
 * 主要功能：
 * 1. 基于命令生成新的工作流实例
 * 2. 合并命令参数和工作流全局参数
 * 3. 构建工作流执行图（DAG拓扑结构）
 * 4. 创建任务执行节点和依赖关系
 * 5. 支持指定启动节点的部分执行
 * 
 * 工作流程：
 * 1. 解析命令，获取工作流定义和参数
 * 2. 创建工作流实例，设置状态为RUNNING
 * 3. 构建执行图，创建所有任务节点
 * 4. 建立任务间的依赖关系
 * 5. 返回完整的执行上下文
 * 
 * 简单理解：就像一个"项目启动专员"，收到启动指令后，
 * 把项目计划书（工作流定义）变成具体的执行方案（工作流实例和执行图）。
 */
@Component
public class RunWorkflowCommandHandler extends AbstractCommandHandler {

    @Autowired
    private WorkflowInstanceDao workflowInstanceDao;

    @Autowired
    private TaskInstanceDao taskInstanceDao;

    @Autowired
    private MasterConfig masterConfig;

    @Autowired
    private ApplicationContext applicationContext;

    @Autowired
    private CuringParamsService curingParamsService;

    /**
     * 组装工作流实例
     * 
     * 基于命令生成一个新的工作流实例，设置必要的属性和状态。
     * 
     * @param workflowExecuteContextBuilder 工作流执行上下文构建器
     */
    @Override
    protected void assembleWorkflowInstance(final WorkflowExecuteContextBuilder workflowExecuteContextBuilder) {
        final WorkflowDefinition workflowDefinition = workflowExecuteContextBuilder.getWorkflowDefinition();
        final Command command = workflowExecuteContextBuilder.getCommand();
        // 获取已存在的工作流实例（通常在命令创建时已经创建）
        final WorkflowInstance workflowInstance = workflowInstanceDao.queryById(command.getWorkflowInstanceId());
        
        // 设置工作流实例为运行状态
        workflowInstance.setStateWithDesc(WorkflowExecutionStatus.RUNNING_EXECUTION, command.getCommandType().name());
        // 设置当前Master节点为该工作流的处理节点
        workflowInstance.setHost(masterConfig.getMasterAddress());
        // 设置命令参数
        workflowInstance.setCommandParam(command.getCommandParam());
        // 合并命令参数和工作流全局参数
        workflowInstance.setGlobalParams(mergeCommandParamsWithWorkflowParams(command, workflowDefinition));
        
        // 更新数据库中的工作流实例
        workflowInstanceDao.upsertWorkflowInstance(workflowInstance);
        workflowExecuteContextBuilder.setWorkflowInstance(workflowInstance);
    }

    /**
     * 组装工作流执行图
     * 
     * 基于工作流图构建完整的执行图，包含所有任务节点和依赖关系。
     * 这是工作流执行的核心数据结构。
     * 
     * @param workflowExecuteContextBuilder 工作流执行上下文构建器
     */
    @Override
    protected void assembleWorkflowExecutionGraph(final WorkflowExecuteContextBuilder workflowExecuteContextBuilder) {
        final IWorkflowGraph workflowGraph = workflowExecuteContextBuilder.getWorkflowGraph();
        final WorkflowExecutionGraph workflowExecutionGraph = new WorkflowExecutionGraph();
        
        // 任务执行节点创建器 - 为每个任务创建执行节点
        final BiConsumer<String, Set<String>> taskExecutionRunnableCreator = (task, successors) -> {
            final TaskExecutionRunnableBuilder taskExecutionRunnableBuilder =
                    TaskExecutionRunnableBuilder
                            .builder()
                            .workflowExecutionGraph(workflowExecutionGraph)
                            .workflowDefinition(workflowExecuteContextBuilder.getWorkflowDefinition())
                            .project(workflowExecuteContextBuilder.getProject())
                            .workflowInstance(workflowExecuteContextBuilder.getWorkflowInstance())
                            .taskDefinition(workflowGraph.getTaskNodeByName(task))
                            .workflowEventBus(workflowExecuteContextBuilder.getWorkflowEventBus())
                            .applicationContext(applicationContext)
                            .build();
            // 将任务节点添加到执行图中
            workflowExecutionGraph.addNode(new TaskExecutionRunnable(taskExecutionRunnableBuilder));
            // 添加任务间的依赖边
            workflowExecutionGraph.addEdge(task, successors);
        };

        // 工作流图拓扑逻辑访问器 - 按照DAG拓扑顺序遍历所有任务
        final WorkflowGraphTopologyLogicalVisitor workflowGraphTopologyLogicalVisitor =
                WorkflowGraphTopologyLogicalVisitor.builder()
                        .taskDependType(workflowExecuteContextBuilder.getWorkflowInstance().getTaskDependType())
                        .onWorkflowGraph(workflowGraph)
                        .fromTask(parseStartNodesFromWorkflowInstance(workflowExecuteContextBuilder))
                        .doVisitFunction(taskExecutionRunnableCreator)
                        .build();
        // 执行拓扑遍历，构建完整的执行图
        workflowGraphTopologyLogicalVisitor.visit();

        workflowExecuteContextBuilder.setWorkflowExecutionGraph(workflowExecutionGraph);
    }

    /**
     * 合并命令参数与工作流参数
     * 
     * 将命令中的参数与工作流定义中的全局参数合并。
     * 如果有重复的键，命令参数将覆盖工作流参数。
     * 
     * @param command 命令对象，包含执行时的参数
     * @param workflowDefinition 工作流定义，包含默认的全局参数
     * @return 合并后的参数JSON字符串
     */
    private String mergeCommandParamsWithWorkflowParams(final Command command,
                                                        final WorkflowDefinition workflowDefinition) {
        // 解析命令参数
        final List<Property> commandParams =
                Optional.ofNullable(JSONUtils.parseObject(command.getCommandParam(), ICommandParam.class))
                        .map(ICommandParam::getCommandParams)
                        .orElse(null);
        // 解析工作流全局参数
        final List<Property> globalParamsList = JSONUtils.toList(workflowDefinition.getGlobalParams(), Property.class);
        Map<String, Property> finalParams = new HashMap<>();
        
        // 先添加工作流全局参数
        if (CollectionUtils.isNotEmpty(globalParamsList)) {
            globalParamsList.forEach(globalParam -> finalParams.put(globalParam.getProp(), globalParam));
        }
        // 再添加命令参数，会覆盖同名的全局参数
        if (CollectionUtils.isNotEmpty(commandParams)) {
            commandParams.forEach(commandParam -> finalParams.put(commandParam.getProp(), commandParam));
        }
        return JSONUtils.toJsonString(finalParams.values());
    }

    /**
     * 返回该处理器匹配的命令类型
     * 
     * @return START_PROCESS命令类型，用于手动启动工作流
     */
    @Override
    public CommandType commandType() {
        return CommandType.START_PROCESS;
    }
}
