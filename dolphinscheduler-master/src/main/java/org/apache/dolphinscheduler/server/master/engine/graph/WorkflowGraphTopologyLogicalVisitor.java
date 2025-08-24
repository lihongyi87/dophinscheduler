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

package org.apache.dolphinscheduler.server.master.engine.graph;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.dolphinscheduler.common.enums.TaskDependType;
import org.apache.dolphinscheduler.dao.entity.TaskDefinition;

import org.apache.commons.collections4.CollectionUtils;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;

/**
 * 工作流图拓扑逻辑访问器
 * 
 * 这是一个基于拓扑排序的工作流图遍历器，支持多种访问模式的图遍历算法。
 * 类比：就像一个智能的项目管理助手，能够按照不同的策略安排任务的执行顺序。
 * 
 * 核心功能：
 * 1. 拓扑遍历：按照任务依赖关系的拓扑顺序遍历图中的节点
 * 2. 多种访问模式：支持仅访问起始节点、前向遍历、后向遍历等模式
 * 3. 子图访问：可以限定在特定的子图范围内进行遍历
 * 4. 自定义访问函数：支持自定义的节点访问处理逻辑
 * 5. 入度管理：使用入度算法确保遍历的正确性
 * 
 * 访问模式说明：
 * - TASK_ONLY：仅访问指定的起始节点，不扩展遍历
 * - TASK_PRE：向前遍历，访问能够到达起始节点的所有节点
 * - TASK_POST：向后遍历，访问从起始节点能够到达的所有节点
 * 
 * 算法特点：
 * - 拓扑排序：保证访问顺序符合依赖关系，前置任务总是在后置任务之前被访问
 * - 入度计算：通过入度为0的节点作为遍历起点，逐步减少后继节点的入度
 * - 环路检测：拓扑排序天然具备环路检测能力，有环图无法完成排序
 * - 子图支持：可以在原图的子集中进行拓扑遍历
 * 
 * 使用场景：
 * - 工作流执行调度：按依赖关系确定任务执行顺序
 * - 任务状态更新：根据依赖关系传播状态变化
 * - 图结构分析：分析特定节点的影响范围
 * - 并行度计算：确定可以并行执行的任务集合
 * 
 * 设计模式：
 * - 建造者模式：使用Builder模式构建复杂的访问器对象
 * - 策略模式：根据不同的TaskDependType采用不同的遍历策略
 * - 访问者模式：通过visitFunction自定义节点访问逻辑
 * 
 * 类比理解：
 * 就像规划一个复杂项目的执行计划：
 * - 起始节点 = 可以立即开始的任务
 * - 拓扑排序 = 按照任务依赖关系排出执行顺序
 * - 入度计算 = 统计每个任务还有多少前置条件未完成
 * - 访问函数 = 对每个任务执行具体的处理动作
 * - 子图遍历 = 只关注项目中的某个模块或阶段
 */
public class WorkflowGraphTopologyLogicalVisitor {

    /**
     * 工作流图实例
     * 
     * 要进行拓扑遍历的工作流图对象，包含所有任务节点和它们之间的依赖关系。
     * 提供图的基本操作接口，如获取前驱后继节点、查询起始节点等。
     * 类比：需要安排执行计划的项目甘特图，包含所有任务和依赖关系。
     */
    private final IWorkflowGraph workflowGraph;

    /**
     * 任务依赖类型
     * 
     * 决定访问器的遍历策略，不同类型对应不同的遍历方向和范围：
     * - TASK_ONLY：仅处理指定的起始任务，不扩展
     * - TASK_PRE：反向遍历，找出能到达起始任务的所有前驱任务
     * - TASK_POST：正向遍历，找出从起始任务能到达的所有后继任务
     * 类比：项目分析的不同视角，可以向前看影响范围，也可以向后看依赖来源。
     */
    private final TaskDependType taskDependType;

    /**
     * 起始节点集合
     * 
     * 遍历的起点任务集合，根据不同的访问模式，这些节点的作用不同：
     * - TASK_ONLY模式：仅访问这些节点
     * - TASK_PRE模式：以这些节点为目标，找出所有能到达它们的节点
     * - TASK_POST模式：以这些节点为起点，找出所有从它们能到达的节点
     * 类比：项目分析的关注焦点，可能是关键里程碑或重要交付物。
     */
    private final Set<String> startNodes;

    /**
     * 访问函数
     * 
     * 自定义的节点访问处理函数，在遍历过程中对每个符合条件的节点执行。
     * 函数接收两个参数：当前任务名称和该任务的后继任务集合。
     * 这是访问者模式的体现，将遍历算法和具体处理逻辑解耦。
     * 类比：对每个任务执行的具体操作，可能是更新状态、记录日志、发送通知等。
     */
    private final BiConsumer<String, Set<String>> visitFunction;

    /**
     * 私有构造方法
     * 
     * 使用建造者模式的构造方法，通过Builder对象初始化访问器的所有必要参数。
     * 进行严格的参数验证，确保访问器能够正确工作。
     * 
     * 初始化逻辑：
     * 1. 复制建造者中的配置参数
     * 2. 对关键参数进行非空检查
     * 3. 处理起始节点：如果未指定则使用图的默认起始节点
     * 4. 确保所有必要的组件都已正确配置
     * 
     * @param workflowGraphBfsVisitorBuilder 建造者对象，包含所有配置参数
     * 
     * 类比：根据项目经理提供的分析需求，配置项目管理助手的工作参数。
     */
    private WorkflowGraphTopologyLogicalVisitor(WorkflowGraphBfsVisitorBuilder workflowGraphBfsVisitorBuilder) {
        this.taskDependType = workflowGraphBfsVisitorBuilder.taskDependType;
        this.workflowGraph = checkNotNull(workflowGraphBfsVisitorBuilder.workflowGraph);
        this.visitFunction = checkNotNull(workflowGraphBfsVisitorBuilder.visitFunction);
        if (CollectionUtils.isEmpty(workflowGraphBfsVisitorBuilder.startNodes)) {
            this.startNodes = new HashSet<>(workflowGraph.getStartNodes());
        } else {
            this.startNodes = new HashSet<>(checkNotNull(workflowGraphBfsVisitorBuilder.startNodes));
        }
    }

    /**
     * 创建建造者对象
     * 
     * 提供创建WorkflowGraphBfsVisitorBuilder的静态工厂方法。
     * 这是建造者模式的标准入口点。
     * 
     * @return 新的建造者实例
     */
    public static WorkflowGraphBfsVisitorBuilder builder() {
        return new WorkflowGraphBfsVisitorBuilder();
    }

    /**
     * 执行图遍历访问
     * 
     * 根据配置的任务依赖类型选择相应的遍历策略并执行访问。
     * 这是访问器的主入口方法，封装了不同访问模式的选择逻辑。
     * 
     * 策略选择：
     * - TASK_ONLY：仅访问起始节点，适用于单点分析
     * - TASK_PRE：反向遍历，分析影响起始节点的所有前驱任务
     * - TASK_POST：正向遍历，分析起始节点影响的所有后继任务
     * 
     * @throws IllegalArgumentException 如果遇到不支持的任务依赖类型
     * 
     * 类比：项目经理根据不同的分析需求，指示助手采用不同的分析方法。
     */
    public void visit() {
        switch (taskDependType) {
            case TASK_ONLY:
                visitStartNodesOnly();
                break;
            case TASK_PRE:
                visitToStartNodes();
                break;
            case TASK_POST:
                visitFromStartNodes();
                break;
            default:
                throw new IllegalArgumentException("Unsupported task depend type: " + taskDependType);
        }
    }

    /**
     * Visit start nodes only.
     */
    private void visitStartNodesOnly() {
        doVisitationInSubGraph(Sets.newHashSet(startNodes));
    }

    /**
     * Find the graph nodes that can be reached to the start nodes, and then do visitation with topology logical.
     */
    private void visitToStartNodes() {
        final LinkedList<String> bootstrapTaskCodes = new LinkedList<>(startNodes);
        final Set<String> subGraphNodes = new HashSet<>();
        while (!bootstrapTaskCodes.isEmpty()) {
            String taskName = bootstrapTaskCodes.removeFirst();
            if (subGraphNodes.contains(taskName)) {
                continue;
            }
            subGraphNodes.add(taskName);
            final Set<String> successors = workflowGraph.getPredecessors(taskName);
            bootstrapTaskCodes.addAll(successors);
        }
        doVisitationInSubGraph(subGraphNodes);
    }

    /**
     * Find the graph nodes that can be reached from the start nodes, and then do visitation with topology logical.
     */
    private void visitFromStartNodes() {
        final LinkedList<String> bootstrapTaskCodes = new LinkedList<>(startNodes);
        final Set<String> subGraphNodes = new HashSet<>();
        while (!bootstrapTaskCodes.isEmpty()) {
            String taskName = bootstrapTaskCodes.removeFirst();
            if (subGraphNodes.contains(taskName)) {
                continue;
            }
            subGraphNodes.add(taskName);
            final Set<String> successors = workflowGraph.getSuccessors(taskName);
            bootstrapTaskCodes.addAll(successors);
        }
        doVisitationInSubGraph(subGraphNodes);
    }

    private void doVisitationInSubGraph(Set<String> subGraphNodes) {
        // visit from the workflow graph by topology
        // If the node is not in the subGraph, then skip it.
        Map<String, Integer> inDegreeMap = workflowGraph.getAllTaskNodes()
                .stream()
                .collect(Collectors.toMap(TaskDefinition::getName,
                        taskDefinition -> workflowGraph.getPredecessors(taskDefinition.getName()).size()));
        final LinkedList<String> bootstrapTaskCodes = inDegreeMap
                .entrySet()
                .stream()
                .filter(entry -> entry.getValue() == 0)
                .map(Map.Entry::getKey)
                .collect(Collectors.toCollection(LinkedList::new));
        // Visited table, used to record the visited nodes
        Set<String> visitedTaskCodes = new HashSet<>();

        while (!bootstrapTaskCodes.isEmpty()) {
            String taskName = bootstrapTaskCodes.removeFirst();
            if (inDegreeMap.get(taskName) > 0) {
                continue;
            }
            // Visit only when the in-degree is 0
            if (!visitedTaskCodes.contains(taskName)) {
                visitedTaskCodes.add(taskName); // Record the nodes
                final Set<String> successors = workflowGraph.getSuccessors(taskName);
                if (subGraphNodes.contains(taskName)) {
                    visitFunction.accept(taskName, successors);
                }
                for (String successor : successors) {
                    inDegreeMap.put(successor, inDegreeMap.get(successor) - 1);
                }
                bootstrapTaskCodes.addAll(successors);
            }
        }
    }

    public static class WorkflowGraphBfsVisitorBuilder {

        private IWorkflowGraph workflowGraph;

        private List<String> startNodes;

        private TaskDependType taskDependType = TaskDependType.TASK_POST;

        private BiConsumer<String, Set<String>> visitFunction;

        public WorkflowGraphBfsVisitorBuilder onWorkflowGraph(IWorkflowGraph workflowGraph) {
            this.workflowGraph = workflowGraph;
            return this;
        }

        public WorkflowGraphBfsVisitorBuilder taskDependType(TaskDependType taskDependType) {
            this.taskDependType = taskDependType;
            return this;
        }

        public WorkflowGraphBfsVisitorBuilder fromTask(List<String> startNodes) {
            this.startNodes = startNodes;
            return this;
        }

        public WorkflowGraphBfsVisitorBuilder doVisitFunction(BiConsumer<String, Set<String>> visitFunction) {
            this.visitFunction = visitFunction;
            return this;
        }

        public WorkflowGraphTopologyLogicalVisitor build() {
            return new WorkflowGraphTopologyLogicalVisitor(this);
        }
    }
}
