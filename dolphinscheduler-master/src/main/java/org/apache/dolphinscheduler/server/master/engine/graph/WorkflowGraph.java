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

import org.apache.dolphinscheduler.dao.entity.TaskDefinition;
import org.apache.dolphinscheduler.dao.entity.WorkflowTaskRelation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 工作流图实现类
 * 
 * 这是DolphinScheduler中工作流依赖关系图的核心实现，管理任务间的前驱后继关系。
 * 类比：就像一个项目甘特图或工序流程图，清晰展示各个任务的执行顺序和依赖关系。
 * 
 * 核心职责：
 * 1. 图结构管理：维护任务节点和边的有向无环图(DAG)结构
 * 2. 依赖关系存储：管理每个任务的前驱任务和后继任务列表
 * 3. 任务查询服务：提供按名称、编码查询任务定义的快速访问
 * 4. 图遍历支持：为工作流执行引擎提供图遍历所需的基础方法
 * 5. 数据完整性：确保图结构的完整性和一致性
 * 
 * 数据结构设计：
 * - taskDefinitionMap：任务名称到任务定义的映射，支持按名称快速查找
 * - taskDefinitionCodeMap：任务编码到任务定义的映射，支持按编码快速查找
 * - predecessors：每个任务的前驱任务列表，表示依赖关系
 * - successors：每个任务的后继任务列表，表示被依赖关系
 * 
 * 图的特性：
 * - 有向图：任务间的依赖关系是有方向的
 * - 无环图：不允许存在循环依赖，确保工作流能正常执行
 * - 连通性：支持多个起始节点和结束节点
 * - 完整性：每个任务都在图中有对应的节点
 * 
 * 构建过程：
 * 1. 参数验证：检查任务定义和任务关系列表的有效性
 * 2. 索引构建：构建任务名称和编码的快速查找映射
 * 3. 节点添加：将所有任务定义添加为图节点
 * 4. 边添加：根据任务关系添加有向边，建立依赖关系
 * 
 * 异常处理：
 * - 重复任务检测：防止添加重复的任务节点
 * - 重复关系检测：防止添加重复的任务依赖关系  
 * - 无效关系检测：检测和拒绝无效的任务关系
 * - 缺失任务检测：确保关系中引用的任务都存在
 * 
 * 类比理解：
 * 就像建筑施工的工序图：
 * - 任务定义 = 各个施工工序（打地基、建框架、装修等）
 * - 前驱关系 = 工序的先决条件（必须先打地基才能建框架）
 * - 后继关系 = 工序完成后可以进行的下一步（框架完成后可以开始装修）
 * - 起始节点 = 可以立即开始的工序（测量、准备材料等）
 * - 图遍历 = 按正确顺序安排施工进度
 */
public class WorkflowGraph implements IWorkflowGraph {

    /**
     * 任务编码到任务定义的映射表
     * 
     * 使用任务编码(taskCode)作为键，任务定义作为值的快速查找表。
     * 任务编码是任务的唯一标识符，通常用于系统内部的任务引用。
     * 类比：员工工号到员工档案的索引，通过工号快速找到员工信息。
     */
    private final Map<Long, TaskDefinition> taskDefinitionCodeMap;

    /**
     * 任务名称到任务定义的映射表
     * 
     * 使用任务名称作为键，任务定义作为值的快速查找表。
     * 任务名称是用户可读的任务标识，通常用于用户界面和日志显示。
     * 类比：员工姓名到员工档案的索引，通过姓名快速找到员工信息。
     */
    private final Map<String, TaskDefinition> taskDefinitionMap;

    /**
     * 任务前驱关系映射表
     * 
     * 存储每个任务的前驱任务列表，表示该任务依赖的其他任务。
     * Key: 任务名称, Value: 该任务的所有前驱任务名称列表
     * 前驱任务必须全部完成后，当前任务才能开始执行。
     * 类比：工序的前置条件清单，列出开始当前工序前必须完成的所有前置工序。
     */
    private final Map<String, List<String>> predecessors;

    /**
     * 任务后继关系映射表
     * 
     * 存储每个任务的后继任务列表，表示依赖该任务的其他任务。
     * Key: 任务名称, Value: 依赖该任务的所有后继任务名称列表
     * 当前任务完成后，其所有后继任务的依赖条件就满足了一个。
     * 类比：工序的影响范围清单，列出当前工序完成后可以启动的所有后续工序。
     */
    private final Map<String, List<String>> successors;

    /**
     * 工作流图构造方法
     * 
     * 根据任务关系列表和任务定义列表构建完整的工作流依赖关系图。
     * 这是图构建的唯一入口，确保图的完整性和一致性。
     * 
     * 构建步骤：
     * 1. 参数验证：检查输入参数的有效性，防止空值输入
     * 2. 容器初始化：初始化前驱和后继关系的存储容器
     * 3. 映射表构建：构建任务名称和编码的快速查找映射表
     * 4. 节点添加：将所有任务定义添加为图的节点
     * 5. 边添加：根据任务关系添加有向边，建立依赖关系
     * 
     * 数据结构说明：
     * - 使用Stream API和函数式编程提高代码简洁性
     * - 同时维护名称和编码两种索引方式，满足不同查询需求
     * - 前驱和后继关系分开存储，支持双向快速查询
     * 
     * 异常安全：
     * - 使用checkNotNull进行空值检查，在构建开始就发现问题
     * - 节点和边的添加过程中进行重复检查和有效性验证
     * 
     * @param workflowTaskRelations 任务关系列表，定义任务间的依赖关系
     * @param taskDefinitions 任务定义列表，包含所有任务的元数据
     * 
     * 类比：根据施工图纸和工序表建立完整的施工流程图，
     * 既要知道每道工序的详细要求，又要明确工序间的先后顺序。
     */
    public WorkflowGraph(List<WorkflowTaskRelation> workflowTaskRelations, List<TaskDefinition> taskDefinitions) {
        checkNotNull(taskDefinitions, "taskDefinitions can not be null");
        checkNotNull(workflowTaskRelations, "taskDefinitions can not be null");
        this.predecessors = new HashMap<>();
        this.successors = new HashMap<>();

        this.taskDefinitionMap = taskDefinitions
                .stream()
                .collect(Collectors.toMap(TaskDefinition::getName, Function.identity()));
        this.taskDefinitionCodeMap = taskDefinitions
                .stream()
                .collect(Collectors.toMap(TaskDefinition::getCode, Function.identity()));

        addTaskNodes(taskDefinitions);
        addTaskEdge(workflowTaskRelations);
    }

    /**
     * 获取工作流的起始节点列表
     * 
     * 返回没有前驱任务的所有任务，这些任务可以作为工作流执行的起点。
     * 起始节点是工作流执行时首先被调度的任务，不依赖任何其他任务。
     * 
     * 实现逻辑：
     * 遍历前驱关系映射表，找出前驱任务列表为空的所有任务。
     * 使用Stream API进行函数式编程，代码简洁易读。
     * 
     * @return 起始节点的任务名称列表
     * 
     * 类比：找出施工流程中可以立即开始的工序，比如材料准备、场地清理等
     * 不需要等待其他工序完成就能开始的工作。
     */
    @Override
    public List<String> getStartNodes() {
        return predecessors.entrySet()
                .stream()
                .filter(entry -> entry.getValue().isEmpty())
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
    }

    /**
     * 获取指定任务的前驱任务集合
     * 
     * 返回指定任务的所有前驱任务，即该任务依赖的其他任务。
     * 这些前驱任务必须全部成功完成后，指定任务才能开始执行。
     * 
     * @param taskName 任务名称
     * @return 前驱任务名称的集合，使用Set确保唯一性
     * 
     * 类比：查询某道工序需要等待哪些前置工序完成，
     * 比如装修工序需要等待水电工程和泥工工程都完成。
     */
    @Override
    public Set<String> getPredecessors(String taskName) {
        return new HashSet<>(predecessors.get(taskName));
    }

    /**
     * 获取指定任务的后继任务集合
     * 
     * 返回依赖指定任务的所有后继任务。
     * 当指定任务完成后，这些后继任务的依赖条件就满足了一个。
     * 
     * @param taskName 任务名称  
     * @return 后继任务名称的集合，使用Set确保唯一性
     * 
     * 类比：查询某道工序完成后可以启动哪些后续工序，
     * 比如地基工程完成后可以开始框架搭建和管线铺设。
     */
    @Override
    public Set<String> getSuccessors(String taskName) {
        return new HashSet<>(successors.get(taskName));
    }

    /**
     * 根据任务名称获取任务定义
     * 
     * 通过任务名称快速查找对应的任务定义对象。
     * 如果任务不存在会抛出异常，确保调用方能及时发现问题。
     * 
     * @param taskName 任务名称
     * @return 任务定义对象
     * @throws IllegalArgumentException 如果找不到指定名称的任务
     * 
     * 类比：根据工序名称查找详细的工序作业指导书，
     * 获取该工序的具体要求和操作步骤。
     */
    @Override
    public TaskDefinition getTaskNodeByName(String taskName) {
        TaskDefinition taskDefinition = taskDefinitionMap.get(taskName);
        if (taskDefinition == null) {
            throw new IllegalArgumentException("Cannot find task: " + taskName);
        }
        return taskDefinition;
    }

    /**
     * 根据任务编码获取任务定义
     * 
     * 通过任务编码快速查找对应的任务定义对象。
     * 任务编码通常用于系统内部引用，是任务的唯一标识符。
     * 
     * @param taskCode 任务编码
     * @return 任务定义对象
     * @throws IllegalArgumentException 如果找不到指定编码的任务
     * 
     * 类比：根据工序编号查找详细的工序规范，
     * 编号是系统分配的唯一标识，确保准确定位。
     */
    @Override
    public TaskDefinition getTaskNodeByCode(Long taskCode) {
        TaskDefinition taskDefinition = taskDefinitionCodeMap.get(taskCode);
        if (taskDefinition == null) {
            throw new IllegalArgumentException("Cannot find task: " + taskCode);
        }
        return taskDefinition;
    }

    /**
     * 获取所有任务节点
     * 
     * 返回工作流图中包含的所有任务定义的列表。
     * 这是图的完整节点集合，用于图的遍历和统计分析。
     * 
     * @return 所有任务定义的列表
     * 
     * 类比：获取整个施工项目的所有工序清单，
     * 用于项目管理和进度跟踪。
     */
    @Override
    public List<TaskDefinition> getAllTaskNodes() {
        return new ArrayList<>(taskDefinitionMap.values());
    }

    /**
     * 添加任务节点到图中
     * 
     * 将所有任务定义添加为图的节点，并初始化每个节点的前驱和后继关系容器。
     * 这是图构建的第一步，为后续添加边做准备。
     * 
     * 处理逻辑：
     * 1. 遍历所有任务定义，提取任务名称
     * 2. 检查任务名称是否已存在，防止重复添加
     * 3. 为每个任务初始化空的前驱和后继任务列表
     * 
     * 异常处理：
     * 如果发现重复的任务名称，抛出IllegalArgumentException
     * 确保图中每个任务名称的唯一性
     * 
     * @param taskDefinitions 任务定义列表
     * @throws IllegalArgumentException 如果存在重复的任务名称
     * 
     * 类比：在施工流程图上标记所有工序节点，
     * 确保每个工序都有唯一的标识和独立的依赖关系记录。
     */
    private void addTaskNodes(List<TaskDefinition> taskDefinitions) {
        taskDefinitions
                .stream()
                .map(TaskDefinition::getName)
                .forEach(taskDefinition -> {
                    if (predecessors.containsKey(taskDefinition) || successors.containsKey(taskDefinition)) {
                        throw new IllegalArgumentException("The task " + taskDefinition + " is already exists");
                    }
                    predecessors.put(taskDefinition, new ArrayList<>());
                    successors.put(taskDefinition, new ArrayList<>());
                });
    }

    /**
     * 添加任务关系边到图中
     * 
     * 根据工作流任务关系列表，在图中添加有向边，建立任务间的依赖关系。
     * 这是图构建的第二步，建立节点间的连接关系。
     * 
     * 处理逻辑：
     * 1. 遍历所有任务关系，提取前驱任务和后继任务的编码
     * 2. 验证任务编码的有效性（大于0）和任务的存在性
     * 3. 检查关系是否已存在，防止重复添加
     * 4. 同时更新前驱关系和后继关系映射表
     * 
     * 边的类型：
     * - 有效边：pre > 0 && post > 0，正常的依赖关系
     * - 无效边：pre <= 0 && post <= 0，非法的关系定义
     * - 部分边：只有一个编码有效的情况，通常表示起始或结束节点
     * 
     * 异常处理：
     * - 任务不存在：引用的任务编码在图中找不到对应节点
     * - 重复关系：相同的依赖关系已经存在
     * - 无效关系：前驱和后继任务编码都无效
     * 
     * @param workflowTaskRelations 工作流任务关系列表
     * @throws IllegalArgumentException 各种无效关系的异常情况
     * 
     * 类比：在施工流程图中画出工序间的依赖箭头，
     * 明确哪个工序完成后才能开始下一个工序，建立完整的执行顺序。
     */
    private void addTaskEdge(List<WorkflowTaskRelation> workflowTaskRelations) {
        for (WorkflowTaskRelation workflowTaskRelation : workflowTaskRelations) {
            long pre = workflowTaskRelation.getPreTaskCode();
            long post = workflowTaskRelation.getPostTaskCode();
            if (pre > 0 && post > 0) {

                if (!taskDefinitionCodeMap.containsKey(pre)) {
                    throw new IllegalArgumentException("Cannot find task: " + pre);
                }
                if (!taskDefinitionCodeMap.containsKey(post)) {
                    throw new IllegalArgumentException("Cannot find task: " + post);
                }
                TaskDefinition preTask = checkNotNull(taskDefinitionCodeMap.get(pre), "Cannot find task: " + pre);
                TaskDefinition postTask = checkNotNull(taskDefinitionCodeMap.get(post), "Cannot find task: " + pre);
                List<String> predecessorsTasks = predecessors.get(postTask.getName());
                if (predecessorsTasks.contains(preTask.getName())) {
                    throw new IllegalArgumentException("The task relation from " + preTask.getName() + " to "
                            + postTask.getName() + " is already exists");
                }
                predecessorsTasks.add(preTask.getName());

                List<String> successTasks = successors.get(preTask.getName());
                if (successTasks.contains(postTask.getName())) {
                    throw new IllegalArgumentException("The task relation from " + preTask.getName() + " to "
                            + postTask.getName() + " is already exists");
                }
                successTasks.add(postTask.getName());
            }

            if (pre <= 0 && post <= 0) {
                throw new IllegalArgumentException("The task relation from " + pre + " to " + post + " is invalid");
            }

        }
    }
}
