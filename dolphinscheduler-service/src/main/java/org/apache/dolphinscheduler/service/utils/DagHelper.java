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

package org.apache.dolphinscheduler.service.utils;

import org.apache.dolphinscheduler.common.graph.DAG;
import org.apache.dolphinscheduler.common.model.TaskNodeRelation;
import org.apache.dolphinscheduler.dao.entity.WorkflowTaskRelation;
import org.apache.dolphinscheduler.service.model.TaskNode;
import org.apache.dolphinscheduler.service.process.WorkflowDag;

import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;

/**
 * DAG图工具类
 *
 * 提供有向无环图（Directed Acyclic Graph）的构建和处理功能。
 * 工作流中的任务依赖关系形成DAG结构，该工具类负责DAG的构建和转换。
 *
 * DAG特点：
 * - 有向性：边有方向，表示任务执行顺序
 * - 无环性：不存在环路，避免死循环
 * - 拓扑性：可进行拓扑排序，确定执行顺序
 *
 * 主要功能：
 * - 构建DAG图：从任务节点和关系构建DAG结构
 * - 生成工作流DAG：将任务列表和关系转换为WorkflowDag对象
 * - 验证DAG：检查图是否满足DAG性质
 *
 * 应用场景：
 * - 工作流解析：解析工作流定义中的任务依赖
 * - 执行调度：根据DAG确定任务执行顺序
 * - 并行分析：找出可并行执行的任务
 * - 依赖检查：检查任务间的依赖关系
 *
 * 类比：就像项目管理中的甘特图，每个任务都有前置依赖，
 *      形成一个有向无环的任务网络。
 */
@Slf4j
public class DagHelper {

    /**
     * 构建DAG图
     *
     * 从工作流DAG对象构建标准的DAG图结构。
     * 先添加所有节点（顶点），再添加边（依赖关系）。
     *
     * @param workflowDag 工作流DAG对象，包含节点和边信息
     * @return DAG图对象，包含完整的任务依赖关系
     */
    public static DAG<Long, TaskNode, TaskNodeRelation> buildDagGraph(WorkflowDag workflowDag) {

        DAG<Long, TaskNode, TaskNodeRelation> dag = new DAG<>();

        // 添加所有顶点（任务节点）
        if (CollectionUtils.isNotEmpty(workflowDag.getNodes())) {
            for (TaskNode node : workflowDag.getNodes()) {
                dag.addNode(node.getCode(), node);
            }
        }

        // 添加所有边（依赖关系）
        if (CollectionUtils.isNotEmpty(workflowDag.getEdges())) {
            for (TaskNodeRelation edge : workflowDag.getEdges()) {
                dag.addEdge(edge.getStartNode(), edge.getEndNode());
            }
        }
        return dag;
    }

    /**
     * 获取工作流DAG
     *
     * 将任务节点列表和任务关系列表转换为WorkflowDag对象。
     * 处理过程中会过滤掉无效的关系（如节点不存在的关系）。
     *
     * @param taskNodeList 任务节点列表
     * @param workflowTaskRelations 工作流任务关系列表
     * @return 工作流DAG对象，包含节点和边信息
     */
    public static WorkflowDag getWorkflowDag(List<TaskNode> taskNodeList,
                                             List<WorkflowTaskRelation> workflowTaskRelations) {
        // 构建任务节点映射，方便快速查找
        Map<Long, TaskNode> taskNodeMap = new HashMap<>();

        taskNodeList.forEach(taskNode -> {
            taskNodeMap.putIfAbsent(taskNode.getCode(), taskNode);
        });

        // 构建任务节点关系列表
        List<TaskNodeRelation> taskNodeRelations = new ArrayList<>();
        for (WorkflowTaskRelation workflowTaskRelation : workflowTaskRelations) {
            long preTaskCode = workflowTaskRelation.getPreTaskCode();
            long postTaskCode = workflowTaskRelation.getPostTaskCode();

            // 只添加有效的关系：
            // 1. 前置任务不为0（0表示没有前置任务）
            // 2. 前置和后置任务都存在于节点列表中
            if (workflowTaskRelation.getPreTaskCode() != 0
                    && taskNodeMap.containsKey(preTaskCode) && taskNodeMap.containsKey(postTaskCode)) {
                TaskNode preNode = taskNodeMap.get(preTaskCode);
                TaskNode postNode = taskNodeMap.get(postTaskCode);
                taskNodeRelations
                        .add(new TaskNodeRelation(preNode.getCode(), postNode.getCode()));
            }
        }

        // 构建并返回工作流DAG
        WorkflowDag workflowDag = new WorkflowDag();
        workflowDag.setEdges(taskNodeRelations);
        workflowDag.setNodes(taskNodeList);
        return workflowDag;
    }

}
