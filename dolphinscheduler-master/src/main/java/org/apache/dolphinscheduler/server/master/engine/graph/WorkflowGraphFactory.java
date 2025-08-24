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

import org.apache.dolphinscheduler.dao.entity.TaskDefinition;
import org.apache.dolphinscheduler.dao.entity.WorkflowDefinition;
import org.apache.dolphinscheduler.dao.entity.WorkflowTaskRelation;
import org.apache.dolphinscheduler.dao.repository.TaskDefinitionLogDao;
import org.apache.dolphinscheduler.service.process.ProcessService;

import java.util.List;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * 工作流图工厂
 * 
 * 这个工厂类负责根据工作流定义创建工作流图对象。
 * 工作流图是DolphinScheduler中表示任务依赖关系的核心数据结构。
 * 
 * 类比：建筑师根据建筑图纸（工作流定义）构建实际的施工网络图，
 *      明确每个施工步骤（任务）之间的先后顺序和依赖关系。
 * 
 * 主要功能：
 * 1. 从数据库查询工作流的任务关系定义
 * 2. 获取所有相关的任务定义信息
 * 3. 构建完整的工作流图对象
 */
@Slf4j
@Component
public class WorkflowGraphFactory {

    /**
     * 流程服务
     * 
     * 提供工作流相关的数据访问功能，包括查询任务关系等。
     * 类比：项目资料库，存储所有项目的施工流程信息。
     */
    @Autowired
    private ProcessService processService;

    /**
     * 任务定义日志数据访问对象
     * 
     * 用于查询任务定义的详细信息，包括任务的配置和参数。
     * 类比：任务档案管理员，负责提供每个具体任务的详细说明。
     */
    @Autowired
    private TaskDefinitionLogDao taskDefinitionLogDao;

    /**
     * 创建工作流图
     * 
     * 根据工作流定义构建完整的工作流图对象，这个图包含了所有任务节点和它们之间的依赖关系。
     * 
     * 构建过程：
     * 1. 根据工作流定义的代码和版本查询任务关系
     * 2. 根据任务关系查询所有相关的任务定义
     * 3. 使用任务关系和任务定义构建工作流图
     * 
     * 类比：建筑师根据建筑图纸（工作流定义）：
     *      1. 查找施工步骤的先后顺序关系（任务关系）
     *      2. 获取每个施工步骤的具体操作说明（任务定义）
     *      3. 绘制完整的施工网络图（工作流图）
     * 
     * @param workflowDefinition 工作流定义，包含工作流的基本信息
     * @return 构建好的工作流图对象
     */
    public IWorkflowGraph createWorkflowGraph(WorkflowDefinition workflowDefinition) {

        // 查询工作流的任务关系定义
        // 类比：查找施工步骤之间的先后顺序关系
        List<WorkflowTaskRelation> workflowTaskRelations = processService.findRelationByCode(
                workflowDefinition.getCode(),
                workflowDefinition.getVersion());

        // 根据任务关系查询所有相关的任务定义
        // 类比：根据施工顺序关系，查找每个施工步骤的具体操作说明
        List<TaskDefinition> taskDefinitions = taskDefinitionLogDao.queryTaskDefineLogList(workflowTaskRelations)
                .stream()
                .map(TaskDefinition.class::cast)
                .collect(Collectors.toList());
        
        // 使用任务关系和任务定义构建工作流图
        // 类比：根据施工关系和具体操作说明，绘制完整的施工网络图
        return new WorkflowGraph(workflowTaskRelations, taskDefinitions);
    }

}
