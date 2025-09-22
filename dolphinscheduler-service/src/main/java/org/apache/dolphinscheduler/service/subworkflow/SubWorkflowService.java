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

package org.apache.dolphinscheduler.service.subworkflow;

import org.apache.dolphinscheduler.dao.entity.RelationSubWorkflow;
import org.apache.dolphinscheduler.dao.entity.WorkflowInstance;
import org.apache.dolphinscheduler.plugin.task.api.model.Property;

import java.util.List;

import org.springframework.stereotype.Component;

/**
 * 子工作流服务接口
 *
 * 管理子工作流相关的业务逻辑，包括动态子工作流的查询、
 * 子工作流关系的维护、子工作流实例的过滤和参数传递。
 *
 * 子工作流特性：
 * - 动态子工作流：支持运行时动态创建子工作流
 * - 参数传递：支持父子工作流之间的参数传递
 * - 状态同步：子工作流状态影响父工作流执行
 * - 关系维护：维护父子工作流的关联关系
 *
 * 应用场景：
 * - 复杂工作流拆分：将复杂工作流拆分为多个子工作流
 * - 工作流复用：通过子工作流实现通用逻辑复用
 * - 动态流程：根据条件动态选择执行不同子工作流
 *
 * 类比：就像企业的多级管理体系，总公司（父工作流）可以
 *      调度各个分公司（子工作流）完成不同任务。
 */
@Component
public interface SubWorkflowService {

    /**
     * 获取所有动态子工作流
     *
     * @param processInstanceId 父工作流实例ID
     * @param taskCode 任务代码
     * @return 动态子工作流实例列表
     */
    List<WorkflowInstance> getAllDynamicSubWorkflow(long processInstanceId, long taskCode);

    /**
     * 批量插入子工作流关系
     *
     * @param relationSubWorkflowList 子工作流关系列表
     * @return 插入数量
     */
    int batchInsertRelationSubWorkflow(List<RelationSubWorkflow> relationSubWorkflowList);

    /**
     * 过滤已完成的工作流实例
     *
     * @param workflowInstanceList 工作流实例列表
     * @return 已完成的工作流实例列表
     */
    List<WorkflowInstance> filterFinishProcessInstances(List<WorkflowInstance> workflowInstanceList);

    /**
     * 过滤成功的工作流实例
     *
     * @param workflowInstanceList 工作流实例列表
     * @return 成功的工作流实例列表
     */
    List<WorkflowInstance> filterSuccessProcessInstances(List<WorkflowInstance> workflowInstanceList);

    /**
     * 过滤运行中的工作流实例
     *
     * @param workflowInstanceList 工作流实例列表
     * @return 运行中的工作流实例列表
     */
    List<WorkflowInstance> filterRunningProcessInstances(List<WorkflowInstance> workflowInstanceList);

    /**
     * 过滤失败的工作流实例
     *
     * @param workflowInstanceList 工作流实例列表
     * @return 失败的工作流实例列表
     */
    List<WorkflowInstance> filterFailedProcessInstances(List<WorkflowInstance> workflowInstanceList);

    /**
     * 获取工作流输出参数
     *
     * 获取子工作流的输出参数，用于传递给父工作流
     *
     * @param workflowInstance 工作流实例
     * @return 输出参数列表
     */
    List<Property> getWorkflowOutputParameters(WorkflowInstance workflowInstance);
}
