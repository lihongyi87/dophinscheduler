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

package org.apache.dolphinscheduler.server.master.engine.task.runnable;

import org.apache.dolphinscheduler.dao.entity.Project;
import org.apache.dolphinscheduler.dao.entity.TaskDefinition;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.dao.entity.WorkflowDefinition;
import org.apache.dolphinscheduler.dao.entity.WorkflowInstance;
import org.apache.dolphinscheduler.server.master.engine.graph.IWorkflowExecutionGraph;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

/**
 * 任务执行上下文创建请求
 *
 * 封装了创建任务执行上下文所需的所有信息的请求对象。使用建造者模式简化复杂对象的构造，
 * 确保任务执行上下文工厂能够获取到完整的创建信息。
 *
 * 设计目的：
 * - 信息聚合：将分散的任务执行信息聚合到单一请求对象中
 * - 参数传递：简化任务执行上下文工厂的参数传递
 * - 类型安全：通过强类型确保传递信息的正确性
 * - 扩展性：便于后续增加新的上下文创建参数
 *
 * 包含的信息：
 * 1. 工作流执行图：任务依赖关系和执行顺序
 * 2. 工作流定义：工作流的元数据配置
 * 3. 工作流实例：当前执行的工作流实例
 * 4. 任务定义：任务的元数据配置
 * 5. 任务实例：当前执行的任务实例
 * 6. 项目信息：任务所属的项目上下文
 *
 * 使用流程：
 * 1. 收集所有必需的任务执行信息
 * 2. 使用建造者模式构建请求对象
 * 3. 将请求传递给任务执行上下文工厂
 * 4. 工厂根据请求信息创建完整的执行上下文
 *
 * 数据一致性：
 * - 所有信息必须来自同一次任务执行
 * - 工作流实例和任务实例必须匹配
 * - 任务定义和任务实例必须对应
 * - 项目信息必须与工作流定义一致
 *
 * 类比理解：
 * 就像一份完整的任务委托书，包含了执行任务所需的所有背景信息：
 * - 项目背景：项目信息和工作流定义
 * - 执行计划：工作流执行图和实例
 * - 具体任务：任务定义和实例
 * - 完整信息：确保执行者能够完全理解任务要求
 */
@Getter
@Builder
@AllArgsConstructor
public class TaskExecutionContextCreateRequest {

    /**
     * 工作流执行图
     *
     * 包含任务依赖关系和执行顺序的完整图结构。
     * 执行上下文需要此信息来判断任务的前置依赖状态。
     */
    private IWorkflowExecutionGraph workflowExecutionGraph;

    /**
     * 工作流定义
     *
     * 工作流的元数据定义，包含工作流级别的配置和属性。
     * 提供任务执行上下文所需的工作流基础信息。
     */
    private WorkflowDefinition workflowDefinition;

    /**
     * 工作流实例
     *
     * 当前执行的工作流实例，包含运行时状态和参数。
     * 为任务执行提供工作流级别的运行时上下文。
     */
    private WorkflowInstance workflowInstance;

    /**
     * 任务定义
     *
     * 任务的元数据定义，包含任务类型、参数配置等静态信息。
     * 定义了任务应该如何执行的模板和规范。
     */
    private TaskDefinition taskDefinition;

    /**
     * 任务实例
     *
     * 当前执行的任务实例，包含任务的运行时状态和配置。
     * 是任务执行上下文的核心数据来源。
     */
    private TaskInstance taskInstance;

    /**
     * 项目信息
     *
     * 任务所属的项目信息，用于权限控制和资源配置。
     * 提供任务执行所需的项目级别上下文。
     */
    private Project project;

}
