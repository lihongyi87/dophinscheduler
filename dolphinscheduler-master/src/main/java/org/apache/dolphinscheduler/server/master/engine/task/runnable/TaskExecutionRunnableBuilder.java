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
import org.apache.dolphinscheduler.server.master.engine.WorkflowEventBus;
import org.apache.dolphinscheduler.server.master.engine.graph.IWorkflowExecutionGraph;

import javax.annotation.Nullable;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import org.springframework.context.ApplicationContext;

/**
 * 任务执行运行器建造者
 *
 * 用于构建TaskExecutionRunnable实例的建造者类。封装了任务执行器创建所需的
 * 所有组件和依赖，使用Lombok注解简化建造者模式的实现。
 *
 * 设计特点：
 * - 不可变性：使用@AllArgsConstructor确保所有字段在构造时设置
 * - 建造者模式：使用@Builder注解自动生成建造者方法
 * - 访问器：使用@Getter提供所有字段的访问方法
 * - 类型安全：通过泛型和强类型确保组件的正确匹配
 *
 * 包含的组件：
 * 1. 工作流执行图：任务依赖关系和执行顺序
 * 2. 工作流定义：工作流的元数据定义
 * 3. 项目信息：任务所属的项目上下文
 * 4. 工作流实例：当前执行的工作流实例
 * 5. 任务定义：任务的元数据定义
 * 6. 任务实例：可选的现有任务实例（用于重试、故障转移等）
 * 7. 事件总线：工作流事件通信机制
 * 8. 应用上下文：Spring容器，用于依赖注入
 *
 * 使用场景：
 * - 首次执行：创建全新的任务执行器
 * - 重试执行：基于失败任务创建重试执行器
 * - 故障转移：为故障任务创建转移执行器
 * - 恢复执行：为暂停任务创建恢复执行器
 *
 * 类比理解：
 * 就像一个完整的项目团队配置表：
 * - 项目计划：工作流执行图和定义
 * - 项目信息：项目和工作流实例
 * - 具体任务：任务定义和实例
 * - 通信机制：事件总线
 * - 资源支持：Spring应用上下文
 */
@Getter
@Builder
@AllArgsConstructor
public class TaskExecutionRunnableBuilder {

    /**
     * 工作流执行图
     *
     * 包含工作流中所有任务的依赖关系、执行顺序和状态信息。
     * 任务执行器使用执行图来判断前置依赖是否满足。
     */
    private final IWorkflowExecutionGraph workflowExecutionGraph;

    /**
     * 工作流定义
     *
     * 工作流的元数据定义，包含工作流的基本配置和属性。
     * 提供任务执行所需的工作流级别信息。
     */
    private final WorkflowDefinition workflowDefinition;

    /**
     * 项目信息
     *
     * 任务所属的项目信息，用于权限控制和资源隔离。
     * 不同项目的任务具有不同的执行权限和资源配置。
     */
    private final Project project;

    /**
     * 工作流实例
     *
     * 当前执行的工作流实例，包含运行时信息和状态。
     * 每次工作流执行都会创建新的实例。
     */
    private final WorkflowInstance workflowInstance;

    /**
     * 任务定义
     *
     * 任务的元数据定义，包含任务类型、参数、配置等信息。
     * 这是任务执行的模板和规范。
     */
    private final TaskDefinition taskDefinition;

    /**
     * 任务实例（可选）
     *
     * 可能存在的任务实例，主要用于重试、故障转移等场景。
     * 首次执行时通常为null，后续会通过工厂创建。
     */
    private final @Nullable TaskInstance taskInstance;

    /**
     * 工作流事件总线
     *
     * 用于工作流内部组件间的事件通信。
     * 任务执行过程中的状态变化都通过事件总线进行通知。
     */
    private final WorkflowEventBus workflowEventBus;

    /**
     * Spring应用上下文
     *
     * 提供Spring容器的依赖注入能力。
     * 任务执行器通过应用上下文获取各种服务和组件。
     */
    private final ApplicationContext applicationContext;
}
