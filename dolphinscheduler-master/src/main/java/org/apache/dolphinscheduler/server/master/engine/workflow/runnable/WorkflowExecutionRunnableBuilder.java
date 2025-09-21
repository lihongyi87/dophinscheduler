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

package org.apache.dolphinscheduler.server.master.engine.workflow.runnable;

import org.apache.dolphinscheduler.server.master.runner.WorkflowExecuteContext;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import org.springframework.context.ApplicationContext;

/**
 * 工作流执行运行器构建器
 * <p>
 * 该构建器类使用建造者模式来构建工作流执行运行器实例。它封装了构建运行器所需的
 * 核心组件，包括工作流执行上下文构建器和Spring应用上下文。通过这种设计，
 * 可以灵活地配置和创建不同的工作流执行运行器实例。
 * <p>
 * 主要功能包括：
 * <ul>
 *   <li>封装工作流执行上下文构建器，用于创建执行上下文</li>
 *   <li>提供Spring应用上下文的访问，支持依赖注入</li>
 *   <li>通过建造者模式提供灵活的构建方式</li>
 *   <li>支持链式调用和参数配置</li>
 * </ul>
 * <p>
 * 该构建器与WorkflowExecutionRunnable配合使用，通过工作流执行上下文构建器
 * 来创建完整的工作流执行环境，包括任务图、监听器、事件总线等核心组件。
 *
 * @author DolphinScheduler Team
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class WorkflowExecutionRunnableBuilder {

    /**
     * 工作流执行上下文构建器
     * <p>
     * 该构建器负责创建工作流执行过程中需要的完整上下文环境，
     * 包括工作流实例、任务实例、依赖关系图、配置参数、
     * 生命周期监听器、事件总线等核心组件。
     * <p>
     * 通过该构建器可以灵活地配置工作流执行所需的各种资源和参数，
     * 最终通过build()方法创建完整的工作流执行上下文。
     */
    private WorkflowExecuteContext.WorkflowExecuteContextBuilder workflowExecuteContextBuilder;

    /**
     * Spring应用上下文
     * <p>
     * 提供对Spring容器的访问，支持依赖注入和Bean管理。
     * 在工作流执行过程中，可能需要获取各种Spring管理的组件，
     * 如数据访问对象、服务类、配置Bean等。
     * <p>
     * 该上下文为工作流执行运行器提供了与Spring生态系统集成的能力，
     * 使得运行器可以利用Spring的各种特性和功能。
     */
    private ApplicationContext applicationContext;

}
