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

package org.apache.dolphinscheduler.server.master.runner;

import org.apache.dolphinscheduler.dao.entity.Command;
import org.apache.dolphinscheduler.dao.entity.Project;
import org.apache.dolphinscheduler.dao.entity.WorkflowDefinition;
import org.apache.dolphinscheduler.dao.entity.WorkflowInstance;
import org.apache.dolphinscheduler.server.master.engine.WorkflowEventBus;
import org.apache.dolphinscheduler.server.master.engine.graph.IWorkflowExecutionGraph;
import org.apache.dolphinscheduler.server.master.engine.graph.IWorkflowGraph;
import org.apache.dolphinscheduler.server.master.engine.workflow.listener.IWorkflowLifecycleListener;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * 工作流执行上下文实现类
 * <p>
 * 实现了{@link IWorkflowExecuteContext}接口，提供工作流执行过程中所需的所有上下文信息。
 * 该类作为数据载体，封装了从命令解析到工作流实际执行所需的所有核心组件和配置信息。
 * </p>
 *
 * <h3>包含的核心组件：</h3>
 * <ul>
 *   <li><strong>命令信息</strong>：触发工作流执行的命令对象</li>
 *   <li><strong>工作流定义</strong>：工作流的静态配置和元数据</li>
 *   <li><strong>项目信息</strong>：工作流所属的项目上下文</li>
 *   <li><strong>工作流实例</strong>：当前执行的工作流实例信息</li>
 *   <li><strong>工作流图</strong>：任务间的静态依赖关系图</li>
 *   <li><strong>执行图</strong>：运行时的任务执行状态图</li>
 *   <li><strong>事件总线</strong>：工作流事件的发布和订阅机制</li>
 *   <li><strong>监听器</strong>：工作流生命周期事件的监听器列表</li>
 * </ul>
 *
 * <p>
 * 该类采用不可变设计，所有字段在构造时初始化后不可修改，确保了线程安全性和数据一致性。
 * 同时提供了建造者模式的支持，方便灵活地创建和配置上下文对象。
 * </p>
 *
 * @author DolphinScheduler
 * @since 1.0.0
 * @see IWorkflowExecuteContext
 * @see WorkflowExecuteContextBuilder
 */
@Getter
@AllArgsConstructor
public class WorkflowExecuteContext implements IWorkflowExecuteContext {

    /**
     * 触发工作流执行的命令对象
     * <p>
     * 包含了工作流执行的触发信息，如执行类型、调度时间、执行参数等。
     * </p>
     */
    private final Command command;

    /**
     * 工作流定义信息
     * <p>
     * 包含工作流的静态配置信息，如工作流名称、版本、任务节点定义等。
     * </p>
     */
    private final WorkflowDefinition workflowDefinition;

    /**
     * 项目信息
     * <p>
     * 工作流所属的项目上下文，包含项目名称、配置信息等。
     * </p>
     */
    private final Project project;

    /**
     * 工作流实例信息
     * <p>
     * 当前执行的工作流实例，包含实例 ID、执行状态、开始时间等运行时信息。
     * </p>
     */
    private final WorkflowInstance workflowInstance;

    /**
     * 工作流图结构
     * <p>
     * 表示工作流中任务节点的静态拓扑结构和依赖关系，用于任务调度和执行顺序的确定。
     * </p>
     */
    private final IWorkflowGraph workflowGraph;

    /**
     * 工作流执行图
     * <p>
     * 在工作流图基础上构建的运行时图结构，包含任务实例的执行状态、依赖关系和执行顺序等动态信息。
     * </p>
     */
    private final IWorkflowExecutionGraph workflowExecutionGraph;

    /**
     * 工作流事件总线
     * <p>
     * 负责工作流执行过程中各种事件的发布和订阅，实现组件间的解耦通信。
     * </p>
     */
    private final WorkflowEventBus workflowEventBus;

    /**
     * 工作流生命周期监听器列表
     * <p>
     * 用于监听工作流实例在执行过程中的各个生命周期事件，如启动、暂停、完成、失败等状态变更。
     * </p>
     */
    private final List<IWorkflowLifecycleListener> workflowInstanceLifecycleListeners;

    /**
     * 创建工作流执行上下文建造者
     * <p>
     * 返回一个新的建造者实例，用于灵活地配置和构建工作流执行上下文对象。
     * </p>
     *
     * @return 工作流执行上下文建造者实例
     */
    public static WorkflowExecuteContextBuilder builder() {
        // 创建并返回一个新的建造者实例，用于通过链式调用的方式构建上下文对象
        return new WorkflowExecuteContextBuilder();
    }

    /**
     * 工作流执行上下文建造者类
     * <p>
     * 实现了建造者模式，提供了灵活的方式来构建工作流执行上下文对象。
     * 通过链式调用的方式，可以逐步设置各个组件，最后调用build()方法来创建
     * 最终的上下文对象。
     * </p>
     *
     * <h4>使用示例：</h4>
     * <pre>
     * WorkflowExecuteContext context = WorkflowExecuteContext.builder()
     *     .withCommand(command)
     *     .workflowDefinition(definition)
     *     .workflowInstance(instance)
     *     .workflowGraph(graph)
     *     .workflowExecutionGraph(executionGraph)
     *     .workflowEventBus(eventBus)
     *     .workflowInstanceLifecycleListeners(listeners)
     *     .project(project)
     *     .build();
     * </pre>
     *
     * @author DolphinScheduler
     * @since 1.0.0
     */
    @Data
    @NoArgsConstructor
    public static class WorkflowExecuteContextBuilder {

        /**
         * 命令对象
         */
        private Command command;

        /**
         * 工作流定义
         */
        private WorkflowDefinition workflowDefinition;

        /**
         * 工作流实例
         */
        private WorkflowInstance workflowInstance;

        /**
         * 工作流图结构
         */
        private IWorkflowGraph workflowGraph;

        /**
         * 工作流执行图
         */
        private IWorkflowExecutionGraph workflowExecutionGraph;

        /**
         * 工作流事件总线
         */
        private WorkflowEventBus workflowEventBus;

        /**
         * 工作流生命周期监听器列表
         */
        private List<IWorkflowLifecycleListener> workflowInstanceLifecycleListeners;

        /**
         * 项目信息
         */
        private Project project;

        /**
         * 设置命令对象
         *
         * @param command 命令对象
         * @return 建造者实例，支持链式调用
         */
        public WorkflowExecuteContextBuilder withCommand(Command command) {
            // 保存传入的命令对象到建造者中
            this.command = command;
            // 返回建造者实例本身，支持方法链式调用
            return this;
        }

        /**
         * 设置工作流定义
         *
         * @param workflowDefinition 工作流定义对象
         * @return 建造者实例，支持链式调用
         */
        public WorkflowExecuteContextBuilder workflowDefinition(WorkflowDefinition workflowDefinition) {
            // 保存传入的工作流定义对象到建造者中
            this.workflowDefinition = workflowDefinition;
            // 返回建造者实例本身，支持方法链式调用
            return this;
        }

        /**
         * 设置工作流实例
         *
         * @param workflowInstance 工作流实例对象
         * @return 建造者实例，支持链式调用
         */
        public WorkflowExecuteContextBuilder workflowInstance(WorkflowInstance workflowInstance) {
            // 保存传入的工作流实例对象到建造者中
            this.workflowInstance = workflowInstance;
            // 返回建造者实例本身，支持方法链式调用
            return this;
        }

        /**
         * 设置工作流图结构
         *
         * @param workflowGraph 工作流图对象
         * @return 建造者实例，支持链式调用
         */
        public WorkflowExecuteContextBuilder workflowGraph(IWorkflowGraph workflowGraph) {
            // 保存传入的工作流图对象到建造者中
            this.workflowGraph = workflowGraph;
            // 返回建造者实例本身，支持方法链式调用
            return this;
        }

        /**
         * 设置工作流执行图
         *
         * @param workflowExecutionGraph 工作流执行图对象
         * @return 建造者实例，支持链式调用
         */
        public WorkflowExecuteContextBuilder workflowExecutionGraph(IWorkflowExecutionGraph workflowExecutionGraph) {
            // 保存传入的工作流执行图对象到建造者中
            this.workflowExecutionGraph = workflowExecutionGraph;
            // 返回建造者实例本身，支持方法链式调用
            return this;
        }

        /**
         * 设置工作流事件总线
         *
         * @param workflowEventBus 工作流事件总线对象
         * @return 建造者实例，支持链式调用
         */
        public WorkflowExecuteContextBuilder workflowEventBus(WorkflowEventBus workflowEventBus) {
            // 保存传入的工作流事件总线对象到建造者中
            this.workflowEventBus = workflowEventBus;
            // 返回建造者实例本身，支持方法链式调用
            return this;
        }

        /**
         * 设置工作流生命周期监听器列表
         *
         * @param workflowInstanceLifecycleListeners 工作流生命周期监听器列表
         * @return 建造者实例，支持链式调用
         */
        public WorkflowExecuteContextBuilder workflowInstanceLifecycleListeners(List<IWorkflowLifecycleListener> workflowInstanceLifecycleListeners) {
            // 保存传入的工作流生命周期监听器列表到建造者中
            this.workflowInstanceLifecycleListeners = workflowInstanceLifecycleListeners;
            // 返回建造者实例本身，支持方法链式调用
            return this;
        }

        /**
         * 设置项目信息
         *
         * @param project 项目对象
         * @return 建造者实例，支持链式调用
         */
        public WorkflowExecuteContextBuilder project(Project project) {
            // 保存传入的项目对象到建造者中
            this.project = project;
            // 返回建造者实例本身，支持方法链式调用
            return this;
        }

        /**
         * 构建工作流执行上下文对象
         * <p>
         * 使用当前建造者中设置的所有组件创建一个不可变的工作流执行上下文对象。
         * </p>
         *
         * @return 新创建的工作流执行上下文对象
         */
        public WorkflowExecuteContext build() {
            // ========== 创建工作流执行上下文对象 ==========
            // 使用建造者中设置的所有组件，通过构造函数创建一个不可变的上下文对象
            // 参数顺序必须与WorkflowExecuteContext构造函数的参数顺序保持一致
            return new WorkflowExecuteContext(
                    command,                                    // 触发工作流执行的命令对象
                    workflowDefinition,                        // 工作流定义信息
                    project,                                   // 项目信息
                    workflowInstance,                          // 工作流实例信息
                    workflowGraph,                             // 工作流图结构
                    workflowExecutionGraph,                    // 工作流执行图
                    workflowEventBus,                          // 工作流事件总线
                    workflowInstanceLifecycleListeners);       // 工作流生命周期监听器列表
        }
    }

}
