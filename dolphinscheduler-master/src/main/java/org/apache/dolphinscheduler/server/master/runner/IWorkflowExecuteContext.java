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
import org.apache.dolphinscheduler.dao.entity.WorkflowDefinition;
import org.apache.dolphinscheduler.dao.entity.WorkflowInstance;
import org.apache.dolphinscheduler.server.master.engine.WorkflowEventBus;
import org.apache.dolphinscheduler.server.master.engine.graph.IWorkflowExecutionGraph;
import org.apache.dolphinscheduler.server.master.engine.graph.IWorkflowGraph;
import org.apache.dolphinscheduler.server.master.engine.workflow.listener.IWorkflowLifecycleListener;

import java.util.List;

/**
 * 工作流执行上下文接口
 * <p>
 * 定义了工作流执行过程中所需的所有核心组件和信息的访问接口。
 * 该接口为工作流执行提供了统一的上下文访问方式，包含了从命令解析、
 * 工作流定义加载、实例管理到图结构处理和事件总线等各个方面的信息。
 * </p>
 *
 * @author DolphinScheduler
 * @since 1.0.0
 */
public interface IWorkflowExecuteContext {

    /**
     * 获取触发工作流执行的命令对象
     * <p>
     * 命令对象包含了工作流执行的触发信息，如执行类型、调度时间、
     * 执行参数等关键信息，是工作流执行的起始点。
     * </p>
     *
     * @return 命令对象，包含工作流执行的触发信息
     */
    Command getCommand();

    /**
     * 获取工作流定义信息
     * <p>
     * 工作流定义包含了工作流的静态配置信息，如工作流名称、版本、
     * 任务节点定义、依赖关系等元数据信息。
     * </p>
     *
     * @return 工作流定义对象，包含工作流的元数据信息
     */
    WorkflowDefinition getWorkflowDefinition();

    /**
     * 获取工作流实例信息
     * <p>
     * 工作流实例是工作流定义的具体执行实体，包含了本次执行的
     * 运行时信息，如实例ID、执行状态、开始时间、结束时间等。
     * </p>
     *
     * @return 工作流实例对象，包含本次执行的运行时信息
     */
    WorkflowInstance getWorkflowInstance();

    /**
     * 获取工作流图结构
     * <p>
     * 工作流图表示工作流中任务节点的静态拓扑结构和依赖关系，
     * 用于任务调度和执行顺序的确定。
     * </p>
     *
     * @return 工作流图对象，表示任务节点的拓扑结构
     */
    IWorkflowGraph getWorkflowGraph();

    /**
     * 获取工作流执行图
     * <p>
     * 工作流执行图是在工作流图基础上构建的运行时图结构，
     * 包含了任务实例的执行状态、依赖关系和执行顺序等动态信息。
     * </p>
     *
     * @return 工作流执行图对象，包含任务执行的动态信息
     */
    IWorkflowExecutionGraph getWorkflowExecutionGraph();

    /**
     * 获取工作流事件总线
     * <p>
     * 事件总线负责工作流执行过程中各种事件的发布和订阅，
     * 实现组件间的解耦通信和状态变更通知。
     * </p>
     *
     * @return 工作流事件总线，用于事件的发布和订阅
     */
    WorkflowEventBus getWorkflowEventBus();

    /**
     * 获取工作流生命周期监听器列表
     * <p>
     * 监听器用于监听工作流实例在执行过程中的各个生命周期事件，
     * 如启动、暂停、完成、失败等状态变更，提供扩展和自定义处理能力。
     * </p>
     *
     * @return 工作流生命周期监听器列表
     */
    List<IWorkflowLifecycleListener> getWorkflowInstanceLifecycleListeners();

}
