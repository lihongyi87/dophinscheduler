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

import org.apache.dolphinscheduler.common.enums.WorkflowExecutionStatus;
import org.apache.dolphinscheduler.dao.entity.WorkflowInstance;
import org.apache.dolphinscheduler.server.master.engine.WorkflowEventBus;
import org.apache.dolphinscheduler.server.master.engine.graph.IWorkflowExecutionGraph;
import org.apache.dolphinscheduler.server.master.engine.workflow.listener.IWorkflowLifecycleListener;
import org.apache.dolphinscheduler.server.master.runner.IWorkflowExecuteContext;

import java.util.List;

/**
 * 工作流执行运行器接口
 * <p>
 * 该接口定义了工作流执行运行器的核心功能，是DolphinScheduler Master引擎中
 * 用于管理和控制工作流实例执行的核心抽象。每个工作流实例都对应一个运行器，
 * 负责工作流的暂停、停止、状态管理以及生命周期监听器的注册和管理。
 * <p>
 * 主要功能包括：
 * <ul>
 *   <li>工作流实例的基本信息获取（ID、名称、状态）</li>
 *   <li>工作流执行控制（暂停、停止）</li>
 *   <li>工作流执行上下文和图结构访问</li>
 *   <li>事件总线和生命周期监听器管理</li>
 * </ul>
 *
 * @author DolphinScheduler Team
 */
public interface IWorkflowExecutionRunnable {

    /**
     * 获取工作流执行运行器的ID
     * <p>
     * 运行器的ID等同于其对应的工作流实例ID，用于唯一标识一个运行中的工作流
     *
     * @return 工作流执行运行器ID
     */
    default int getId() {
        return getWorkflowInstance().getId();
    }

    /**
     * 获取工作流执行运行器的名称
     * <p>
     * 运行器的名称等同于其对应的工作流实例名称，用于标识工作流的业务含义
     *
     * @return 工作流执行运行器名称
     */
    default String getName() {
        return getWorkflowInstance().getName();
    }

    /**
     * 暂停工作流执行运行器
     * <p>
     * 通过发布暂停事件到工作流事件总线来实现工作流的暂停操作。
     * 暂停后的工作流可以稍后恢复执行。
     */
    void pause();

    /**
     * 检查工作流是否准备好暂停
     * <p>
     * 通过检查工作流实例的当前状态来判断是否可以执行暂停操作。
     * 只有当工作流状态为READY_PAUSE时才允许暂停。
     *
     * @return true如果工作流准备好暂停，否则返回false
     */
    default boolean isWorkflowReadyPause() {
        // 获取当前工作流实例的执行状态
        final WorkflowExecutionStatus workflowExecutionStatus = getWorkflowInstance().getState();
        // 检查状态是否为准备暂停状态
        return workflowExecutionStatus == WorkflowExecutionStatus.READY_PAUSE;
    }

    /**
     * 停止工作流执行运行器
     * <p>
     * 通过发布停止事件到工作流事件总线来实现工作流的停止操作。
     * 停止后的工作流将结束执行，无法恢复。
     */
    void stop();

    /**
     * 检查工作流是否准备好停止
     * <p>
     * 通过检查工作流实例的当前状态来判断是否可以执行停止操作。
     * 只有当工作流状态为READY_STOP时才允许停止。
     *
     * @return true如果工作流准备好停止，否则返回false
     */
    default boolean isWorkflowReadyStop() {
        // 获取当前工作流实例的执行状态
        final WorkflowExecutionStatus workflowExecutionStatus = getWorkflowInstance().getState();
        // 检查状态是否为准备停止状态
        return workflowExecutionStatus == WorkflowExecutionStatus.READY_STOP;
    }

    /**
     * 获取工作流执行上下文
     * <p>
     * 工作流执行上下文包含了工作流执行过程中需要的所有信息和资源，
     * 包括工作流实例、任务实例、依赖关系、配置参数等。
     *
     * @return 工作流执行上下文对象
     */
    IWorkflowExecuteContext getWorkflowExecuteContext();

    /**
     * 获取工作流实例
     * <p>
     * 通过工作流执行上下文获取对应的工作流实例对象，
     * 工作流实例包含了工作流的基本信息和执行状态。
     *
     * @return 工作流实例对象
     */
    default WorkflowInstance getWorkflowInstance() {
        return getWorkflowExecuteContext().getWorkflowInstance();
    }

    /**
     * 获取工作流执行运行器的状态
     * <p>
     * 运行器的状态等同于其对应工作流实例的状态，
     * 用于表示当前工作流的执行状态（如运行中、暂停、完成等）。
     *
     * @return 工作流执行状态枚举
     */
    default WorkflowExecutionStatus getState() {
        return getWorkflowInstance().getState();
    }

    /**
     * 获取工作流事件总线
     * <p>
     * 事件总线用于工作流执行过程中的事件发布和订阅，
     * 实现工作流各组件间的解耦通信。
     *
     * @return 工作流事件总线对象
     */
    default WorkflowEventBus getWorkflowEventBus() {
        return getWorkflowExecuteContext().getWorkflowEventBus();
    }

    /**
     * 获取工作流执行图
     * <p>
     * 工作流执行图包含了工作流中所有任务的依赖关系和执行顺序，
     * 是调度引擎进行任务调度的核心数据结构。
     *
     * @return 工作流执行图对象
     */
    default IWorkflowExecutionGraph getWorkflowExecutionGraph() {
        return getWorkflowExecuteContext().getWorkflowExecutionGraph();
    }

    /**
     * 获取工作流生命周期监听器列表
     * <p>
     * 生命周期监听器用于监听工作流执行过程中的各种事件，
     * 如开始、暂停、停止、完成等，实现自定义的业务逻辑处理。
     *
     * @return 工作流生命周期监听器列表
     */
    List<IWorkflowLifecycleListener> getWorkflowLifecycleListeners();

    /**
     * 注册工作流实例生命周期监听器
     * <p>
     * 向当前工作流运行器注册一个新的生命周期监听器，
     * 该监听器将会接收到工作流执行过程中的各种生命周期事件。
     *
     * @param listener 要注册的工作流生命周期监听器，不能为null
     */
    void registerWorkflowInstanceLifecycleListener(IWorkflowLifecycleListener listener);

}
