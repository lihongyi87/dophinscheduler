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

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.dolphinscheduler.dao.entity.WorkflowInstance;
import org.apache.dolphinscheduler.server.master.engine.workflow.lifecycle.event.WorkflowPauseLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.workflow.lifecycle.event.WorkflowStopLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.workflow.listener.IWorkflowLifecycleListener;
import org.apache.dolphinscheduler.server.master.runner.IWorkflowExecuteContext;

import java.util.List;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * 工作流执行运行器实现类
 * <p>
 * 该类是工作流执行运行器接口的默认实现，负责管理单个工作流实例的执行过程。
 * 它封装了工作流的执行上下文，提供工作流暂停、停止等控制功能，并管理
 * 工作流生命周期监听器。
 * <p>
 * 主要功能包括：
 * <ul>
 *   <li>维护工作流执行上下文和相关资源</li>
 *   <li>提供工作流暂停和停止的事件发布机制</li>
 *   <li>管理工作流生命周期监听器的注册和获取</li>
 *   <li>提供工作流运行器的字符串表示</li>
 * </ul>
 * <p>
 * 该类通过事件总线机制实现工作流控制操作的解耦，
 * 暂停和停止操作都是通过发布相应的生命周期事件来实现的。
 *
 * @author DolphinScheduler Team
 */
@Slf4j
public class WorkflowExecutionRunnable implements IWorkflowExecutionRunnable {

    /**
     * 工作流执行上下文
     * <p>
     * 包含工作流执行过程中需要的所有信息和资源，
     * 如工作流实例、任务实例、依赖关系、配置参数等
     */
    @Getter
    private final IWorkflowExecuteContext workflowExecuteContext;

    /**
     * 工作流实例生命周期监听器列表
     * <p>
     * 用于监听工作流执行过程中的各种生命周期事件，
     * 支持动态注册和获取监听器
     */
    @Getter
    private final List<IWorkflowLifecycleListener> workflowInstanceLifecycleListeners;

    /**
     * 构造工作流执行运行器
     * <p>
     * 通过构建器模式创建工作流执行运行器实例，初始化执行上下文和监听器列表。
     * 执行上下文通过构建器的build方法创建，监听器列表从执行上下文中获取。
     *
     * @param workflowExecutionRunnableBuilder 工作流执行运行器构建器，包含构建所需的所有信息
     */
    public WorkflowExecutionRunnable(WorkflowExecutionRunnableBuilder workflowExecutionRunnableBuilder) {
        // 通过构建器创建工作流执行上下文
        this.workflowExecuteContext = workflowExecutionRunnableBuilder.getWorkflowExecuteContextBuilder().build();
        // 从执行上下文中获取生命周期监听器列表
        this.workflowInstanceLifecycleListeners = workflowExecuteContext.getWorkflowInstanceLifecycleListeners();
    }

    /**
     * 暂停工作流执行
     * <p>
     * 通过工作流事件总线发布暂停生命周期事件来实现工作流的暂停操作。
     * 这种事件驱动的方式实现了暂停操作的解耦，允许多个组件监听和响应暂停事件。
     */
    @Override
    public void pause() {
        // 发布工作流暂停生命周期事件
        getWorkflowEventBus().publish(WorkflowPauseLifecycleEvent.of(this));
    }

    /**
     * 停止工作流执行
     * <p>
     * 通过工作流事件总线发布停止生命周期事件来实现工作流的停止操作。
     * 这种事件驱动的方式实现了停止操作的解耦，允许多个组件监听和响应停止事件。
     */
    @Override
    public void stop() {
        // 发布工作流停止生命周期事件
        getWorkflowEventBus().publish(WorkflowStopLifecycleEvent.of(this));
    }

    /**
     * 获取工作流生命周期监听器列表
     * <p>
     * 返回当前工作流运行器注册的所有生命周期监听器，
     * 这些监听器用于监听工作流执行过程中的各种生命周期事件。
     *
     * @return 工作流生命周期监听器列表
     */
    @Override
    public List<IWorkflowLifecycleListener> getWorkflowLifecycleListeners() {
        return workflowInstanceLifecycleListeners;
    }

    /**
     * 注册工作流实例生命周期监听器
     * <p>
     * 向当前工作流运行器注册一个新的生命周期监听器。
     * 注册的监听器将会接收到工作流执行过程中的各种生命周期事件。
     *
     * @param listener 要注册的工作流生命周期监听器，不能为null
     * @throws IllegalArgumentException 如果监听器为null
     */
    @Override
    public void registerWorkflowInstanceLifecycleListener(IWorkflowLifecycleListener listener) {
        // 检查监听器不能为null
        checkArgument(listener != null, "listener cannot be null");
        // 将监听器添加到监听器列表中
        workflowInstanceLifecycleListeners.add(listener);
    }

    /**
     * 返回工作流执行运行器的字符串表示
     * <p>
     * 提供包含工作流名称和状态的简洁字符串表示，
     * 便于日志记录和调试。
     *
     * @return 工作流执行运行器的字符串表示，格式为"WorkflowExecutionRunnable{name=工作流名称, state=状态名称}"
     */
    @Override
    public String toString() {
        // 获取工作流实例信息
        final WorkflowInstance workflowInstance = workflowExecuteContext.getWorkflowInstance();
        return "WorkflowExecutionRunnable{" +
                "name=" + workflowInstance.getName() +
                ", state=" + workflowInstance.getState().name() +
                '}';
    }
}
