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

package org.apache.dolphinscheduler.server.master.engine.workflow.lifecycle.handler;

import org.apache.dolphinscheduler.server.master.engine.ILifecycleEventHandler;
import org.apache.dolphinscheduler.server.master.engine.workflow.lifecycle.AbstractWorkflowLifecycleLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.workflow.listener.IWorkflowLifecycleListener;
import org.apache.dolphinscheduler.server.master.engine.workflow.runnable.IWorkflowExecutionRunnable;
import org.apache.dolphinscheduler.server.master.engine.workflow.statemachine.IWorkflowStateAction;
import org.apache.dolphinscheduler.server.master.engine.workflow.statemachine.WorkflowStateActionFactory;

import org.apache.commons.collections4.CollectionUtils;

import java.util.List;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;

/**
 * 工作流生命周期事件处理器抽象基类
 *
 * 这是所有工作流生命周期事件处理器的基础类，提供了事件处理的通用框架。
 * 所有具体的事件处理器（如启动处理器、暂停处理器等）都必须继承这个类。
 *
 * 主要功能：
 * 1. 事件处理的统一流程控制
 * 2. 状态机操作的委托和路由
 * 3. 工作流生命周期监听器的触发
 * 4. 事件处理过程的日志记录
 *
 * 处理流程：
 * 1. 根据工作流当前状态获取对应的状态操作对象
 * 2. 记录事件处理开始日志
 * 3. 委托给具体的子类处理器执行业务逻辑
 * 4. 记录事件处理完成日志
 * 5. 触发相关的生命周期监听器
 *
 * 简单理解：这就像一个事件处理的"总调度员"，负责协调各个组件完成事件处理。
 *
 * @param <T> 具体的工作流生命周期事件类型
 * @author DolphinScheduler
 */
@Slf4j
public abstract class AbstractWorkflowLifecycleEventHandler<T extends AbstractWorkflowLifecycleLifecycleEvent>
        implements
            ILifecycleEventHandler<T> {

    /**
     * 工作流状态操作工厂
     *
     * 用于根据工作流的当前状态，获取对应的状态操作对象。
     * 不同状态下对同一事件的处理逻辑可能不同，通过状态机模式实现。
     */
    @Autowired
    private WorkflowStateActionFactory workflowStateActionFactory;

    /**
     * 处理工作流生命周期事件的主入口方法
     *
     * 这是事件处理的核心方法，定义了完整的事件处理流程。
     * 采用模板方法模式，在基类中定义处理流程，具体的业务逻辑由子类实现。
     *
     * 处理步骤：
     * 1. 根据工作流当前状态获取对应的状态操作对象
     * 2. 记录事件处理开始的日志
     * 3. 调用子类的具体处理方法
     * 4. 记录事件处理完成的日志
     * 5. 触发工作流生命周期监听器
     *
     * @param workflowExecutionRunnable 工作流执行对象，包含工作流的完整信息
     * @param event 要处理的工作流生命周期事件
     */
    @Override
    public void handle(final IWorkflowExecutionRunnable workflowExecutionRunnable, final T event) {
        // 根据工作流当前状态获取对应的状态操作对象
        final IWorkflowStateAction action = workflowStateActionFactory.getAction(workflowExecutionRunnable.getState());

        // 记录事件处理开始日志，便于问题追踪和调试
        log.info("Begin fire workflow {} LifecycleEvent[{}] with state: {}",
                workflowExecutionRunnable.getName(),
                event,
                workflowExecutionRunnable.getState().name());

        // 委托给子类的具体处理方法
        handle(action, workflowExecutionRunnable, event);

        // 记录事件处理完成日志
        log.info("Fired workflow {} LifecycleEvent[{}] with state: {}",
                workflowExecutionRunnable.getName(),
                event,
                workflowExecutionRunnable.getState().name());

        // 触发相关的生命周期监听器
        doTriggerWorkflowLifecycleListener(workflowExecutionRunnable, event);
    }

    /**
     * 处理具体工作流生命周期事件的抽象方法
     *
     * 这是由子类实现的具体事件处理方法。不同类型的事件处理器
     * 会实现不同的业务逻辑。
     *
     * @param workflowStateAction 工作流状态操作对象，包含状态相关的处理逻辑
     * @param workflowExecutionRunnable 工作流执行对象，包含工作流的完整信息
     * @param event 要处理的具体事件对象
     */
    public abstract void handle(
                                final IWorkflowStateAction workflowStateAction,
                                final IWorkflowExecutionRunnable workflowExecutionRunnable,
                                final T event);

    /**
     * 触发工作流生命周期监听器
     *
     * 在事件处理完成后，通知所有相关的监听器。
     * 监听器可以用于实现额外的业务逻辑，如日志记录、指标统计、告警等。
     *
     * 实现说明：
     * 1. 获取工作流关联的所有监听器
     * 2. 遍历每个监听器，检查是否匹配当前事件
     * 3. 对匹配的监听器发送事件通知
     * 4. 异常处理：监听器处理失败不会影响主流程
     *
     * @param workflowExecutionRunnable 工作流执行对象
     * @param event 要通知的事件对象
     */
    private void doTriggerWorkflowLifecycleListener(
                                                    final IWorkflowExecutionRunnable workflowExecutionRunnable,
                                                    final T event) {
        // 获取工作流关联的所有生命周期监听器
        final List<IWorkflowLifecycleListener> listeners = workflowExecutionRunnable.getWorkflowLifecycleListeners();
        if (CollectionUtils.isEmpty(listeners)) {
            // 如果没有监听器，直接返回
            return;
        }

        // 遍历所有监听器并触发相应的事件
        for (final IWorkflowLifecycleListener listener : listeners) {
            try {
                // 检查监听器是否匹配当前事件类型
                if (listener.match(event)) {
                    // 通知监听器处理事件
                    listener.notifyWorkflowLifecycleEvent(workflowExecutionRunnable, event);
                }
            } catch (Exception e) {
                // 监听器处理异常不应影响主流程，只记录警告日志
                log.warn("Trigger WorkflowLifecycleListener on event: {} failed", event, e);
            }
        }
    }

}
