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

package org.apache.dolphinscheduler.server.master.failover;

import org.apache.dolphinscheduler.plugin.task.api.utils.LogUtils;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event.TaskFailoverLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.task.runnable.ITaskExecutionRunnable;

import org.springframework.stereotype.Component;

/**
 * 任务故障转移处理器
 *
 * 专门处理单个任务级别的故障转移操作，是Worker节点故障转移的核心执行组件。
 * 当Worker节点故障时，该组件负责将运行在故障Worker上的任务重新调度到健康的Worker节点。
 *
 * <p>任务故障转移机制：
 * <ul>
 *   <li>基于事件驱动的架构：通过发布TaskFailoverLifecycleEvent触发故障转移</li>
 *   <li>无状态设计：不维护任务状态，依赖事件总线进行状态流转</li>
 *   <li>日志追踪：使用MDC记录工作流实例ID，便于问题追踪</li>
 * </ul>
 *
 * <p>与工作流故障转移的区别：
 * <ul>
 *   <li>粒度更细：针对单个任务而不是整个工作流</li>
 *   <li>影响更小：不影响工作流的整体执行流程</li>
 *   <li>恢复更快：任务级别的重新调度速度更快</li>
 * </ul>
 *
 * <p>故障转移流程：
 * <ol>
 *   <li>设置日志追踪上下文（工作流实例ID）</li>
 *   <li>发布任务故障转移生命周期事件</li>
 *   <li>清理日志追踪上下文</li>
 * </ol>
 *
 * @author DolphinScheduler Team
 * @since 1.0.0
 */
@Component
public class TaskFailover {

    /**
     * 执行任务故障转移
     *
     * 该方法是任务故障转移的入口点，通过发布TaskFailoverLifecycleEvent事件
     * 来触发任务的故障转移处理。事件发布后，相应的生命周期处理器会接收
     * 并处理该事件，完成任务的重新调度。
     *
     * <p>执行流程：
     * <ol>
     *   <li>设置MDC日志上下文，记录当前处理的工作流实例ID</li>
     *   <li>通过工作流事件总线发布任务故障转移事件</li>
     *   <li>清理MDC日志上下文，避免线程复用时的上下文污染</li>
     * </ol>
     *
     * <p>事件驱动的优势：
     * <ul>
     *   <li>解耦：故障转移触发与具体处理逻辑分离</li>
     *   <li>异步：事件发布后立即返回，不阻塞调用线程</li>
     *   <li>可扩展：便于添加其他故障转移相关的处理逻辑</li>
     * </ul>
     *
     * @param taskExecutionRunnable 需要故障转移的任务执行实例，包含任务的完整上下文信息
     */
    public void failoverTask(final ITaskExecutionRunnable taskExecutionRunnable) {
        // 设置MDC日志上下文，便于追踪特定工作流实例的日志
        LogUtils.setWorkflowInstanceIdMDC(taskExecutionRunnable.getWorkflowInstance().getId());

        // 通过工作流事件总线发布任务故障转移生命周期事件
        // 该事件将被相应的生命周期处理器接收并处理
        taskExecutionRunnable.getWorkflowEventBus().publish(TaskFailoverLifecycleEvent.of(taskExecutionRunnable));

        // 清理MDC日志上下文，避免线程复用时的上下文污染
        LogUtils.removeWorkflowInstanceIdMDC();
    }

}
