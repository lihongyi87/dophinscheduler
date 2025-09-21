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

package org.apache.dolphinscheduler.server.master.engine.task.client;

import org.apache.dolphinscheduler.server.master.engine.task.runnable.ITaskExecutionRunnable;
import org.apache.dolphinscheduler.server.master.exception.dispatch.TaskDispatchException;
import org.apache.dolphinscheduler.task.executor.eventbus.ITaskExecutorLifecycleEventReporter;

/**
 * 任务执行器客户端代理接口
 *
 * 这是任务执行器客户端代理的核心接口，定义了向任务执行器服务器发送
 * 各种操作指令的方法。该接口采用委托模式，根据任务类型的不同，
 * 将具体的操作委托给不同的实现类。
 *
 * 架构设计：
 * - 使用策略模式，根据任务类型选择不同的实现策略
 * - 封装了与任务执行器的所有RPC通信细节
 * - 提供统一的接口，屏蔽了底层通信的复杂性
 *
 * 主要实现类：
 * @see LogicTaskExecutorClientDelegator 逻辑任务执行器客户端代理（处理条件、依赖等逻辑任务）
 * @see PhysicalTaskExecutorClientDelegator 物理任务执行器客户端代理（处理Shell、SQL等物理任务）
 */
public interface ITaskExecutorClientDelegator {

    /**
     * 分发任务到任务执行器
     *
     * 此方法负责将任务分发到适合的执行器。不同类型的任务会有不同的
     * 分发策略：
     * - 逻辑任务：直接在Master节点上执行
     * - 物理任务：选择合适的Worker节点执行
     *
     * @param taskExecutionRunnable 要分发的任务执行对象
     * @throws TaskDispatchException 分发失败时抛出
     */
    void dispatch(final ITaskExecutionRunnable taskExecutionRunnable) throws TaskDispatchException;

    /**
     * 重新分配Master主机
     *
     * 当Master节点发生故障转移时，需要将任务的管理权从旧的Master转移到
     * 新的Master节点。此方法通知任务执行器更新其内部的Master信息。
     *
     * 注意：
     * - 逻辑任务不支持重新分配，因为它们在Master节点上执行
     * - 物理任务支持重新分配，因为它们在Worker节点上执行
     *
     * @param taskExecutionRunnable 要重新分配的任务执行对象
     * @return true-重新分配成功, false-重新分配失败或不支持
     */
    boolean reassignMasterHost(final ITaskExecutionRunnable taskExecutionRunnable);

    /**
     * 暂停任务执行
     *
     * 向任务执行器发送暂停指令。此方法不保证任务一定会暂停成功，
     * 因为不是所有任务都支持暂停操作。
     *
     * 操作特点：
     * - 异步操作，发送指令后立即返回
     * - 不保证执行成功
     * - 部分任务类型可能不支持暂停
     *
     * @param taskExecutionRunnable 要暂停的任务执行对象
     */
    void pause(final ITaskExecutionRunnable taskExecutionRunnable);

    /**
     * 杀死任务执行
     *
     * 向任务执行器发送杀死指令。此方法不保证任务一定会被杀死成功，
     * 但所有任务都应该支持杀死操作。
     *
     * 操作特点：
     * - 异步操作，发送指令后立即返回
     * - 所有任务都应该支持此操作
     * - 某些情况下可能需要较长时间才能完成
     *
     * @param taskExecutionRunnable 要杀死的任务执行对象
     */
    void kill(final ITaskExecutionRunnable taskExecutionRunnable);

    /**
     * 确认任务执行器生命周期事件
     *
     * 向任务执行器发送生命周期事件的确认消息。这是Master与TaskExecutor
     * 之间双向通信机制的组成部分，确保事件的可靠传递。
     *
     * 通信流程：
     * 1. TaskExecutor发送生命周期事件到Master
     * 2. Master处理事件并更新状态
     * 3. Master通过此方法发送确认消息
     * 4. TaskExecutor收到确认后完成整个事件处理
     *
     * @param taskExecutionRunnable 相关的任务执行对象
     * @param taskExecutorLifecycleEventAck 生命周期事件确认消息
     */
    void ackTaskExecutorLifecycleEvent(
                                       final ITaskExecutionRunnable taskExecutionRunnable,
                                       final ITaskExecutorLifecycleEventReporter.TaskExecutorLifecycleEventAck taskExecutorLifecycleEventAck);
}
