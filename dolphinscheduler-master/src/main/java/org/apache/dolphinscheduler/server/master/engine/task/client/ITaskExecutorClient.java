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

import org.apache.dolphinscheduler.server.master.engine.exceptions.TaskKillException;
import org.apache.dolphinscheduler.server.master.engine.exceptions.TaskPauseException;
import org.apache.dolphinscheduler.server.master.engine.exceptions.TaskReassignMasterHostException;
import org.apache.dolphinscheduler.server.master.engine.task.runnable.ITaskExecutionRunnable;
import org.apache.dolphinscheduler.server.master.exception.dispatch.TaskDispatchException;
import org.apache.dolphinscheduler.task.executor.TaskEngine;
import org.apache.dolphinscheduler.task.executor.eventbus.ITaskExecutorLifecycleEventReporter;

/**
 * 任务执行器客户端接口
 *
 * 这是Master节点与TaskEngine通信的核心客户端接口，定义了所有与任务执行器
 * 交互的操作方法。它负责处理任务的完整生命周期管理。
 *
 * 主要功能：
 * 1. 任务分发：将任务分发到适合的执行器
 * 2. 主机重新分配：支持Master故障转移时的任务转移
 * 3. 任务控制：暂停和杀死正在执行的任务
 * 4. 生命周期管理：处理任务执行器的生命周期事件确认
 *
 * RPC通信机制：
 * - 使用基于Netty的RPC框架进行通信
 * - 支持同步和异步调用
 * - 具备自动重试和故障转移机制
 * - 提供请求响应的完整生命周期管理
 *
 * 实现类：{@link TaskExecutorClient}
 * 通信目标：{@link org.apache.dolphinscheduler.task.executor.TaskEngine}
 */
public interface ITaskExecutorClient {

    /**
     * 分发任务到任务执行器
     *
     * 这是任务执行的入口点，Master通过此方法将待执行的任务分发给合适的执行器。
     * 分发过程包括：
     * 1. 选择合适的执行器（Worker节点或Master本地执行器）
     * 2. 构建任务执行上下文
     * 3. 通过RPC调用发送任务到目标执行器
     * 4. 等待并处理分发响应
     *
     * @param taskExecutionRunnable 要分发的任务执行对象，包含任务实例和执行上下文
     * @throws TaskDispatchException 任务分发失败时抛出，包括网络异常、执行器不可用等情况
     */
    void dispatch(final ITaskExecutionRunnable taskExecutionRunnable) throws TaskDispatchException;

    /**
     * 重新分配工作流实例主机
     *
     * 当Master节点发生故障转移时，原本由故障Master管理的任务需要转移到新的Master。
     * 此方法通知任务执行器更新其内部的Master主机信息，确保后续的生命周期事件
     * 能够正确回调到新的Master节点。
     *
     * 应用场景：
     * - Master节点故障恢复
     * - 集群负载均衡调整
     * - 主备切换场景
     *
     * @param taskExecutionRunnable 需要重新分配的任务执行对象
     * @return true-重新分配成功, false-重新分配失败或不支持
     * @throws TaskReassignMasterHostException 重新分配过程中发生错误时抛出
     */
    boolean reassignWorkflowInstanceHost(final ITaskExecutionRunnable taskExecutionRunnable) throws TaskReassignMasterHostException;

    /**
     * 暂停任务执行
     *
     * 向任务执行器发送暂停指令，要求暂停正在执行的任务。这是一个异步操作，
     * 方法会在发送暂停请求并收到响应后立即返回，但不保证任务一定会被暂停。
     *
     * 重要说明：
     * - 这是一个异步方法，调用后立即返回
     * - 不是所有任务都支持暂停操作
     * - 如果任务不支持暂停，会忽略暂停请求
     * - 对于不支持暂停的任务，需要等待其自然完成
     *
     * 暂停机制：
     * 1. Master发送暂停请求到执行器
     * 2. 执行器检查任务是否支持暂停
     * 3. 如果支持，执行器尝试暂停任务
     * 4. 执行器返回暂停操作的结果
     *
     * @param taskExecutionRunnable 要暂停的任务执行对象
     * @throws TaskPauseException 暂停操作过程中发生错误时抛出
     */
    void pause(final ITaskExecutionRunnable taskExecutionRunnable) throws TaskPauseException;

    /**
     * 强制杀死任务
     *
     * 向任务执行器发送强制杀死指令，要求立即终止正在执行的任务。这是一个
     * 异步操作，方法会在发送杀死请求后立即返回，但不保证任务一定会被杀死。
     *
     * 重要说明：
     * - 这是一个异步方法，调用后立即返回
     * - 所有任务都应该支持杀死操作
     * - 某些杀死操作可能耗时较长，因此存在不确定性
     * - 杀死操作可能涉及资源清理，需要一定时间
     *
     * 杀死机制：
     * 1. Master发送杀死请求到执行器
     * 2. 执行器中断任务执行线程
     * 3. 执行器清理任务相关资源
     * 4. 执行器返回杀死操作的结果
     * 5. 如果正常杀死失败，可能使用强制杀死（kill -9）
     *
     * @param taskExecutionRunnable 要杀死的任务执行对象
     * @throws TaskKillException 杀死操作过程中发生错误时抛出
     */
    void kill(final ITaskExecutionRunnable taskExecutionRunnable) throws TaskKillException;

    /**
     * 确认任务执行器生命周期事件
     *
     * 向TaskEngine发送生命周期事件的确认消息，这是Master与TaskExecutor之间
     * 双向通信机制的重要组成部分。通过此方法，Master告知执行器已成功接收
     * 并处理了相应的生命周期事件。
     *
     * 生命周期事件包括：
     * - 任务开始执行
     * - 任务执行成功
     * - 任务执行失败
     * - 任务被杀死
     * - 任务暂停
     * - 任务恢复
     *
     * 确认机制：
     * 1. TaskExecutor发送生命周期事件到Master
     * 2. Master处理事件并更新任务状态
     * 3. Master通过此方法发送确认消息
     * 4. TaskExecutor收到确认后完成事件处理流程
     *
     * 容错机制：
     * - 此方法不会抛出异常
     * - 如果发送确认失败，执行器引擎会自动重试
     * - 支持幂等操作，重复确认不会产生副作用
     *
     * @param taskExecutionRunnable 相关的任务执行对象
     * @param taskExecutorLifecycleEventAck 生命周期事件确认消息
     */
    void ackTaskExecutorLifecycleEvent(
                                       final ITaskExecutionRunnable taskExecutionRunnable,
                                       final ITaskExecutorLifecycleEventReporter.TaskExecutorLifecycleEventAck taskExecutorLifecycleEventAck);

}
