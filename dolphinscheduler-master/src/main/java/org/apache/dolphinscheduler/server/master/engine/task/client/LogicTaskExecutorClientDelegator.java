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

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.extract.base.client.Clients;
import org.apache.dolphinscheduler.extract.master.ILogicTaskExecutorOperator;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.server.master.config.MasterConfig;
import org.apache.dolphinscheduler.server.master.engine.exceptions.TaskKillException;
import org.apache.dolphinscheduler.server.master.engine.task.runnable.ITaskExecutionRunnable;
import org.apache.dolphinscheduler.server.master.exception.dispatch.TaskDispatchException;
import org.apache.dolphinscheduler.task.executor.eventbus.ITaskExecutorLifecycleEventReporter;
import org.apache.dolphinscheduler.task.executor.operations.TaskExecutorDispatchRequest;
import org.apache.dolphinscheduler.task.executor.operations.TaskExecutorDispatchResponse;
import org.apache.dolphinscheduler.task.executor.operations.TaskExecutorKillRequest;
import org.apache.dolphinscheduler.task.executor.operations.TaskExecutorKillResponse;
import org.apache.dolphinscheduler.task.executor.operations.TaskExecutorPauseRequest;
import org.apache.dolphinscheduler.task.executor.operations.TaskExecutorPauseResponse;

import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * 逻辑任务执行器客户端代理
 *
 * 这个类是专门处理逻辑任务的客户端代理实现。逻辑任务是指那些不需要在Worker节点
 * 上执行实际代码的任务，而是在Master节点内部进行逻辑判断和流程控制的任务。
 *
 * 支持的逻辑任务类型：
 * - 条件任务（Conditions）：根据条件判断决定后续流程
 * - 依赖任务（Dependent）：等待其他工作流或任务的完成
 * - 子工作流任务（SubProcess）：启动和管理子工作流
 * - 切换任务（Switch）：根据条件切换到不同的分支
 * - 阻塞任务（Blocking）：阻塞等待外部条件满足
 *
 * 执行特点：
 * - 所有逻辑任务都在Master节点本地执行
 * - 不需要进行负载均衡选择Worker节点
 * - 不支持主机重新分配（因为任务就在Master上执行）
 * - RPC通信目标是Master节点自身的逻辑任务执行器
 *
 * 与物理任务的区别：
 * - 物理任务：在Worker节点执行具体的Shell、SQL等代码
 * - 逻辑任务：在Master节点执行流程控制和状态判断
 *
 * RPC通信机制：
 * - 使用ILogicTaskExecutorOperator接口进行通信
 * - 通信目标是Master节点的逻辑任务执行器组件
 * - 支持任务分发、暂停、杀死和生命周期事件确认
 */
@Slf4j
@Component
public class LogicTaskExecutorClientDelegator implements ITaskExecutorClientDelegator {

    /**
     * Master节点配置信息
     * 包含Master节点的地址、端口等配置信息，用于获取Master节点的RPC服务地址
     */
    @Autowired
    private MasterConfig masterConfig;

    /**
     * 分发逻辑任务到Master节点的逻辑任务执行器
     *
     * 与物理任务不同，逻辑任务不需要选择Worker节点，直接在Master节点本地执行。
     * 这种设计可以减少网络通信开销，提高逻辑判断的响应速度。
     *
     * 分发流程：
     * 1. 获取Master节点的服务地址
     * 2. 设置任务执行上下文的主机信息为Master地址
     * 3. 通过RPC调用Master本地的逻辑任务执行器
     * 4. 检查分发响应结果
     *
     * @param taskExecutionRunnable 要分发的逻辑任务执行对象
     * @throws TaskDispatchException 分发失败时抛出，可能原因包括Master服务不可用、RPC通信失败等
     */
    @Override
    public void dispatch(final ITaskExecutionRunnable taskExecutionRunnable) throws TaskDispatchException {
        // 获取Master节点的逻辑任务执行器地址（就是Master自身地址）
        final String logicTaskExecutorAddress = masterConfig.getMasterAddress();
        final TaskExecutionContext taskExecutionContext = taskExecutionRunnable.getTaskExecutionContext();

        // 设置任务执行主机为Master节点
        taskExecutionContext.setHost(logicTaskExecutorAddress);
        taskExecutionRunnable.getTaskInstance().setHost(logicTaskExecutorAddress);

        // 通过RPC客户端向Master节点的逻辑任务执行器发送分发请求
        final TaskExecutorDispatchResponse logicTaskDispatchResponse = Clients
                .withService(ILogicTaskExecutorOperator.class)
                .withHost(logicTaskExecutorAddress)
                .dispatchTask(TaskExecutorDispatchRequest.of(taskExecutionContext));

        // 检查分发是否成功，失败则抛出异常
        if (!logicTaskDispatchResponse.isDispatchSuccess()) {
            throw new TaskDispatchException(
                    String.format("Dispatch LogicTask to %s failed, response is: %s",
                            taskExecutionContext.getHost(), logicTaskDispatchResponse));
        }
    }

    /**
     * 重新分配Master主机（逻辑任务不支持）
     *
     * 逻辑任务不支持主机重新分配操作，因为逻辑任务是在Master节点上执行的，
     * 而不是在Worker节点上执行。当Master发生故障转移时，逻辑任务会随着
     * Master一起转移，不需要单独的重新分配操作。
     *
     * 设计原理：
     * - 逻辑任务的生命周期与Master节点绑定
     * - Master故障时，逻辑任务会在新的Master上重新开始执行
     * - 不存在跨节点的任务状态迁移问题
     *
     * @param taskExecutionRunnable 任务执行对象（此参数在逻辑任务中无效）
     * @return 始终返回false，表示不支持重新分配操作
     */
    @Override
    public boolean reassignMasterHost(final ITaskExecutionRunnable taskExecutionRunnable) {
        // 逻辑任务不支持接管操作，因为逻辑任务不在Worker节点上执行
        return false;
    }

    /**
     * 暂停逻辑任务执行
     *
     * 向Master节点的逻辑任务执行器发送暂停指令。对于逻辑任务，暂停操作主要是
     * 停止当前的逻辑判断流程，等待后续的恢复指令。
     *
     * 逻辑任务暂停特点：
     * - 响应速度较快，因为是本地操作
     * - 主要暂停等待型任务（如依赖任务、阻塞任务）
     * - 条件判断类任务可能无法暂停（执行时间很短）
     *
     * @param taskExecutionRunnable 要暂停的逻辑任务执行对象
     */
    @Override
    public void pause(final ITaskExecutionRunnable taskExecutionRunnable) {
        final TaskInstance taskInstance = taskExecutionRunnable.getTaskInstance();
        final String executorHost = taskInstance.getHost();
        final String taskName = taskInstance.getName();
        checkArgument(StringUtils.isNotEmpty(executorHost), "Executor host is empty");

        // 向逻辑任务执行器发送暂停请求
        final TaskExecutorPauseResponse pauseResponse = Clients
                .withService(ILogicTaskExecutorOperator.class)
                .withHost(taskInstance.getHost())
                .pauseTask(TaskExecutorPauseRequest.of(taskInstance.getId()));

        // 记录暂停操作结果
        if (pauseResponse.isSuccess()) {
            log.info("Pause task {} on executor {} successfully", taskName, executorHost);
        } else {
            log.warn("Pause task {} on executor {} failed with response {}", taskName, executorHost, pauseResponse);
        }
    }

    /**
     * 杀死逻辑任务执行
     *
     * 向Master节点的逻辑任务执行器发送杀死指令，强制终止正在执行的逻辑任务。
     * 对于逻辑任务，杀死操作主要是停止相关的等待和判断逻辑。
     *
     * 逻辑任务杀死特点：
     * - 操作简单，主要是停止等待循环和状态检查
     * - 不涉及外部进程的强制终止
     * - 清理相关的内存状态和计时器
     * - 响应速度快，因为是本地操作
     *
     * @param taskExecutionRunnable 要杀死的逻辑任务执行对象
     * @throws TaskKillException 杀死操作过程中发生错误时抛出
     */
    @Override
    public void kill(final ITaskExecutionRunnable taskExecutionRunnable) throws TaskKillException {
        final TaskInstance taskInstance = taskExecutionRunnable.getTaskInstance();
        final String executorHost = taskInstance.getHost();
        final String taskName = taskInstance.getName();
        checkArgument(StringUtils.isNotEmpty(executorHost), "Executor host is empty");

        // 向逻辑任务执行器发送杀死请求
        final TaskExecutorKillResponse killResponse = Clients
                .withService(ILogicTaskExecutorOperator.class)
                .withHost(taskInstance.getHost())
                .killTask(TaskExecutorKillRequest.of(taskInstance.getId()));

        // 记录杀死操作结果
        if (killResponse.isSuccess()) {
            log.info("Kill task {} on executor {} successfully", taskName, executorHost);
        } else {
            log.warn("Kill task {} on executor {} failed with response {}", taskName, executorHost, killResponse);
        }
    }

    /**
     * 确认逻辑任务执行器生命周期事件
     *
     * 向Master节点的逻辑任务执行器发送生命周期事件确认消息。由于逻辑任务
     * 在Master节点本地执行，这个确认操作实际上是Master内部组件之间的通信。
     *
     * 逻辑任务生命周期事件包括：
     * - 任务开始执行
     * - 逻辑判断完成
     * - 等待条件满足
     * - 任务执行成功/失败
     * - 任务被暂停/恢复
     * - 任务被杀死
     *
     * 确认机制的作用：
     * - 确保Master内部状态的一致性
     * - 完成事件处理的闭环
     * - 支持事件的可靠传递和处理
     *
     * @param taskExecutionRunnable 相关的逻辑任务执行对象
     * @param taskExecutorLifecycleEventAck 生命周期事件确认消息
     */
    @Override
    public void ackTaskExecutorLifecycleEvent(
                                              final ITaskExecutionRunnable taskExecutionRunnable,
                                              final ITaskExecutorLifecycleEventReporter.TaskExecutorLifecycleEventAck taskExecutorLifecycleEventAck) {
        final TaskInstance taskInstance = taskExecutionRunnable.getTaskInstance();
        final String executorHost = taskInstance.getHost();
        checkArgument(StringUtils.isNotEmpty(executorHost), "Executor host is empty");

        // 向逻辑任务执行器发送生命周期事件确认
        Clients
                .withService(ILogicTaskExecutorOperator.class)
                .withHost(taskInstance.getHost())
                .ackTaskExecutorLifecycleEvent(taskExecutorLifecycleEventAck);
    }

}
