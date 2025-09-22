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

package org.apache.dolphinscheduler.server.master.rpc;

import org.apache.dolphinscheduler.extract.master.ILogicTaskExecutorOperator;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.server.master.engine.executor.LogicTaskEngineDelegator;
import org.apache.dolphinscheduler.task.executor.eventbus.ITaskExecutorLifecycleEventReporter;
import org.apache.dolphinscheduler.task.executor.log.TaskExecutorMDCUtils;
import org.apache.dolphinscheduler.task.executor.operations.TaskExecutorDispatchRequest;
import org.apache.dolphinscheduler.task.executor.operations.TaskExecutorDispatchResponse;
import org.apache.dolphinscheduler.task.executor.operations.TaskExecutorKillRequest;
import org.apache.dolphinscheduler.task.executor.operations.TaskExecutorKillResponse;
import org.apache.dolphinscheduler.task.executor.operations.TaskExecutorPauseRequest;
import org.apache.dolphinscheduler.task.executor.operations.TaskExecutorPauseResponse;

import org.apache.commons.lang3.exception.ExceptionUtils;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * 逻辑任务执行器操作实现类
 *
 * <p>该类是Master服务器中负责处理逻辑任务执行相关RPC操作的核心组件。
 * 通过RPC接口接收来自其他节点的任务调度、暂停、终止等请求，
 * 并委托给LogicTaskEngineDelegator进行具体的任务处理。</p>
 *
 * <p>主要功能包括：</p>
 * <ul>
 *   <li>接收并处理任务分发请求</li>
 *   <li>接收并处理任务暂停请求</li>
 *   <li>接收并处理任务终止请求</li>
 *   <li>处理任务执行器生命周期事件确认</li>
 * </ul>
 *
 * <p>该类采用了统一的错误处理和日志记录机制，
 * 所有操作都通过MDC（Mapped Diagnostic Context）进行上下文追踪。</p>
 *
 * @author DolphinScheduler Community
 * @see ILogicTaskExecutorOperator
 * @see LogicTaskEngineDelegator
 */
@Slf4j
@Service
public class LogicTaskExecutorOperatorImpl implements ILogicTaskExecutorOperator {

    /**
     * 逻辑任务引擎委托器
     * 负责实际执行任务相关的操作，包括任务分发、暂停、终止等
     */
    @Autowired
    private LogicTaskEngineDelegator logicTaskEngineDelegator;

    /**
     * 分发任务到逻辑任务执行器
     *
     * <p>接收来自RPC客户端的任务分发请求，并委托给逻辑任务引擎进行处理。
     * 该方法是任务调度的入口点，负责将任务执行上下文传递给底层的任务引擎。</p>
     *
     * <p>处理流程：</p>
     * <ol>
     *   <li>从分发请求中提取任务执行上下文</li>
     *   <li>设置MDC上下文用于日志追踪</li>
     *   <li>委托给逻辑任务引擎进行任务分发</li>
     *   <li>返回分发结果响应</li>
     * </ol>
     *
     * @param taskExecutorDispatchRequest 任务执行器分发请求，包含任务执行上下文等信息
     * @return TaskExecutorDispatchResponse 任务分发响应，包含分发成功或失败的状态信息
     */
    @Override
    public TaskExecutorDispatchResponse dispatchTask(final TaskExecutorDispatchRequest taskExecutorDispatchRequest) {
        final TaskExecutionContext taskExecutionContext = taskExecutorDispatchRequest.getTaskExecutionContext();
        try (
                final TaskExecutorMDCUtils.MDCAutoClosable ignore =
                        TaskExecutorMDCUtils.logWithMDC(taskExecutionContext.getTaskInstanceId())) {
            log.info("Receive  {}", taskExecutorDispatchRequest);
            try {
                logicTaskEngineDelegator.dispatchLogicTask(taskExecutionContext);
                log.info("Handle {} success", taskExecutorDispatchRequest);
                return TaskExecutorDispatchResponse.success();
            } catch (Throwable throwable) {
                log.error("Handle {} failed", taskExecutorDispatchRequest, throwable);
                return TaskExecutorDispatchResponse.failed(ExceptionUtils.getMessage(throwable));
            }
        }
    }

    /**
     * 暂停逻辑任务执行
     *
     * <p>接收来自RPC客户端的任务暂停请求，并委托给逻辑任务引擎进行处理。
     * 该方法用于暂停正在执行的任务，使任务进入暂停状态而不是直接终止。</p>
     *
     * <p>处理流程：</p>
     * <ol>
     *   <li>从暂停请求中提取任务实例ID</li>
     *   <li>设置MDC上下文用于日志追踪</li>
     *   <li>委托给逻辑任务引擎进行任务暂停</li>
     *   <li>返回暂停结果响应</li>
     * </ol>
     *
     * @param taskPauseRequest 任务暂停请求，包含要暂停的任务实例ID
     * @return TaskExecutorPauseResponse 任务暂停响应，包含暂停成功或失败的状态信息
     */
    @Override
    public TaskExecutorPauseResponse pauseTask(final TaskExecutorPauseRequest taskPauseRequest) {
        final int taskInstanceId = taskPauseRequest.getTaskInstanceId();
        try (final TaskExecutorMDCUtils.MDCAutoClosable ignore = TaskExecutorMDCUtils.logWithMDC(taskInstanceId)) {
            log.info("Receive {}", taskPauseRequest);
            try {
                logicTaskEngineDelegator.pauseLogicTask(taskInstanceId);
                log.info("Handle {} success", taskPauseRequest);
                return TaskExecutorPauseResponse.success();
            } catch (Throwable throwable) {
                log.error("Handle {} failed", taskPauseRequest, throwable);
                return TaskExecutorPauseResponse.fail(ExceptionUtils.getMessage(throwable));
            }
        }
    }

    /**
     * 确认任务执行器生命周期事件
     *
     * <p>接收来自任务执行器的生命周期事件确认消息，并委托给逻辑任务引擎进行处理。
     * 该方法用于处理任务执行过程中的各种生命周期事件，如任务开始、完成、失败等。</p>
     *
     * <p>生命周期事件确认是Master与TaskExecutor之间通信的重要机制，
     * 确保任务状态变更能够被正确跟踪和处理。</p>
     *
     * <p>处理流程：</p>
     * <ol>
     *   <li>从确认消息中提取任务执行器ID</li>
     *   <li>设置MDC上下文用于日志追踪</li>
     *   <li>委托给逻辑任务引擎处理生命周期事件</li>
     * </ol>
     *
     * @param taskExecutorLifecycleEventAck 任务执行器生命周期事件确认消息
     */
    @Override
    public void ackTaskExecutorLifecycleEvent(final ITaskExecutorLifecycleEventReporter.TaskExecutorLifecycleEventAck taskExecutorLifecycleEventAck) {
        final int taskExecutorId = taskExecutorLifecycleEventAck.getTaskExecutorId();
        try (final TaskExecutorMDCUtils.MDCAutoClosable ignore = TaskExecutorMDCUtils.logWithMDC(taskExecutorId)) {
            log.info("Receive : {}", taskExecutorLifecycleEventAck);
            logicTaskEngineDelegator.ackLogicTaskExecutionEvent(taskExecutorLifecycleEventAck);
        }
    }

    /**
     * 终止逻辑任务执行
     *
     * <p>接收来自RPC客户端的任务终止请求，并委托给逻辑任务引擎进行处理。
     * 该方法用于强制终止正在执行的任务，使任务立即停止并释放相关资源。</p>
     *
     * <p>与暂停操作不同，终止操作是不可逆的，任务一旦被终止就无法恢复执行。
     * 通常在任务出现异常、超时或用户主动取消时使用。</p>
     *
     * <p>处理流程：</p>
     * <ol>
     *   <li>从终止请求中提取任务实例ID</li>
     *   <li>设置MDC上下文用于日志追踪</li>
     *   <li>委托给逻辑任务引擎进行任务终止</li>
     *   <li>返回终止结果响应</li>
     * </ol>
     *
     * @param taskKillRequest 任务终止请求，包含要终止的任务实例ID
     * @return TaskExecutorKillResponse 任务终止响应，包含终止成功或失败的状态信息
     */
    @Override
    public TaskExecutorKillResponse killTask(final TaskExecutorKillRequest taskKillRequest) {
        final int taskInstanceId = taskKillRequest.getTaskInstanceId();
        try (final TaskExecutorMDCUtils.MDCAutoClosable ignore = TaskExecutorMDCUtils.logWithMDC(taskInstanceId)) {
            log.info("Receive  {}", taskKillRequest);
            try {
                logicTaskEngineDelegator.killLogicTask(taskInstanceId);
                log.info("Handle  {} success", taskKillRequest);
                return TaskExecutorKillResponse.success();
            } catch (Throwable throwable) {
                log.error("Handle  {} failed", taskKillRequest, throwable);
                return TaskExecutorKillResponse.fail(ExceptionUtils.getMessage(throwable));
            }
        }
    }

}
