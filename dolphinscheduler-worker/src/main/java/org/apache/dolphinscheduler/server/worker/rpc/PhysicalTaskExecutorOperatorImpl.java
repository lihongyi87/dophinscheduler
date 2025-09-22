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

package org.apache.dolphinscheduler.server.worker.rpc;

import org.apache.dolphinscheduler.extract.worker.IPhysicalTaskExecutorOperator;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.server.worker.executor.PhysicalTaskEngineDelegator;
import org.apache.dolphinscheduler.task.executor.eventbus.ITaskExecutorLifecycleEventReporter;
import org.apache.dolphinscheduler.task.executor.operations.TaskExecutorDispatchRequest;
import org.apache.dolphinscheduler.task.executor.operations.TaskExecutorDispatchResponse;
import org.apache.dolphinscheduler.task.executor.operations.TaskExecutorKillRequest;
import org.apache.dolphinscheduler.task.executor.operations.TaskExecutorKillResponse;
import org.apache.dolphinscheduler.task.executor.operations.TaskExecutorPauseRequest;
import org.apache.dolphinscheduler.task.executor.operations.TaskExecutorPauseResponse;
import org.apache.dolphinscheduler.task.executor.operations.TaskExecutorReassignMasterRequest;
import org.apache.dolphinscheduler.task.executor.operations.TaskExecutorReassignMasterResponse;

import org.apache.commons.lang3.exception.ExceptionUtils;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * 物理任务执行器操作实现类
 *
 * <p>该类是Worker节点中负责处理物理任务执行相关RPC请求的核心组件。
 * 实现了IPhysicalTaskExecutorOperator接口，提供任务分发、终止、暂停等操作。</p>
 *
 * <p>主要功能：</p>
 * <ul>
 *   <li>接收并处理Master发送的任务分发请求</li>
 *   <li>处理任务终止请求，立即停止正在执行的任务</li>
 *   <li>处理任务暂停请求，暂停任务执行但保留状态</li>
 *   <li>处理工作流实例主机重分配请求</li>
 *   <li>处理任务执行器生命周期事件确认</li>
 * </ul>
 *
 * <p>该类作为Worker节点对外提供的RPC服务入口，负责协调物理任务引擎
 * 的各种操作，确保任务执行的可靠性和可控性。</p>
 *
 * @see IPhysicalTaskExecutorOperator
 * @see PhysicalTaskEngineDelegator
 */
@Slf4j
@Component
public class PhysicalTaskExecutorOperatorImpl implements IPhysicalTaskExecutorOperator {

    @Autowired
    private PhysicalTaskEngineDelegator physicalTaskEngineDelegator;

    /**
     * 分发任务到Worker节点执行
     *
     * <p>该方法是Worker节点接收Master节点任务分发请求的核心入口。
     * 当Master节点需要在Worker节点上执行任务时，会通过RPC调用此方法。</p>
     *
     * <p>处理流程：</p>
     * <ol>
     *   <li>接收Master发送的任务分发请求</li>
     *   <li>提取任务执行上下文信息</li>
     *   <li>委托物理任务引擎执行逻辑任务</li>
     *   <li>返回任务分发结果</li>
     * </ol>
     *
     * @param taskExecutorDispatchRequest 任务分发请求，包含任务执行上下文等信息
     * @return TaskExecutorDispatchResponse 任务分发响应，包含执行结果
     */
    @Override
    public TaskExecutorDispatchResponse dispatchTask(final TaskExecutorDispatchRequest taskExecutorDispatchRequest) {
        // 记录接收到的任务分发请求日志
        log.info("Receive TaskExecutorDispatchResponse: {}", taskExecutorDispatchRequest);
        // 从分发请求中提取任务执行上下文，包含任务执行所需的全部信息
        final TaskExecutionContext taskExecutionContext = taskExecutorDispatchRequest.getTaskExecutionContext();
        try {
            // 委托物理任务引擎执行逻辑任务，启动实际的任务执行流程
            physicalTaskEngineDelegator.dispatchLogicTask(taskExecutionContext);
            // 记录任务分发成功的日志
            log.info("Handle TaskExecutorDispatchResponse: {} success", taskExecutorDispatchRequest);
            // 返回成功响应给Master节点
            return TaskExecutorDispatchResponse.success();
        } catch (Throwable throwable) {
            // 捕获任务分发过程中的任何异常
            log.error("Handle TaskExecutorDispatchResponse: {} failed", taskExecutorDispatchRequest, throwable);
            // 返回失败响应，包含详细的错误信息
            return TaskExecutorDispatchResponse.failed(ExceptionUtils.getMessage(throwable));
        }
    }

    /**
     * 终止正在执行的任务
     *
     * <p>该方法用于接收Master节点的任务终止请求，立即停止指定任务的执行。
     * 当需要取消任务执行或工作流被手动停止时，Master会调用此方法。</p>
     *
     * <p>处理流程：</p>
     * <ol>
     *   <li>接收任务终止请求</li>
     *   <li>提取任务实例ID</li>
     *   <li>委托物理任务引擎终止逻辑任务</li>
     *   <li>释放相关资源</li>
     *   <li>返回终止操作结果</li>
     * </ol>
     *
     * @param taskExecutorKillRequest 任务终止请求，包含要终止的任务实例ID
     * @return TaskExecutorKillResponse 任务终止响应，包含操作结果
     */
    @Override
    public TaskExecutorKillResponse killTask(final TaskExecutorKillRequest taskExecutorKillRequest) {
        // 记录接收到的任务终止请求日志
        log.info("Receive TaskExecutorKillRequest: {}", taskExecutorKillRequest);
        // 从终止请求中提取任务实例ID，用于标识要终止的具体任务
        final int taskInstanceId = taskExecutorKillRequest.getTaskInstanceId();
        try {
            // 委托物理任务引擎执行任务终止逻辑，强制停止任务执行
            physicalTaskEngineDelegator.killLogicTask(taskInstanceId);
            // 记录任务终止成功的日志
            log.info("Handle TaskExecutorKillRequest: {} success", taskExecutorKillRequest);
            // 返回成功响应给Master节点
            return TaskExecutorKillResponse.success();
        } catch (Throwable throwable) {
            // 捕获任务终止过程中的任何异常
            log.error("Handle TaskExecutorKillRequest: {} failed", taskExecutorKillRequest, throwable);
            // 返回失败响应，包含详细的错误信息
            return TaskExecutorKillResponse.fail(ExceptionUtils.getMessage(throwable));
        }
    }

    /**
     * 暂停正在执行的任务
     *
     * <p>该方法用于接收Master节点的任务暂停请求，暂停指定任务的执行但保留其状态。
     * 与终止任务不同，暂停的任务可以稍后恢复执行。</p>
     *
     * <p>处理流程：</p>
     * <ol>
     *   <li>接收任务暂停请求</li>
     *   <li>提取任务实例ID</li>
     *   <li>委托物理任务引擎暂停逻辑任务</li>
     *   <li>保存任务执行状态</li>
     *   <li>返回暂停操作结果</li>
     * </ol>
     *
     * @param taskPauseRequest 任务暂停请求，包含要暂停的任务实例ID
     * @return TaskExecutorPauseResponse 任务暂停响应，包含操作结果
     */
    @Override
    public TaskExecutorPauseResponse pauseTask(final TaskExecutorPauseRequest taskPauseRequest) {
        // 记录接收到的任务暂停请求日志
        log.info("Receive TaskExecutorPauseRequest: {}", taskPauseRequest);
        // 从暂停请求中提取任务实例ID，用于标识要暂停的具体任务
        final int taskInstanceId = taskPauseRequest.getTaskInstanceId();
        try {
            // 委托物理任务引擎执行任务暂停逻辑，保存任务状态并暂停执行
            physicalTaskEngineDelegator.pauseLogicTask(taskInstanceId);
            // 记录任务暂停成功的日志
            log.info("Handle TaskExecutorPauseRequest: {} success", taskPauseRequest);
            // 返回成功响应给Master节点
            return TaskExecutorPauseResponse.success();
        } catch (Throwable throwable) {
            // 捕获任务暂停过程中的任何异常
            log.error("Handle TaskExecutorPauseRequest: {} failed", taskPauseRequest, throwable);
            // 返回失败响应，包含详细的错误信息
            return TaskExecutorPauseResponse.fail(ExceptionUtils.getMessage(throwable));
        }
    }

    /**
     * 重新分配工作流实例的Master主机
     *
     * <p>该方法用于处理工作流实例Master主机的重新分配请求。
     * 当原Master节点故障时，需要将工作流实例分配给新的Master节点管理。</p>
     *
     * <p>处理流程：</p>
     * <ol>
     *   <li>接收Master主机重分配请求</li>
     *   <li>委托物理任务引擎处理主机重分配</li>
     *   <li>更新任务执行器的Master主机信息</li>
     *   <li>返回重分配操作结果</li>
     * </ol>
     *
     * @param taskExecutorReassignMasterRequest Master主机重分配请求
     * @return TaskExecutorReassignMasterResponse 重分配响应，包含操作结果
     */
    @Override
    public TaskExecutorReassignMasterResponse reassignWorkflowInstanceHost(final TaskExecutorReassignMasterRequest taskExecutorReassignMasterRequest) {
        // 委托物理任务引擎处理工作流实例Master主机重分配请求
        // 当原Master节点故障时，需要将工作流实例迁移到新的Master节点
        boolean success =
                physicalTaskEngineDelegator.reassignWorkflowInstanceHost(taskExecutorReassignMasterRequest);
        // 检查重分配操作是否成功
        if (success) {
            // 返回成功响应给Master节点
            return TaskExecutorReassignMasterResponse.success();
        }
        // 返回失败响应，说明重分配操作失败
        return TaskExecutorReassignMasterResponse.failed("Reassign master host failed");
    }

    /**
     * 确认物理任务执行器生命周期事件
     *
     * <p>该方法用于接收和处理Master节点对物理任务执行器生命周期事件的确认消息。
     * 这是Worker与Master之间事件通信机制的重要组成部分。</p>
     *
     * <p>处理流程：</p>
     * <ol>
     *   <li>接收生命周期事件确认消息</li>
     *   <li>记录确认消息信息</li>
     *   <li>委托物理任务引擎处理确认消息</li>
     *   <li>更新任务执行器状态</li>
     * </ol>
     *
     * @param taskExecutorLifecycleEventAck 任务执行器生命周期事件确认消息
     */
    @Override
    public void ackPhysicalTaskExecutorLifecycleEvent(final ITaskExecutorLifecycleEventReporter.TaskExecutorLifecycleEventAck taskExecutorLifecycleEventAck) {
        // 记录接收到的任务执行器生命周期事件确认消息
        log.info("Receive TaskExecutorLifecycleEventAck: {}", taskExecutorLifecycleEventAck);
        // 委托物理任务引擎处理生命周期事件确认，更新任务执行器状态
        // 这是Worker与Master之间事件通信机制的重要组成部分，确保事件的可靠传递
        physicalTaskEngineDelegator.ackPhysicalTaskExecutorLifecycleEventACK(taskExecutorLifecycleEventAck);
    }
}
