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

package org.apache.dolphinscheduler.extract.worker;

import org.apache.dolphinscheduler.extract.base.RpcMethod;
import org.apache.dolphinscheduler.extract.base.RpcService;
import org.apache.dolphinscheduler.task.executor.eventbus.ITaskExecutorLifecycleEventReporter;
import org.apache.dolphinscheduler.task.executor.operations.TaskExecutorDispatchRequest;
import org.apache.dolphinscheduler.task.executor.operations.TaskExecutorDispatchResponse;
import org.apache.dolphinscheduler.task.executor.operations.TaskExecutorKillRequest;
import org.apache.dolphinscheduler.task.executor.operations.TaskExecutorKillResponse;
import org.apache.dolphinscheduler.task.executor.operations.TaskExecutorPauseRequest;
import org.apache.dolphinscheduler.task.executor.operations.TaskExecutorPauseResponse;
import org.apache.dolphinscheduler.task.executor.operations.TaskExecutorReassignMasterRequest;
import org.apache.dolphinscheduler.task.executor.operations.TaskExecutorReassignMasterResponse;

/**
 * 物理任务执行器操作接口
 *
 * <p>该接口定义了Worker节点上物理任务执行器的操作方法。</p>
 *
 * <p>主要功能：</p>
 * <ul>
 *   <li>分派任务到Worker节点执行</li>
 *   <li>控制任务的生命周期（暂停、终止）</li>
 *   <li>重新分配任务的Master节点</li>
 *   <li>确认任务生命周期事件</li>
 * </ul>
 */
@RpcService
public interface IPhysicalTaskExecutorOperator {

    /**
     * 分派任务
     * 将任务分配到Worker节点执行
     *
     * @param taskExecutorDispatchRequest 任务分派请求，包含任务信息和执行参数
     * @return 任务分派响应，包含分派结果和状态
     */
    @RpcMethod
    TaskExecutorDispatchResponse dispatchTask(final TaskExecutorDispatchRequest taskExecutorDispatchRequest);

    /**
     * 终止任务
     * 强制停止正在执行的任务
     *
     * @param taskExecutorKillRequest 任务终止请求，包含任务标识信息
     * @return 任务终止响应，包含终止操作的执行结果
     */
    @RpcMethod
    TaskExecutorKillResponse killTask(final TaskExecutorKillRequest taskExecutorKillRequest);

    /**
     * 暂停任务
     * 暂时停止任务执行，可以后续恢复
     *
     * @param taskExecutorPauseRequest 任务暂停请求，包含任务标识信息
     * @return 任务暂停响应，包含暂停操作的执行结果
     */
    @RpcMethod
    TaskExecutorPauseResponse pauseTask(final TaskExecutorPauseRequest taskExecutorPauseRequest);

    /**
     * 重新分配工作流实例的Master节点
     * 当Master节点故障时，将任务重新分配给新的Master节点管理
     *
     * @param taskExecutorReassignMasterRequest 重新分配Master请求，包含新的Master节点信息
     * @return 重新分配Master响应，包含重新分配的执行结果
     */
    @RpcMethod
    TaskExecutorReassignMasterResponse reassignWorkflowInstanceHost(final TaskExecutorReassignMasterRequest taskExecutorReassignMasterRequest);

    /**
     * 确认物理任务执行器生命周期事件
     * 用于确认已接收和处理的任务生命周期事件
     *
     * @param taskExecutorLifecycleEventAck 生命周期事件确认信息
     */
    @RpcMethod
    void ackPhysicalTaskExecutorLifecycleEvent(final ITaskExecutorLifecycleEventReporter.TaskExecutorLifecycleEventAck taskExecutorLifecycleEventAck);

}
