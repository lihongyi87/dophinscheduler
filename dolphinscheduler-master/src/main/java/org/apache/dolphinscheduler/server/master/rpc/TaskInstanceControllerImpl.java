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

import org.apache.dolphinscheduler.extract.master.ITaskInstanceController;
import org.apache.dolphinscheduler.extract.master.transportor.TaskGroupSlotAcquireSuccessNotifyRequest;
import org.apache.dolphinscheduler.extract.master.transportor.TaskGroupSlotAcquireSuccessNotifyResponse;
import org.apache.dolphinscheduler.plugin.task.api.utils.LogUtils;
import org.apache.dolphinscheduler.server.master.engine.IWorkflowRepository;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event.TaskDispatchLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.task.runnable.ITaskExecutionRunnable;
import org.apache.dolphinscheduler.server.master.engine.workflow.runnable.IWorkflowExecutionRunnable;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * 任务实例控制器实现类
 *
 * <p>该类是Master节点中负责任务实例控制相关操作的RPC服务实现。
 * 主要处理与任务实例状态管理、资源分配相关的远程调用请求。</p>
 *
 * <p>作为任务实例控制的核心组件，该类主要负责：</p>
 * <ul>
 *   <li>处理任务组槽位获取成功的通知</li>
 *   <li>唤醒等待资源的任务实例</li>
 *   <li>管理任务实例的生命周期状态</li>
 *   <li>维护任务与工作流的关联关系</li>
 * </ul>
 *
 * <p>该控制器是任务资源管理机制的重要组成部分，特别是在处理
 * 任务组(TaskGroup)资源分配时发挥关键作用。当任务获取到所需的
 * 资源槽位后，通过该控制器通知Master节点继续执行任务。</p>
 *
 * @author DolphinScheduler Community
 * @see ITaskInstanceController
 * @see IWorkflowRepository
 */
@Slf4j
@Component
public class TaskInstanceControllerImpl implements ITaskInstanceController {

    /**
     * 工作流执行运行时内存仓库
     * 用于存储和管理当前正在执行的工作流实例的运行时信息
     */
    @Autowired
    private IWorkflowRepository workflowExecutionRunnableMemoryRepository;

    /**
     * 通知任务组槽位获取成功
     *
     * <p>当任务成功获取到任务组(TaskGroup)中的资源槽位时，
     * 通过该方法通知Master节点可以继续执行该任务。</p>
     *
     * <p>任务组是DolphinScheduler中的资源管理机制，用于控制并发执行的任务数量。
     * 当任务需要特定资源时，必须先获取任务组中的槽位，获取成功后才能开始执行。</p>
     *
     * <p>处理流程：</p>
     * <ol>
     *   <li>接收任务组槽位获取成功的通知请求</li>
     *   <li>从请求中提取工作流实例ID和任务实例ID</li>
     *   <li>设置MDC上下文用于日志追踪</li>
     *   <li>查找对应的工作流执行运行时对象</li>
     *   <li>查找对应的任务执行运行时对象</li>
     *   <li>发布任务分发生命周期事件，唤醒等待的任务</li>
     *   <li>返回处理结果</li>
     * </ol>
     *
     * <p>错误处理：</p>
     * <ul>
     *   <li>如果找不到对应的工作流，返回失败响应</li>
     *   <li>如果找不到对应的任务，返回失败响应</li>
     *   <li>处理完成后清理MDC上下文</li>
     * </ul>
     *
     * @param taskGroupSlotAcquireSuccessNotifyRequest 任务组槽位获取成功通知请求
     * @return TaskGroupSlotAcquireSuccessNotifyResponse 通知处理响应
     */
    @Override
    public TaskGroupSlotAcquireSuccessNotifyResponse notifyTaskGroupSlotAcquireSuccess(
                                                                                       final TaskGroupSlotAcquireSuccessNotifyRequest taskGroupSlotAcquireSuccessNotifyRequest) {
        log.info("Received TaskGroupSlotAcquireSuccessRequest request{}", taskGroupSlotAcquireSuccessNotifyRequest);
        try {
            final int workflowInstanceId = taskGroupSlotAcquireSuccessNotifyRequest.getWorkflowInstanceId();
            final int taskInstanceId = taskGroupSlotAcquireSuccessNotifyRequest.getTaskInstanceId();
            LogUtils.setWorkflowAndTaskInstanceIDMDC(workflowInstanceId, taskInstanceId);
            final IWorkflowExecutionRunnable workflowExecutionRunnable =
                    workflowExecutionRunnableMemoryRepository.get(workflowInstanceId);
            if (workflowExecutionRunnable == null) {
                log.warn("cannot find WorkflowExecuteRunnable: {}, no need to Wakeup task", workflowInstanceId);
                return TaskGroupSlotAcquireSuccessNotifyResponse
                        .failed("cannot find WorkflowExecuteRunnable: " + workflowInstanceId);
            }
            final ITaskExecutionRunnable taskExecutionRunnable = workflowExecutionRunnable
                    .getWorkflowExecuteContext()
                    .getWorkflowExecutionGraph()
                    .getTaskExecutionRunnableById(taskInstanceId);
            if (taskExecutionRunnable == null) {
                log.warn("Cannot find TaskExecutionRunnable: {}, no need to Wakeup task", taskInstanceId);
                return TaskGroupSlotAcquireSuccessNotifyResponse
                        .failed("Cannot find TaskExecutionRunnable: " + taskInstanceId);
            }
            workflowExecutionRunnable.getWorkflowEventBus()
                    .publish(TaskDispatchLifecycleEvent.of(taskExecutionRunnable));
            log.info("Success Wakeup TaskInstance: {}", taskInstanceId);
            return TaskGroupSlotAcquireSuccessNotifyResponse.success();
        } finally {
            LogUtils.removeWorkflowAndTaskInstanceIdMDC();
        }
    }
}
