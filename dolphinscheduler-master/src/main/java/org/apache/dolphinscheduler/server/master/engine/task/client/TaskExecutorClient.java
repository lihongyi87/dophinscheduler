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
import org.apache.dolphinscheduler.plugin.task.api.utils.TaskTypeUtils;
import org.apache.dolphinscheduler.server.master.engine.exceptions.TaskKillException;
import org.apache.dolphinscheduler.server.master.engine.exceptions.TaskPauseException;
import org.apache.dolphinscheduler.server.master.engine.exceptions.TaskReassignMasterHostException;
import org.apache.dolphinscheduler.server.master.engine.task.runnable.ITaskExecutionRunnable;
import org.apache.dolphinscheduler.server.master.exception.dispatch.TaskDispatchException;
import org.apache.dolphinscheduler.task.executor.eventbus.ITaskExecutorLifecycleEventReporter;

import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * 任务执行器客户端
 * 
 * 这个类是Master节点与任务执行器通信的客户端，负责处理所有与任务执行器
 * 相关的通信操作。它根据任务类型（逻辑任务或物理任务）选择合适的客户端代理。
 * 
 * 主要功能：
 * 1. 任务分发：将任务分发到合适的执行器
 * 2. 任务控制：支持任务的暂停、杀死操作
 * 3. 主机重新分配：支持工作流实例主机的转移
 * 4. 生命周期事件确认：处理任务执行器的生命周期事件确认
 * 5. 任务类型路由：根据任务类型选择合适的处理方式
 * 
 * 设计模式：
 * - 使用委托模式，根据任务类型委托给不同的客户端代理
 * - LogicTaskExecutorClientDelegator：处理逻辑任务（如条件、依赖等）
 * - PhysicalTaskExecutorClientDelegator：处理物理任务（如Shell、SQL等）
 * 
 * 简单理解：就像一个"任务管理员"，负责把任务安排给合适的执行器，
 * 并管理任务的整个执行过程。
 */
@Slf4j
@Component
public class TaskExecutorClient implements ITaskExecutorClient {

    /**
     * 逻辑任务执行器客户端代理
     * 处理逻辑类型的任务（如条件任务、依赖任务、子工作流等）
     */
    @Autowired
    private LogicTaskExecutorClientDelegator logicTaskExecutorClientDelegator;

    /**
     * 物理任务执行器客户端代理
     * 处理物理类型的任务（如Shell任务、SQL任务、Python任务等）
     */
    @Autowired
    private PhysicalTaskExecutorClientDelegator physicalTaskExecutorClientDelegator;

    /**
     * 分发任务到执行器
     * 
     * 根据任务类型选择合适的客户端代理来分发任务。
     * 
     * @param taskExecutionRunnable 要分发的任务执行对象
     * @throws TaskDispatchException 任务分发异常
     */
    @Override
    public void dispatch(ITaskExecutionRunnable taskExecutionRunnable) throws TaskDispatchException {
        try {
            // 根据任务类型获取对应的客户端代理，然后分发任务
            getTaskExecutorClientDelegator(taskExecutionRunnable).dispatch(taskExecutionRunnable);
        } catch (TaskDispatchException taskDispatchException) {
            // 重新抛出任务分发异常
            throw taskDispatchException;
        } catch (Exception ex) {
            // 包装其他异常为任务分发异常
            throw new TaskDispatchException("Dispatch task: " + taskExecutionRunnable.getName() + " to executor failed",
                    ex);
        }
    }

    /**
     * 重新分配工作流实例主机
     * 
     * 当Master节点发生故障转移时，需要将任务的执行主机重新分配给新的Master。
     * 
     * @param taskExecutionRunnable 需要重新分配的任务执行对象
     * @return true-重新分配成功, false-重新分配失败
     * @throws TaskReassignMasterHostException 主机重新分配异常
     */
    @Override
    public boolean reassignWorkflowInstanceHost(final ITaskExecutionRunnable taskExecutionRunnable) throws TaskReassignMasterHostException {
        try {
            return getTaskExecutorClientDelegator(taskExecutionRunnable)
                    .reassignMasterHost(taskExecutionRunnable);
        } catch (Exception ex) {
            throw new TaskReassignMasterHostException(
                    "Take over task: " + taskExecutionRunnable.getName() + " from executor failed",
                    ex);
        }
    }

    /**
     * 暂停任务执行
     * 
     * 向任务执行器发送暂停指令，要求暂停正在执行的任务。
     * 
     * @param taskExecutionRunnable 要暂停的任务执行对象
     * @throws TaskPauseException 任务暂停异常
     */
    @Override
    public void pause(final ITaskExecutionRunnable taskExecutionRunnable) throws TaskPauseException {
        try {
            getTaskExecutorClientDelegator(taskExecutionRunnable).pause(taskExecutionRunnable);
        } catch (Exception ex) {
            throw new TaskPauseException("Pause task: " + taskExecutionRunnable.getName() + " from executor failed",
                    ex);
        }
    }

    /**
     * 杀死任务执行
     * 
     * 向任务执行器发送杀死指令，强制终止正在执行的任务。
     * 
     * @param taskExecutionRunnable 要杀死的任务执行对象
     * @throws TaskKillException 任务杀死异常
     */
    @Override
    public void kill(final ITaskExecutionRunnable taskExecutionRunnable) throws TaskKillException {
        try {
            getTaskExecutorClientDelegator(taskExecutionRunnable).kill(taskExecutionRunnable);
        } catch (Exception ex) {
            throw new TaskKillException("Kill task: " + taskExecutionRunnable.getName() + " from executor failed", ex);
        }
    }

    /**
     * 确认任务执行器生命周期事件
     * 
     * 向任务执行器发送生命周期事件的确认消息，告知Master已收到并处理了相应事件。
     * 这是Master与TaskExecutor之间的双向通信机制的一部分。
     * 
     * @param taskExecutionRunnable 相关的任务执行对象
     * @param taskExecutorLifecycleEventAck 生命周期事件确认消息
     */
    @Override
    public void ackTaskExecutorLifecycleEvent(
                                              final ITaskExecutionRunnable taskExecutionRunnable,
                                              final ITaskExecutorLifecycleEventReporter.TaskExecutorLifecycleEventAck taskExecutorLifecycleEventAck) {
        try {
            // 检查任务是否已分发到执行器
            if (StringUtils.isEmpty(taskExecutionRunnable.getTaskInstance().getHost())) {
                log.info("The task: {} is didn't dispatched to executor, skip ack taskExecutorLifecycleEventAck: {}",
                        taskExecutionRunnable.getName(), taskExecutorLifecycleEventAck);
                return;
            }
            // 发送确认消息给任务执行器
            getTaskExecutorClientDelegator(taskExecutionRunnable)
                    .ackTaskExecutorLifecycleEvent(taskExecutionRunnable, taskExecutorLifecycleEventAck);
        } catch (Exception ex) {
            log.error("Send taskExecutorLifecycleEventAck: {} failed", taskExecutorLifecycleEventAck, ex);
        }
    }

    /**
     * 根据任务类型获取对应的客户端代理
     * 
     * 这是核心的路由方法，根据任务类型决定使用哪个客户端代理来处理请求。
     * 
     * @param taskExecutionRunnable 任务执行对象
     * @return 对应的任务执行器客户端代理
     */
    private ITaskExecutorClientDelegator getTaskExecutorClientDelegator(final ITaskExecutionRunnable taskExecutionRunnable) {
        final TaskInstance taskInstance = taskExecutionRunnable.getTaskInstance();
        checkArgument(taskInstance != null, "taskType cannot be empty");
        
        // 根据任务类型选择合适的客户端代理
        if (TaskTypeUtils.isLogicTask(taskInstance.getTaskType())) {
            // 逻辑任务使用逻辑任务客户端代理
            return logicTaskExecutorClientDelegator;
        }
        // 物理任务使用物理任务客户端代理
        return physicalTaskExecutorClientDelegator;
    }
}
