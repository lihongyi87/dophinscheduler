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

package org.apache.dolphinscheduler.server.master.engine.task.lifecycle.handler;

import org.apache.dolphinscheduler.server.master.engine.ILifecycleEventType;
import org.apache.dolphinscheduler.server.master.engine.task.client.ITaskExecutorClient;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.TaskLifecycleEventType;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event.TaskRunningLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.task.runnable.ITaskExecutionRunnable;
import org.apache.dolphinscheduler.server.master.engine.task.statemachine.ITaskStateAction;
import org.apache.dolphinscheduler.server.master.engine.workflow.runnable.IWorkflowExecutionRunnable;
import org.apache.dolphinscheduler.task.executor.eventbus.ITaskExecutorLifecycleEventReporter;
import org.apache.dolphinscheduler.task.executor.events.TaskExecutorLifecycleEventType;

import lombok.extern.slf4j.Slf4j;

import org.springframework.stereotype.Component;

/**
 * 任务运行中生命周期事件处理器
 * 
 * 这个类专门处理任务开始运行事件。当任务在Worker节点上开始执行时，
 * 会发送一个运行中事件，由这个处理器负责状态更新和监控。
 * 
 * 主要功能：
 * 1. 接收任务开始运行事件
 * 2. 更新任务状态为RUNNING（运行中）
 * 3. 启动任务执行监控和超时检查
 * 4. 向TaskExecutor发送确认消息
 * 5. 记录任务开始执行的时间和信息
 * 6. 更新工作流执行进度
 * 
 * 简单理解：就像一个专门监督"工人开工汇报"的现场管理员，
 * 收到工人开始干活的通知后，开始计时并监督工作进度。
 */
@Slf4j
@Component
public class TaskRunningLifecycleEventHandler extends AbstractTaskLifecycleEventHandler<TaskRunningLifecycleEvent> {

    /**
     * 任务执行器客户端，用于与TaskExecutor通信
     */
    private final ITaskExecutorClient taskExecutorClient;

    public TaskRunningLifecycleEventHandler(final ITaskExecutorClient taskExecutorClient) {
        this.taskExecutorClient = taskExecutorClient;
    }

    /**
     * 处理任务开始运行事件
     * 
     * 当任务在Worker节点上开始执行后，会触发这个方法。
     * 处理器会更新任务状态，启动监控，并向TaskExecutor发送确认消息。
     * 
     * @param taskStateAction 任务状态操作对象，包含具体的处理逻辑
     * @param workflowExecutionRunnable 工作流执行对象，提供工作流上下文
     * @param taskExecutionRunnable 任务执行对象，包含任务的所有信息
     * @param taskRunningEvent 任务运行中事件对象，包含执行开始信息
     */
    @Override
    public void handle(final ITaskStateAction taskStateAction,
                       final IWorkflowExecutionRunnable workflowExecutionRunnable,
                       final ITaskExecutionRunnable taskExecutionRunnable,
                       final TaskRunningLifecycleEvent taskRunningEvent) {
        // 委托给对应的状态操作类处理开始运行事件
        taskStateAction.onStartedEvent(workflowExecutionRunnable, taskExecutionRunnable, taskRunningEvent);
        
        // 向TaskExecutor发送确认消息，告知已收到并处理了运行中事件
        taskExecutorClient.ackTaskExecutorLifecycleEvent(
                taskExecutionRunnable,
                new ITaskExecutorLifecycleEventReporter.TaskExecutorLifecycleEventAck(
                        taskExecutionRunnable.getId(),
                        TaskExecutorLifecycleEventType.RUNNING));
    }

    /**
     * 返回该处理器匹配的事件类型
     * 
     * 返回RUNNING事件类型，系统会将所有的任务开始运行事件
     * 路由到这个处理器进行处理。
     * 
     * @return 任务运行中事件类型
     */
    @Override
    public ILifecycleEventType matchEventType() {
        return TaskLifecycleEventType.RUNNING;
    }

}
