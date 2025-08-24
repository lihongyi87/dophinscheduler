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
import org.apache.dolphinscheduler.server.master.engine.task.client.TaskExecutorClient;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.TaskLifecycleEventType;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event.TaskKilledLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.task.runnable.ITaskExecutionRunnable;
import org.apache.dolphinscheduler.server.master.engine.task.statemachine.ITaskStateAction;
import org.apache.dolphinscheduler.server.master.engine.workflow.runnable.IWorkflowExecutionRunnable;
import org.apache.dolphinscheduler.task.executor.eventbus.ITaskExecutorLifecycleEventReporter;
import org.apache.dolphinscheduler.task.executor.events.TaskExecutorLifecycleEventType;

import org.springframework.stereotype.Component;

/**
 * 任务被杀死生命周期事件处理器
 * 
 * 这个类专门处理任务被杀死事件。当任务在Worker节点上被强制终止时，
 * 会发送一个杀死事件，由这个处理器负责清理和状态更新。
 * 
 * 主要功能：
 * 1. 接收任务被杀死事件
 * 2. 更新任务状态为KILLED
 * 3. 释放任务占用的资源
 * 4. 向TaskExecutor发送确认消息
 * 5. 记录任务终止的详细信息
 * 6. 触发工作流的相应处理逻辑
 * 
 * 简单理解：就像一个专门处理"任务强制终止汇报"的清洁工，
 * 收到任务被强行停止的通知后，负责清理现场和记录事件。
 */
@Component
public class TaskKilledLifecycleEventHandler extends AbstractTaskLifecycleEventHandler<TaskKilledLifecycleEvent> {

    /**
     * 任务执行器客户端，用于与TaskExecutor通信
     */
    private final TaskExecutorClient taskExecutorClient;

    public TaskKilledLifecycleEventHandler(final TaskExecutorClient taskExecutorClient) {
        this.taskExecutorClient = taskExecutorClient;
    }

    /**
     * 处理任务被杀死事件
     * 
     * 当任务在Worker节点上被强制终止后，会触发这个方法。
     * 处理器会更新任务状态，释放资源，并向TaskExecutor发送确认消息。
     * 
     * @param taskStateAction 任务状态操作对象，包含具体的处理逻辑
     * @param workflowExecutionRunnable 工作流执行对象，提供工作流上下文
     * @param taskExecutionRunnable 任务执行对象，包含任务的所有信息
     * @param taskKilledEvent 任务被杀死事件对象，包含终止详细信息
     */
    @Override
    public void handle(final ITaskStateAction taskStateAction,
                       final IWorkflowExecutionRunnable workflowExecutionRunnable,
                       final ITaskExecutionRunnable taskExecutionRunnable,
                       final TaskKilledLifecycleEvent taskKilledEvent) {
        // 委托给对应的状态操作类处理被杀死事件
        taskStateAction.onKilledEvent(workflowExecutionRunnable, taskExecutionRunnable, taskKilledEvent);
        
        // 向TaskExecutor发送确认消息，告知已收到并处理了杀死事件
        taskExecutorClient.ackTaskExecutorLifecycleEvent(
                taskExecutionRunnable,
                new ITaskExecutorLifecycleEventReporter.TaskExecutorLifecycleEventAck(
                        taskExecutionRunnable.getId(),
                        TaskExecutorLifecycleEventType.KILLED));
    }

    /**
     * 返回该处理器匹配的事件类型
     * 
     * 返回KILLED事件类型，系统会将所有的任务被杀死事件
     * 路由到这个处理器进行处理。
     * 
     * @return 任务被杀死事件类型
     */
    @Override
    public ILifecycleEventType matchEventType() {
        return TaskLifecycleEventType.KILLED;
    }
}
