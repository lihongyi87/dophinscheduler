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
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event.TaskSuccessLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.task.runnable.ITaskExecutionRunnable;
import org.apache.dolphinscheduler.server.master.engine.task.statemachine.ITaskStateAction;
import org.apache.dolphinscheduler.server.master.engine.workflow.runnable.IWorkflowExecutionRunnable;
import org.apache.dolphinscheduler.task.executor.eventbus.ITaskExecutorLifecycleEventReporter;
import org.apache.dolphinscheduler.task.executor.events.TaskExecutorLifecycleEventType;

import org.springframework.stereotype.Component;

/**
 * 任务成功生命周期事件处理器
 * 
 * 这个类专门处理任务成功完成事件。当任务在Worker节点上成功执行完成时，
 * 会发送一个成功事件，由这个处理器负责后续的收尾工作。
 * 
 * 主要功能：
 * 1. 接收任务成功事件
 * 2. 更新任务状态为SUCCESS
 * 3. 记录任务执行结果和统计信息
 * 4. 向TaskExecutor发送确认消息
 * 5. 触发工作流的后续任务调度
 * 
 * 简单理解：就像一个专门处理"任务完成汇报"的质检员，
 * 收到工人完工汇报后，做记录并安排下一步工作。
 */
@Component
public class TaskSuccessLifecycleEventHandler extends AbstractTaskLifecycleEventHandler<TaskSuccessLifecycleEvent> {

    /**
     * 任务执行器客户端，用于与TaskExecutor通信
     */
    private final TaskExecutorClient taskExecutorClient;

    public TaskSuccessLifecycleEventHandler(final TaskExecutorClient taskExecutorClient) {
        this.taskExecutorClient = taskExecutorClient;
    }

    /**
     * 处理任务成功事件
     * 
     * 当任务在Worker节点上成功完成后，会触发这个方法。
     * 处理器会更新任务状态，并向TaskExecutor发送确认消息。
     * 
     * @param taskStateAction 任务状态操作对象，包含具体的处理逻辑
     * @param workflowExecutionRunnable 工作流执行对象，提供工作流上下文
     * @param taskExecutionRunnable 任务执行对象，包含任务的所有信息
     * @param taskSuccessEvent 任务成功事件对象
     */
    @Override
    public void handle(final ITaskStateAction taskStateAction,
                       final IWorkflowExecutionRunnable workflowExecutionRunnable,
                       final ITaskExecutionRunnable taskExecutionRunnable,
                       final TaskSuccessLifecycleEvent taskSuccessEvent) {
        // 委托给对应的状态操作类处理成功事件
        taskStateAction.onSucceedEvent(workflowExecutionRunnable, taskExecutionRunnable, taskSuccessEvent);
        
        // 向TaskExecutor发送确认消息，告知已收到并处理了成功事件
        taskExecutorClient.ackTaskExecutorLifecycleEvent(
                taskExecutionRunnable,
                new ITaskExecutorLifecycleEventReporter.TaskExecutorLifecycleEventAck(
                        taskExecutionRunnable.getId(),
                        TaskExecutorLifecycleEventType.SUCCESS));
    }

    /**
     * 返回该处理器匹配的事件类型
     * 
     * 返回SUCCEEDED事件类型，系统会将所有的任务成功事件
     * 路由到这个处理器进行处理。
     * 
     * @return 任务成功事件类型
     */
    @Override
    public ILifecycleEventType matchEventType() {
        return TaskLifecycleEventType.SUCCEEDED;
    }
}
