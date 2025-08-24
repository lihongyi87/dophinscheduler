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
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event.TaskFailedLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.task.runnable.ITaskExecutionRunnable;
import org.apache.dolphinscheduler.server.master.engine.task.statemachine.ITaskStateAction;
import org.apache.dolphinscheduler.server.master.engine.workflow.runnable.IWorkflowExecutionRunnable;
import org.apache.dolphinscheduler.task.executor.eventbus.ITaskExecutorLifecycleEventReporter;
import org.apache.dolphinscheduler.task.executor.events.TaskExecutorLifecycleEventType;

import org.springframework.stereotype.Component;

/**
 * 任务失败生命周期事件处理器
 * 
 * 这个类专门处理任务失败事件。当任务在Worker节点上执行失败时，
 * 会发送一个失败事件，由这个处理器负责失败处理和后续操作。
 * 
 * 主要功能：
 * 1. 接收任务失败事件
 * 2. 更新任务状态为FAILED
 * 3. 记录失败原因和错误信息
 * 4. 向TaskExecutor发送确认消息
 * 5. 根据配置决定是否重试任务
 * 6. 触发工作流的失败处理逻辑
 * 
 * 简单理解：就像一个专门处理"任务异常汇报"的应急响应员，
 * 收到工人出错汇报后，记录问题并决定下一步行动。
 */
@Component
public class TaskFailedLifecycleEventHandler extends AbstractTaskLifecycleEventHandler<TaskFailedLifecycleEvent> {

    /**
     * 任务执行器客户端，用于与TaskExecutor通信
     */
    private final TaskExecutorClient taskExecutorClient;

    public TaskFailedLifecycleEventHandler(final TaskExecutorClient taskExecutorClient) {
        this.taskExecutorClient = taskExecutorClient;
    }

    /**
     * 处理任务失败事件
     * 
     * 当任务在Worker节点上执行失败后，会触发这个方法。
     * 处理器会更新任务状态，记录失败信息，并向TaskExecutor发送确认消息。
     * 
     * @param taskStateAction 任务状态操作对象，包含具体的处理逻辑
     * @param workflowExecutionRunnable 工作流执行对象，提供工作流上下文
     * @param taskExecutionRunnable 任务执行对象，包含任务的所有信息
     * @param event 任务失败事件对象，包含失败详细信息
     */
    @Override
    public void handle(final ITaskStateAction taskStateAction,
                       final IWorkflowExecutionRunnable workflowExecutionRunnable,
                       final ITaskExecutionRunnable taskExecutionRunnable,
                       final TaskFailedLifecycleEvent event) {
        // 委托给对应的状态操作类处理失败事件
        taskStateAction.onFailedEvent(workflowExecutionRunnable, taskExecutionRunnable, event);
        
        // 向TaskExecutor发送确认消息，告知已收到并处理了失败事件
        taskExecutorClient.ackTaskExecutorLifecycleEvent(
                taskExecutionRunnable,
                new ITaskExecutorLifecycleEventReporter.TaskExecutorLifecycleEventAck(
                        taskExecutionRunnable.getId(),
                        TaskExecutorLifecycleEventType.FAILED));
    }

    /**
     * 返回该处理器匹配的事件类型
     * 
     * 返回FAILED事件类型，系统会将所有的任务失败事件
     * 路由到这个处理器进行处理。
     * 
     * @return 任务失败事件类型
     */
    @Override
    public ILifecycleEventType matchEventType() {
        return TaskLifecycleEventType.FAILED;
    }
}
