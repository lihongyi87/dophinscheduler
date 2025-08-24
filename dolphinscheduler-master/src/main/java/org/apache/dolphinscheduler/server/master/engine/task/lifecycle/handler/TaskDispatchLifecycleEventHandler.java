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
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.TaskLifecycleEventType;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event.TaskDispatchLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.task.runnable.ITaskExecutionRunnable;
import org.apache.dolphinscheduler.server.master.engine.task.statemachine.ITaskStateAction;
import org.apache.dolphinscheduler.server.master.engine.workflow.runnable.IWorkflowExecutionRunnable;

import lombok.extern.slf4j.Slf4j;

import org.springframework.stereotype.Component;

/**
 * 任务分发生命周期事件处理器
 * 
 * 这个类专门处理任务分发事件。当任务需要分发到Worker节点执行时，
 * 会发送一个分发事件，由这个处理器负责处理分发逻辑。
 * 
 * 主要功能：
 * 1. 接收任务分发事件
 * 2. 根据任务当前状态，路由到对应的状态操作类
 * 3. 执行具体的任务分发逻辑
 * 4. 更新任务状态为DISPATCH（分发中）
 * 
 * 简单理解：就像一个专门负责"派发工单"的调度员，
 * 把任务分配给合适的工人去处理。
 */
@Slf4j
@Component
public class TaskDispatchLifecycleEventHandler extends AbstractTaskLifecycleEventHandler<TaskDispatchLifecycleEvent> {

    /**
     * 处理任务分发事件
     * 
     * 当任务需要分发到Worker节点执行时，会触发这个方法。
     * 处理器会将分发事件路由到当前任务状态对应的状态操作类。
     * 
     * @param taskStateAction 任务状态操作对象，包含具体的处理逻辑
     * @param workflowExecutionRunnable 工作流执行对象，提供工作流上下文
     * @param taskExecutionRunnable 任务执行对象，包含任务的所有信息
     * @param event 任务分发事件对象
     */
    @Override
    public void handle(final ITaskStateAction taskStateAction,
                       final IWorkflowExecutionRunnable workflowExecutionRunnable,
                       final ITaskExecutionRunnable taskExecutionRunnable,
                       final TaskDispatchLifecycleEvent event) {
        // 委托给对应的状态操作类处理分发事件
        taskStateAction.onDispatchEvent(workflowExecutionRunnable, taskExecutionRunnable, event);
    }

    /**
     * 返回该处理器匹配的事件类型
     * 
     * 这个方法用于事件路由，系统会根据返回的事件类型，
     * 将对应的事件路由到这个处理器。
     * 
     * @return 任务分发事件类型
     */
    @Override
    public ILifecycleEventType matchEventType() {
        return TaskLifecycleEventType.DISPATCH;
    }
}
