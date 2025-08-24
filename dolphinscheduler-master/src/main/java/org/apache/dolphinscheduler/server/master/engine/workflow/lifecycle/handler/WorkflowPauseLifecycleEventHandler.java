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

package org.apache.dolphinscheduler.server.master.engine.workflow.lifecycle.handler;

import org.apache.dolphinscheduler.server.master.engine.ILifecycleEventType;
import org.apache.dolphinscheduler.server.master.engine.workflow.lifecycle.WorkflowLifecycleEventType;
import org.apache.dolphinscheduler.server.master.engine.workflow.lifecycle.event.WorkflowPauseLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.workflow.runnable.IWorkflowExecutionRunnable;
import org.apache.dolphinscheduler.server.master.engine.workflow.statemachine.IWorkflowStateAction;

import lombok.extern.slf4j.Slf4j;

import org.springframework.stereotype.Component;

/**
 * 工作流暂停生命周期事件处理器
 * 
 * 这个类专门处理工作流暂停请求事件。当用户请求暂停正在运行的工作流时，
 * 会发送一个暂停事件，由这个处理器负责执行暂停操作。
 * 
 * 主要功能：
 * 1. 接收工作流暂停请求事件
 * 2. 将工作流状态转换为READY_PAUSE（准备暂停）
 * 3. 暂停所有正在执行的任务
 * 4. 保存暂停时的执行状态和上下文
 * 5. 等待所有任务暂停完成
 * 6. 最终将状态转换为PAUSED（已暂停）
 * 
 * 简单理解：就像一个专门处理"项目暂停申请"的管理员，
 * 收到暂停指令后，有序地让所有工人停下手头工作。
 */
@Slf4j
@Component
public class WorkflowPauseLifecycleEventHandler
        extends
            AbstractWorkflowLifecycleEventHandler<WorkflowPauseLifecycleEvent> {

    /**
     * 处理工作流暂停事件
     * 
     * 当用户请求暂停工作流时，会触发这个方法。
     * 处理器会将暂停请求路由到当前工作流状态对应的状态操作类。
     * 
     * @param workflowStateAction 工作流状态操作对象，包含具体的处理逻辑
     * @param workflowExecutionRunnable 工作流执行对象，包含工作流的所有信息
     * @param pauseEvent 工作流暂停事件对象
     */
    @Override
    public void handle(final IWorkflowStateAction workflowStateAction,
                       final IWorkflowExecutionRunnable workflowExecutionRunnable,
                       final WorkflowPauseLifecycleEvent pauseEvent) {

        // 委托给对应的状态操作类处理暂停事件
        workflowStateAction.onPauseEvent(workflowExecutionRunnable, pauseEvent);
    }

    /**
     * 返回该处理器匹配的事件类型
     * 
     * 返回PAUSE事件类型，系统会将所有的工作流暂停请求事件
     * 路由到这个处理器进行处理。
     * 
     * @return 工作流暂停事件类型
     */
    @Override
    public ILifecycleEventType matchEventType() {
        return WorkflowLifecycleEventType.PAUSE;
    }

}
