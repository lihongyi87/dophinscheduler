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
import org.apache.dolphinscheduler.server.master.engine.workflow.lifecycle.event.WorkflowFailedLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.workflow.runnable.IWorkflowExecutionRunnable;
import org.apache.dolphinscheduler.server.master.engine.workflow.statemachine.IWorkflowStateAction;

import lombok.extern.slf4j.Slf4j;

import org.springframework.stereotype.Component;

/**
 * 工作流失败生命周期事件处理器
 * 
 * 这个类专门处理工作流失败事件。当工作流中的关键任务失败导致整个工作流失败时，
 * 会发送一个失败事件，由这个处理器负责失败处理和收尾工作。
 * 
 * 主要功能：
 * 1. 接收工作流失败事件
 * 2. 更新工作流状态为FAILED
 * 3. 停止所有正在运行的任务
 * 4. 记录失败原因和错误信息
 * 5. 触发失败告警和通知
 * 6. 清理相关资源和临时数据
 * 
 * 简单理解：就像一个专门处理"项目失败"的应急处理员，
 * 当项目出现重大问题时，负责紧急叫停并做善后工作。
 */
@Slf4j
@Component
public class WorkflowFailedLifecycleEventHandler
        extends
            AbstractWorkflowLifecycleEventHandler<WorkflowFailedLifecycleEvent> {

    /**
     * 处理工作流失败事件
     * 
     * 当工作流因关键任务失败而失败时，会触发这个方法。
     * 处理器会执行失败处理逻辑，如停止运行任务、更新状态、发送告警等。
     * 
     * @param workflowStateAction 工作流状态操作对象
     * @param workflowExecutionRunnable 工作流执行对象
     * @param workflowFailedEvent 工作流失败事件对象
     */
    @Override
    public void handle(final IWorkflowStateAction workflowStateAction,
                       final IWorkflowExecutionRunnable workflowExecutionRunnable,
                       final WorkflowFailedLifecycleEvent workflowFailedEvent) {
        // 委托给对应的状态操作类处理失败事件
        workflowStateAction.onFailedEvent(workflowExecutionRunnable, workflowFailedEvent);
    }

    /**
     * 返回该处理器匹配的事件类型
     * 
     * 返回FAILED事件类型，系统会将所有的工作流失败事件
     * 路由到这个处理器进行处理。
     * 
     * @return 工作流失败事件类型
     */
    @Override
    public ILifecycleEventType matchEventType() {
        return WorkflowLifecycleEventType.FAILED;
    }
}
