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
import org.apache.dolphinscheduler.server.master.engine.workflow.lifecycle.event.WorkflowStopLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.workflow.runnable.IWorkflowExecutionRunnable;
import org.apache.dolphinscheduler.server.master.engine.workflow.statemachine.IWorkflowStateAction;

import lombok.extern.slf4j.Slf4j;

import org.springframework.stereotype.Component;

/**
 * 工作流停止生命周期事件处理器
 * 
 * 这个类专门处理工作流停止请求事件。当用户请求停止正在运行的工作流时，
 * 会发送一个停止事件，由这个处理器负责执行停止操作。
 * 
 * 主要功能：
 * 1. 接收工作流停止请求事件
 * 2. 将工作流状态转换为READY_STOP（准备停止）
 * 3. 强制终止所有正在执行的任务
 * 4. 清理工作流相关资源和临时数据
 * 5. 等待所有任务终止完成
 * 6. 最终将状态转换为STOPPED（已停止）
 * 
 * 简单理解：就像一个专门处理"项目紧急停止"的应急指挥员，
 * 收到停止指令后，立即强制终止所有正在进行的工作。
 */
@Slf4j
@Component
public class WorkflowStopLifecycleEventHandler
        extends
            AbstractWorkflowLifecycleEventHandler<WorkflowStopLifecycleEvent> {

    /**
     * 处理工作流停止事件
     * 
     * 当用户请求停止工作流时，会触发这个方法。
     * 处理器会将停止请求路由到当前工作流状态对应的状态操作类。
     * 
     * @param workflowStateAction 工作流状态操作对象，包含具体的处理逻辑
     * @param workflowExecutionRunnable 工作流执行对象，包含工作流的所有信息
     * @param event 工作流停止事件对象
     */
    @Override
    public void handle(final IWorkflowStateAction workflowStateAction,
                       final IWorkflowExecutionRunnable workflowExecutionRunnable,
                       final WorkflowStopLifecycleEvent event) {
        // 委托给对应的状态操作类处理停止事件
        workflowStateAction.onStopEvent(workflowExecutionRunnable, event);
    }

    /**
     * 返回该处理器匹配的事件类型
     * 
     * 返回STOP事件类型，系统会将所有的工作流停止请求事件
     * 路由到这个处理器进行处理。
     * 
     * @return 工作流停止事件类型
     */
    @Override
    public ILifecycleEventType matchEventType() {
        return WorkflowLifecycleEventType.STOP;
    }
}
