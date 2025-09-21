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
import org.apache.dolphinscheduler.server.master.engine.workflow.lifecycle.event.WorkflowStartLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.workflow.runnable.IWorkflowExecutionRunnable;
import org.apache.dolphinscheduler.server.master.engine.workflow.statemachine.IWorkflowStateAction;

import lombok.extern.slf4j.Slf4j;

import org.springframework.stereotype.Component;

/**
 * 工作流启动生命周期事件处理器
 * 
 * 这个类专门处理工作流的启动事件。当工作流需要开始执行时，
 * 会发送一个启动事件，由这个处理器捕获并路由到对应的状态机。
 * 
 * 主要功能：
 * 1. 接收工作流启动事件
 * 2. 根据工作流当前状态，路由到对应的状态操作类
 * 3. 执行具体的启动逻辑
 * 
 * 简单理解：就像一个专门处理“开始工作”指令的接线员。
 */
@Slf4j
@Component
public class WorkflowStartLifecycleEventHandler
        extends
            AbstractWorkflowLifecycleEventHandler<WorkflowStartLifecycleEvent> {

    /**
     * 处理工作流启动事件
     *
     * 这个方法会将启动事件路由到当前工作流状态对应的状态操作类。
     * 不同的状态下，对启动事件的处理逻辑不同。
     *
     * @param workflowStateAction 工作流状态操作对象，包含具体的处理逻辑
     * @param workflowExecutionRunnable 工作流执行对象，包含工作流的所有信息
     * @param workflowStartEvent 工作流启动事件对象
     */
    @Override
    public void handle(final IWorkflowStateAction workflowStateAction,
                       final IWorkflowExecutionRunnable workflowExecutionRunnable,
                       final WorkflowStartLifecycleEvent workflowStartEvent) {

        // =========================================================================
        // 工作流启动事件处理的核心逻辑
        // =========================================================================

        // 将启动事件委托给当前工作流状态对应的状态操作类进行处理
        //
        // 处理流程说明：
        // 1. workflowStateAction是根据工作流当前状态（如SUBMITTED、RUNNING等）
        //    从WorkflowStateActionFactory获取的具体状态操作实现类
        // 2. 不同状态下的启动事件处理逻辑：
        //    - SUBMITTED状态：执行工作流的实际启动，创建初始任务实例，设置状态为RUNNING
        //    - RUNNING状态：通常是重复启动，可能直接忽略或记录警告日志
        //    - PAUSED状态：可能是恢复操作，需要重新激活暂停的任务
        //    - 其他状态：根据状态机规则处理，可能抛出非法状态转换异常
        // 3. 状态操作类会根据工作流DAG结构，找到入口任务（没有上游依赖的任务）
        // 4. 为入口任务创建TaskInstance实例，并提交到任务调度队列
        // 5. 更新工作流状态，记录启动时间和相关元数据
        //
        // 简单理解：就像一个项目启动会议，根据项目当前状态决定下一步行动：
        // - 如果是新项目，开始分配第一批任务
        // - 如果是暂停项目，恢复之前的任务
        // - 如果是已完成项目，可能需要重新评估是否允许重启
        workflowStateAction.onStartEvent(workflowExecutionRunnable, workflowStartEvent);
    }

    /**
     * 返回该处理器匹配的事件类型
     * 
     * 这个方法用于事件路由，系统会根据返回的事件类型，
     * 将对应的事件路由到这个处理器。
     * 
     * @return 工作流启动事件类型
     */
    @Override
    public ILifecycleEventType matchEventType() {
        return WorkflowLifecycleEventType.START;
    }
}
