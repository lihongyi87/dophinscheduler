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
 * 这是专门处理任务分发事件的处理器，负责将任务从Master节点分发到Worker节点执行。
 * 类比：就像一个专业的任务调度员，负责将工作任务分配给最合适的工人。
 * 
 * 核心职责：
 * 1. 分发处理：接收并处理任务分发生命周期事件
 * 2. 资源选择：协调选择合适的Worker节点执行任务
 * 3. 状态转换：将任务状态从待分发转换为分发中
 * 4. 分发监控：跟踪分发过程，确保任务成功下发
 * 5. 异常处理：处理分发过程中的各种异常情况
 * 
 * 分发流程：
 * - 任务准备：确保任务已准备好分发（参数、资源等）
 * - Worker选择：根据任务要求和Worker状态选择执行节点
 * - 任务下发：将任务信息和执行上下文发送给Worker
 * - 状态更新：更新任务状态为DISPATCH（分发中）
 * - 结果跟踪：跟踪分发结果，处理成功或失败情况
 * 
 * 分发策略：
 * - 负载均衡：选择负载较低的Worker节点
 * - 资源匹配：确保Worker具备执行任务的必要资源
 * - 故障避免：避免分发到不可用或故障的Worker节点
 * - 优先级考虑：高优先级任务优先分发到性能更好的Worker
 * 
 * 处理特点：
 * - 状态驱动：根据任务当前状态选择合适的分发策略
 * - 异步分发：分发过程不阻塞其他任务的处理
 * - 重试机制：分发失败时支持重试到其他Worker节点
 * - 事件委托：将具体分发逻辑委托给任务状态动作执行
 * 
 * 类比理解：
 * 就像快递公司的分拣中心调度员：
 * - 接收订单：接收需要分发的任务（分发事件）
 * - 选择配送员：根据区域和负载选择合适的配送员（Worker选择）
 * - 派发快递：将包裹交给配送员并记录状态（任务下发）
 * - 跟踪配送：跟踪配送过程直到送达客户手中（分发监控）
 */
@Slf4j
@Component
public class TaskDispatchLifecycleEventHandler extends AbstractTaskLifecycleEventHandler<TaskDispatchLifecycleEvent> {

    /**
     * 处理任务分发生命周期事件
     * 
     * 这是任务分发的核心处理方法，负责协调任务从Master到Worker的分发过程。
     * 方法将具体的分发逻辑委托给任务状态动作执行，实现状态驱动的分发处理。
     * 
     * 分发处理流程：
     * - 事件解析：解析分发事件中包含的任务和分发要求
     * - 状态检查：确认任务当前状态适合进行分发操作
     * - 分发执行：通过状态动作执行具体的分发逻辑
     * - 结果处理：处理分发结果，更新任务状态
     * 
     * 状态动作职责：
     * - Worker选择：根据任务要求选择合适的Worker节点
     * - 任务下发：构建执行上下文并发送给选中的Worker
     * - 状态更新：将任务状态更新为DISPATCH（分发中）
     * - 异常处理：处理分发过程中的各种异常情况
     * 
     * 设计优势：
     * - 职责分离：事件处理器专注于事件协调，状态动作专注于分发执行
     * - 状态感知：根据不同任务状态采用不同的分发策略
     * - 策略模式：不同状态对应不同的分发处理策略
     * - 可扩展性：新增分发策略时只需实现相应的状态动作
     * 
     * 异常处理：
     * - Worker不可用：选择其他可用的Worker节点重试
     * - 网络异常：重试分发或标记任务为分发失败
     * - 资源不足：等待资源释放或选择其他Worker
     * - 配置错误：记录错误信息并标记任务为失败
     * 
     * 类比：就像调度员收到派工单后，根据工单要求和当前工人状态，
     * 选择最合适的工人并将工作任务交给他们。
     * 
     * @param taskStateAction 任务状态对应的处理动作，负责执行具体分发逻辑
     * @param workflowExecutionRunnable 工作流执行器，提供工作流级别的上下文信息
     * @param taskExecutionRunnable 任务执行器，提供任务级别的操作接口和状态信息
     * @param event 任务分发生命周期事件，包含分发相关的详细信息和配置
     */
    @Override
    public void handle(final ITaskStateAction taskStateAction,
                       final IWorkflowExecutionRunnable workflowExecutionRunnable,
                       final ITaskExecutionRunnable taskExecutionRunnable,
                       final TaskDispatchLifecycleEvent event) {
        // ==================== 任务分发处理核心逻辑 ====================
        //
        // 这里是任务分发事件的核心处理逻辑，通过委托模式将具体的分发操作
        // 交给对应的任务状态动作处理器执行。这种设计实现了事件处理和状态
        // 操作的解耦，使得不同状态下的任务可以有不同的分发处理策略。

        // ========== 状态动作委托执行 ==========
        // 调用任务状态动作的分发事件处理方法
        // 参数说明：
        // - workflowExecutionRunnable: 工作流执行上下文，提供工作流级别的信息和操作接口
        // - taskExecutionRunnable: 任务执行上下文，提供任务级别的信息和操作接口
        // - event: 分发事件对象，包含本次分发的具体要求和配置信息
        //
        // 执行效果：
        // 1. 状态检查：验证任务当前状态是否适合进行分发操作
        // 2. Worker选择：根据任务要求和负载均衡策略选择合适的Worker节点
        // 3. 任务下发：构建任务执行包并发送到选中的Worker节点
        // 4. 状态更新：将任务状态更新为DISPATCH（分发中），表示任务正在分发过程中
        // 5. 监控启动：启动分发超时监控，防止分发过程长时间无响应
        //
        // 分发策略说明：
        // - 负载均衡：优先选择负载较低的Worker节点，避免单点过载
        // - 资源匹配：确保Worker节点具备执行该任务所需的资源和环境
        // - 故障避免：排除已知故障或不可用的Worker节点
        // - 亲和性考虑：考虑数据本地性和网络延迟等因素
        //
        // 类比：就像快递分拣中心的调度员接到派送任务后，
        // 根据包裹目的地、配送员负载情况、交通状况等因素，
        // 选择最合适的配送员并将包裹交给他们派送。
        taskStateAction.onDispatchEvent(workflowExecutionRunnable, taskExecutionRunnable, event);
    }

    /**
     * 匹配任务分发事件类型
     * 
     * 返回当前处理器能够处理的生命周期事件类型。
     * 这是事件路由系统的重要组成部分，确保分发事件能够被正确的处理器处理。
     * 
     * 事件匹配机制：
     * - 类型标识：每个处理器都有唯一的事件类型标识
     * - 自动路由：事件总线根据这个标识自动路由事件到对应处理器
     * - 处理器注册：Spring容器根据这个标识注册事件处理器
     * - 类型安全：确保事件类型和处理器类型的严格匹配
     * 
     * 路由流程：
     * - 事件发布：当任务需要分发时，发布TaskDispatchLifecycleEvent
     * - 类型匹配：事件总线调用各处理器的matchEventType方法进行匹配
     * - 路由执行：匹配成功的处理器接收并处理该事件
     * - 处理完成：处理器完成分发逻辑并更新任务状态
     * 
     * 设计价值：
     * - 解耦设计：事件发布者不需要知道具体的处理器实现
     * - 扩展性：新增事件类型时只需实现相应的处理器
     * - 类型安全：编译时确保事件类型的正确性
     * - 自动化：支持事件总线的自动事件分发机制
     * 
     * 类比：就像邮局的分拣标识，每种邮件都有特定的标识，
     * 分拣员根据标识将邮件送到对应的处理部门。
     * 
     * @return TaskLifecycleEventType.DISPATCH，表示处理任务分发类型的生命周期事件
     */
    @Override
    public ILifecycleEventType matchEventType() {
        return TaskLifecycleEventType.DISPATCH;
    }
}
