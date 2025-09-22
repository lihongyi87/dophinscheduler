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
        // ==================== 任务运行中事件处理核心逻辑 ====================
        //
        // 当任务在Worker节点上开始执行时，会发送运行中生命周期事件到Master节点。
        // 这个方法负责处理该事件，更新任务状态，启动监控，并向Worker发送确认消息。
        // 整个处理过程确保Master和Worker之间的状态同步和可靠通信。

        // ========== 第一步：状态动作处理 ==========
        // 委托给对应的状态操作类处理开始运行事件
        //
        // 执行内容：
        // 1. 状态验证：确认任务状态是否符合开始运行的前置条件
        //    - 检查任务是否处于DISPATCH（已分发）或SUBMITTED（已提交）状态
        //    - 验证任务是否已正确分配到Worker节点
        //    - 确认任务参数和环境配置是否完整
        //
        // 2. 状态转换：将任务状态从分发中转换为运行中
        //    - 更新任务状态为RUNNING（运行中）
        //    - 记录任务开始执行的时间戳
        //    - 更新任务实例的执行信息
        //
        // 3. 监控启动：启动任务执行监控机制
        //    - 启动任务执行超时检查定时器
        //    - 初始化任务进度监控
        //    - 设置任务资源使用情况跟踪
        //
        // 4. 日志记录：记录任务开始运行的详细信息
        //    - 记录任务开始时间、Worker节点信息
        //    - 记录任务执行参数和环境配置
        //    - 更新工作流执行进度统计
        //
        // 类比：就像工厂现场管理员收到工人"开始干活"的汇报后，
        // 在生产记录本上更新状态，启动计时器，开始监督工作进度。
        taskStateAction.onStartedEvent(workflowExecutionRunnable, taskExecutionRunnable, taskRunningEvent);

        // ========== 第二步：确认消息发送 ==========
        // 向TaskExecutor发送确认消息，告知已收到并处理了运行中事件
        //
        // 确认机制的重要性：
        // 1. 状态同步：确保Master和Worker对任务状态的认知一致
        //    - Worker知道Master已经收到开始运行的通知
        //    - 避免Worker重复发送运行中事件
        //    - 保证双方状态的最终一致性
        //
        // 2. 可靠通信：建立Master和Worker之间的可靠通信机制
        //    - 采用消息确认机制防止消息丢失
        //    - 支持网络异常时的重传机制
        //    - 维护通信连接的健康状态
        //
        // 3. 流程控制：为后续的任务执行流程提供控制基础
        //    - Worker确认Master已知悉任务开始运行
        //    - 为后续的心跳和状态上报建立基础
        //    - 支持任务执行过程中的动态控制
        //
        // 确认消息内容：
        // - taskExecutionRunnable: 任务执行上下文，提供任务标识和状态信息
        // - TaskExecutorLifecycleEventAck: 确认消息对象，包含以下信息：
        //   - taskExecutionRunnable.getId(): 任务唯一标识符，确保确认消息的准确性
        //   - TaskExecutorLifecycleEventType.RUNNING: 确认的事件类型，表示确认运行中事件
        //
        // 通信特点：
        // - 异步发送：确认消息采用异步方式发送，不阻塞当前处理流程
        // - 可靠传输：支持消息重传和异常恢复机制
        // - 幂等处理：Worker能够处理重复的确认消息
        //
        // 类比：就像现场管理员收到工人汇报后，通过对讲机回复"收到，继续工作"，
        // 让工人知道管理层已经了解工作状态，可以安心继续工作。
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
