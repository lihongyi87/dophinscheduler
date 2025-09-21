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
 * 这是专门处理任务执行成功事件的处理器，负责任务成功完成后的后续处理工作。
 * 类比：就像项目验收专员，负责确认工作完成质量并进行项目收尾工作。
 * 
 * 核心职责：
 * 1. 成功确认：接收并确认任务在Worker节点上的成功执行
 * 2. 状态更新：将任务状态更新为SUCCESS（成功）
 * 3. 结果记录：记录任务执行结果、输出数据和统计信息
 * 4. 通信确认：向TaskExecutor发送成功事件确认消息
 * 5. 后续触发：触发依赖于该任务的后续任务或工作流操作
 * 
 * 成功处理流程：
 * - 事件接收：接收来自Worker的任务成功完成事件
 * - 状态验证：验证任务确实处于可以成功的状态
 * - 结果处理：保存任务执行结果和输出数据
 * - 状态转换：将任务状态从RUNNING转换为SUCCESS
 * - 确认回复：向Worker发送成功事件确认消息
 * - 依赖触发：触发依赖该任务的后续任务开始执行
 * 
 * 通信机制：
 * - 双向确认：Master和Worker之间的双向事件确认机制
 * - 可靠传输：确保成功事件的可靠传输和处理
 * - 幂等处理：支持重复事件的幂等处理机制
 * - 异常恢复：处理通信过程中的网络异常和重试
 * 
 * 处理特点：
 * - 状态驱动：根据任务当前状态执行相应的成功处理逻辑
 * - 结果保存：完整保存任务执行的结果数据和统计信息
 * - 双向通信：与Worker建立可靠的双向通信确认机制
 * - 依赖感知：能够识别和触发依赖该任务的后续操作
 * 
 * 业务价值：
 * - 执行确认：确保任务真正执行成功，避免状态不一致
 * - 数据完整：保证任务输出数据的完整性和可靠性
 * - 流程连续：保证工作流的连续执行，及时触发后续任务
 * - 状态同步：保持Master和Worker之间的状态同步
 * 
 * 类比理解：
 * 就像工厂的质检验收部门：
 * - 收到完工报告：接收工人的完工汇报（成功事件）
 * - 质量检验：验证产品质量是否合格（状态验证）
 * - 记录归档：记录产品信息和质检结果（结果保存）
 * - 确认回复：向工人确认验收完成（确认消息）
 * - 安排下工序：安排产品进入下一个加工环节（后续触发）
 */
@Component
public class TaskSuccessLifecycleEventHandler extends AbstractTaskLifecycleEventHandler<TaskSuccessLifecycleEvent> {

    /**
     * 任务执行器客户端
     * 
     * 用于与TaskExecutor进行通信的客户端组件。
     * 主要负责向Worker节点发送各种确认消息和控制指令。
     * 
     * 通信功能：
     * - 事件确认：向Worker发送生命周期事件的确认消息
     * - 状态同步：同步Master和Worker之间的任务状态
     * - 控制指令：发送任务控制指令（暂停、终止等）
     * - 信息查询：查询Worker上的任务执行信息
     * 
     * 设计特点：
     * - 可靠通信：支持网络异常时的重试和恢复机制
     * - 异步处理：采用异步通信模式，不阻塞事件处理流程
     * - 协议统一：使用统一的通信协议和消息格式
     * - 状态管理：维护通信连接的状态和健康检查
     * 
     * 类比：就像质检部门的对讲机，用于与车间工人保持联系，
     * 确认工作进展和传达管理指令。
     */
    private final TaskExecutorClient taskExecutorClient;

    /**
     * 构造函数 - 注入任务执行器客户端依赖
     * 
     * 通过构造函数注入的方式获取TaskExecutorClient依赖，
     * 这是Spring推荐的依赖注入方式，确保组件的不可变性和可测试性。
     * 
     * 依赖注入优势：
     * - 不可变性：通过final字段确保依赖的不可变性
     * - 测试友好：便于单元测试时进行依赖模拟
     * - 明确依赖：构造函数明确显示组件的依赖关系
     * - Spring支持：Spring容器自动解析和注入依赖
     * 
     * @param taskExecutorClient 任务执行器客户端，用于与Worker节点通信
     */
    public TaskSuccessLifecycleEventHandler(final TaskExecutorClient taskExecutorClient) {
        this.taskExecutorClient = taskExecutorClient;
    }

    /**
     * 处理任务成功生命周期事件
     * 
     * 这是任务成功处理的核心方法，负责完整的成功事件处理流程。
     * 包括状态更新、结果保存、确认通信等多个环节的协调处理。
     * 
     * 成功处理流程：
     * 1. 状态处理：通过状态动作执行具体的成功处理逻辑
     * 2. 结果保存：保存任务执行结果和输出数据到数据库
     * 3. 状态更新：将任务状态从RUNNING更新为SUCCESS
     * 4. 确认回复：向Worker发送成功事件确认消息
     * 5. 依赖触发：通知工作流引擎触发后续任务
     * 
     * 状态动作职责：
     * - 结果验证：验证任务执行结果的有效性和完整性
     * - 数据处理：处理任务输出的数据和文件
     * - 状态转换：安全地进行任务状态转换
     * - 依赖计算：计算和更新任务依赖关系
     * - 通知发送：发送成功通知给相关的监听组件
     * 
     * 确认机制：
     * - 消息内容：包含任务ID和事件类型的确认消息
     * - 可靠传输：确保确认消息能够可靠地传达到Worker
     * - 重试机制：支持确认消息发送失败时的重试
     * - 幂等处理：Worker能够处理重复的确认消息
     * 
     * 异常处理：
     * - 网络异常：确认消息发送失败时的重试和记录
     * - 状态冲突：任务状态不一致时的修正处理
     * - 数据异常：任务输出数据异常时的处理策略
     * - 依赖异常：后续任务触发异常时的错误处理
     * 
     * 类比：就像质检员完成产品验收后，既要更新质检记录，
     * 又要通知工人验收结果，还要安排产品进入下一个环节。
     * 
     * @param taskStateAction 任务状态对应的处理动作，负责执行具体成功处理逻辑
     * @param workflowExecutionRunnable 工作流执行器，提供工作流级别的上下文信息
     * @param taskExecutionRunnable 任务执行器，提供任务级别的操作接口和状态信息
     * @param taskSuccessEvent 任务成功生命周期事件，包含成功相关的详细信息
     */
    @Override
    public void handle(final ITaskStateAction taskStateAction,
                       final IWorkflowExecutionRunnable workflowExecutionRunnable,
                       final ITaskExecutionRunnable taskExecutionRunnable,
                       final TaskSuccessLifecycleEvent taskSuccessEvent) {
        // ==================== 任务成功事件处理核心逻辑 ====================
        //
        // 当任务在Worker节点上成功完成执行时，会发送成功生命周期事件到Master节点。
        // 这个方法负责处理该事件，进行结果收集、状态更新、依赖触发，并向Worker发送确认。
        // 整个处理过程确保任务成功的完整性和工作流的连续性。

        // ========== 第一步：成功状态处理 ==========
        // 委托给对应的状态操作类处理任务成功完成事件
        //
        // 执行内容：
        // 1. 前置条件验证：确认任务状态转换的合法性
        //    - 验证任务当前状态是否为RUNNING（运行中）
        //    - 检查任务是否确实在Worker节点上执行完成
        //    - 确认任务执行结果的完整性和有效性
        //
        // 2. 执行结果处理：收集和保存任务执行结果
        //    - 收集任务的输出数据和结果文件
        //    - 保存任务执行的统计信息（执行时长、资源使用等）
        //    - 验证输出数据的格式和内容完整性
        //    - 更新任务实例的执行结果字段
        //
        // 3. 状态转换：将任务状态从运行中转换为成功
        //    - 更新任务状态为SUCCESS（成功完成）
        //    - 记录任务完成的时间戳
        //    - 计算任务的实际执行时长
        //    - 更新任务实例的完成信息
        //
        // 4. 依赖关系处理：触发依赖该任务的后续操作
        //    - 检查依赖于该任务的其他任务
        //    - 判断后续任务的启动条件是否满足
        //    - 通知工作流引擎进行下一步调度
        //    - 更新工作流整体执行进度
        //
        // 5. 资源清理：清理任务执行相关的临时资源
        //    - 清理临时文件和工作目录（如需要）
        //    - 释放任务占用的系统资源
        //    - 更新资源使用统计信息
        //
        // 6. 监控和统计：更新任务和工作流的监控数据
        //    - 更新任务成功率统计
        //    - 记录任务执行性能指标
        //    - 触发成功事件的监控告警（如配置）
        //
        // 类比：就像质检部门收到工人的"完工报告"后，验收产品质量，
        // 记录生产数据，通知下一道工序准备开工，更新生产进度看板。
        taskStateAction.onSucceedEvent(workflowExecutionRunnable, taskExecutionRunnable, taskSuccessEvent);

        // ========== 第二步：成功确认消息发送 ==========
        // 向TaskExecutor发送成功事件确认消息，告知已收到并处理了任务成功事件
        //
        // 确认机制的关键作用：
        // 1. 双向状态同步：确保Master和Worker对任务成功状态的一致性
        //    - Worker知道Master已经确认了任务成功完成
        //    - 避免Worker重复发送成功事件通知
        //    - 保证双方对任务最终状态的统一认知
        //    - 为工作流后续调度提供可靠的状态基础
        //
        // 2. 资源清理协调：协调Master和Worker的资源清理工作
        //    - 通知Worker可以开始清理任务执行环境
        //    - 确认Worker可以释放为该任务分配的资源
        //    - 协调临时文件和数据的清理时机
        //    - 避免过早清理导致的数据丢失
        //
        // 3. 通信完整性：完成任务生命周期的通信闭环
        //    - 形成完整的任务执行通信链路
        //    - 为后续的任务调度提供通信基础
        //    - 支持任务执行的全程追踪和审计
        //    - 确保通信协议的完整性和可靠性
        //
        // 4. 错误处理支持：为异常情况下的处理提供支持
        //    - 支持成功事件的重复处理（幂等性）
        //    - 处理网络异常导致的确认消息丢失
        //    - 为Master和Worker状态不一致提供修复机制
        //
        // 确认消息的构成：
        // - taskExecutionRunnable: 任务执行上下文，提供任务的完整信息
        // - TaskExecutorLifecycleEventAck: 确认消息对象，包含：
        //   - taskExecutionRunnable.getId(): 任务的唯一标识符
        //   - TaskExecutorLifecycleEventType.SUCCESS: 明确标识这是对成功事件的确认
        //
        // 通信特性：
        // - 可靠传输：确保确认消息能够可靠地送达Worker节点
        // - 异步处理：确认消息发送不阻塞当前的成功处理流程
        // - 幂等支持：Worker能够正确处理重复的成功确认消息
        // - 超时重试：支持确认消息发送失败时的自动重试机制
        //
        // 业务价值：
        // - 状态一致性：保证分布式环境下的状态最终一致性
        // - 流程完整性：确保任务生命周期的完整性和可追溯性
        // - 系统可靠性：提高系统在异常情况下的恢复能力
        // - 运维友好：为运维监控和问题诊断提供完整的事件链
        //
        // 类比：就像质检员验收完产品后，通过对讲机通知工人"验收通过，可以收拾工具了"，
        // 确保工人知道产品已经正式交付，可以开始清理工作台准备下一个任务。
        taskExecutorClient.ackTaskExecutorLifecycleEvent(
                taskExecutionRunnable,
                new ITaskExecutorLifecycleEventReporter.TaskExecutorLifecycleEventAck(
                        taskExecutionRunnable.getId(),
                        TaskExecutorLifecycleEventType.SUCCESS));
    }

    /**
     * 匹配任务成功事件类型
     * 
     * 返回当前处理器能够处理的生命周期事件类型。
     * 这是事件路由系统的核心组成部分，确保成功事件能够被正确的处理器处理。
     * 
     * 事件匹配机制：
     * - 类型识别：通过返回特定的事件类型标识自己的处理能力
     * - 自动路由：事件总线根据类型自动将事件路由到对应处理器
     * - 多态支持：支持不同类型事件的多态处理机制
     * - 类型安全：编译时确保事件类型和处理器的匹配关系
     * 
     * 路由流程：
     * - 事件产生：Worker完成任务后发布TaskSuccessLifecycleEvent
     * - 类型匹配：事件总线遍历所有处理器进行类型匹配
     * - 处理器选择：选择返回SUCCEEDED类型的处理器
     * - 事件分发：将成功事件分发给当前处理器处理
     * - 处理完成：处理器完成所有成功处理逻辑
     * 
     * 设计价值：
     * - 自动化：支持事件的自动识别和路由机制
     * - 解耦合：事件发布者和处理器之间的完全解耦
     * - 可扩展：新增事件处理器时不需要修改现有代码
     * - 类型安全：确保事件处理的类型安全性
     * 
     * 生命周期集成：
     * - 与其他事件处理器形成完整的任务生命周期处理链
     * - 成功事件通常是任务生命周期的终点之一
     * - 为工作流的后续调度提供重要的状态信息
     * - 与监控和统计系统集成，提供任务成功率数据
     * 
     * 类比：就像医院各科室的专业标牌，清楚地标明自己的专业领域，
     * 让患者和分诊系统能够准确地找到对应的科室。
     * 
     * @return TaskLifecycleEventType.SUCCEEDED，表示处理任务成功类型的生命周期事件
     */
    @Override
    public ILifecycleEventType matchEventType() {
        return TaskLifecycleEventType.SUCCEEDED;
    }
}
