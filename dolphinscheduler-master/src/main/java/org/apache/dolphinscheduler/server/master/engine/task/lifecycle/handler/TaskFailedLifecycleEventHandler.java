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
 * 这是专门处理任务执行失败事件的处理器，负责任务失败后的应急处理和恢复工作。
 * 类比：就像项目应急响应专员，负责处理项目执行过程中的各种异常和失败情况。
 * 
 * 核心职责：
 * 1. 失败确认：接收并确认任务在Worker节点上的执行失败
 * 2. 状态更新：将任务状态更新为FAILED（失败）
 * 3. 错误记录：详细记录失败原因、错误信息和堆栈跟踪
 * 4. 通信确认：向TaskExecutor发送失败事件确认消息
 * 5. 恢复决策：根据配置决定是否进行重试或故障转移
 * 
 * 失败处理流程：
 * - 事件接收：接收来自Worker的任务执行失败事件
 * - 错误分析：分析失败原因，判断是否可恢复
 * - 状态转换：将任务状态从RUNNING转换为FAILED
 * - 信息记录：记录详细的错误信息和执行日志
 * - 确认回复：向Worker发送失败事件确认消息
 * - 恢复评估：评估是否满足重试或故障转移条件
 * 
 * 错误分类处理：
 * - 系统错误：网络异常、资源不足等系统级问题
 * - 应用错误：代码异常、参数错误等应用级问题
 * - 环境错误：依赖服务不可用、配置错误等环境问题
 * - 用户错误：输入数据错误、权限不足等用户操作问题
 * 
 * 恢复策略：
 * - 自动重试：对于临时性错误，自动进行任务重试
 * - 故障转移：Worker节点故障时，转移到其他节点执行
 * - 人工干预：对于复杂错误，标记为需要人工处理
 * - 流程中断：对于致命错误，中断整个工作流执行
 * 
 * 处理特点：
 * - 错误分析：能够分析和分类不同类型的执行错误
 * - 状态驱动：根据任务当前状态和错误类型选择处理策略
 * - 双向通信：与Worker建立可靠的失败事件确认机制
 * - 恢复导向：以任务和工作流的恢复为主要目标
 * 
 * 业务价值：
 * - 快速响应：快速识别和响应任务执行失败事件
 * - 信息完整：完整记录失败信息，便于问题定位和分析
 * - 自动恢复：通过自动重试和故障转移提高系统可用性
 * - 流程保护：防止单个任务失败影响整个工作流的执行
 * 
 * 类比理解：
 * 就像医院的急救科：
 * - 接收急救：接收紧急的医疗事故报告（失败事件）
 * - 快速诊断：快速判断问题的严重程度和类型（错误分析）
 * - 详细记录：记录患者的症状和处理过程（信息记录）
 * - 回复确认：确认已接收并开始处理（确认消息）
 * - 治疗方案：根据病情选择治疗方案（恢复策略）
 */
@Component
public class TaskFailedLifecycleEventHandler extends AbstractTaskLifecycleEventHandler<TaskFailedLifecycleEvent> {

    /**
     * 任务执行器客户端
     * 
     * 用于与TaskExecutor进行通信的客户端组件，特别是在任务失败场景下的通信确认。
     * 在失败处理中，确认通信尤为重要，确保Worker知道Master已经收到失败信息。
     * 
     * 失败通信功能：
     * - 失败确认：向Worker确认已收到失败事件，避免重复上报
     * - 状态同步：同步Master和Worker之间关于任务失败的状态信息
     * - 控制指令：在故障转移或重试时发送相应的控制指令
     * - 清理通知：通知Worker进行相关资源的清理工作
     * 
     * 可靠性保障：
     * - 重试机制：确认消息发送失败时的自动重试
     * - 幂等处理：支持Worker对重复确认消息的幂等处理
     * - 超时处理：设置合理的超时时间，避免无限等待
     * - 异常恢复：处理网络异常等通信故障
     * 
     * 类比：就像急救科的通讯设备，确保与现场救护人员的可靠通信，
     * 及时确认指令接收和现场情况反馈。
     */
    private final TaskExecutorClient taskExecutorClient;

    /**
     * 构造函数 - 注入任务执行器客户端依赖
     * 
     * 通过构造函数注入TaskExecutorClient依赖，确保失败处理器能够
     * 与Worker节点进行可靠的失败事件确认通信。
     * 
     * 依赖注入优势：
     * - 依赖明确：构造函数清晰地声明了对通信客户端的依赖
     * - 不可变性：通过final关键字确保客户端引用的不可变性
     * - 测试支持：便于在单元测试中进行依赖模拟和隔离测试
     * - Spring管理：由Spring容器负责依赖的创建和注入
     * 
     * @param taskExecutorClient 任务执行器客户端，用于与Worker节点的失败确认通信
     */
    public TaskFailedLifecycleEventHandler(final TaskExecutorClient taskExecutorClient) {
        this.taskExecutorClient = taskExecutorClient;
    }

    /**
     * 处理任务失败生命周期事件
     * 
     * 这是任务失败处理的核心方法，负责完整的失败事件处理流程。
     * 包括错误分析、状态更新、信息记录、确认通信和恢复策略等多个环节。
     * 
     * 失败处理流程：
     * 1. 状态处理：通过状态动作执行具体的失败处理逻辑
     * 2. 错误记录：详细记录失败原因和错误信息到数据库
     * 3. 状态更新：将任务状态从RUNNING更新为FAILED
     * 4. 确认回复：向Worker发送失败事件确认消息
     * 5. 恢复评估：评估重试或故障转移的可能性
     * 
     * 状态动作职责：
     * - 错误分析：分析失败原因，判断错误类型和严重程度
     * - 信息记录：记录完整的错误信息、堆栈跟踪和执行日志
     * - 状态转换：安全地进行任务状态转换，避免状态不一致
     * - 恢复决策：根据任务配置和错误类型决定后续处理策略
     * - 通知发送：发送失败通知给相关的监控和告警系统
     * 
     * 确认机制重要性：
     * - 状态同步：确保Master和Worker对任务失败状态的一致认知
     * - 避免重复：防止Worker重复上报相同的失败事件
     * - 资源清理：通知Worker可以开始清理相关的执行资源
     * - 监控准确：确保监控系统能够准确统计任务失败情况
     * 
     * 异常处理策略：
     * - 网络异常：确认消息发送失败时的重试和备选方案
     * - 状态冲突：Master和Worker状态不一致时的协调处理
     * - 资源异常：清理资源时遇到异常的处理方式
     * - 依赖异常：后续恢复操作异常时的回退处理
     * 
     * 恢复机制触发：
     * - 重试条件：满足重试条件时自动触发任务重试
     * - 故障转移：Worker故障时触发到其他节点的故障转移
     * - 告警机制：严重失败时触发相应的告警和通知
     * - 人工介入：复杂失败时标记为需要人工干预
     * 
     * 类比：就像急救医生接到紧急病情报告后，既要快速诊断和处理，
     * 又要详细记录病情，还要与现场人员确认处理结果。
     * 
     * @param taskStateAction 任务状态对应的处理动作，负责执行具体失败处理逻辑
     * @param workflowExecutionRunnable 工作流执行器，提供工作流级别的上下文信息
     * @param taskExecutionRunnable 任务执行器，提供任务级别的操作接口和状态信息
     * @param event 任务失败生命周期事件，包含失败相关的详细信息和错误堆栈
     */
    @Override
    public void handle(final ITaskStateAction taskStateAction,
                       final IWorkflowExecutionRunnable workflowExecutionRunnable,
                       final ITaskExecutionRunnable taskExecutionRunnable,
                       final TaskFailedLifecycleEvent event) {
        // ==================== 任务失败事件处理核心逻辑 ====================
        //
        // 当任务在Worker节点上执行失败时，会发送失败生命周期事件到Master节点。
        // 这个方法负责处理该事件，进行错误分析、状态更新、恢复决策，并向Worker发送确认。
        // 整个处理过程确保失败的及时响应和系统的自愈能力。

        // ========== 第一步：失败状态处理 ==========
        // 委托给对应的状态操作类处理任务执行失败事件
        //
        // 执行内容：
        // 1. 失败原因分析：深入分析任务失败的根本原因
        //    - 解析失败事件中的错误信息和堆栈跟踪
        //    - 分类错误类型：系统错误、应用错误、环境错误、用户错误
        //    - 评估错误的严重程度和影响范围
        //    - 判断错误是否为可恢复的临时性错误
        //
        // 2. 状态转换：将任务状态从运行中转换为失败
        //    - 更新任务状态为FAILED（执行失败）
        //    - 记录任务失败的时间戳和执行时长
        //    - 保存详细的错误信息和异常堆栈
        //    - 更新任务实例的失败原因字段
        //
        // 3. 错误信息记录：完整记录失败相关的信息
        //    - 记录错误消息、异常类型和堆栈跟踪
        //    - 保存任务执行的上下文信息
        //    - 记录Worker节点的执行环境信息
        //    - 生成问题排查所需的诊断数据
        //
        // 4. 重试决策：根据失败类型和配置决定是否重试
        //    - 检查任务的重试配置（重试次数、重试间隔等）
        //    - 分析当前已重试次数是否达到上限
        //    - 评估重试成功的可能性（基于错误类型）
        //    - 决定是否立即重试、延迟重试或放弃重试
        //
        // 5. 故障转移评估：评估是否需要进行故障转移
        //    - 判断失败是否由Worker节点故障引起
        //    - 检查是否有其他可用的Worker节点
        //    - 评估故障转移的成本和成功率
        //    - 决定是否将任务转移到其他节点执行
        //
        // 6. 依赖关系处理：处理失败对后续任务的影响
        //    - 分析依赖该任务的其他任务
        //    - 根据依赖配置决定后续任务的处理策略
        //    - 可能的策略：继续执行、跳过执行、整体失败
        //    - 更新工作流的整体执行状态
        //
        // 7. 告警和通知：触发相应的告警和通知机制
        //    - 根据错误严重程度触发不同级别的告警
        //    - 通知相关的运维人员或开发人员
        //    - 记录告警信息到监控系统
        //    - 更新任务和工作流的统计指标
        //
        // 类比：就像医院急诊科收到紧急病情报告后，快速诊断病因，
        // 记录病历信息，决定治疗方案，通知相关科室和家属。
        taskStateAction.onFailedEvent(workflowExecutionRunnable, taskExecutionRunnable, event);

        // ========== 第二步：失败确认消息发送 ==========
        // 向TaskExecutor发送失败事件确认消息，告知已收到并处理了任务失败事件
        //
        // 确认机制的关键价值：
        // 1. 状态同步确认：确保Master和Worker对任务失败状态的一致认知
        //    - Worker知道Master已经收到失败通知并开始处理
        //    - 避免Worker重复发送失败事件，减少无效通信
        //    - 保证双方对任务最终状态的统一理解
        //    - 为后续的恢复操作提供可靠的状态基础
        //
        // 2. 资源清理协调：协调Master和Worker进行失败后的资源清理
        //    - 通知Worker可以开始清理失败任务的执行环境
        //    - 确认Worker可以释放为该任务分配的系统资源
        //    - 协调临时文件、进程、连接等资源的清理时机
        //    - 避免资源泄露和长时间占用，提高系统资源利用率
        //
        // 3. 重试流程控制：为可能的重试操作建立控制基础
        //    - 告知Worker当前失败已被Master确认和处理
        //    - 为后续可能的重试建立清晰的起点
        //    - 避免重试过程中的状态混乱和重复执行
        //    - 支持重试时的状态重置和环境准备
        //
        // 4. 故障转移支持：为故障转移操作提供通信支持
        //    - 确认当前Worker节点的任务执行已经结束
        //    - 为任务在其他Worker节点上的重新执行做准备
        //    - 避免多个Worker节点同时执行同一任务
        //    - 支持分布式环境下的故障转移协调
        //
        // 5. 监控和审计：完善任务执行的监控和审计链路
        //    - 形成完整的任务失败处理事件链
        //    - 为系统监控提供可靠的事件数据
        //    - 支持失败事件的全程追踪和分析
        //    - 为问题排查和系统优化提供数据基础
        //
        // 确认消息构成：
        // - taskExecutionRunnable: 任务执行上下文，包含任务标识和状态信息
        // - TaskExecutorLifecycleEventAck: 失败事件确认对象，包含：
        //   - taskExecutionRunnable.getId(): 任务唯一标识符，确保确认的准确性
        //   - TaskExecutorLifecycleEventType.FAILED: 明确标识这是对失败事件的确认
        //
        // 通信保障机制：
        // - 可靠传输：确保确认消息能够可靠送达Worker节点
        // - 异步处理：确认消息发送采用异步模式，不阻塞失败处理流程
        // - 超时重试：支持确认消息发送失败时的自动重试机制
        // - 幂等处理：Worker能够正确处理重复的失败确认消息
        // - 异常恢复：处理网络异常等通信故障情况
        //
        // 业务影响：
        // - 快速响应：确保失败事件得到及时确认和处理
        // - 系统稳定：避免失败事件处理过程中的状态不一致
        // - 恢复能力：为系统的自动恢复和故障转移提供基础
        // - 运维效率：为运维监控和问题处理提供准确的事件信息
        //
        // 类比：就像急诊医生处理完紧急病情后，通过对讲机通知现场救护人员
        // "病人已收治，你们可以清理救护车准备下一次出诊"，确保整个急救流程的闭环。
        taskExecutorClient.ackTaskExecutorLifecycleEvent(
                taskExecutionRunnable,
                new ITaskExecutorLifecycleEventReporter.TaskExecutorLifecycleEventAck(
                        taskExecutionRunnable.getId(),
                        TaskExecutorLifecycleEventType.FAILED));
    }

    /**
     * 匹配任务失败事件类型
     * 
     * 返回当前处理器能够处理的生命周期事件类型。
     * 这是事件路由系统的重要组成部分，确保失败事件能够被正确的处理器处理。
     * 
     * 事件匹配机制：
     * - 专业标识：通过返回FAILED类型标识专门处理失败事件
     * - 自动路由：事件总线根据类型自动将失败事件路由到当前处理器
     * - 优先处理：失败事件通常需要优先和快速处理
     * - 类型安全：编译时确保失败事件和处理器的类型匹配
     * 
     * 失败事件特点：
     * - 紧急性：失败事件通常需要紧急处理，避免问题扩散
     * - 复杂性：失败处理涉及错误分析、恢复决策等复杂逻辑
     * - 关键性：失败处理的效果直接影响系统的可用性和可靠性
     * - 多样性：不同类型的失败需要不同的处理策略
     * 
     * 路由流程：
     * - 失败发生：Worker遇到任务执行失败，发布TaskFailedLifecycleEvent
     * - 紧急路由：事件总线优先处理失败类型的事件
     * - 处理器匹配：选择返回FAILED类型的专业处理器
     * - 应急处理：失败处理器立即开始应急处理流程
     * - 恢复尝试：根据情况尝试各种恢复策略
     * 
     * 系统集成：
     * - 监控集成：与监控系统集成，及时上报失败统计
     * - 告警集成：与告警系统集成，触发相应的告警通知
     * - 恢复集成：与重试和故障转移机制集成
     * - 日志集成：与日志系统集成，完整记录失败处理过程
     * 
     * 生命周期位置：
     * - 失败事件是任务生命周期的异常分支
     * - 可能触发重试，重新进入正常生命周期
     * - 可能触发故障转移，在新节点重新开始
     * - 可能成为最终状态，结束任务生命周期
     * 
     * 类比：就像医院急诊科的专业标牌，清楚地表明这里专门处理
     * 各种医疗急救事件，病人和救护车都知道要来这里。
     * 
     * @return TaskLifecycleEventType.FAILED，表示处理任务失败类型的生命周期事件
     */
    @Override
    public ILifecycleEventType matchEventType() {
        return TaskLifecycleEventType.FAILED;
    }
}
