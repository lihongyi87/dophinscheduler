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

package org.apache.dolphinscheduler.server.master.engine.task.lifecycle;

import org.apache.dolphinscheduler.server.master.engine.ILifecycleEventType;

/**
 * 任务生命周期事件类型枚举
 * 
 * 定义了任务在整个生命周期中可能发生的所有事件类型。
 * 类比：就像医院患者就诊流程中的各个阶段标识，从挂号到出院的每个关键节点。
 * 
 * 事件分类：
 * 1. 控制事件：主动触发的任务控制操作（START, DISPATCH, PAUSE, KILL等）
 * 2. 状态事件：任务状态变化的通知（DISPATCHED, RUNNING, PAUSED, KILLED等）
 * 3. 结果事件：任务执行结果的通知（SUCCEEDED, FAILED）
 * 4. 异常事件：异常情况的处理（TIMEOUT, RETRY, FAILOVER）
 * 5. 上下文事件：运行时环境变化（RUNTIME_CONTEXT_CHANGED）
 * 
 * 事件驱动模式：
 * - 解耦设计：事件发布者和处理者完全解耦
 * - 异步处理：事件处理可以异步执行，提高系统响应性
 * - 可扩展性：新的事件类型可以轻松添加
 * - 状态一致性：通过事件确保任务状态的一致性
 * 
 * 生命周期流程：
 * START -> DISPATCH -> DISPATCHED -> RUNNING -> (SUCCEEDED/FAILED)
 * 在此过程中可能穿插PAUSE/PAUSED, KILL/KILLED, TIMEOUT, RETRY, FAILOVER等事件
 * 
 * 类比理解：
 * 就像快递包裹的配送状态：
 * - START: 包裹开始配送
 * - DISPATCH: 分拣中心分发包裹
 * - DISPATCHED: 包裹已发出
 * - RUNNING: 配送员配送中
 * - SUCCEEDED: 成功签收
 * - FAILED: 配送失败
 */
public enum TaskLifecycleEventType implements ILifecycleEventType {

    /**
     * 启动任务实例
     * 
     * 触发任务开始执行的控制事件。这是任务生命周期的起点，
     * 表示Master决定开始执行某个任务实例。
     * 
     * 触发时机：
     * - 工作流执行到该任务节点时
     * - 任务的前置依赖条件全部满足时
     * - 手动触发任务执行时
     * 
     * 处理逻辑：
     * - 初始化任务执行上下文
     * - 准备任务执行所需的资源
     * - 触发后续的调度分发流程
     * 
     * 类比：就像医生开始为患者进行治疗。
     */
    START,

    /**
     * 分发任务实例
     * 
     * 将任务实例分发到目标执行器的控制事件。
     * Master根据任务配置和资源情况，选择合适的Worker节点进行任务分发。
     * 
     * 分发策略：
     * - Worker组选择：根据任务配置的Worker组筛选可用节点
     * - 负载均衡：在可用Worker中选择负载较低的节点
     * - 资源匹配：确保目标Worker具备执行该任务的资源
     * 
     * 处理逻辑：
     * - 构建任务执行上下文
     * - 选择目标Worker节点
     * - 发送任务执行请求
     * 
     * 类比：就像医院将患者分配给具体的医生进行治疗。
     */
    DISPATCH,

    /**
     * 任务实例已分发
     * 
     * 任务实例已成功分发到目标执行器服务的状态事件。
     * 表示Worker节点已收到任务执行请求并准备开始执行。
     * 
     * 状态变化：
     * - 任务状态从SUBMITTED_SUCCESS变为DISPATCH_SUCCESS
     * - Master记录任务的执行节点信息
     * - 开始监控任务的执行进度
     * 
     * 处理逻辑：
     * - 更新任务实例状态
     * - 记录执行节点信息
     * - 设置执行监控
     * 
     * 类比：就像患者已经到达指定科室，准备接受治疗。
     */
    DISPATCHED,

    /**
     * 任务实例运行中
     * 
     * 任务实例正在目标执行器服务上运行的状态事件。
     * TODO: 可能可以移除此事件，一旦任务被分发就应该自动开始运行。
     * 
     * 状态特点：
     * - 任务已在Worker节点开始实际执行
     * - Master可以跟踪任务的执行进度
     * - 任务可能产生中间结果和日志
     * 
     * 监控内容：
     * - 任务执行进度
     * - 系统资源使用情况
     * - 任务运行日志
     * - 可能的异常或警告
     * 
     * 类比：就像患者正在接受治疗，医护人员密切监控治疗过程。
     */
    RUNNING,

    /**
     * 任务实例运行时上下文变更
     * 
     * 任务实例的运行时上下文发生变化的事件。
     * 运行时上下文包括环境变量、资源配置、连接信息等动态变化的信息。
     * 
     * 变更触发场景：
     * - 环境变量动态更新
     * - 数据源连接信息变化
     * - 资源配置动态调整
     * - 任务参数运行时修改
     * 
     * 处理策略：
     * - 实时同步：将上下文变更同步到执行节点
     * - 安全检查：验证变更的安全性和有效性
     * - 影响评估：评估变更对任务执行的影响
     * 
     * 类比：就像治疗过程中调整治疗方案或用药剂量。
     */
    RUNTIME_CONTEXT_CHANGED,

    /**
     * 任务实例超时处理
     * 
     * 执行任务实例的超时策略事件。
     * 当任务执行时间超过配置的超时阈值时触发。
     * 
     * 超时策略：
     * - 告警超时：发送告警通知但继续执行
     * - 失败超时：直接标记任务为失败状态
     * - 终止超时：强制终止任务执行
     * 
     * 处理流程：
     * - 检测任务执行时间
     * - 根据配置执行相应策略
     * - 发送超时通知
     * - 可能触发任务清理
     * 
     * 类比：就像手术时间过长，需要根据预案采取相应措施。
     */
    TIMEOUT,

    /**
     * 重试任务实例
     * 
     * 重新执行任务实例的控制事件。
     * 当任务执行失败且满足重试条件时触发。
     * 
     * 重试条件：
     * - 任务配置允许重试
     * - 未达到最大重试次数
     * - 失败原因支持重试
     * - 重试间隔时间已到
     * 
     * 重试策略：
     * - 立即重试：失败后立即重新执行
     * - 延迟重试：等待一定时间后重试
     * - 指数退避：重试间隔逐渐增加
     * 
     * 类比：就像治疗失败后，医生尝试其他治疗方案。
     */
    RETRY,

    /**
     * 暂停任务实例
     * 
     * 暂停任务实例执行的控制事件。
     * 通常由用户手动触发或系统自动暂停机制触发。
     * 
     * 暂停触发场景：
     * - 用户手动暂停
     * - 系统资源不足
     * - 依赖服务不可用
     * - 工作流级别的暂停
     * 
     * 暂停处理：
     * - 优雅停止：等待当前操作完成后暂停
     * - 强制暂停：立即中断任务执行
     * - 状态保存：保存任务的中间状态
     * 
     * 类比：就像医生暂停手术，等待更好的条件。
     */
    PAUSE,

    /**
     * 任务实例已暂停
     * 
     * 任务实例已成功暂停的状态事件。
     * 表示任务已停止执行并保存了中间状态。
     * 
     * 暂停状态特点：
     * - 任务执行已停止
     * - 中间状态已保存
     * - 可以恢复执行
     * - 资源已释放或保留
     * 
     * 后续操作：
     * - 可以恢复执行
     * - 可以终止任务
     * - 可以修改任务配置
     * 
     * 类比：就像患者的治疗被暂时中断，可以稍后继续。
     */
    PAUSED,

    /**
     * 故障转移任务实例
     * 
     * 对任务实例执行故障转移的事件。
     * 当Worker节点发生故障时，将任务转移到其他节点执行。
     * 
     * 故障转移场景：
     * - Worker节点宕机
     * - 网络连接中断
     * - 系统资源耗尽
     * - 服务无响应
     * 
     * 转移策略：
     * - 自动转移：系统自动选择新的执行节点
     * - 状态恢复：恢复任务的执行状态
     * - 数据同步：同步必要的执行数据
     * 
     * 类比：就像主治医生无法继续治疗时，转给其他医生。
     */
    FAILOVER,

    /**
     * 终止任务实例
     * 
     * 终止任务实例执行的控制事件。
     * 强制停止任务执行，通常用于异常情况或用户主动取消。
     * 
     * 终止触发场景：
     * - 用户手动取消
     * - 工作流被终止
     * - 系统异常处理
     * - 资源清理需要
     * 
     * 终止处理：
     * - 强制中断执行
     * - 清理占用资源
     * - 发送终止信号
     * - 记录终止原因
     * 
     * 类比：就像紧急情况下强制停止治疗。
     */
    KILL,

    /**
     * 任务实例已终止
     * 
     * 任务实例已成功终止的状态事件。
     * 表示任务已被强制停止并清理了相关资源。
     * 
     * 终止状态特点：
     * - 任务执行已完全停止
     * - 系统资源已释放
     * - 无法恢复执行
     * - 终止原因已记录
     * 
     * 清理工作：
     * - 释放系统资源
     * - 清理临时文件
     * - 断开外部连接
     * - 更新状态记录
     * 
     * 类比：就像治疗被完全停止，患者离开治疗室。
     */
    KILLED,

    /**
     * 任务实例执行成功
     * 
     * 任务实例成功完成的结果事件。
     * 表示任务已按预期完成执行并产生了正确的结果。
     * 
     * 成功标准：
     * - 任务正常完成执行
     * - 返回码为0（成功）
     * - 产生了预期的输出
     * - 没有严重错误或异常
     * 
     * 成功处理：
     * - 更新任务状态
     * - 记录执行结果
     * - 触发后续任务
     * - 发送成功通知
     * 
     * 类比：就像治疗成功完成，患者康复出院。
     */
    SUCCEEDED,

    /**
     * 任务实例执行失败
     * 
     * 任务实例执行失败的结果事件。
     * 表示任务执行过程中出现了错误，无法正常完成。
     * 
     * 失败原因：
     * - 程序执行错误
     * - 资源不足或不可用
     * - 配置错误或参数问题
     * - 外部依赖服务故障
     * - 网络连接问题
     * 
     * 失败处理：
     * - 记录失败原因
     * - 保存错误日志
     * - 评估重试可能性
     * - 发送失败通知
     * - 可能触发工作流失败
     * 
     * 类比：就像治疗失败，需要分析原因并考虑后续方案。
     */
    FAILED,
    ;

}
