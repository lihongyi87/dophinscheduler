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

package org.apache.dolphinscheduler.extract.master;

import org.apache.dolphinscheduler.extract.base.RpcMethod;
import org.apache.dolphinscheduler.extract.base.RpcService;
import org.apache.dolphinscheduler.extract.master.transportor.TaskGroupSlotAcquireSuccessNotifyRequest;
import org.apache.dolphinscheduler.extract.master.transportor.TaskGroupSlotAcquireSuccessNotifyResponse;

/**
 * 任务实例控制器RPC接口
 *
 * 这是Master节点用于任务实例控制操作的核心RPC服务接口。
 * 它提供了任务实例生命周期管理、资源控制、状态通知等功能。
 * 类比：一个工厂的生产线控制中心，负责管理每个工作任务的执行状态和资源分配。
 *
 * 主要职责：
 * 1. 任务资源管理：处理任务组槽位的分配和释放
 * 2. 状态通知：接收和处理任务执行状态变化通知
 * 3. 执行控制：支持任务的暂停、恢复、停止等操作
 * 4. 资源协调：确保任务执行不会超出系统资源限制
 *
 * 设计特点：
 * - 异步通知：支持异步的状态变化通知机制
 * - 资源隔离：通过任务组实现资源的逻辑隔离
 * - 状态一致性：确保任务状态在分布式环境下的一致性
 * - 故障恢复：支持任务失败后的恢复和重试
 *
 * 使用场景：
 * - Worker节点向Master汇报任务执行状态
 * - 资源管理器通知槽位分配结果
 * - 外部系统触发任务控制操作
 * - 集群内部的任务协调和同步
 *
 * 架构位置：
 * - 位于Master节点，接收来自Worker和其他组件的请求
 * - 与任务调度器、资源管理器紧密协作
 * - 为Web界面和API提供任务控制能力
 */
@RpcService
public interface ITaskInstanceController {

    /**
     * 通知任务组槽位获取成功
     *
     * 当任务成功获取到任务组槽位时，通过此方法通知Master节点。
     * 这是任务资源管理的关键环节，确保任务可以正式开始执行。
     *
     * 功能详解：
     * 1. 槽位确认：确认指定任务已成功获取到执行槽位
     * 2. 状态更新：将任务状态从等待资源转为准备执行
     * 3. 资源记录：记录槽位分配信息用于后续管理
     * 4. 执行触发：可能触发任务的实际执行流程
     *
     * 任务组槽位机制：
     * - 限制并发：通过槽位数量限制同时执行的任务数
     * - 资源隔离：不同任务组有独立的槽位池
     * - 优先级控制：高优先级任务可以优先获取槽位
     * - 公平调度：确保长时间等待的任务能够获得执行机会
     *
     * 执行流程：
     * 1. 任务提交到任务组队列
     * 2. 资源管理器检查槽位可用性
     * 3. 分配槽位给等待的任务
     * 4. 调用此方法通知Master槽位分配成功
     * 5. Master更新任务状态并开始执行
     *
     * 异常处理：
     * - 槽位冲突：多个任务同时获取同一槽位
     * - 任务取消：任务在获取槽位前被取消
     * - 超时处理：长时间未响应的槽位分配
     * - 系统故障：槽位分配过程中的系统异常
     *
     * 并发控制：
     * - 原子操作：确保槽位分配的原子性
     * - 状态同步：保证任务状态的一致性
     * - 死锁避免：防止资源分配的死锁情况
     * - 优雅降级：资源不足时的降级策略
     *
     * 监控和度量：
     * - 记录槽位使用率和等待时间
     * - 监控任务组的资源消耗情况
     * - 分析任务调度的性能瓶颈
     * - 提供资源优化的数据支持
     *
     * @param taskGroupSlotAcquireSuccessNotifyRequest 槽位获取成功通知请求，包含任务和槽位信息
     * @return 槽位获取成功通知响应，包含处理结果和后续指令
     */
    @RpcMethod
    TaskGroupSlotAcquireSuccessNotifyResponse notifyTaskGroupSlotAcquireSuccess(TaskGroupSlotAcquireSuccessNotifyRequest taskGroupSlotAcquireSuccessNotifyRequest);

}
