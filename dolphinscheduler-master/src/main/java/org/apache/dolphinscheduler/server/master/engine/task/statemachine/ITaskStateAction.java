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

package org.apache.dolphinscheduler.server.master.engine.task.statemachine;

import org.apache.dolphinscheduler.plugin.task.api.enums.TaskExecutionStatus;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event.TaskDispatchLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event.TaskDispatchedLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event.TaskFailedLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event.TaskFailoverLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event.TaskKillLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event.TaskKilledLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event.TaskPauseLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event.TaskPausedLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event.TaskRetryLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event.TaskRunningLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event.TaskRuntimeContextChangedEvent;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event.TaskStartLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event.TaskSuccessLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.task.runnable.ITaskExecutionRunnable;
import org.apache.dolphinscheduler.server.master.engine.workflow.runnable.IWorkflowExecutionRunnable;

/**
 * 任务状态动作接口
 *
 * 定义当任务处于特定状态时，接收到目标事件后需要执行的动作。
 * 这是任务状态机模式的核心抽象，每个任务执行状态都应该有对应的状态动作实现。
 *
 * 设计理念：
 * 状态机模式 - 将任务的状态变迁逻辑封装到各自的状态处理器中，避免复杂的条件判断
 * 策略模式 - 不同状态对应不同的处理策略，便于扩展和维护
 * 命令模式 - 将事件处理封装为可执行的动作命令
 *
 * 状态转换规则：
 * 1. 每个状态只能处理特定的事件类型
 * 2. 状态转换必须遵循预定义的状态流转图
 * 3. 非法状态转换会触发异常或警告日志
 * 4. 某些状态是终态，不允许进一步转换
 *
 * 事件处理流程：
 * 1. 事件验证：检查当前状态是否匹配期望状态
 * 2. 业务逻辑：执行状态特定的业务处理逻辑
 * 3. 状态更新：更新任务实例的状态信息
 * 4. 事件传播：发布后续的生命周期事件
 * 5. 工作流协调：通知工作流引擎进行拓扑调度
 *
 * 错误处理策略：
 * - 状态不匹配：抛出IllegalStateException异常
 * - 无效操作：记录警告日志，不执行实际操作
 * - 系统异常：向上抛出，由调用方处理
 *
 * 并发安全：
 * 实现类需要考虑多线程并发访问的安全性，特别是状态检查和更新操作。
 *
 * 类比理解：
 * 就像现实生活中的工序流程管理系统：
 * - 每个工序状态（提交、执行、完成、失败等）都有专门的处理程序
 * - 不同事件（开始、暂停、取消等）在不同状态下有不同的处理方式
 * - 确保工序按照既定流程有序推进，防止违规操作
 *
 * @see TaskSubmittedStateAction 任务提交状态动作
 * @see TaskDelayExecutionStateAction 任务延迟执行状态动作
 * @see TaskDispatchStateAction 任务分发状态动作
 * @see TaskRunningStateAction 任务运行状态动作
 * @see TaskPauseStateAction 任务暂停状态动作
 * @see TaskKillStateAction 任务终止状态动作
 * @see TaskFailureStateAction 任务失败状态动作
 * @see TaskSuccessStateAction 任务成功状态动作
 * @see TaskForceSuccessStateAction 任务强制成功状态动作
 * @see TaskFailoverStateAction 任务故障转移状态动作
 */
public interface ITaskStateAction {

    /**
     * 处理任务启动事件
     *
     * 当任务处于特定状态时接收到任务启动生命周期事件时执行必要的动作。
     * 这个方法在需要启动任务时被调用。
     *
     * 处理逻辑：
     * 1. 状态验证：确认当前任务状态是否允许启动操作
     * 2. 工作流检查：检查工作流是否处于暂停或停止状态
     * 3. 资源获取：获取任务执行所需的资源（如任务组槽位）
     * 4. 任务分发：将任务分发给合适的执行器
     * 5. 状态转换：更新任务状态为下一个阶段
     *
     * 不同状态下的处理方式：
     * - SUBMITTED状态：检查工作流状态，尝试分发任务
     * - RUNNING状态：触发故障转移流程
     * - SUCCESS/FAILURE状态：重新处理成功/失败逻辑
     * - PAUSE状态：标记任务链暂停并通知工作流
     *
     * @param workflowExecutionRunnable 工作流执行实例
     * @param taskExecutionRunnable 任务执行实例
     * @param taskStartEvent 任务启动事件
     */
    void onStartEvent(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                      final ITaskExecutionRunnable taskExecutionRunnable,
                      final TaskStartLifecycleEvent taskStartEvent);

    /**
     * 处理任务运行事件
     *
     * 当任务处于特定状态时接收到任务运行生命周期事件时执行必要的动作。
     * 这个方法在Master收到来自执行器的任务运行事件时被调用。
     *
     * 处理逻辑：
     * 1. 状态验证：确认当前任务状态是否匹配预期状态
     * 2. 信息更新：更新任务的运行时信息（开始时间、日志路径等）
     * 3. 状态转换：将任务状态更新为运行中
     * 4. 数据持久化：将状态变更信息保存到数据库
     *
     * 状态适用性：
     * - DISPATCH状态：正常接收运行事件，更新为运行状态
     * - RUNNING状态：可能是重复事件，更新运行信息
     * - 其他状态：记录警告日志，不执行实际操作
     *
     * @param workflowExecutionRunnable 工作流执行实例
     * @param taskExecutionRunnable 任务执行实例
     * @param taskRunningEvent 任务运行事件，包含开始时间和日志路径等信息
     */
    void onStartedEvent(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                        final ITaskExecutionRunnable taskExecutionRunnable,
                        final TaskRunningLifecycleEvent taskRunningEvent);

    /**
     * 处理任务运行时上下文变更事件
     *
     * 当任务处于特定状态时接收到任务运行时上下文变更事件时执行必要的动作。
     * 这个方法在Master收到来自执行器的任务运行时上下文变更事件时被调用。
     *
     * 处理逻辑：
     * 1. 状态验证：确认当前任务状态是否允许上下文更新
     * 2. 上下文解析：解析运行时上下文信息（如应用链接、监控信息等）
     * 3. 信息更新：更新任务实例的运行时上下文信息
     * 4. 数据持久化：将上下文信息保存到数据库
     *
     * 运行时上下文通常包括：
     * - 应用访问链接（如Spark UI、Flink Dashboard等）
     * - 监控指标信息
     * - 执行环境信息
     * - 其他动态生成的运行时数据
     *
     * @param workflowExecutionRunnable 工作流执行实例
     * @param taskExecutionRunnable 任务执行实例
     * @param taskRuntimeContextChangedEvent 任务运行时上下文变更事件
     */
    void onRuntimeContextChangedEvent(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                                      final ITaskExecutionRunnable taskExecutionRunnable,
                                      final TaskRuntimeContextChangedEvent taskRuntimeContextChangedEvent);

    /**
     * 处理任务重试事件
     *
     * 当任务处于特定状态时接收到任务重试生命周期事件时执行必要的动作。
     * 这个方法在任务需要重试时被调用。
     *
     * 处理逻辑：
     * 1. 重试条件检查：验证任务是否还能进行重试（重试次数、重试策略等）
     * 2. 状态重置：重置任务状态为可重试状态
     * 3. 重试计数：更新任务的重试次数
     * 4. 延迟处理：根据重试策略设置重试延迟时间
     * 5. 重新调度：将任务重新加入调度队列
     *
     * 重试策略：
     * - 最大重试次数限制
     * - 指数退避延迟
     * - 重试间隔配置
     * - 失败类型过滤（某些错误不重试）
     *
     * 状态适用性：
     * - FAILURE状态：检查重试条件，执行重试逻辑
     * - 其他状态：记录警告，不执行重试
     *
     * @param workflowExecutionRunnable 工作流执行实例
     * @param taskExecutionRunnable 任务执行实例
     * @param taskRetryEvent 任务重试事件
     */
    void onRetryEvent(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                      final ITaskExecutionRunnable taskExecutionRunnable,
                      final TaskRetryLifecycleEvent taskRetryEvent);

    /**
     * 处理任务分发事件
     *
     * 当任务处于特定状态时接收到任务分发生命周期事件时执行必要的动作。
     * 这个方法在需要分发任务到执行器时被调用。
     *
     * 处理逻辑：
     * 1. 延迟检查：计算任务的延迟执行时间，判断是否需要延迟
     * 2. 状态更新：如果需要延迟，更新状态为DELAY_EXECUTION
     * 3. 上下文初始化：初始化任务执行上下文信息
     * 4. 工作组分发：通过工作组分发协调器将任务分发给合适的执行器
     * 5. 分发确认：等待执行器确认接收任务
     *
     * 分发策略：
     * - 工作组选择：根据任务配置选择合适的工作组
     * - 负载均衡：在工作组内选择负载较低的执行器
     * - 资源匹配：确保执行器有足够的资源执行任务
     * - 容错处理：分发失败时的重试和故障转移
     *
     * 延迟执行处理：
     * - 计算剩余延迟时间
     * - 更新任务状态为延迟执行
     * - 加入延迟执行队列
     *
     * @param workflowExecutionRunnable 工作流执行实例
     * @param taskExecutionRunnable 任务执行实例
     * @param taskDispatchEvent 任务分发事件
     */
    void onDispatchEvent(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                         final ITaskExecutionRunnable taskExecutionRunnable,
                         final TaskDispatchLifecycleEvent taskDispatchEvent);

    /**
     * 处理任务已分发事件
     *
     * 当任务处于特定状态时接收到任务已分发生命周期事件时执行必要的动作。
     * 这个方法在任务已成功分发到执行器时被调用。
     *
     * 处理逻辑：
     * 1. 状态验证：确认当前任务状态是否匹配预期状态
     * 2. 状态更新：将任务状态更新为DISPATCH
     * 3. 执行器信息：记录任务分发到的执行器主机信息
     * 4. 数据持久化：将分发信息保存到数据库
     *
     * 分发确认信息：
     * - 执行器主机地址
     * - 分发时间戳
     * - 任务分发ID
     * - 执行器资源分配情况
     *
     * 后续处理：
     * - 等待执行器返回任务运行事件
     * - 监控任务执行状态
     * - 处理可能的执行器故障
     *
     * @param workflowExecutionRunnable 工作流执行实例
     * @param taskExecutionRunnable 任务执行实例
     * @param taskDispatchedEvent 任务已分发事件，包含执行器主机等信息
     */
    void onDispatchedEvent(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                           final ITaskExecutionRunnable taskExecutionRunnable,
                           final TaskDispatchedLifecycleEvent taskDispatchedEvent);

    /**
     * 处理任务暂停事件
     *
     * 当任务处于特定状态时接收到任务暂停生命周期事件时执行必要的动作。
     * 这个方法在需要暂停任务时被调用。
     *
     * 处理逻辑：
     * 1. 状态检查：验证当前任务状态是否允许暂停操作
     * 2. 暂停类型判断：根据任务当前状态选择不同的暂停策略
     * 3. 资源清理：释放任务占用的资源（如任务组槽位）
     * 4. 执行器通知：向执行器发送暂停指令
     * 5. 状态转换：更新任务状态为暂停状态
     *
     * 不同状态下的暂停处理：
     * - SUBMITTED状态：尝试从分发队列中移除，失败则延迟暂停
     * - DISPATCH状态：通过执行器客户端发送暂停指令
     * - RUNNING状态：通过执行器客户端发送暂停指令
     * - FAILURE状态：如果在重试队列中，可以直接暂停
     * - 终态：记录警告，不执行暂停
     *
     * 延迟暂停机制：
     * - 当任务已分发但暂时无法取消时，延迟一段时间再重试
     * - 给执行器时间响应暂停指令
     *
     * @param workflowExecutionRunnable 工作流执行实例
     * @param taskExecutionRunnable 任务执行实例
     * @param taskPauseEvent 任务暂停事件
     */
    void onPauseEvent(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                      final ITaskExecutionRunnable taskExecutionRunnable,
                      final TaskPauseLifecycleEvent taskPauseEvent);

    /**
     * 处理任务已暂停事件
     *
     * 当任务处于特定状态时接收到任务已暂停生命周期事件时执行必要的动作。
     * 这个方法在任务已成功暂停时被调用。
     *
     * 处理逻辑：
     * 1. 状态验证：确认当前任务状态是否允许接收暂停确认
     * 2. 资源释放：释放任务占用的资源（任务组槽位、执行器资源等）
     * 3. 状态更新：将任务状态更新为PAUSE
     * 4. 链式暂停：标记任务执行链为暂停状态
     * 5. 工作流通知：发布工作流拓扑逐转换事件
     * 6. 数据持久化：将暂停状态保存到数据库
     *
     * 暂停效果：
     * - 任务停止执行，但保留执行上下文
     * - 依赖该任务的后续任务也会被暂停
     * - 可以通过恢复操作重新启动
     * - 保持工作流的一致性状态
     *
     * 特殊情况处理：
     * - 如果任务在重试队列中，暂停可以取消重试
     * - 如果任务在延迟执行队列中，暂停可以取消延迟
     *
     * @param workflowExecutionRunnable 工作流执行实例
     * @param taskExecutionRunnable 任务执行实例
     * @param taskPausedEvent 任务已暂停事件
     */
    void onPausedEvent(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                       final ITaskExecutionRunnable taskExecutionRunnable,
                       final TaskPausedLifecycleEvent taskPausedEvent);

    /**
     * 处理任务终止事件
     *
     * 当任务处于特定状态时接收到任务终止生命周期事件时执行必要的动作。
     * 这个方法在需要终止任务时被调用。
     *
     * 处理逻辑：
     * 1. 状态检查：验证当前任务状态是否允许终止操作
     * 2. 终止类型判断：根据任务当前状态选择不同的终止策略
     * 3. 资源清理：释放任务占用的所有资源
     * 4. 执行器通知：向执行器发送终止指令
     * 5. 状态转换：更新任务状态为终止状态
     * 6. 清理工作：清理任务相关的临时文件和中间结果
     *
     * 不同状态下的终止处理：
     * - SUBMITTED状态：尝试从分发队列中移除，失败则延迟终止
     * - DISPATCH状态：通过执行器客户端发送终止指令
     * - RUNNING状态：通过执行器客户端发送终止指令
     * - FAILURE状态：如果在重试队列中，可以直接终止
     * - 终态：记录警告，不执行终止
     *
     * 终止与暂停的区别：
     * - 终止是不可逆的，任务不能恢复执行
     * - 终止会释放所有资源，清理中间数据
     * - 终止会影响依赖任务的执行策略
     *
     * 延迟终止机制：
     * - 当任务已分发但暂时无法取消时，延迟一段时间再重试
     * - 给执行器时间响应终止指令
     *
     * @param workflowExecutionRunnable 工作流执行实例
     * @param taskExecutionRunnable 任务执行实例
     * @param taskKillEvent 任务终止事件
     */
    void onKillEvent(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                     final ITaskExecutionRunnable taskExecutionRunnable,
                     final TaskKillLifecycleEvent taskKillEvent);

    /**
     * 处理任务已终止事件
     *
     * 当任务处于特定状态时接收到任务已终止生命周期事件时执行必要的动作。
     * 这个方法在任务已成功终止时被调用。
     *
     * 处理逻辑：
     * 1. 状态验证：确认当前任务状态是否允许接收终止确认
     * 2. 资源释放：释放任务占用的所有资源
     * 3. 状态更新：将任务状态更新为KILL
     * 4. 结束时间：记录任务的终止时间
     * 5. 链式终止：标记任务执行链为终止状态
     * 6. 工作流通知：发布工作流拓扑逻辑转换事件
     * 7. 数据持久化：将终止状态和时间保存到数据库
     *
     * 终止效果：
     * - 任务完全停止执行，无法恢复
     * - 所有资源被释放，中间结果被清理
     * - 依赖该任务的后续任务根据配置决定是否继续执行
     * - 工作流可能需要停止或者继续执行其他分支
     *
     * 特殊情况处理：
     * - 如果任务在重试队列中，终止可以取消重试
     * - 如果任务在延迟执行队列中，终止可以取消延迟
     * - 如果是条件任务，终止可能不影响后续执行
     *
     * @param workflowExecutionRunnable 工作流执行实例
     * @param taskExecutionRunnable 任务执行实例
     * @param taskKilledEvent 任务已终止事件，包含终止时间等信息
     */
    void onKilledEvent(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                       final ITaskExecutionRunnable taskExecutionRunnable,
                       final TaskKilledLifecycleEvent taskKilledEvent);

    /**
     * 处理任务失败事件
     *
     * 当任务处于特定状态时接收到任务失败生命周期事件时执行必要的动作。
     * 这个方法在任务执行失败时被调用。
     *
     * 处理逻辑：
     * 1. 状态验证：确认当前任务状态是否匹配预期状态
     * 2. 资源释放：释放任务占用的资源（任务组槽位等）
     * 3. 失败信息：记录任务失败的原因和时间
     * 4. 重试检查：检查任务是否还能进行重试
     * 5. 继续执行检查：对于某些情况，判断是否可以继续执行后续任务
     * 6. 状态转换：更新任务状态为失败状态
     * 7. 工作流协调：通知工作流引擎进行后续处理
     *
     * 失败处理策略：
     * - 重试策略：如果任务还能重试，发起重试事件
     * - 跳过策略：如果所有后续任务都是条件任务，则继续执行
     * - 失败策略：标记任务链失败，停止后续任务执行
     * - 终止策略：根据工作流配置决定是否终止整个工作流
     *
     * 条件任务特殊处理：
     * - 如果失败任务的所有后续任务都是条件任务，则DAG可以继续执行
     * - 条件任务可以根据前置任务的失败状态做出相应的逐路判断
     *
     * @param workflowExecutionRunnable 工作流执行实例
     * @param taskExecutionRunnable 任务执行实例
     * @param taskFailedEvent 任务失败事件，包含失败原因和时间等信息
     */
    void onFailedEvent(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                       final ITaskExecutionRunnable taskExecutionRunnable,
                       final TaskFailedLifecycleEvent taskFailedEvent);

    /**
     * 处理任务成功事件
     *
     * 当任务处于特定状态时接收到任务成功生命周期事件时执行必要的动作。
     * 这个方法在任务执行成功时被调用。
     *
     * 处理逻辑：
     * 1. 状态验证：确认当前任务状态是否匹配预期状态
     * 2. 资源释放：释放任务占用的资源（任务组槽位等）
     * 3. 成功信息：记录任务成功的时间和结果信息
     * 4. 状态更新：将任务状态更新为成功状态
     * 5. 变量池合并：将任务产生的变量合并到工作流变量池中
     * 6. 输出数据：处理任务的输出数据和结果文件
     * 7. 工作流协调：通知工作流引擎激活后续任务
     * 8. 数据持久化：将成功状态和结果保存到数据库
     *
     * 变量池合并逻辑：
     * - 获取任务执行过程中产生的变量
     * - 将任务变量与工作流变量进行合并
     * - 解决变量名称冲突，保持数据一致性
     * - 更新工作流实例的变量池
     *
     * 后续任务激活：
     * - 根据依赖关系激活后续任务
     * - 检查后续任务的前置依赖是否已满足
     * - 发布工作流拓扑逻辑转换事件
     *
     * 特殊类型任务处理：
     * - 数据同步任务：处理数据同步结果和统计信息
     * - 脚本任务：处理脚本输出和退出码
     * - 子流程任务：处理子流程的执行结果
     *
     * @param workflowExecutionRunnable 工作流执行实例
     * @param taskExecutionRunnable 任务执行实例
     * @param taskSuccessEvent 任务成功事件，包含成功时间、输出变量等信息
     */
    void onSucceedEvent(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                        final ITaskExecutionRunnable taskExecutionRunnable,
                        final TaskSuccessLifecycleEvent taskSuccessEvent);

    /**
     * 处理任务故障转移事件
     *
     * 当任务处于特定状态时接收到任务故障转移生命周期事件时执行必要的动作。
     * 这个方法在任务需要进行故障转移时被调用。
     *
     * 处理逻辑：
     * 1. 状态验证：确认当前任务状态是否允许故障转移
     * 2. 转移类型判断：根据任务状态和故障原因选择转移策略
     * 3. 任务接管：尝试从失败的执行器接管任务执行
     * 4. 状态恢复：恢复任务的执行状态和上下文
     * 5. 重新分发：如果接管失败，则生成故障转移任务实例
     * 6. 状态转换：更新任务状态为需要故障容错状态
     *
     * 故障转移类型：
     * - 热备等待：对于正在运行的任务，尝试从原执行器接管
     * - 重新调度：对于已分发但未启动的任务，重新分发到其他执行器
     * - 状态恢复：恢复任务到之前的可执行状态
     * - 失败容错：如果接管失败，创建故障容错任务实例
     *
     * 接管流程：
     * 1. 连接原执行器，获取任务状态
     * 2. 检查任务是否还在运行
     * 3. 如果任务仍在运行，尝试接管控制权
     * 4. 如果接管成功，更新任务的执行器信息
     * 5. 如果接管失败，转入重新调度流程
     *
     * 故障容错任务创建：
     * - 复制原任务的配置和参数
     * - 设置任务类型为故障容错任务
     * - 标记原任务为需要故障容错状态
     * - 将新任务加入调度队列
     *
     * @param workflowExecutionRunnable 工作流执行实例
     * @param taskExecutionRunnable 任务执行实例
     * @param taskFailoverEvent 任务故障转移事件。包含故障原因和转移类型等信息
     */
    void onFailoverEvent(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                         final ITaskExecutionRunnable taskExecutionRunnable,
                         final TaskFailoverLifecycleEvent taskFailoverEvent);

    /**
     * 获取该动作匹配的任务执行状态
     *
     * 返回该状态动作实现对应的任务执行状态枚举值。
     * 这个方法用于状态动作工厂的注册和分发机制。
     *
     * 设计原则：
     * - 一对一映射：每个状态只对应一个动作实现
     * - 全覆盖：每个任务执行状态都必须有对应的动作实现
     * - 类型安全：使用枚举类型保证编译时类型安全
     * - 不可变：状态匹配关系在运行时不可变更
     *
     * 使用场景：
     * - 状态动作工厂初始化时注册状态映射关系
     * - 任务状态变更时查找对应的处理动作
     * - 系统启动时验证状态动作的完整性
     * - 调试和日志记录中显示状态信息
     *
     * 实现要求：
     * - 必须返回一个有效的TaskExecutionStatus枚举值
     * - 返回值必须与实际处理的状态一致
     * - 不允许返回null或无效状态
     * - 子类必须覆盖该方法并返回正确的状态
     *
     * @return 该动作对应的任务执行状态
     */
    TaskExecutionStatus matchState();
}
