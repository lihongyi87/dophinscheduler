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

package org.apache.dolphinscheduler.server.master.engine.graph;

import org.apache.dolphinscheduler.server.master.engine.task.runnable.ITaskExecutionRunnable;

import java.util.List;
import java.util.Set;

/**
 * 工作流执行图接口
 *
 * 表示运行时的实际DAG图，它可能是工作流DAG的一个子图。
 * 与IWorkflowGraph不同，执行图专注于运行时的状态管理和任务调度。
 *
 * 类比：如果说IWorkflowGraph是建筑图纸，那么IWorkflowExecutionGraph就是实际的施工现场管理系统，
 * 不仅要知道建筑结构，还要实时跟踪每个工序的施工状态、工人分配和进度管理。
 *
 * 核心职责：
 * 1. 动态图构建：支持运行时动态添加节点和边
 * 2. 任务状态管理：跟踪每个任务的执行状态（活跃、非活跃、失败、暂停、终止等）
 * 3. 执行条件判断：判断任务是否满足触发条件
 * 4. 状态传播：处理任务状态变化对后续任务的影响
 * 5. 链式状态管理：管理任务链的整体状态
 * 6. 任务跳过逻辑：处理条件任务和开关任务的分支跳过
 *
 * 状态类型：
 * - Active：任务正在被工作流处理中
 * - Inactive：任务已完成处理（成功或失败）
 * - Killed：任务被主动终止
 * - Failed：任务执行失败
 * - Paused：任务被暂停
 * - Skipped：任务被跳过执行
 * - Forbidden：任务被禁用
 * - Retrying：任务失败后正在重试
 *
 * 执行图特性：
 * - 动态性：支持运行时修改图结构
 * - 状态性：每个节点都有丰富的状态信息
 * - 条件性：支持条件分支和动态路径选择
 * - 容错性：支持任务重试和失败处理
 * - 监控性：提供详细的执行进度和状态查询
 *
 * 与静态图的区别：
 * - 静态图：设计时确定的DAG结构，只关心依赖关系
 * - 执行图：运行时的动态图，关心状态变化和执行流程
 *
 * 使用场景：
 * - 工作流引擎调度：确定下一步要执行的任务
 * - 状态监控：实时查看工作流执行状态
 * - 异常处理：处理任务失败、暂停、终止等异常情况
 * - 条件分支：根据执行结果选择不同的执行路径
 * - 进度统计：计算工作流整体执行进度
 *
 * @see WorkflowExecutionGraph
 */
public interface IWorkflowExecutionGraph {

    /**
     * 向图中添加新的任务节点
     *
     * 将可执行的任务对象添加到执行图中，作为图的一个节点。
     * 新添加的节点初始状态为未激活，需要后续通过addEdge方法建立依赖关系。
     *
     * 添加规则：
     * - 任务名称在图中必须唯一，不能重复添加
     * - 任务对象必须完整初始化，包含必要的元数据
     * - 添加后自动初始化该节点的前驱和后继关系容器
     *
     * @param taskExecutionRunnable 可执行的任务对象，包含任务定义和运行时信息
     *
     * 类比：在施工现场登记一个新的工序，分配工序编号和基本信息，
     * 但还没有确定与其他工序的先后关系。
     */
    void addNode(final ITaskExecutionRunnable taskExecutionRunnable);

    /**
     * 向图中添加新的边（依赖关系）
     *
     * 在指定的任务之间建立依赖关系，表示一个任务完成后可以触发其他任务。
     * 当前实现要求在所有任务节点添加完成后再调用此方法。
     *
     * 边的语义：
     * - fromTaskName：前驱任务，必须先完成的任务
     * - toTaskName：后继任务集合，依赖前驱任务的所有任务
     * - 建立的是一对多的依赖关系
     *
     * 调用时序：
     * 1. 首先调用addNode添加所有节点
     * 2. 然后调用addEdge建立节点间的依赖关系
     * 3. 最后进行图的完整性验证
     *
     * @param fromTaskName 前驱任务名称，必须在图中已存在
     * @param toTaskName 后继任务名称集合，所有任务都必须在图中已存在
     *
     * 类比：确定施工工序间的先后顺序，规定某个工序完成后
     * 可以开始哪些后续工序。
     */
    void addEdge(final String fromTaskName, final Set<String> toTaskName);

    /**
     * 获取执行图的起始任务
     *
     * 返回工作流执行图中的起始任务列表，这些任务的前驱任务为空。
     * 起始任务是工作流执行时首先被调度的任务。
     *
     * 与静态图的区别：
     * - 静态图返回任务名称，执行图返回可执行的任务对象
     * - 执行图的起始节点可能因为动态跳过而发生变化
     * - 包含任务的完整运行时状态信息
     *
     * @return 起始任务的可执行对象列表
     *
     * 类比：找出当前施工阶段可以立即开始的工序，
     * 包含具体的施工队伍和资源分配信息。
     */
    List<ITaskExecutionRunnable> getStartNodes();

    /**
     * 获取指定任务的前驱任务
     *
     * 返回指定任务的所有前驱任务对象，这些任务必须完成后
     * 指定任务才能开始执行。
     *
     * 返回对象特性：
     * - 包含完整的任务执行状态信息
     * - 可以直接查询前驱任务的执行结果
     * - 支持基于前驱任务状态的条件判断
     *
     * @param taskName 任务名称
     * @return 前驱任务的可执行对象列表
     *
     * 类比：查询某个工序需要等待哪些前置工序完成，
     * 并且能够实时了解这些前置工序的进度状态。
     */
    List<ITaskExecutionRunnable> getPredecessors(final String taskName);

    /**
     * 获取指定任务的后继任务（按任务名称）
     *
     * 返回依赖指定任务的所有后继任务对象。
     * 当指定任务完成后，这些后继任务的依赖条件会得到满足。
     *
     * 状态考虑：
     * - 会过滤掉被跳过、禁用的后继任务
     * - 返回当前活跃的后继任务
     * - 包含后继任务的完整状态信息
     *
     * @param taskName 任务名称
     * @return 后继任务的可执行对象列表
     */
    List<ITaskExecutionRunnable> getSuccessors(final String taskName);

    /**
     * 获取指定任务的后继任务（按任务对象）
     *
     * 返回依赖指定任务的所有后继任务对象。
     * 这是getSuccessors(String)的重载方法，提供基于任务对象的查询。
     *
     * 使用场景：
     * - 已有任务对象时，避免重复的名称查找
     * - 在任务执行过程中动态查询后继任务
     * - 性能优化：减少字符串比较的开销
     *
     * @param taskExecutionRunnable 任务的可执行对象
     * @return 后继任务的可执行对象列表
     */
    List<ITaskExecutionRunnable> getSuccessors(final ITaskExecutionRunnable taskExecutionRunnable);

    /**
     * 根据任务名称获取可执行任务对象
     *
     * 通过任务名称快速查找对应的可执行任务对象。
     * 这是执行图中最常用的查询方法之一。
     *
     * 查询特性：
     * - 基于任务名称的哈希查找，时间复杂度O(1)
     * - 返回包含完整运行时状态的任务对象
     * - 如果任务不存在通常返回null
     *
     * @param taskName 任务名称
     * @return 对应的可执行任务对象，如果不存在则返回null
     */
    ITaskExecutionRunnable getTaskExecutionRunnableByName(final String taskName);

    /**
     * 根据任务实例ID获取可执行任务对象
     *
     * 通过任务实例的唯一ID查找对应的可执行任务对象。
     * 任务实例ID是数据库中任务实例记录的主键。
     *
     * 使用场景：
     * - 从外部系统通过任务实例ID查询任务状态
     * - 处理任务实例相关的回调和通知
     * - 数据库查询结果到执行图对象的映射
     *
     * @param taskInstanceId 任务实例ID，数据库主键
     * @return 对应的可执行任务对象，如果不存在则返回null
     */
    ITaskExecutionRunnable getTaskExecutionRunnableById(final Integer taskInstanceId);

    /**
     * 根据任务编码获取可执行任务对象
     *
     * 通过任务定义的唯一编码查找对应的可执行任务对象。
     * 任务编码是任务定义的业务标识符。
     *
     * 应用场景：
     * - 工作流模板和实例之间的映射
     * - 跨工作流实例的任务引用
     * - 任务定义到执行实例的转换
     *
     * @param taskCode 任务编码，任务定义的唯一标识
     * @return 对应的可执行任务对象，如果不存在则返回null
     */
    ITaskExecutionRunnable getTaskExecutionRunnableByTaskCode(final Long taskCode);

    /**
     * 判断指定任务是否处于活跃状态
     *
     * 活跃状态表示任务正在被工作流处理中，包括正在执行、等待资源、排队等状态。
     * 这是任务生命周期管理的重要状态判断。
     *
     * 活跃状态的含义：
     * - 任务已被工作流引擎接管处理
     * - 任务可能正在执行、等待或准备执行
     * - 工作流引擎会持续监控和管理该任务
     * - 任务的状态变化会影响后续任务的调度
     *
     * @param taskExecutionRunnable 要检查的任务对象
     * @return true表示任务处于活跃状态，false表示任务未被激活
     *
     * 类比：检查某个工序是否已经开工，正在施工现场进行中。
     */
    boolean isTaskExecutionRunnableActive(final ITaskExecutionRunnable taskExecutionRunnable);

    /**
     * 判断指定任务是否处于非活跃状态
     *
     * 非活跃状态表示任务已经被执行完成（不论成功或失败）。
     * 这是活跃状态的对立面，表示任务已完成其生命周期。
     *
     * 非活跃状态的含义：
     * - 任务已经执行完成，不再需要工作流引擎处理
     * - 任务已产生明确的执行结果（成功、失败、终止等）
     * - 任务的执行结果可以被后续任务作为依赖条件使用
     * - 工作流引擎不再对该任务进行状态监控
     *
     * 状态转换：
     * - 从活跃状态转为非活跃状态是不可逆的
     * - 除非任务需要重试，否则不会重新变为活跃状态
     *
     * @param taskExecutionRunnable 要检查的任务对象
     * @return true表示任务已完成执行，false表示任务仍在处理中
     *
     * 类比：检查某个工序是否已经完工，不再需要现场管理。
     */
    boolean isTaskExecutionRunnableInActive(final ITaskExecutionRunnable taskExecutionRunnable);

    /**
     * 判断指定任务是否被终止
     *
     * 终止状态表示任务被主动停止执行，通常是由于用户操作或系统异常。
     * 这是一种特殊的完成状态，不同于正常的成功或失败。
     *
     * 终止状态的特征：
     * - 任务被强制停止，不是自然完成
     * - 通常由用户手动操作或系统自动触发
     * - 终止的任务通常不会触发后续任务执行
     * - 可能需要清理已分配的资源
     *
     * 触发场景：
     * - 用户手动停止工作流或任务
     * - 系统资源不足时的自动清理
     * - 超时或异常情况下的强制终止
     * - 工作流被取消时的级联终止
     *
     * @param taskExecutionRunnable 要检查的任务对象
     * @return true表示任务被终止，false表示任务未被终止
     *
     * 类比：检查某个工序是否被强制停工，比如因为安全问题或资源调配。
     */
    boolean isTaskExecutionRunnableKilled(final ITaskExecutionRunnable taskExecutionRunnable);

    /**
     * Whether the given task is failure.
     */
    boolean isTaskExecutionRunnableFailed(final ITaskExecutionRunnable taskExecutionRunnable);

    /**
     * Whether the given task is paused.
     */
    boolean isTaskExecutionRunnablePaused(final ITaskExecutionRunnable taskExecutionRunnable);

    /**
     * Get the active TaskExecutionRunnable list.
     * <p> The active TaskExecutionRunnable means the task is handling in the workflow execution graph.
     */
    List<ITaskExecutionRunnable> getActiveTaskExecutionRunnable();

    /**
     * Get all the TaskExecutionRunnable in the graph, this method will return all the TaskExecutionRunnable in the graph,
     * include active and inactive TaskExecutionRunnable.
     */
    List<ITaskExecutionRunnable> getAllTaskExecutionRunnable();

    /**
     * Check whether the given task can be trigger now.
     * <p> The task can be trigger only all the predecessors are finished and all predecessors are not failure/pause/kill.
     * <p> Once the task has been triggered, then will also return false.
     */
    boolean isTriggerConditionMet(final ITaskExecutionRunnable taskExecutionRunnable);

    /**
     * Mark the TaskExecutionRunnable is active.
     * <p> If the TaskExecutionRunnable is active means the task is handling by the workflow.
     * <p> Once we begin to handle a task, we should mark the TaskExecutionRunnable active.
     */
    void markTaskExecutionRunnableActive(final ITaskExecutionRunnable taskExecutionRunnable);

    /**
     * Mark the TaskExecutionRunnable is inactive.
     * <p> If the TaskExecutionRunnable is inactive means the task has not been handled by the workflow.
     * <p> Once we finish to handle a task, we should mark the TaskExecutionRunnable inactive.
     */
    void markTaskExecutionRunnableInActive(final ITaskExecutionRunnable taskExecutionRunnable);

    /**
     * Mark the TaskExecutionRunnable is skipped.
     * <p> Once the TaskExecutionRunnable is marked as skipped, this means the task will not be trigger.
     */
    void markTaskSkipped(final ITaskExecutionRunnable taskExecutionRunnable);

    /**
     * Mark the Task is skipped.
     * <p> Once the Task is marked as skipped, this means the task will not be trigger.
     */
    void markTaskSkipped(final String taskName);

    /**
     * Mark the TaskExecutionRunnable chain is failure.
     * <p> Once the TaskExecutionRunnable chain is failure, then the successors will not be trigger, and the workflow execution graph might be failure.
     */
    void markTaskExecutionRunnableChainFailure(final ITaskExecutionRunnable taskExecutionRunnable);

    /**
     * Mark the TaskExecutionRunnable chain is pause.
     * <p> Once the TaskExecutionRunnable chain is pause, then the successors will not be trigger, and the workflow execution graph might be paused.
     */
    void markTaskExecutionRunnableChainPause(final ITaskExecutionRunnable taskExecutionRunnable);

    /**
     * Mark the TaskExecutionRunnable chain is kill.
     * <p> Once the TaskExecutionRunnable chain is kill, then the successors will not be trigger, and the workflow execution graph might be stop.
     */
    void markTaskExecutionRunnableChainKill(final ITaskExecutionRunnable taskExecutionRunnable);

    /**
     * Whether all the TaskExecutionRunnable chain in the graph is finish.
     */
    boolean isAllTaskExecutionRunnableChainFinish();

    /**
     * Whether all the TaskExecutionRunnable chain in the graph is finish with success.
     */
    boolean isAllTaskExecutionRunnableChainSuccess();

    /**
     * Whether there exist the TaskExecutionRunnable chain in the graph is finish with failure.
     */
    boolean isExistFailureTaskExecutionRunnableChain();

    /**
     * Whether there exist the TaskExecutionRunnable chain in the graph is finish with paused.
     */
    boolean isExistPausedTaskExecutionRunnableChain();

    /**
     * Whether there exist the TaskExecutionRunnable chain in the graph is finish with kill.
     */
    boolean isExistKilledTaskExecutionRunnableChain();

    /**
     * Check whether the given task is the end of the task chain.
     * <p> If the given task has no successor, then it is the end of the task chain.
     * <p> If the given task is killed or paused, then it is the end of the task chain.
     * <p> If the given task is failure, and all its successors are condition task then it is not end of a task chain.
     */
    boolean isEndOfTaskChain(final ITaskExecutionRunnable taskExecutionRunnable);

    /**
     * Whether the given task is skipped.
     * <p> Once we mark the task is skipped, then the task will not be trigger, and will trigger its successors.
     */
    boolean isTaskExecutionRunnableSkipped(final ITaskExecutionRunnable taskExecutionRunnable);

    /**
     * Whether the given task is forbidden.
     * <p> Once the task is forbidden then it will be passed, and will trigger its successors.
     */
    boolean isTaskExecutionRunnableForbidden(final ITaskExecutionRunnable taskExecutionRunnable);

    /**
     * Whether the given task's execution is failure and waiting for retry.
     */
    boolean isTaskExecutionRunnableRetrying(final ITaskExecutionRunnable taskExecutionRunnable);

    /**
     * Whether all predecessors task is skipped.
     * <p> Once all predecessors are marked as skipped, then the task will be marked as skipped, and will trigger its successors.
     */
    boolean isAllPredecessorsSkipped(final ITaskExecutionRunnable taskExecutionRunnable);

    /**
     * Whether all predecessors task are condition task.
     */
    boolean isAllSuccessorsAreConditionTask(final ITaskExecutionRunnable taskExecutionRunnable);
}
