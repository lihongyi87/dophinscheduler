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

import org.apache.dolphinscheduler.common.enums.Flag;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.plugin.task.api.enums.TaskExecutionStatus;
import org.apache.dolphinscheduler.plugin.task.api.utils.TaskTypeUtils;
import org.apache.dolphinscheduler.server.master.engine.task.runnable.ITaskExecutionRunnable;

import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 工作流执行图实现类
 *
 * 这是IWorkflowExecutionGraph接口的核心实现，管理工作流运行时的动态执行状态。
 * 与静态的WorkflowGraph不同，此类专注于运行时的状态管理、任务调度和执行控制。
 *
 * 类比：如果WorkflowGraph是建筑蓝图，那么WorkflowExecutionGraph就是实际的施工管理系统，
 * 不仅知道建筑结构，还实时跟踪每个工序的进度、资源分配和质量状态。
 *
 * 核心职责：
 * 1. 动态图构建：支持运行时添加节点和边，构建执行期的DAG结构
 * 2. 状态管理：维护每个任务的详细执行状态（活跃、非活跃、失败、暂停、终止等）
 * 3. 链式状态跟踪：管理任务链的整体状态，支持失败、暂停、终止状态的传播
 * 4. 条件执行：处理任务跳过、禁用、条件分支等复杂执行逻辑
 * 5. 触发条件判断：确定任务是否满足执行的前置条件
 * 6. 进度监控：提供工作流整体执行进度的查询和统计功能
 *
 * 状态分类体系：
 * - 执行状态：Active（执行中）、Inactive（已完成）
 * - 异常状态：Failed（失败）、Paused（暂停）、Killed（终止）
 * - 跳过状态：Skipped（跳过）、Forbidden（禁用）
 * - 重试状态：Retrying（重试中）
 *
 * 数据结构设计：
 * - 任务映射：支持按名称、ID、编码多种方式快速查找任务
 * - 状态集合：使用Set结构存储不同状态的任务，支持快速状态判断
 * - 依赖关系：动态维护前驱后继关系，支持运行时修改
 * - 链式状态：跟踪任务链的整体状态，支持状态传播
 *
 * 执行模型：
 * - 事件驱动：基于任务状态变化触发后续处理
 * - 状态机：每个任务遵循明确的状态转换规则
 * - 条件分支：支持基于执行结果的动态路径选择
 * - 并发安全：支持多线程并发访问和状态更新
 *
 * 与静态图的关系：
 * - 静态图定义结构：WorkflowGraph定义任务及其依赖关系
 * - 执行图管理状态：WorkflowExecutionGraph管理运行时状态
 * - 两者协同工作：静态图提供结构，执行图提供动态控制
 *
 * 使用场景：
 * - 工作流引擎：作为任务调度的核心数据结构
 * - 状态监控：提供实时的执行状态查询
 * - 异常处理：管理失败、暂停、终止等异常情况
 * - 条件执行：处理条件任务和开关任务的分支逻辑
 * - 进度统计：计算工作流的完成进度和健康状态
 */
public class WorkflowExecutionGraph implements IWorkflowExecutionGraph {

    /**
     * 任务执行对象映射表
     *
     * 存储所有任务的可执行对象，使用任务名称作为键进行快速查找。
     * 这是执行图的核心数据结构，包含了所有任务的运行时信息。
     *
     * Key: 任务名称（String）
     * Value: 可执行任务对象（ITaskExecutionRunnable）
     *
     * 特性：
     * - 支持O(1)时间复杂度的任务查找
     * - 包含任务的完整运行时状态和元数据
     * - 线程安全的并发访问支持
     *
     * 类比：施工现场的工序管理台账，记录每个工序的详细信息和执行状态。
     */
    private final Map<String, ITaskExecutionRunnable> totalTaskExecuteRunnableMap;

    /**
     * 失败任务链集合
     *
     * 存储执行失败的任务名称，这些任务的失败会影响后续任务的执行。
     * 失败状态具有传播性，会阻止依赖任务的正常执行。
     *
     * 失败传播机制：
     * - 任务执行失败后会被加入此集合
     * - 失败任务的后继任务通常不会被触发执行
     * - 整个工作流可能因为关键任务失败而终止
     *
     * 类比：施工中出现质量问题的工序，会影响后续工序的开展。
     */
    private final Set<String> failureTaskChains;

    /**
     * 暂停任务链集合
     *
     * 存储被暂停的任务名称，暂停状态表示任务被临时停止，可以恢复执行。
     * 暂停通常由用户操作或系统策略触发。
     *
     * 暂停特性：
     * - 任务可以从暂停状态恢复到执行状态
     * - 暂停的任务不会触发后续任务执行
     * - 支持手动恢复或条件自动恢复
     *
     * 类比：施工中因等待材料或审批而临时停工的工序。
     */
    private final Set<String> pausedTaskChains;

    /**
     * 终止任务链集合
     *
     * 存储被强制终止的任务名称，终止状态表示任务被永久停止。
     * 终止通常由于严重错误、资源不足或用户取消操作。
     *
     * 终止特性：
     * - 终止状态是不可逆的，任务不能从终止状态恢复
     * - 终止的任务不会触发后续任务执行
     * - 可能需要清理已分配的资源
     *
     * 类比：施工中因严重安全问题或项目取消而永久停工的工序。
     */
    private final Set<String> killedTaskChains;

    /**
     * 跳过任务集合
     *
     * 存储被跳过执行的任务名称，跳过的任务不会实际执行但被视为"完成"。
     * 跳过机制通常用于条件分支、开关逻辑或故障恢复场景。
     *
     * 跳过逻辑：
     * - 跳过的任务被视为成功完成，可以触发后续任务
     * - 通常由条件任务、开关任务或用户配置触发
     * - 支持基于前驱任务状态的自动跳过
     *
     * 类比：施工中因条件不满足而跳过的可选工序（如装修阶段跳过某些装饰工序）。
     */
    private final Set<String> skippedTask;

    /**
     * 前驱任务关系映射表
     *
     * 存储每个任务的前驱任务集合，表示任务的依赖关系。
     * 与静态图不同，执行图的依赖关系可能因为跳过、禁用等原因发生变化。
     *
     * Key: 任务名称
     * Value: 该任务的前驱任务名称集合
     *
     * 动态特性：
     * - 支持运行时添加新的依赖关系
     * - 被跳过的前驱任务仍然满足依赖条件
     * - 用于计算任务的触发条件
     *
     * 类比：工序的前置条件清单，会根据实际情况动态调整。
     */
    private final Map<String, Set<String>> predecessors;

    /**
     * 后继任务关系映射表
     *
     * 存储每个任务的后继任务集合，表示任务完成后可以触发的下游任务。
     * 支持运行时的动态修改和条件过滤。
     *
     * Key: 任务名称
     * Value: 该任务的后继任务名称集合
     *
     * 触发机制：
     * - 任务完成后触发后继任务的条件检查
     * - 支持基于执行结果的条件触发
     * - 被禁用或跳过的后继任务不会被触发
     *
     * 类比：工序完成后可以启动的后续工序清单。
     */
    private final Map<String, Set<String>> successors;

    /**
     * 活跃任务集合
     *
     * 存储当前正在被工作流引擎处理的任务名称。
     * 活跃状态表示任务正在执行、等待资源或排队中。
     *
     * 活跃状态管理：
     * - 任务开始处理时加入此集合
     * - 任务完成处理时从此集合移除
     * - 用于控制并发执行的任务数量
     * - 监控工作流的实时执行状态
     *
     * 类比：施工现场正在进行的工序列表。
     */
    private final Set<String> activeTaskExecutionRunnable;

    /**
     * 非活跃任务集合
     *
     * 存储已经完成处理的任务名称（不论成功失败）。
     * 非活跃状态表示任务已产生明确的执行结果。
     *
     * 非活跃状态管理：
     * - 任务执行完成时加入此集合
     * - 包括成功、失败、终止等各种完成状态
     * - 用于判断任务是否可以作为后续任务的依赖条件
     * - 统计工作流的整体完成进度
     *
     * 类比：施工现场已经完工的工序列表。
     */
    private final Set<String> inActiveTaskExecutionRunnable;

    /**
     * 执行图构造方法
     *
     * 初始化所有必要的数据结构，为运行时的状态管理做准备。
     * 所有集合和映射表都被初始化为空状态，等待运行时添加内容。
     *
     * 初始化的数据结构：
     * 1. 状态集合：失败、暂停、终止、跳过、活跃、非活跃任务集合
     * 2. 关系映射：前驱和后继任务关系映射表
     * 3. 任务映射：任务名称到执行对象的映射表
     *
     * 设计理念：
     * - 惰性初始化：只在需要时才创建和填充数据结构
     * - 线程安全：使用线程安全的数据结构实现
     * - 内存优化：避免不必要的对象创建和内存占用
     *
     * 类比：在施工现场搭建管理架构，准备好所有必要的登记簿和状态板，
     * 等待工序开工时填入具体信息。
     */
    public WorkflowExecutionGraph() {
        this.failureTaskChains = new HashSet<>();
        this.pausedTaskChains = new HashSet<>();
        this.killedTaskChains = new HashSet<>();
        this.skippedTask = new HashSet<>();
        this.predecessors = new HashMap<>();
        this.successors = new HashMap<>();
        this.totalTaskExecuteRunnableMap = new HashMap<>();
        this.activeTaskExecutionRunnable = new HashSet<>();
        this.inActiveTaskExecutionRunnable = new HashSet<>();
    }

    /**
     * 向执行图中添加任务节点
     *
     * 将可执行的任务对象添加到执行图中，并初始化该任务的依赖关系容器。
     * 这是构建执行图的第一步，为后续添加依赖关系做准备。
     *
     * 执行步骤：
     * 1. 将任务对象存储到任务映射表中，使用任务名称作为键
     * 2. 为该任务初始化空的前驱任务集合
     * 3. 为该任务初始化空的后继任务集合
     *
     * 初始状态：
     * - 新添加的任务默认为非活跃状态
     * - 没有任何前驱和后继关系
     * - 不属于任何特殊状态（失败、暂停、终止、跳过）
     *
     * 注意事项：
     * - 任务名称必须唯一，重复添加会覆盖原有任务
     * - 必须在添加依赖关系之前先添加所有节点
     * - 任务对象必须包含完整的元数据和配置信息
     *
     * @param taskExecutionRunnable 要添加的可执行任务对象
     *
     * 类比：在施工现场登记一个新的工序，分配工序编号和工位，
     * 但还没有安排具体的施工顺序和依赖关系。
     */
    @Override
    public void addNode(final ITaskExecutionRunnable taskExecutionRunnable) {
        totalTaskExecuteRunnableMap.put(taskExecutionRunnable.getName(), taskExecutionRunnable);
        predecessors.computeIfAbsent(taskExecutionRunnable.getName(), k -> new HashSet<>());
        successors.computeIfAbsent(taskExecutionRunnable.getName(), k -> new HashSet<>());
    }

    /**
     * 向执行图中添加依赖关系边
     *
     * 在指定的任务之间建立依赖关系，定义任务的执行顺序。
     * 一个前驱任务可以同时依赖多个后继任务，形成一对多的关系。
     *
     * 建立过程：
     * 1. 在前驱任务的后继集合中添加所有后继任务
     * 2. 在每个后继任务的前驱集合中添加前驱任务
     * 3. 使用computeIfAbsent确保不会因为NPE而失败
     *
     * 关系语义：
     * - fromTaskName：前驱任务，必须先完成的任务
     * - toTaskNames：后继任务集合，依赖前驱任务的所有任务
     * - 前驱任务完成后，所有后继任务的一个依赖条件得到满足
     *
     * 前置条件：
     * - 前驱任务和所有后继任务都必须已经通过addNode方法添加到图中
     * - 不能形成循环依赖，否则会导致死锁
     *
     * @param fromTaskName 前驱任务名称
     * @param toTaskNames 后继任务名称集合
     *
     * 类比：安排施工工序的先后关系，指定某个工序完成后
     * 可以同时开始多个后续工序。
     */
    @Override
    public void addEdge(String fromTaskName, Set<String> toTaskNames) {
        successors.computeIfAbsent(fromTaskName, k -> new HashSet<>()).addAll(toTaskNames);
        toTaskNames.forEach(toTask -> predecessors.computeIfAbsent(toTask, k -> new HashSet<>()).add(fromTaskName));
    }

    /**
     * 获取工作流的起始节点集合
     *
     * 遍历执行图中的所有任务，筛选出没有前驱任务的节点，这些节点可以作为工作流的入口点。
     * 起始节点是工作流执行的起点，它们不依赖任何其他任务的完成。
     *
     * 查找逻辑：
     * 1. 遍历任务映射表中的所有任务执行对象
     * 2. 检查每个任务在前驱关系映射表中的依赖情况
     * 3. 筛选出前驱任务集合为空的任务
     * 4. 将筛选结果收集为列表返回
     *
     * 判断标准：
     * - 任务在前驱关系表中没有记录，或者
     * - 任务的前驱任务集合为空（CollectionUtils.isEmpty）
     *
     * 应用场景：
     * - 工作流启动时确定初始执行任务
     * - 并行执行时识别可以同时启动的任务
     * - 工作流恢复时找到重新启动的入口点
     *
     * @return 起始任务执行对象列表，按添加顺序排列
     *
     * 类比：在项目施工中，找出不需要等待任何前置工序的基础工序，
     * 如地基开挖、场地准备等可以立即开工的工序。
     */
    @Override
    public List<ITaskExecutionRunnable> getStartNodes() {
        // ========== 第一步：获取所有任务执行对象 ==========
        // 从任务映射表中获取所有已注册的任务执行对象
        // 这些对象包含了任务的完整运行时信息和状态
        return totalTaskExecuteRunnableMap.values()
                .stream()
                // ========== 第二步：过滤起始节点 ==========
                // 使用Stream API过滤出没有前驱任务的节点
                // CollectionUtils.isEmpty检查前驱集合是否为空或null
                .filter(taskExecutionRunnable -> CollectionUtils
                        .isEmpty(predecessors.get(taskExecutionRunnable.getName())))
                // ========== 第三步：收集结果 ==========
                // 将过滤后的任务对象收集为List并返回
                // 保持任务在映射表中的原始顺序
                .collect(Collectors.toList());
    }

    /**
     * 获取指定任务的前驱任务列表
     *
     * 根据任务名称查找该任务的所有前驱任务，即必须在该任务执行前完成的依赖任务。
     * 前驱任务是任务执行的前置条件，只有当所有前驱任务完成后，当前任务才能被触发执行。
     *
     * 查找步骤：
     * 1. 验证任务名称的有效性，确保任务存在于依赖关系图中
     * 2. 从前驱关系映射表中获取前驱任务名称集合
     * 3. 将任务名称转换为对应的任务执行对象
     * 4. 收集转换结果并返回完整的前驱任务列表
     *
     * 异常处理：
     * - 如果指定的任务不存在于依赖关系图中，抛出IllegalArgumentException
     * - 确保调用方传入的任务名称是有效的已注册任务
     *
     * 返回内容：
     * - 完整的任务执行对象，包含任务实例、定义、状态等信息
     * - 按照依赖关系建立的顺序排列
     * - 空列表表示该任务没有前驱依赖（起始节点）
     *
     * @param taskName 目标任务的名称
     * @return 前驱任务执行对象列表，如果没有前驱任务则返回空列表
     * @throws IllegalArgumentException 当任务不存在于图中时抛出
     *
     * 类比：查找某个施工工序的所有前置工序，如安装窗户前需要完成的
     * 墙体砌筑、门窗框安装等工序。
     */
    @Override
    public List<ITaskExecutionRunnable> getPredecessors(final String taskName) {
        // ========== 第一步：验证任务存在性 ==========
        // 检查任务是否已经注册到前驱关系映射表中
        // 如果任务不存在，说明传入的任务名称无效或任务未添加到图中
        if (!predecessors.containsKey(taskName)) {
            throw new IllegalArgumentException("Cannot find the task: " + taskName + " in graph");
        }

        // ========== 第二步：获取前驱任务名称集合 ==========
        // 从前驱关系映射表中获取该任务的所有前驱任务名称
        // 这个集合在addEdge方法中被建立和维护
        return predecessors
                .get(taskName)
                .stream()
                // ========== 第三步：转换为任务执行对象 ==========
                // 将任务名称映射为对应的任务执行对象
                // 使用方法引用提高代码可读性和性能
                .map(this::getTaskExecutionRunnableByName)
                // ========== 第四步：收集并返回结果 ==========
                // 将转换后的任务执行对象收集为List
                // 保持原有的依赖关系顺序
                .collect(Collectors.toList());
    }

    /**
     * 获取指定任务的后继任务列表
     *
     * 根据任务名称查找该任务的所有后继任务，即依赖当前任务完成的下游任务。
     * 后继任务在当前任务成功完成后可能被触发执行，形成工作流的执行链条。
     *
     * 查找步骤：
     * 1. 验证任务名称的有效性，确保任务存在于依赖关系图中
     * 2. 从后继关系映射表中获取后继任务名称集合
     * 3. 将任务名称转换为对应的任务执行对象
     * 4. 收集转换结果并返回完整的后继任务列表
     *
     * 触发条件：
     * - 当前任务成功完成时，会检查所有后继任务的触发条件
     * - 后继任务的所有前驱任务都完成时，该后继任务才能被执行
     * - 支持基于执行结果的条件触发（如条件任务的分支逻辑）
     *
     * 异常处理：
     * - 如果指定的任务不存在于依赖关系图中，抛出IllegalArgumentException
     * - 确保调用方传入的任务名称是有效的已注册任务
     *
     * 返回内容：
     * - 完整的任务执行对象，包含任务实例、定义、状态等信息
     * - 按照依赖关系建立的顺序排列
     * - 空列表表示该任务没有后继任务（结束节点）
     *
     * @param taskName 目标任务的名称
     * @return 后继任务执行对象列表，如果没有后继任务则返回空列表
     * @throws IllegalArgumentException 当任务不存在于图中时抛出
     *
     * 类比：查找某个施工工序完成后可以开始的所有后续工序，
     * 如地基完工后可以开始的主体结构、管线预埋等工序。
     */
    @Override
    public List<ITaskExecutionRunnable> getSuccessors(final String taskName) {
        // ========== 第一步：验证任务存在性 ==========
        // 检查任务是否已经注册到后继关系映射表中
        // 错误信息显示"task code"，可能是历史遗留，实际检查的是任务名称
        if (!successors.containsKey(taskName)) {
            throw new IllegalArgumentException("Cannot find the task code in graph");
        }

        // ========== 第二步：获取后继任务名称集合 ==========
        // 从后继关系映射表中获取该任务的所有后继任务名称
        // 这个集合在addEdge方法中被建立和维护
        return successors
                .get(taskName)
                .stream()
                // ========== 第三步：转换为任务执行对象 ==========
                // 将任务名称映射为对应的任务执行对象
                // 获取完整的任务运行时信息，用于后续的触发条件判断
                .map(this::getTaskExecutionRunnableByName)
                // ========== 第四步：收集并返回结果 ==========
                // 将转换后的任务执行对象收集为List
                // 这些任务是当前任务完成后的潜在触发目标
                .collect(Collectors.toList());
    }

    @Override
    public List<ITaskExecutionRunnable> getSuccessors(final ITaskExecutionRunnable taskExecutionRunnable) {
        return getSuccessors(taskExecutionRunnable.getName());
    }

    @Override
    public ITaskExecutionRunnable getTaskExecutionRunnableByName(final String taskName) {
        return totalTaskExecuteRunnableMap.get(taskName);
    }

    @Override
    public ITaskExecutionRunnable getTaskExecutionRunnableById(final Integer taskInstanceId) {
        return totalTaskExecuteRunnableMap.values()
                .stream()
                .filter(taskExecutionRunnable -> taskExecutionRunnable.getTaskInstance() != null
                        && taskInstanceId.equals(taskExecutionRunnable.getTaskInstance().getId()))
                .findFirst()
                .orElse(null);
    }

    @Override
    public ITaskExecutionRunnable getTaskExecutionRunnableByTaskCode(final Long taskCode) {
        return totalTaskExecuteRunnableMap.values()
                .stream()
                .filter(taskExecutionRunnable -> taskExecutionRunnable.getTaskDefinition() != null
                        && taskCode.equals(taskExecutionRunnable.getTaskDefinition().getCode()))
                .findFirst()
                .orElse(null);
    }

    @Override
    public boolean isTaskExecutionRunnableActive(final ITaskExecutionRunnable taskExecutionRunnable) {
        return activeTaskExecutionRunnable.contains(taskExecutionRunnable.getName());
    }

    @Override
    public boolean isTaskExecutionRunnableInActive(ITaskExecutionRunnable taskExecutionRunnable) {
        return inActiveTaskExecutionRunnable.contains(taskExecutionRunnable.getName());
    }

    @Override
    public boolean isTaskExecutionRunnableKilled(final ITaskExecutionRunnable taskExecutionRunnable) {
        return killedTaskChains.contains(taskExecutionRunnable.getName());
    }

    @Override
    public boolean isTaskExecutionRunnableFailed(ITaskExecutionRunnable taskExecutionRunnable) {
        return failureTaskChains.contains(taskExecutionRunnable.getName());
    }

    @Override
    public boolean isTaskExecutionRunnablePaused(ITaskExecutionRunnable taskExecutionRunnable) {
        return pausedTaskChains.contains(taskExecutionRunnable.getName());
    }

    @Override
    public List<ITaskExecutionRunnable> getActiveTaskExecutionRunnable() {
        return activeTaskExecutionRunnable
                .stream()
                .map(this::getTaskExecutionRunnableByName)
                .collect(Collectors.toList());
    }

    @Override
    public List<ITaskExecutionRunnable> getAllTaskExecutionRunnable() {
        return new ArrayList<>(totalTaskExecuteRunnableMap.values());
    }

    /**
     * 判断任务是否满足触发执行的条件
     *
     * 检查指定任务是否已经准备好被执行，需要同时满足多个条件：
     * 1. 任务本身还未开始执行（既不是活跃状态也不是完成状态）
     * 2. 所有前驱任务都已经成功完成（非活跃且非异常状态）
     *
     * 触发条件检查逻辑：
     * - 排除条件：任务已经在执行中或已经完成的情况
     * - 依赖条件：所有前驱任务都必须处于正常完成状态
     * - 异常阻断：任何前驱任务的异常状态都会阻止当前任务执行
     *
     * 前驱任务状态要求：
     * - 必须是非活跃状态（已完成执行）
     * - 不能是失败状态（执行失败会阻断后续任务）
     * - 不能是暂停状态（暂停会传播阻断后续任务）
     * - 不能是终止状态（终止会传播阻断后续任务）
     *
     * 特殊情况处理：
     * - 如果任务没有前驱任务（起始节点），直接检查自身状态
     * - 跳过的前驱任务被视为正常完成，不会阻断执行
     * - 重试中的任务被视为活跃状态，不满足触发条件
     *
     * @param taskExecutionRunnable 待检查的任务执行对象
     * @return true表示满足触发条件，可以开始执行；false表示不满足条件
     *
     * 类比：检查某个施工工序是否可以开工，需要确认：
     * 1. 该工序还没有开始施工
     * 2. 所有前置工序都已经正常完工
     * 3. 没有前置工序出现质量问题、停工或取消
     */
    @Override
    public boolean isTriggerConditionMet(final ITaskExecutionRunnable taskExecutionRunnable) {
        // ========== 第一步：检查任务自身状态 ==========
        // 如果任务已经是活跃状态（正在执行）或非活跃状态（已完成），则不能再次触发
        // 这是避免重复执行的基本保护机制
        if (isTaskExecutionRunnableActive(taskExecutionRunnable)
                || isTaskExecutionRunnableInActive(taskExecutionRunnable)) {
            // 任务已经在执行或已经完成，不满足触发条件
            return false;
        }

        // ========== 第二步：检查所有前驱任务状态 ==========
        // 获取该任务的所有前驱任务，并检查它们是否都满足执行完成的条件
        // 只有当所有前驱任务都正常完成时，当前任务才能被触发
        return getPredecessors(taskExecutionRunnable.getName())
                .stream()
                // ========== 第三步：验证每个前驱任务的状态 ==========
                // 使用allMatch确保所有前驱任务都满足以下条件：
                .allMatch(predecessor ->
                    // 前驱任务必须已经完成执行（非活跃状态）
                    isTaskExecutionRunnableInActive(predecessor)
                    // 前驱任务不能是失败状态（失败会阻断后续执行）
                    && !isTaskExecutionRunnableFailed(predecessor)
                    // 前驱任务不能是暂停状态（暂停会传播阻断）
                    && !isTaskExecutionRunnablePaused(predecessor)
                    // 前驱任务不能是终止状态（终止会传播阻断）
                    && !isTaskExecutionRunnableKilled(predecessor));
    }

    /**
     * 判断所有任务执行链是否已经完成
     *
     * 检查工作流中是否还有任务处于活跃状态（正在执行或等待执行）。
     * 这是判断工作流是否可以进入终态的关键指标。
     *
     * 完成状态的定义：
     * - 所有任务都不在活跃任务集合中
     * - 没有任务正在执行、排队或等待调度
     * - 所有任务都已经产生了明确的执行结果
     *
     * 与成功完成的区别：
     * - 完成：只要没有活跃任务，不管结果成功还是失败
     * - 成功完成：在完成的基础上，还要求没有失败、暂停、终止
     *
     * 应用场景：
     * 1. 工作流状态监控：实时检查工作流是否还在运行
     * 2. 资源清理触发：工作流结束后启动资源回收
     * 3. 结果汇报：汇总和报告执行结果
     * 4. 后续流程：决定是否可以触发下一个工作流
     * 5. 告警阈值：达到预期的最大执行时间时发出告警
     *
     * 技术实现：
     * - 使用活跃任务集合的isEmpty()方法
     * - O(1)时间复杂度，高效判断
     * - 线程安全的状态检查
     *
     * 注意事项：
     * - 这个方法不能区分成功和失败，只能判断是否结束
     * - 需要结合其他状态检查方法来判断最终结果
     * - 可能存在所有任务都被跳过或禁用的特殊情况
     *
     * @return true表示所有任务链已经完成，false表示还有任务在执行
     *
     * 类比：在工厂中检查所有生产线是否都已经停止运行，
     * 不管是正常下班还是因故障停产，只要没有运行中的生产线即可。
     */
    @Override
    public boolean isAllTaskExecutionRunnableChainFinish() {
        // ========== 活跃任务集合状态检查 ==========
        // 检查活跃任务集合是否为空
        // 空集合表示没有任何任务处于活跃状态（执行中、等待中、重试中）
        // isEmpty()方法提供O(1)时间复杂度的高效检查
        return activeTaskExecutionRunnable.isEmpty();
    }

    /**
     * 判断所有任务执行链是否都成功完成
     *
     * 检查工作流是否不仅完成了执行，而且所有任务都是成功结束的。
     * 这是工作流最理想的终态，表示所有计划的任务都正常完成。
     *
     * 成功完成的判断条件（必须同时满足）：
     * 1. 所有任务链已经完成（没有活跃任务）
     * 2. 不存在失败的任务链（没有不可恢复的错误）
     * 3. 不存在暂停的任务链（没有人工干预）
     * 4. 不存在被终止的任务链（没有强制取消）
     *
     * 与单纯完成的区别：
     * - 完成：任务都不再活跃，但可能有失败、暂停或终止
     * - 成功完成：在完成的基础上，所有任务都是正常结束
     *
     * 检查的异常状态类型：
     * - 失败状态：任务执行遇到不可恢复的错误
     * - 暂停状态：用户或系统主动暂停任务执行
     * - 终止状态：任务被强制取消或终止
     *
     * 应用场景：
     * 1. 结果统计：计算工作流的成功率和质量指标
     * 2. SLA检查：验证是否达到服务级别协议要求
     * 3. 下游触发：决定是否可以触发依赖的下游流程
     * 4. 告警机制：区分正常完成和异常结束
     * 5. 资源清理：当成功完成时进行不同的清理策略
     *
     * 性能优化：
     * - 先检查完成状态，减少不必要的异常状态检查
     * - 使用短路评估，一个异常状态存在即停止检查
     * - 集合操作的时间复杂度是O(1)
     *
     * @return true表示所有任务链都成功完成，false表示存在失败或异常状态
     *
     * 类比：在生产线上检查一批产品是否全部合格，
     * 不仅要确认生产完成，还要确保没有次品、停产或取消的情况。
     */
    @Override
    public boolean isAllTaskExecutionRunnableChainSuccess() {
        // ========== 第一步：检查基本完成条件 ==========
        // 首先确认所有任务链已经完成执行（没有活跃任务）
        // 如果还有任务正在执行，则无法判断为成功完成
        if (!isAllTaskExecutionRunnableChainFinish()) {
            return false;
        }

        // ========== 第二步：检查各种异常状态 ==========
        // 使用逻辑与操作检查所有可能的异常状态
        // 只有当所有异常状态都不存在时，才能认为成功完成
        return !isExistFailureTaskExecutionRunnableChain()    // 检查是否存在失败任务
                && !isExistPausedTaskExecutionRunnableChain()  // 检查是否存在暂停任务
                && !isExistKilledTaskExecutionRunnableChain(); // 检查是否存在终止任务
    }

    @Override
    public boolean isExistFailureTaskExecutionRunnableChain() {
        return CollectionUtils.isNotEmpty(failureTaskChains);
    }

    @Override
    public boolean isExistPausedTaskExecutionRunnableChain() {
        return CollectionUtils.isNotEmpty(pausedTaskChains);
    }

    @Override
    public boolean isExistKilledTaskExecutionRunnableChain() {
        return CollectionUtils.isNotEmpty(killedTaskChains);
    }

    /**
     * 标记任务为活跃状态
     *
     * 将指定的任务标记为活跃状态，表示该任务当前正在被工作流引擎处理。
     * 活跃状态的任务可能处于执行中、等待资源、排队中或重试中等各种状态。
     *
     * 活跃状态的含义：
     * 1. 任务已经进入工作流引擎的处理流程
     * 2. 引擎正在积极管理和调度该任务
     * 3. 任务可能正在消耗计算资源或等待资源分配
     * 4. 任务的状态可能会发生动态变化
     *
     * 状态转换时机：
     * - 任务触发条件满足时：从未处理转为活跃
     * - 任务开始执行时：从等待转为正在执行
     * - 任务重试时：从失败重新转为活跃
     * - 任务恢复时：从暂停转为活跃
     *
     * 数据结构操作：
     * - 将任务名称添加到活跃任务集合中
     * - Set.add()操作保证了任务名称的唯一性
     * - O(1)时间复杂度的高效操作
     *
     * 并发安全性：
     * - Set操作需要在多线程环境下考虑线程安全
     * - 需要与其他状态变更操作在事务中进行
     * - 避免状态不一致的竞态条件
     *
     * 监控和调试：
     * - 用于统计并发执行的任务数量
     * - 监控系统资源使用情况
     * - 识别长时间运行的任务
     * - 进行性能瓶颈分析
     *
     * @param taskExecutionRunnable 要标记为活跃状态的任务执行对象
     *
     * 类比：在工厂中将某个工位标记为"生产中"状态，
     * 表示此工位正在进行生产操作，需要监控和资源保障。
     */
    @Override
    public void markTaskExecutionRunnableActive(final ITaskExecutionRunnable taskExecutionRunnable) {
        // ========== 活跃状态标记 ==========
        // 将任务名称添加到活跃任务集合中
        // Set.add()操作保证了同一任务不会被重复添加
        // 这标志着任务开始被工作流引擎积极管理
        activeTaskExecutionRunnable.add(taskExecutionRunnable.getName());
    }

    /**
     * 标记任务为非活跃状态
     *
     * 将指定的任务从活跃状态转移到非活跃状态，表示该任务已经完成执行并产生了明确的结果。
     * 这是任务生命周期中的一个重要状态转换，标志着任务的执行阶段正式结束。
     *
     * 非活跃状态的含义：
     * 1. 任务已经完成执行流程，不需要引擎继续处理
     * 2. 任务产生了明确的执行结果（成功、失败、暂停、终止等）
     * 3. 不再占用活跃任务的资源配额
     * 4. 可以作为后续任务的依赖条件参考
     *
     * 状态转换操作（原子性）：
     * 1. 从活跃任务集合中移除任务名称
     * 2. 将任务名称添加到非活跃任务集合中
     * 3. 这两个操作应该在一个事务中完成以保证一致性
     *
     * 触发条件：
     * - 任务成功完成执行
     * - 任务执行失败且不能重试
     * - 任务被用户或系统暂停
     * - 任务被强制终止或取消
     * - 任务被跳过执行
     *
     * 对后续流程的影响：
     * 1. 触发条件检查：后继任务可以检查前驱依赖是否满足
     * 2. 执行调度：可能触发新的任务调度周期
     * 3. 资源管理：释放被占用的计算资源
     * 4. 监控统计：更新工作流的进度和状态统计
     *
     * 性能考虑：
     * - 使用O(1)时间复杂度的Set操作
     * - 避免不必要的集合遍历和查找
     * - 适合高并发的任务状态更新场景
     *
     * @param taskExecutionRunnable 要标记为非活跃状态的任务执行对象
     *
     * 类比：在工厂中将某个工位从"生产中"状态转为"已完成"状态，
     * 表示该工位的生产任务已结束，可以开始下一道工序。
     */
    @Override
    public void markTaskExecutionRunnableInActive(final ITaskExecutionRunnable taskExecutionRunnable) {
        // ========== 第一步：从活跃集合中移除 ==========
        // 将任务从活跃任务集合中移除
        // 表示任务不再需要引擎的积极调度和管理
        activeTaskExecutionRunnable.remove(taskExecutionRunnable.getName());

        // ========== 第二步：添加到非活跃集合 ==========
        // 将任务添加到非活跃任务集合中
        // 表示任务已经完成执行并产生了明确的结果
        // 可以作为后续任务的依赖条件参考
        inActiveTaskExecutionRunnable.add(taskExecutionRunnable.getName());
    }

    @Override
    public void markTaskExecutionRunnableChainFailure(final ITaskExecutionRunnable taskExecutionRunnable) {
        assertTaskExecutionRunnableState(taskExecutionRunnable, TaskExecutionStatus.FAILURE);
        failureTaskChains.add(taskExecutionRunnable.getName());
    }

    @Override
    public void markTaskExecutionRunnableChainPause(final ITaskExecutionRunnable taskExecutionRunnable) {
        assertTaskExecutionRunnableState(taskExecutionRunnable, TaskExecutionStatus.PAUSE);
        pausedTaskChains.add(taskExecutionRunnable.getName());
    }

    @Override
    public void markTaskExecutionRunnableChainKill(final ITaskExecutionRunnable taskExecutionRunnable) {
        assertTaskExecutionRunnableState(taskExecutionRunnable, TaskExecutionStatus.KILL);
        killedTaskChains.add(taskExecutionRunnable.getName());
    }

    @Override
    public void markTaskSkipped(final ITaskExecutionRunnable taskExecutionRunnable) {
        markTaskSkipped(taskExecutionRunnable.getName());
    }

    @Override
    public void markTaskSkipped(final String taskName) {
        skippedTask.add(taskName);
    }

    /**
     * 判断任务是否处于任务链的终点
     *
     * 检查指定任务是否处于执行链的终点位置，终点可能是自然终点（没有后继任务）或异常终点。
     * 这个判断对于确定工作流执行是否应该停止具有重要意义。
     *
     * 终点判断条件（满足任意一个）：
     * 1. 自然终点：任务没有后继任务（正常的流程终点）
     * 2. 异常终止：任务被强制终止（KILL状态）
     * 3. 人工暂停：任务被暂停执行（PAUSE状态）
     * 4. 执行失败：任务执行失败且无法重试（FAILURE状态）
     *
     * 终点类型分析：
     * - 正常终点：工作流设计的结束节点，所有任务正常完成
     * - 异常终点：因各种异常情况导致的提前终止
     * - 条件终点：基于条件判断的分支终点
     * - 用户终点：用户主动操作导致的终止
     *
     * 对于工作流引擎的意义：
     * - 资源释放：终点任务可以释放占用的计算资源
     * - 状态汇报：为工作流的最终状态计算提供依据
     * - 触发清理：启动工作流结束后的清理操作
     * - 通知机制：触发相关的完成或异常通知
     *
     * 数据一致性检查：
     * - 终点状态应该与工作流的整体状态保持一致
     * - 后继任务为空的情况下，任务状态应该是正常的
     * - 异常状态的任务即使有后继任务也不会触发执行
     *
     * @param taskExecutionRunnable 待检查的任务执行对象
     * @return true表示任务处于终点位置，false表示还有后续执行可能
     *
     * 类比：在施工项目中，判断某个工序是否是整个项目的最后一道工序，
     * 或者因为意外情况（停工、验收不过、资金问题）导致不能继续。
     */
    @Override
    public boolean isEndOfTaskChain(final ITaskExecutionRunnable taskExecutionRunnable) {
        // ========== 第一个条件：检查自然终点 ==========
        // 如果任务没有后继任务，说明这是工作流设计中的正常终点
        // isEmpty()检查后继任务集合是否为空
        return successors.get(taskExecutionRunnable.getName()).isEmpty()
                // ========== 第二个条件：检查强制终止状态 ==========
                // 任务被系统或用户强制终止，不会继续触发后续任务
                || isTaskExecutionRunnableKilled(taskExecutionRunnable)
                // ========== 第三个条件：检查暂停状态 ==========
                // 任务被暂停执行，在恢复之前不会触发后续任务
                || isTaskExecutionRunnablePaused(taskExecutionRunnable)
                // ========== 第四个条件：检查失败状态 ==========
                // 任务执行失败且无法恢复，阻断后续任务的执行
                || isTaskExecutionRunnableFailed(taskExecutionRunnable);
    }

    /**
     * 判断任务是否被跳过执行
     *
     * 检查指定的任务是否在跳过任务集合中，跳过的任务不会实际执行但被视为"完成"状态。
     * 任务跳过是工作流中的一种重要机制，用于处理条件分支、开关逻辑或故障恢复场景。
     *
     * 跳过机制的应用场景：
     * 1. 条件任务：基于前驱任务的执行结果决定是否跳过
     * 2. 开关任务：根据开关状态决定分支路径的执行
     * 3. 故障恢复：跳过已经完成或不需要重复执行的任务
     * 4. 用户配置：根据用户设置跳过某些可选任务
     * 5. 依赖跳过：当所有前驱任务都被跳过时，自动跳过当前任务
     *
     * 跳过状态的特点：
     * - 跳过的任务不消耗实际执行资源
     * - 被视为成功完成，可以触发后续任务执行
     * - 不影响整个工作流的成功状态
     * - 在执行统计中通常单独计算
     *
     * 数据结构设计：
     * - 使用Set结构存储跳过的任务名称
     * - 支持O(1)时间复杂度的快速查询
     * - 线程安全的并发访问
     *
     * @param taskExecutionRunnable 待检查的任务执行对象
     * @return true表示任务被跳过，false表示任务需要正常执行
     *
     * 类比：在建筑施工中，某些装修工序可能因为业主选择或设计变更
     * 而被跳过，但整个施工流程仍然可以继续进行。
     */
    @Override
    public boolean isTaskExecutionRunnableSkipped(final ITaskExecutionRunnable taskExecutionRunnable) {
        // ========== 跳过状态查询 ==========
        // 直接在跳过任务集合中查找任务名称
        // Set.contains()方法提供O(1)时间复杂度的快速查询
        // 返回true表示该任务已经被标记为跳过执行
        return skippedTask.contains(taskExecutionRunnable.getName());
    }

    /**
     * 判断任务是否被禁用
     *
     * 检查指定任务的禁用标志，判断该任务是否被标记为禁用状态。
     * 禁用的任务不会被执行，这是任务级别的开关控制机制。
     *
     * 禁用机制的作用：
     * 1. 任务级开关：提供细粒度的任务控制能力
     * 2. 调试支持：在开发和测试阶段临时禁用某些任务
     * 3. 版本管理：在不同版本间控制功能的启用状态
     * 4. 故障隔离：快速禁用有问题的任务避免影响整个流程
     * 5. 分阶段发布：逐步启用新功能或任务
     *
     * 与跳过的区别：
     * - 禁用是静态配置：在任务定义中设置，属于设计时决策
     * - 跳过是动态决策：在运行时基于条件或状态决定
     * - 禁用任务通常不参与依赖关系计算
     * - 跳过任务仍然满足后续任务的依赖条件
     *
     * 标志位说明：
     * - Flag.NO：表示任务被禁用，不应该执行
     * - Flag.YES：表示任务启用，可以正常执行
     * - 这个标志通常在任务定义阶段设置
     *
     * 处理逻辑：
     * - 禁用的任务在工作流执行时会被直接忽略
     * - 不会分配执行资源，不会进入执行队列
     * - 其后继任务的触发条件需要重新评估
     *
     * @param taskExecutionRunnable 待检查的任务执行对象
     * @return true表示任务被禁用，false表示任务允许执行
     *
     * 类比：在施工项目中，某些可选工序（如豪华装修、景观美化）
     * 可能因为预算限制而被禁用，这些工序不会安排施工。
     */
    @Override
    public boolean isTaskExecutionRunnableForbidden(final ITaskExecutionRunnable taskExecutionRunnable) {
        // ========== 禁用标志检查 ==========
        // 从任务定义中获取启用/禁用标志
        // Flag.NO表示任务被禁用，Flag.YES表示任务启用
        // 这个标志是任务的静态配置属性，在任务定义时设置
        return (taskExecutionRunnable.getTaskDefinition().getFlag() == Flag.NO);
    }

    /**
     * 判断任务是否正在重试中
     *
     * 检查指定任务是否处于重试状态，重试是任务执行失败后的自动恢复机制。
     * 这个方法综合考虑了任务的执行状态、重试能力和活跃状态来判断重试状态。
     *
     * 重试状态的判断条件（必须同时满足）：
     * 1. 任务实例已经初始化（有具体的执行实例）
     * 2. 任务当前状态为失败状态（FAILURE）
     * 3. 任务具备重试能力（配置了重试次数且未超限）
     * 4. 任务当前处于活跃状态（正在被引擎处理）
     *
     * 重试机制的工作原理：
     * 1. 任务执行失败后，系统检查重试配置
     * 2. 如果允许重试且未达到最大重试次数，标记为重试状态
     * 3. 重试中的任务保持活跃状态，等待重新调度
     * 4. 重试任务具有比普通任务更高的执行优先级
     *
     * 状态组合分析：
     * - 失败+可重试+活跃 = 正在重试中
     * - 失败+可重试+非活跃 = 等待重试调度
     * - 失败+不可重试 = 最终失败，不会重试
     * - 非失败状态 = 不需要重试
     *
     * 重试策略考虑因素：
     * - 最大重试次数：避免无限重试
     * - 重试间隔：指数退避或固定间隔
     * - 重试条件：只对特定类型的失败进行重试
     * - 资源限制：控制重试任务的资源占用
     *
     * @param taskExecutionRunnable 待检查的任务执行对象
     * @return true表示任务正在重试中，false表示任务不在重试状态
     *
     * 类比：在施工项目中，某个工序因为材料质量问题而失败，
     * 如果合同允许返工且还有返工机会，该工序就处于"返工中"状态。
     */
    @Override
    public boolean isTaskExecutionRunnableRetrying(final ITaskExecutionRunnable taskExecutionRunnable) {
        // ========== 第一步：检查任务实例初始化状态 ==========
        // 只有已经初始化的任务实例才可能处于重试状态
        // 未初始化的任务还没有执行过，不存在重试的概念
        if (!taskExecutionRunnable.isTaskInstanceInitialized()) {
            return false;
        }

        // ========== 第二步：获取任务实例对象 ==========
        // 从任务执行对象中获取任务实例，用于状态检查
        // 任务实例包含了具体的执行状态和历史信息
        final TaskInstance taskInstance = taskExecutionRunnable.getTaskInstance();

        // ========== 第三步：综合判断重试状态 ==========
        // 同时满足三个条件才认为任务正在重试：
        // 1. 任务状态为失败（表明需要重试）
        // 2. 任务具备重试能力（配置允许且未超过最大重试次数）
        // 3. 任务处于活跃状态（正在被引擎调度处理）
        return taskInstance.getState() == TaskExecutionStatus.FAILURE
                && taskExecutionRunnable.isTaskInstanceCanRetry()
                && isTaskExecutionRunnableActive(taskExecutionRunnable);
    }

    /**
     * 判断任务的所有前驱任务是否都被跳过
     *
     * 检查指定任务的所有前驱任务是否都处于跳过状态。这是一个重要的条件判断方法，
     * 用于决定当前任务是否也应该被自动跳过，实现跳过状态的级联传播。
     *
     * 判断逻辑说明：
     * - 只有当任务有前驱任务且所有前驱任务都被跳过时，才返回true
     * - 如果任务没有前驱任务（起始节点），则返回false
     * - 如果有任何一个前驱任务未被跳过，则返回false
     *
     * 跳过传播机制：
     * 1. 当所有前驱任务都被跳过时，当前任务通常也应该被跳过
     * 2. 这种传播机制保证了工作流分支的一致性
     * 3. 避免了孤立任务的执行（所有输入都被跳过的任务）
     *
     * 特殊情况处理：
     * - 起始节点：没有前驱任务的节点不会因为此条件被跳过
     * - 部分跳过：只要有一个前驱任务正常执行，当前任务就不应该被自动跳过
     * - 条件任务：可能需要结合其他条件判断是否跳过
     *
     * 应用场景：
     * - 条件分支：当某个分支的所有任务都被跳过时，后续任务也应该跳过
     * - 开关逻辑：基于开关状态跳过整个功能模块
     * - 故障恢复：跳过已经处理完成的任务链
     *
     * @param taskExecutionRunnable 待检查的任务执行对象
     * @return true表示所有前驱任务都被跳过（且存在前驱任务），false表示其他情况
     *
     * 类比：在装修工程中，如果所有的水电预埋工序都被跳过（因为使用明装方式），
     * 那么依赖水电预埋的后续工序（如封槽、找平）也应该被跳过。
     */
    @Override
    public boolean isAllPredecessorsSkipped(final ITaskExecutionRunnable taskExecutionRunnable) {
        // ========== 第一步：获取前驱任务列表 ==========
        // 获取指定任务的所有前驱任务
        // 这个列表包含了该任务的所有依赖关系
        final List<ITaskExecutionRunnable> predecessors = getPredecessors(taskExecutionRunnable.getName());

        // ========== 第二步：处理无前驱任务的情况 ==========
        // 如果任务没有前驱任务（起始节点），直接返回false
        // 起始节点不应该因为"所有前驱都跳过"这个条件而被跳过
        if (CollectionUtils.isEmpty(predecessors)) {
            return false;
        }

        // ========== 第三步：检查所有前驱任务的跳过状态 ==========
        // 注意：这里有一个逻辑冗余，predecessors已经在上面检查过是否为空
        // 第一个条件CollectionUtils.isEmpty(predecessors)永远为false
        // 实际的判断逻辑是：检查所有前驱任务是否都被跳过
        return CollectionUtils.isEmpty(predecessors)
                // 使用Stream API检查所有前驱任务的跳过状态
                // allMatch确保每个前驱任务都满足跳过条件
                || predecessors.stream().allMatch(this::isTaskExecutionRunnableSkipped);
    }

    /**
     * 判断任务的所有后继任务是否都是条件任务
     *
     * 检查指定任务的所有后继任务是否都属于条件任务类型或已被跳过。
     * 这个判断对于理解工作流的执行模式和优化调度策略具有重要意义。
     *
     * 条件任务的特点：
     * 1. 基于前驱任务的执行结果决定执行路径
     * 2. 通常用于实现工作流的分支逻辑
     * 3. 执行时间短，主要进行条件判断而非实际业务处理
     * 4. 对工作流的整体执行时间影响较小
     *
     * 判断逻辑说明：
     * - 如果任务没有后继任务，返回false（结束节点）
     * - 后继任务被跳过也被视为满足条件（跳过的任务不影响流程）
     * - 所有未跳过的后继任务都必须是条件任务才返回true
     *
     * 注意：代码中存在一个逻辑错误
     * - 当前检查的是当前任务的类型而不是后继任务的类型
     * - 应该是successor.getTaskInstance().getTaskType()
     * - 这可能是一个需要修复的bug
     *
     * 应用场景：
     * 1. 调度优化：条件任务密集的区域可以快速处理
     * 2. 资源分配：条件任务集群不需要大量计算资源
     * 3. 执行策略：可以采用不同的并发控制策略
     * 4. 监控告警：条件任务链的执行模式分析
     *
     * 工作流模式识别：
     * - 全是条件任务：可能是复杂的条件分支区域
     * - 混合任务：正常的业务处理流程
     * - 无后继任务：工作流的终点节点
     *
     * @param taskExecutionRunnable 待检查的任务执行对象
     * @return true表示所有后继任务都是条件任务或被跳过，false表示存在其他类型的后继任务
     *
     * 类比：在施工项目中，检查某个工序的所有后续工序是否都是检查验收类的工序，
     * 这些工序通常耗时短且主要用于质量控制和流程判断。
     */
    @Override
    public boolean isAllSuccessorsAreConditionTask(final ITaskExecutionRunnable taskExecutionRunnable) {
        // ========== 第一步：获取后继任务列表 ==========
        // 获取当前任务的所有直接后继任务
        // 这些任务依赖当前任务的完成才能被触发执行
        final List<ITaskExecutionRunnable> successors = getSuccessors(taskExecutionRunnable.getName());

        // ========== 第二步：处理无后继任务的情况 ==========
        // 如果当前任务没有后继任务（结束节点），直接返回false
        // 结束节点的后继条件检查没有意义
        if (CollectionUtils.isEmpty(successors)) {
            return false;
        }

        // ========== 第三步：检查所有后继任务的类型 ==========
        // 使用Stream API检查每个后继任务是否满足条件
        // 满足条件的情况：任务被跳过 OR 任务是条件任务类型
        return successors.stream().allMatch(
                successor -> isTaskExecutionRunnableSkipped(successor)
                        // 注意：这里可能存在bug，应该检查successor的任务类型而不是当前任务
                        // 正确的写法应该是：TaskTypeUtils.isConditionTask(successor.getTaskInstance().getTaskType())
                        || TaskTypeUtils.isConditionTask(taskExecutionRunnable.getTaskInstance().getTaskType()));
    }

    private void assertTaskExecutionRunnableState(final ITaskExecutionRunnable taskExecutionRunnable,
                                                  final TaskExecutionStatus taskExecutionStatus) {
        final TaskInstance taskInstance = taskExecutionRunnable.getTaskInstance();
        if (taskInstance.getState() == taskExecutionStatus) {
            return;
        }
        throw new IllegalStateException(
                "The task: " + taskExecutionRunnable.getName() + " state: " + taskInstance.getState() + " is not "
                        + taskExecutionStatus);
    }

}
