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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.dolphinscheduler.plugin.task.api.enums.TaskExecutionStatus.DISPATCH;

import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.dao.entity.WorkflowInstance;
import org.apache.dolphinscheduler.dao.repository.TaskInstanceDao;
import org.apache.dolphinscheduler.plugin.task.api.enums.TaskExecutionStatus;
import org.apache.dolphinscheduler.plugin.task.api.model.Property;
import org.apache.dolphinscheduler.plugin.task.api.utils.VarPoolUtils;
import org.apache.dolphinscheduler.server.master.engine.AbstractLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.ITaskGroupCoordinator;
import org.apache.dolphinscheduler.server.master.engine.IWorkflowRepository;
import org.apache.dolphinscheduler.server.master.engine.graph.IWorkflowExecutionGraph;
import org.apache.dolphinscheduler.server.master.engine.task.client.ITaskExecutorClient;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event.TaskDispatchLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event.TaskDispatchedLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event.TaskFailedLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event.TaskKilledLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event.TaskPausedLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event.TaskRetryLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event.TaskRunningLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event.TaskRuntimeContextChangedEvent;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event.TaskSuccessLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.task.runnable.ITaskExecutionRunnable;
import org.apache.dolphinscheduler.server.master.engine.workflow.lifecycle.event.WorkflowTopologyLogicalTransitionWithTaskFinishLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.workflow.runnable.IWorkflowExecutionRunnable;

import org.apache.commons.lang3.StringUtils;

import java.util.List;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;

import com.google.common.collect.Lists;

/**
 * 任务状态动作抽象基类
 *
 * 为所有任务状态动作实现提供通用的基础功能和默认实现。
 * 这个抽象类实现了状态机模式中的通用行为，减少各个具体状态动作类的重复代码。
 *
 * 核心职责：
 * 1. 资源管理：提供任务组资源的获取和释放机制
 * 2. 状态转换：处理常见的状态转换逻辑和数据持久化
 * 3. 事件处理：实现通用的生命周期事件处理流程
 * 4. 异常处理：提供统一的错误处理和状态验证机制
 * 5. 工作流协调：处理任务与工作流之间的协调逻辑
 *
 * 设计模式应用：
 * - 模板方法模式：定义算法骨架，子类实现具体步骤
 * - 策略模式：不同状态采用不同的处理策略
 * - 责任链模式：事件处理可以传递给父类或其他处理器
 * - 观察者模式：通过事件总线发布状态变更事件
 *
 * 资源管理机制：
 * - 任务组槽位：管理任务在任务组中的资源分配
 * - 执行器资源：管理任务在执行器上的资源占用
 * - 数据库连接：管理任务状态的数据库持久化
 * - 网络连接：管理与执行器的通信连接
 *
 * 状态验证机制：
 * - 前置状态检查：验证事件是否适用于当前状态
 * - 状态转换验证：确保状态转换的合法性
 * - 异常状态处理：处理非法状态转换和错误情况
 * - 日志记录：记录状态转换过程和异常信息
 *
 * 通用事件处理流程：
 * 1. 状态验证：检查当前任务状态是否匹配预期
 * 2. 前置处理：执行状态转换前的准备工作
 * 3. 核心逻辑：执行具体的业务处理逻辑
 * 4. 状态更新：更新任务的状态信息
 * 5. 后置处理：执行状态转换后的清理工作
 * 6. 事件传播：发布后续的生命周期事件
 * 7. 工作流协调：通知工作流引擎进行调度决策
 *
 * 变量池管理：
 * - 任务变量收集：收集任务执行过程中产生的变量
 * - 变量合并：将任务变量合并到工作流变量池中
 * - 冲突解决：处理变量名称冲突和类型转换
 * - 作用域管理：管理变量的可见性和生命周期
 *
 * 故障处理机制：
 * - 故障检测：检测任务执行过程中的各种故障
 * - 故障分类：区分可恢复故障和不可恢复故障
 * - 恢复策略：实现任务的重试、故障转移等恢复机制
 * - 降级处理：在无法恢复时的降级和补偿机制
 *
 * 类比理解：
 * 就像一个工厂的通用生产线控制系统：
 * - 每个生产环节（状态）都有标准的操作流程（通用方法）
 * - 不同产品（任务类型）在各环节有特定的处理方式（具体实现）
 * - 统一的质量检查（状态验证）和资源调度（资源管理）
 * - 异常情况的标准处理流程（错误处理机制）
 */
@Slf4j
public abstract class AbstractTaskStateAction implements ITaskStateAction {

    @Autowired
    protected ITaskGroupCoordinator taskGroupCoordinator;

    @Autowired
    protected TaskInstanceDao taskInstanceDao;

    @Autowired
    protected IWorkflowRepository workflowRepository;

    @Autowired
    protected ITaskExecutorClient taskExecutorClient;

    /**
     * 检查任务是否需要获取任务组槽位
     *
     * 判断任务执行实例是否需要在任务组中获取槽位资源。
     * 任务组用于控制并发执行的任务数量，防止资源过载。
     *
     * 检查逻辑：
     * 1. 获取任务实例的任务组配置
     * 2. 检查任务组是否存在且启用
     * 3. 检查任务组是否还有可用槽位
     * 4. 检查任务是否已经获取了槽位
     *
     * 任务组用途：
     * - 资源限制：控制同时执行的任务数量
     * - 优先级管理：高优先级任务优先获取槽位
     * - 负载均衡：在多个执行器之间分散任务
     * - 故障隔离：防止某个任务类型占用所有资源
     *
     * @param taskExecutionRunnable 任务执行实例
     * @return true 如果任务需要获取任务组槽位，false 否则
     */
    protected boolean isTaskNeedAcquireTaskGroupSlot(final ITaskExecutionRunnable taskExecutionRunnable) {
        final TaskInstance taskInstance = taskExecutionRunnable.getTaskInstance();
        return taskGroupCoordinator.needAcquireTaskGroupSlot(taskInstance);
    }

    /**
     * 获取任务实例所需的资源
     *
     * 为任务实例获取执行所需的资源，主要是任务组槽位资源。
     * 如果任务实例使用了任务组，则会获取任务组中的一个槽位。
     *
     * 获取流程：
     * 1. 检查任务是否配置了任务组
     * 2. 检查任务组是否还有可用槽位
     * 3. 尝试获取一个可用的槽位
     * 4. 更新任务实例的资源占用信息
     * 5. 记录资源获取日志
     *
     * 资源类型：
     * - 任务组槽位：限制并发执行数量的逻辑资源
     * - 执行器资源：CPU、内存等物理资源
     * - 网络资源：网络带宽、连接数等
     * - 存储资源：磁盘空间、IO带宽等
     *
     * 异常情况：
     * - 任务组不存在：抛出配置异常
     * - 槽位全部被占用：任务进入等待队列
     * - 资源获取超时：返回获取失败状态
     *
     * @param taskExecutionRunnable 任务执行实例
     * @throws ResourceNotAvailableException 当资源不可用时抛出
     */
    protected void acquireTaskGroupSlot(final ITaskExecutionRunnable taskExecutionRunnable) {
        final TaskInstance taskInstance = taskExecutionRunnable.getTaskInstance();
        taskGroupCoordinator.acquireTaskGroupSlot(taskInstance);
    }

    /**
     * 释放任务实例所占用的资源
     *
     * 在任务执行完成、失败、取消或暂停时释放相关资源。
     * 这是资源管理的关键环节，确保资源可以被其他任务使用。
     *
     * 释放流程：
     * 1. 检查任务是否占用了任务组槽位
     * 2. 检查是否需要释放任务组槽位
     * 3. 释放任务组槽位资源
     * 4. 清理任务资源占用记录
     * 5. 通知等待队列中的任务
     * 6. 记录资源释放日志
     *
     * 释放类型：
     * - 任务组槽位：释放占用的逻辑槽位，供其他任务使用
     * - 执行器资源：释放占用的CPU、内存等资源
     * - 网络连接：关闭与执行器的网络连接
     * - 临时文件：清理任务生成的临时文件
     *
     * 释放策略：
     * - 立即释放：对于已终止的任务，立即释放所有资源
     * - 延迟释放：对于可能重启的任务，延迟释放部分资源
     * - 分段释放：按照资源类型分段释放，避免影响正在执行的操作
     *
     * @param taskExecutionRunnable 任务执行实例
     */
    protected void releaseTaskInstanceResourcesIfNeeded(final ITaskExecutionRunnable taskExecutionRunnable) {
        final TaskInstance taskInstance = taskExecutionRunnable.getTaskInstance();
        if (taskGroupCoordinator.needToReleaseTaskGroupSlot(taskInstance)) {
            taskGroupCoordinator.releaseTaskGroupSlot(taskInstance);
        }
    }

    @Override
    public void onDispatchedEvent(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                                  final ITaskExecutionRunnable taskExecutionRunnable,
                                  final TaskDispatchedLifecycleEvent taskDispatchedEvent) {
        final TaskInstance taskInstance = taskExecutionRunnable.getTaskInstance();
        taskInstance.setState(DISPATCH);
        taskInstance.setHost(taskDispatchedEvent.getExecutorHost());
        taskInstanceDao.updateById(taskInstance);
    }

    @Override
    public void onRuntimeContextChangedEvent(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                                             final ITaskExecutionRunnable taskExecutionRunnable,
                                             final TaskRuntimeContextChangedEvent taskRuntimeContextChangedEvent) {
        final TaskInstance taskInstance = taskExecutionRunnable.getTaskInstance();
        if (StringUtils.isNotEmpty(taskRuntimeContextChangedEvent.getRuntimeContext())) {
            taskInstance.setAppLink(taskRuntimeContextChangedEvent.getRuntimeContext());
        }
        taskInstanceDao.updateById(taskInstance);
    }

    /**
     * 将任务实例启动事件持久化到数据库
     *
     * 将任务实例的运行状态和相关信息保存到数据库中。
     * 这是任务状态管理的核心环节，确保状态变更的持久性和可追滯性。
     *
     * 持久化内容：
     * - 任务状态：更新为RUNNING_EXECUTION
     * - 开始时间：记录任务的实际开始执行时间
     * - 日志路径：保存任务执行日志的存储路径
     * - 执行器信息：记录执行任务的执行器信息
     *
     * 数据一致性保证：
     * - 事务性更新：使用数据库事务保证更新的原子性
     * - 乐观锁：通过版本号防止并发更新冲突
     * - 异常回滚：更新失败时自动回滚状态
     *
     * @param taskExecutionRunnable 任务执行实例
     * @param taskRunningEvent 任务运行事件，包含开始时间和日志路径
     */
    protected void persistentTaskInstanceStartedEventToDB(final ITaskExecutionRunnable taskExecutionRunnable,
                                                          final TaskRunningLifecycleEvent taskRunningEvent) {
        final TaskInstance taskInstance = taskExecutionRunnable.getTaskInstance();
        taskInstance.setState(TaskExecutionStatus.RUNNING_EXECUTION);
        taskInstance.setStartTime(taskRunningEvent.getStartTime());
        taskInstance.setLogPath(taskRunningEvent.getLogPath());
        taskInstanceDao.updateById(taskInstance);
    }

    @Override
    public void onPausedEvent(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                              final ITaskExecutionRunnable taskExecutionRunnable,
                              final TaskPausedLifecycleEvent taskPausedEvent) {
        releaseTaskInstanceResourcesIfNeeded(taskExecutionRunnable);
        persistentTaskInstancePausedEventToDB(taskExecutionRunnable, taskPausedEvent);
        taskExecutionRunnable.getWorkflowExecutionGraph().markTaskExecutionRunnableChainPause(taskExecutionRunnable);
        publishWorkflowInstanceTopologyLogicalTransitionEvent(taskExecutionRunnable);
    }

    /**
     * 将任务实例暂停事件持久化到数据库
     *
     * 将任务的暂停状态保存到数据库，确保状态变更的持久性。
     * 暂停状态表示任务暂停执行但保留执行上下文，可以后续恢复。
     *
     * @param taskExecutionRunnable 任务执行实例
     * @param taskPausedEvent 任务暂停事件
     */
    private void persistentTaskInstancePausedEventToDB(final ITaskExecutionRunnable taskExecutionRunnable,
                                                       final TaskPausedLifecycleEvent taskPausedEvent) {
        final TaskInstance taskInstance = taskExecutionRunnable.getTaskInstance();
        taskInstance.setState(TaskExecutionStatus.PAUSE);
        taskInstanceDao.updateById(taskInstance);
    }

    @Override
    public void onKilledEvent(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                              final ITaskExecutionRunnable taskExecutionRunnable,
                              final TaskKilledLifecycleEvent taskInstanceKillEvent) {
        releaseTaskInstanceResourcesIfNeeded(taskExecutionRunnable);
        persistentTaskInstanceKilledEventToDB(taskExecutionRunnable, taskInstanceKillEvent);
        taskExecutionRunnable.getWorkflowExecutionGraph().markTaskExecutionRunnableChainKill(taskExecutionRunnable);
        publishWorkflowInstanceTopologyLogicalTransitionEvent(taskExecutionRunnable);
    }

    /**
     * 将任务实例终止事件持久化到数据库
     *
     * 将任务的终止状态和结束时间保存到数据库。
     * 终止状态表示任务完全停止执行，无法恢复。
     *
     * @param taskExecutionRunnable 任务执行实例
     * @param taskKilledEvent 任务终止事件，包含结束时间
     */
    private void persistentTaskInstanceKilledEventToDB(final ITaskExecutionRunnable taskExecutionRunnable,
                                                       final TaskKilledLifecycleEvent taskKilledEvent) {
        final TaskInstance taskInstance = taskExecutionRunnable.getTaskInstance();
        taskInstance.setState(TaskExecutionStatus.KILL);
        taskInstance.setEndTime(taskKilledEvent.getEndTime());
        taskInstanceDao.updateById(taskInstance);

    }

    @Override
    public void onFailedEvent(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                              final ITaskExecutionRunnable taskExecutionRunnable,
                              final TaskFailedLifecycleEvent taskFailedEvent) {
        releaseTaskInstanceResourcesIfNeeded(taskExecutionRunnable);
        persistentTaskInstanceFailedEventToDB(taskExecutionRunnable, taskFailedEvent);

        if (taskExecutionRunnable.isTaskInstanceCanRetry()) {
            taskExecutionRunnable.getWorkflowEventBus().publish(TaskRetryLifecycleEvent.of(taskExecutionRunnable));
            return;
        }
        // If all successors are condition tasks, then the task will not be marked as failure.
        // And the DAG will continue to execute.
        final IWorkflowExecutionGraph workflowExecutionGraph = taskExecutionRunnable.getWorkflowExecutionGraph();
        if (workflowExecutionGraph.isAllSuccessorsAreConditionTask(taskExecutionRunnable)) {
            publishWorkflowInstanceTopologyLogicalTransitionEvent(taskExecutionRunnable);
            return;
        }
        taskExecutionRunnable.getWorkflowExecutionGraph().markTaskExecutionRunnableChainFailure(taskExecutionRunnable);
        publishWorkflowInstanceTopologyLogicalTransitionEvent(taskExecutionRunnable);
    }

    /**
     * 将任务实例失败事件持久化到数据库
     *
     * 将任务的失败状态和结束时间保存到数据库。
     * 失败状态表示任务执行失败，可能需要重试或人工干预。
     *
     * @param taskExecutionRunnable 任务执行实例
     * @param taskFailedEvent 任务失败事件，包含失败原因和结束时间
     */
    private void persistentTaskInstanceFailedEventToDB(final ITaskExecutionRunnable taskExecutionRunnable,
                                                       final TaskFailedLifecycleEvent taskFailedEvent) {
        final TaskInstance taskInstance = taskExecutionRunnable.getTaskInstance();
        taskInstance.setState(TaskExecutionStatus.FAILURE);
        taskInstance.setEndTime(taskFailedEvent.getEndTime());
        taskInstanceDao.updateById(taskInstance);
    }

    @Override
    public void onSucceedEvent(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                               final ITaskExecutionRunnable taskExecutionRunnable,
                               final TaskSuccessLifecycleEvent taskSuccessEvent) {
        releaseTaskInstanceResourcesIfNeeded(taskExecutionRunnable);
        persistentTaskInstanceSuccessEventToDB(taskExecutionRunnable, taskSuccessEvent);
        mergeTaskVarPoolToWorkflow(workflowExecutionRunnable, taskExecutionRunnable);
        publishWorkflowInstanceTopologyLogicalTransitionEvent(taskExecutionRunnable);
    }

    /**
     * 将任务变量池合并到工作流变量池
     *
     * 将任务执行过程中产生的变量合并到工作流的全局变量池中。
     * 这些变量可以在后续的任务中使用，实现任务间的数据传递。
     *
     * 合并逻辑：
     * 1. 获取任务的输出变量
     * 2. 获取工作流的当前变量池
     * 3. 合并两个变量池，处理名称冲突
     * 4. 更新工作流实例的变量池
     *
     * 冲突处理策略：
     * - 后来者优先：相同名称的变量，任务变量覆盖工作流变量
     * - 类型转换：自动处理不同类型间的转换
     * - 作用域管理：保持变量的正确作用域
     *
     * @param workflowExecutionRunnable 工作流执行实例
     * @param taskExecutionRunnable 任务执行实例
     */
    protected void mergeTaskVarPoolToWorkflow(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                                              final ITaskExecutionRunnable taskExecutionRunnable) {
        final TaskInstance taskInstance = taskExecutionRunnable.getTaskInstance();
        final WorkflowInstance workflowInstance = workflowExecutionRunnable.getWorkflowInstance();
        final List<Property> finalVarPool = VarPoolUtils.mergeVarPoolJsonString(
                Lists.newArrayList(workflowInstance.getVarPool(), taskInstance.getVarPool()));
        workflowInstance.setVarPool(VarPoolUtils.serializeVarPool(finalVarPool));
    }

    /**
     * 将任务实例成功事件持久化到数据库
     *
     * 将任务的成功状态、结束时间和输出变量保存到数据库。
     * 成功状态表示任务正常完成执行，可以激活后续任务。
     *
     * 持久化内容：
     * - 任务状态：更新为SUCCESS
     * - 结束时间：记录任务的完成时间
     * - 输出变量：保存任务产生的变量和结果
     * - 执行统计：更新执行时间、资源消耗等信息
     *
     * 变量处理：
     * 1. 获取任务原有的变量池
     * 2. 获取任务成功事件中的新变量
     * 3. 合并变量池，处理名称冲突
     * 4. 序列化合并后的变量池
     * 5. 更新任务实例的变量池字段
     *
     * @param taskExecutionRunnable 任务执行实例
     * @param taskSuccessEvent 任务成功事件，包含成功时间和输出变量
     */
    protected void persistentTaskInstanceSuccessEventToDB(final ITaskExecutionRunnable taskExecutionRunnable,
                                                          final TaskSuccessLifecycleEvent taskSuccessEvent) {
        final TaskInstance taskInstance = taskExecutionRunnable.getTaskInstance();
        taskInstance.setState(TaskExecutionStatus.SUCCESS);
        taskInstance.setEndTime(taskSuccessEvent.getEndTime());
        final List<Property> finalVarPool = VarPoolUtils.mergeVarPoolJsonString(taskInstance.getVarPool(),
                JSONUtils.toJsonString(taskSuccessEvent.getVarPool()));
        taskInstance.setVarPool(VarPoolUtils.serializeVarPool(finalVarPool));
        taskInstanceDao.updateById(taskInstance);
    }

    /**
     * 故障转移任务
     *
     * 尝试从远程执行器接管任务，如果接管成功，任务继续执行。
     * 如果接管失败，将生成一个故障转移任务实例，并将原任务实例标记为需要故障容错状态。
     *
     * 故障转移流程：
     * 1. 检查任务当前状态和执行器可用性
     * 2. 尝试连接原执行器，获取任务状态
     * 3. 如果任务仍在运行，尝试接管控制权
     * 4. 接管成功：更新任务的执行器信息，继续执行
     * 5. 接管失败：创建故障转移任务，标记原任务为故障状态
     *
     * 接管策略：
     * - 热备等待：对于正在运行的任务，尝试无缝接管
     * - 状态恢复：恢复任务到之前的可执行状态
     * - 重新调度：对于无法接管的任务，重新分发到其他执行器
     *
     * 故障容错任务创建：
     * - 复制原任务的配置和参数
     * - 设置任务类型为故障容错任务
     * - 继承原任务的执行上下文
     * - 重新加入调度队列
     *
     * @param taskExecutionRunnable 需要故障转移的任务执行实例
     */
    protected void failoverTask(final ITaskExecutionRunnable taskExecutionRunnable) {
        taskExecutionRunnable.failover();
    }

    /**
     * 尝试分发任务
     *
     * 根据任务的资源需求和当前系统状态，尝试将任务分发到合适的执行器。
     * 这是任务调度的核心环节，决定了任务的执行时机和位置。
     *
     * 分发策略：
     * 1. 检查任务是否需要获取任务组槽位
     * 2. 如果需要任务组槽位：
     *    - 获取任务组槽位
     *    - 记录获取成功日志
     *    - 停止后续分发流程（等待槽位释放时自动继续）
     * 3. 如果不需要任务组槽位：
     *    - 直接发布任务分发事件
     *    - 进入正常的分发流程
     *
     * 任务组管理：
     * - 资源限制：控制同时执行的任务数量
     * - 优先级管理：高优先级任务优先获取槽位
     * - 负载均衡：在多个执行器之间分散任务
     *
     * 异常处理：
     * - 任务组不存在：跳过任务组检查，直接分发
     * - 槽位不足：任务进入等待队列
     * - 执行器不可用：选择其他可用执行器
     *
     * @param taskExecutionRunnable 需要分发的任务执行实例
     */
    protected void tryToDispatchTask(final ITaskExecutionRunnable taskExecutionRunnable) {
        if (isTaskNeedAcquireTaskGroupSlot(taskExecutionRunnable)) {
            acquireTaskGroupSlot(taskExecutionRunnable);
            log.info("Task[name={}] using taskGroup, success acquire taskGroup slot", taskExecutionRunnable.getName());
            return;
        }
        taskExecutionRunnable.getWorkflowEventBus().publish(TaskDispatchLifecycleEvent.of(taskExecutionRunnable));
    }

    /**
     * 发布工作流实例拓扑逻辑转换事件
     *
     * 在任务状态发生变化时，通知工作流引擎进行拓扑关系的检查和调度。
     * 这是工作流与任务之间协调的关键机制。
     *
     * 事件处理流程：
     * 1. 获取任务所属的工作流实例
     * 2. 从工作流仓库中获取工作流执行实例
     * 3. 将当前任务标记为非活跃状态
     * 4. 发布工作流拓扑逻辑转换事件
     *
     * 拓扑逻辑转换包括：
     * - 依赖检查：检查后续任务的前置依赖是否已满足
     * - 任务激活：激活可以执行的后续任务
     * - 状态传播：将任务状态变化传播给相关的任务
     * - 工作流决策：根据任务状态决定工作流的后续执行策略
     *
     * 事件类型：
     * - 任务完成事件：任务成功、失败、暂停、终止时触发
     * - 状态变更事件：任务状态发生重要变化时触发
     * - 依赖更新事件：任务依赖关系发生变化时触发
     *
     * @param taskExecutionRunnable 状态发生变化的任务执行实例
     */
    protected void publishWorkflowInstanceTopologyLogicalTransitionEvent(final ITaskExecutionRunnable taskExecutionRunnable) {
        final Integer workflowInstanceId = taskExecutionRunnable.getWorkflowInstance().getId();
        final IWorkflowExecutionRunnable workflowExecutionRunnable = workflowRepository.get(workflowInstanceId);
        taskExecutionRunnable.getWorkflowExecutionGraph().markTaskExecutionRunnableInActive(taskExecutionRunnable);
        taskExecutionRunnable
                .getWorkflowEventBus()
                .publish(
                        WorkflowTopologyLogicalTransitionWithTaskFinishLifecycleEvent.of(
                                workflowExecutionRunnable,
                                taskExecutionRunnable));
    }

    /**
     * 检查任务状态是否匹配，不匹配则抛出异常
     *
     * 验证当前任务实例的状态是否与该动作类期望的状态一致。
     * 这是状态机模式中的关键安全检查，防止非法状态转换。
     *
     * 验证流程：
     * 1. 检查任务执行实例不为null
     * 2. 检查任务实例不为null
     * 3. 获取任务实例的当前状态
     * 4. 获取该动作期望的状态
     * 5. 比较实际状态和期望状态
     * 6. 如果不匹配，抛出IllegalStateException
     *
     * 异常信息包含：
     * - 任务名称
     * - 实际状态
     * - 期望状态
     * - 错误原因描述
     *
     * 使用场景：
     * - 每个事件处理方法的开始
     * - 状态变更前的前置检查
     * - 异常恢复流程中的状态验证
     * - 系统重启后的状态一致性检查
     *
     * @param taskExecutionRunnable 需要检查状态的任务执行实例
     * @throws IllegalStateException 当任务状态与期望不匹配时
     * @throws NullPointerException 当参数为null时
     */
    protected void throwExceptionIfStateIsNotMatch(final ITaskExecutionRunnable taskExecutionRunnable) {
        checkNotNull(taskExecutionRunnable, "taskExecutionRunnable is null");
        final TaskInstance taskInstance = checkNotNull(taskExecutionRunnable.getTaskInstance(), "taskInstance is null");
        final TaskExecutionStatus actualState = taskInstance.getState();
        final TaskExecutionStatus expectState = matchState();
        if (actualState != expectState) {
            final String taskName = taskInstance.getName();
            throw new IllegalStateException(
                    "The task: " + taskName + " state: " + actualState + " is not match:" + expectState);
        }
    }

    /**
     * 在无法执行动作时记录警告日志
     *
     * 当任务的当前状态不适合处理某个事件时，记录警告日志而不抛出异常。
     * 这个方法用于处理非法但可容忍的状态转换尝试。
     *
     * 日志内容包括：
     * - 任务名称：标识发生问题的任务
     * - 当前状态：任务的实际状态
     * - 事件信息：详细的事件内容和类型
     * - 操作类型：无法执行的具体操作
     *
     * 使用场景：
     * - 任务已完成后收到重复的完成事件
     * - 任务已终止后收到停止指令
     * - 任务在不合适的状态下收到重试事件
     * - 其他非关键性的状态不匹配情况
     *
     * 日志级别说明：
     * - WARN级别：表示这是一个需要关注但不会造成系统异常的问题
     * - 日志格式统一，便于日志分析和监控
     * - 包含关键信息，便于问题定位和调试
     *
     * @param taskExecutionRunnable 任务执行实例
     * @param event 导致无法执行的生命周期事件
     */
    protected void logWarningIfCannotDoAction(final ITaskExecutionRunnable taskExecutionRunnable,
                                              final AbstractLifecycleEvent event) {
        final TaskInstance taskInstance = taskExecutionRunnable.getTaskInstance();
        log.warn("Task[name={}] state is {} cannot do action on event: {}",
                taskInstance.getName(),
                taskInstance.getState(),
                event);
    }
}
