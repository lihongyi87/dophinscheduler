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

package org.apache.dolphinscheduler.server.master.engine;

import org.apache.dolphinscheduler.common.constants.Constants;
import org.apache.dolphinscheduler.common.enums.Flag;
import org.apache.dolphinscheduler.common.enums.TaskGroupQueueStatus;
import org.apache.dolphinscheduler.common.enums.WorkflowExecutionStatus;
import org.apache.dolphinscheduler.common.thread.BaseDaemonThread;
import org.apache.dolphinscheduler.common.thread.ThreadUtils;
import org.apache.dolphinscheduler.dao.entity.TaskGroup;
import org.apache.dolphinscheduler.dao.entity.TaskGroupQueue;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.dao.entity.WorkflowInstance;
import org.apache.dolphinscheduler.dao.repository.TaskGroupDao;
import org.apache.dolphinscheduler.dao.repository.TaskGroupQueueDao;
import org.apache.dolphinscheduler.dao.repository.TaskInstanceDao;
import org.apache.dolphinscheduler.dao.repository.WorkflowInstanceDao;
import org.apache.dolphinscheduler.extract.base.client.Clients;
import org.apache.dolphinscheduler.extract.master.ITaskInstanceController;
import org.apache.dolphinscheduler.extract.master.transportor.TaskGroupSlotAcquireSuccessNotifyRequest;
import org.apache.dolphinscheduler.extract.master.transportor.TaskGroupSlotAcquireSuccessNotifyResponse;
import org.apache.dolphinscheduler.plugin.task.api.enums.TaskExecutionStatus;
import org.apache.dolphinscheduler.plugin.task.api.utils.LogUtils;
import org.apache.dolphinscheduler.server.master.utils.TaskGroupUtils;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.time.StopWatch;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;

/**
 * 任务组协调器
 * 
 * 这是DolphinScheduler中负责任务组资源管理和协调的核心组件。
 * 类比：就像一个智能的会议室管理系统，负责分配和管理有限的会议室资源。
 * 
 * 核心职责：
 * 1. 资源池管理：管理任务组的资源池大小和使用情况
 * 2. 队列调度：处理等待队列中的任务，按优先级分配资源
 * 3. 状态同步：定期同步和修正任务组的使用状态
 * 4. 强制启动：处理需要强制启动的特殊任务
 * 5. 资源回收：自动回收已完成或异常任务占用的资源
 * 
 * 工作机制：
 * - 后台轮询：通过独立线程定期扫描和处理任务组状态
 * - 优先级调度：根据任务优先级和提交时间进行公平调度
 * - 资源保护：确保任务组资源不会被超额使用
 * - 异常恢复：自动检测和修复不一致的状态
 * 
 * 应用场景：
 * - 资源限制：限制同时运行的相同类型任务数量
 * - 优先级管理：确保高优先级任务优先获得资源
 * - 系统保护：防止某类任务占用过多系统资源
 * - 公平调度：确保不同优先级任务的公平执行机会
 * 
 * 类比理解：
 * 就像医院的手术室调度系统：
 * - 管理手术室数量（任务组大小）
 * - 安排等待手术的患者（任务队列）
 * - 按紧急程度排序（优先级调度）
 * - 处理紧急手术（强制启动）
 * - 回收使用完的手术室（资源释放）
 */
@Slf4j
@Component
public class TaskGroupCoordinator implements ITaskGroupCoordinator, AutoCloseable {

    /**
     * 任务组数据访问对象
     * 
     * 负责任务组元数据的数据库操作，包括任务组的创建、查询、更新等。
     * 类比：会议室基本信息管理员，负责维护每个会议室的容量、状态等基本信息。
     */
    @Autowired
    private TaskGroupDao taskGroupDao;

    /**
     * 任务组队列数据访问对象
     * 
     * 负责任务组等待队列的数据库操作，管理哪些任务在等待资源分配。
     * 类比：会议室预约系统，记录谁在什么时候预约了会议室，排队等待情况。
     */
    @Autowired
    private TaskGroupQueueDao taskGroupQueueDao;

    /**
     * 任务实例数据访问对象
     * 
     * 负责任务实例的数据库查询操作，用于验证任务状态和获取任务详情。
     * 类比：员工信息管理系统，用来查询预约会议室的员工详细信息。
     */
    @Autowired
    private TaskInstanceDao taskInstanceDao;

    /**
     * 工作流实例数据访问对象
     * 
     * 负责工作流实例的数据库查询，用于验证工作流状态和获取执行节点信息。
     * 类比：项目管理系统，用来查询会议所属的项目信息和项目负责人。
     */
    @Autowired
    private WorkflowInstanceDao workflowInstanceDao;

    /**
     * 运行状态标志
     * 
     * 标识任务组协调器是否正在运行，用于控制后台线程的启动和停止。
     * true表示正在运行，false表示已停止或未启动。
     * 类比：会议室管理系统的开关状态，决定是否继续提供调度服务。
     */
    private boolean flag = false;

    /**
     * 内部后台线程
     * 
     * 执行任务组协调逻辑的后台守护线程，负责定期扫描和处理任务组状态。
     * 类比：会议室管理员，定期巡查会议室使用情况，处理预约和释放。
     */
    private Thread internalThread;

    /**
     * 默认批处理限制
     * 
     * 每次批量处理任务组队列的最大数量，避免一次处理过多数据导致内存问题。
     * 设置为1000是在性能和内存消耗之间的平衡选择。
     * 类比：每次最多处理1000个会议室预约请求，避免系统过载。
     */
    private static final int DEFAULT_LIMIT = 1000;

    /**
     * 启动任务组协调器
     * 
     * 初始化并启动后台协调线程，开始执行任务组资源的管理和调度工作。
     * 使用synchronized确保线程安全，避免重复启动。
     * 
     * 启动流程：
     * 1. 状态检查：确保协调器未启动，避免重复启动
     * 2. 线程检查：确保内部线程未创建，避免线程泄露
     * 3. 状态设置：将运行标志设为true，表示开始运行
     * 4. 线程创建：创建后台守护线程执行协调逻辑
     * 5. 线程启动：启动线程开始后台工作
     * 
     * 异常处理：
     * - 重复启动异常：如果已经启动会抛出IllegalStateException
     * - 线程冲突异常：如果内部线程已存在会抛出IllegalStateException
     * 
     * 类比：就像启动会议室管理系统，开始提供会议室预约和调度服务。
     */
    public synchronized void start() {
        log.info("TaskGroupCoordinator starting...");
        if (flag) {
            throw new IllegalStateException("TaskGroupCoordinator is already started");
        }
        if (internalThread != null) {
            throw new IllegalStateException("InternalThread is already started");
        }
        flag = true;
        internalThread = new BaseDaemonThread(this::doStart) {
        };
        internalThread.start();
        log.info("TaskGroupCoordinator started...");
    }

    /**
     * 检查协调器是否已启动（测试用）
     * 
     * 供单元测试使用的方法，用于验证协调器的启动状态。
     * 返回内部运行标志的值，true表示已启动，false表示未启动。
     * 
     * @return true表示协调器已启动，false表示未启动
     */
    @VisibleForTesting
    boolean isStarted() {
        return flag;
    }

    /**
     * 后台协调线程的主要执行逻辑
     * 
     * 这是任务组协调器的核心执行方法，在独立的后台线程中循环运行。
     * 定期执行各种任务组管理和协调任务，确保系统资源的合理分配。
     * 
     * 执行流程：
     * 1. 启动延迟：等待1分钟确保之前的任务组资源已释放，避免混淆警告
     * 2. 循环处理：在运行标志为true时持续执行以下4个核心任务：
     *    - 修正任务组使用数量：确保数据库中的使用量与实际一致
     *    - 修正任务组队列状态：清理已完成或不存在任务的队列记录
     *    - 处理强制启动队列：处理需要强制启动的任务
     *    - 处理等待队列：为等待中的任务分配可用资源
     * 3. 性能监控：记录每轮执行的耗时，用于性能分析
     * 4. 错误处理：捕获并记录处理过程中的异常，确保线程不会因异常而终止
     * 5. 执行间隔：每轮处理后休眠25秒，避免过于频繁的处理
     * 
     * 容错机制：
     * - 异常隔离：单个处理步骤的异常不会影响其他步骤和下次执行
     * - 持续运行：即使出现异常也会继续下一轮处理
     * - 性能保护：通过休眠间隔避免过度消耗系统资源
     * 
     * 类比：就像会议室管理员的日常巡查工作，定期检查房间状态、处理预约申请、
     * 解决冲突问题，确保会议室资源得到有效利用。
     */
    private void doStart() {
        // Sleep 1 minutes here to make sure the previous task group slot has been released.
        // This step is not necessary, since the wakeup operation is idempotent, but we can avoid confusion warning.
        ThreadUtils.sleep(TimeUnit.MINUTES.toMillis(1));

        while (flag) {
            try {
                final StopWatch taskGroupCoordinatorRoundCost = StopWatch.createStarted();

                amendTaskGroupUseSize();
                amendTaskGroupQueueStatus();
                dealWithForceStartTaskGroupQueue();
                dealWithWaitingTaskGroupQueue();

                taskGroupCoordinatorRoundCost.stop();
                log.debug("TaskGroupCoordinator round cost: {}/ms", taskGroupCoordinatorRoundCost.getTime());
            } catch (Throwable e) {
                log.error("TaskGroupCoordinator error", e);
            } finally {
                // sleep 5s
                ThreadUtils.sleep(Constants.SLEEP_TIME_MILLIS * 5);
            }
        }
    }

    /**
     * 修正任务组使用数量
     * 
     * 确保任务组的使用数量与实际队列中成功获取资源且非强制启动的任务数量一致。
     * 这是数据一致性维护的重要环节，防止因异常情况导致的数据不一致。
     * 
     * 修正逻辑：
     * 1. 获取所有任务组：查询系统中的所有任务组配置
     * 2. 计算实际使用量：统计每个任务组中状态为ACQUIRE_SUCCESS且非强制启动的任务数量
     * 3. 对比修正：如果数据库中记录的使用量与实际不符，则进行修正
     * 4. 更新数据库：将修正后的使用量写入数据库
     * 
     * 触发场景：
     * - 系统异常重启导致内存状态丢失
     * - 网络异常导致状态更新失败
     * - 数据库操作异常导致状态不一致
     * - Master节点故障转移后的状态恢复
     * 
     * 性能考虑：
     * - 批量处理：一次性处理所有任务组，减少数据库访问
     * - 性能监控：记录处理耗时，便于性能分析和优化
     * - 条件更新：只有不一致时才执行更新操作，减少无效写入
     * 
     * 类比：就像会议室管理员定期核对登记表，确保记录的使用中房间数量
     * 与实际被占用的房间数量一致，发现不符时及时修正。
     */
    private void amendTaskGroupUseSize() {
        // The TaskGroup useSize should equal to the TaskGroupQueue which inQueue is YES and forceStart is NO
        List<TaskGroup> taskGroups = taskGroupDao.queryAllTaskGroups();
        if (CollectionUtils.isEmpty(taskGroups)) {
            return;
        }
        StopWatch taskGroupCoordinatorRoundTimeCost = StopWatch.createStarted();

        for (TaskGroup taskGroup : taskGroups) {
            int actualUseSize = taskGroupQueueDao.countUsingTaskGroupQueueByGroupId(taskGroup.getId());
            if (taskGroup.getUseSize() == actualUseSize) {
                continue;
            }
            log.warn("The TaskGroup: {} useSize is {}, but the actual use size is {}, will amend it",
                    taskGroup.getName(),
                    taskGroup.getUseSize(), actualUseSize);
            taskGroup.setUseSize(actualUseSize);
            taskGroupDao.updateById(taskGroup);
        }
        log.info("Success amend TaskGroup useSize cost: {}/ms", taskGroupCoordinatorRoundTimeCost.getTime());
    }

    /**
     * 修正任务组队列状态（主入口方法）
     * 
     * 清理任务组队列中相关任务实例不存在或状态已完成的记录。
     * 这是资源回收和状态维护的重要环节，确保队列中只包含有效的等待任务。
     * 
     * 处理策略：
     * 1. 分页处理：使用DEFAULT_LIMIT进行分页处理，避免一次性处理过多数据
     * 2. 循环处理：持续处理直到没有更多队列记录需要处理
     * 3. 性能监控：记录总体处理耗时，便于性能分析
     * 4. 内存保护：通过分页机制防止大数据量导致的内存溢出
     * 
     * 适用场景：
     * - 任务实例被删除但队列记录未清理
     * - 任务执行完成但队列状态未及时更新
     * - 异常情况导致的状态不一致
     * - 系统重启后的状态清理
     * 
     * 类比：就像定期清理会议室预约系统中的过期预约记录，
     * 释放已取消或已完成会议占用的预约资源。
     */
    private void amendTaskGroupQueueStatus() {
        int minTaskGroupQueueId = -1;
        int limit = DEFAULT_LIMIT;
        StopWatch taskGroupCoordinatorRoundTimeCost = StopWatch.createStarted();
        while (true) {
            List<TaskGroupQueue> taskGroupQueues =
                    taskGroupQueueDao.queryInQueueTaskGroupQueue(minTaskGroupQueueId, limit);
            if (CollectionUtils.isEmpty(taskGroupQueues)) {
                break;
            }
            amendTaskGroupQueueStatus(taskGroupQueues);
            if (taskGroupQueues.size() < limit) {
                break;
            }
            minTaskGroupQueueId = taskGroupQueues.get(taskGroupQueues.size() - 1).getId();
        }
        log.debug("Success amend TaskGroupQueue status cost: {}/ms", taskGroupCoordinatorRoundTimeCost.getTime());
    }

    /**
     * Clear the TaskGroupQueue when the related {@link TaskInstance} is not exist or status is finished.
     */
    private void amendTaskGroupQueueStatus(List<TaskGroupQueue> taskGroupQueues) {
        final List<Integer> taskInstanceIds = taskGroupQueues.stream()
                .map(TaskGroupQueue::getTaskId)
                .collect(Collectors.toList());
        final Map<Integer, TaskInstance> taskInstanceMap = taskInstanceDao.queryByIds(taskInstanceIds)
                .stream()
                .collect(Collectors.toMap(TaskInstance::getId, Function.identity()));

        for (TaskGroupQueue taskGroupQueue : taskGroupQueues) {
            int taskId = taskGroupQueue.getTaskId();
            final TaskInstance taskInstance = taskInstanceMap.get(taskId);

            if (taskInstance == null) {
                log.warn("The TaskInstance: {} is not exist, will release the TaskGroupQueue: {}", taskId,
                        taskGroupQueue);
                deleteTaskGroupQueueSlot(taskGroupQueue);
                continue;
            }

            if (taskInstance.getState().isFinished()) {
                log.warn("The TaskInstance: {} state: {} finished, will release the TaskGroupQueue: {}",
                        taskInstance.getName(), taskInstance.getState(), taskGroupQueue);
                deleteTaskGroupQueueSlot(taskGroupQueue);
            }
        }
    }

    private void dealWithForceStartTaskGroupQueue() {
        // Find the force start task group queue(Which is inQueue and forceStart is YES)
        // Notify the related waiting task instance
        // Set the taskGroupQueue status to RELEASE and remove it from queue
        // We use limit here to avoid OOM, and we will retry to notify force start queue at next time
        int minTaskGroupQueueId = -1;
        int limit = DEFAULT_LIMIT;
        StopWatch taskGroupCoordinatorRoundTimeCost = StopWatch.createStarted();
        while (true) {
            final List<TaskGroupQueue> taskGroupQueues =
                    taskGroupQueueDao.queryWaitNotifyForceStartTaskGroupQueue(minTaskGroupQueueId, limit);
            if (CollectionUtils.isEmpty(taskGroupQueues)) {
                break;
            }
            dealWithForceStartTaskGroupQueue(taskGroupQueues);
            if (taskGroupQueues.size() < limit) {
                break;
            }
            minTaskGroupQueueId = taskGroupQueues.get(taskGroupQueues.size() - 1).getId();
        }
        log.debug("Success deal with force start TaskGroupQueue cost: {}/ms",
                taskGroupCoordinatorRoundTimeCost.getTime());
    }

    private void dealWithForceStartTaskGroupQueue(List<TaskGroupQueue> taskGroupQueues) {
        // Find the force start task group queue(Which is inQueue and forceStart is YES)
        // Notify the related waiting task instance
        // Set the taskGroupQueue status to RELEASE and remove it from queue
        for (final TaskGroupQueue taskGroupQueue : taskGroupQueues) {
            try {
                LogUtils.setTaskInstanceIdMDC(taskGroupQueue.getTaskId());
                // notify the waiting task instance
                // We notify first, it notify failed, the taskGroupQueue will be in queue, and then we will retry it
                // next time.
                notifyWaitingTaskInstance(taskGroupQueue);
                log.info("Notify the ForceStart waiting TaskInstance: {} for taskGroupQueue: {} success",
                        taskGroupQueue.getTaskName(),
                        taskGroupQueue.getId());

                deleteTaskGroupQueueSlot(taskGroupQueue);
                log.info("Release the force start TaskGroupQueue {}", taskGroupQueue);
            } catch (UnsupportedOperationException unsupportedOperationException) {
                deleteTaskGroupQueueSlot(taskGroupQueue);
                log.info(
                        "Notify the ForceStart TaskInstance: {} for taskGroupQueue: {} failed, will release the taskGroupQueue",
                        taskGroupQueue.getTaskName(), taskGroupQueue.getId(), unsupportedOperationException);
            } catch (Throwable throwable) {
                log.info("Notify the force start TaskGroupQueue {} failed", taskGroupQueue, throwable);
            } finally {
                LogUtils.removeTaskInstanceIdMDC();
            }
        }
    }

    private void dealWithWaitingTaskGroupQueue() {
        // Find the TaskGroup which usage < maxSize.
        // Find the highest priority inQueue task group queue(Which is inQueue and status is Waiting and force start is
        // NO) belong to the
        // task group.
        List<TaskGroup> taskGroups = taskGroupDao.queryAvailableTaskGroups();
        if (CollectionUtils.isEmpty(taskGroups)) {
            log.debug("There is no available task group");
            return;
        }
        for (TaskGroup taskGroup : taskGroups) {
            int availableSize = taskGroup.getGroupSize() - taskGroup.getUseSize();
            if (availableSize <= 0) {
                log.info("TaskGroup {} is full, available size is {}", taskGroup, availableSize);
                continue;
            }
            List<TaskGroupQueue> taskGroupQueues =
                    taskGroupQueueDao.queryAllInQueueTaskGroupQueueByGroupId(taskGroup.getId())
                            .stream()
                            .filter(taskGroupQueue -> Flag.NO.getCode() == taskGroupQueue.getForceStart())
                            .filter(taskGroupQueue -> TaskGroupQueueStatus.WAIT_QUEUE == taskGroupQueue.getStatus())
                            .limit(availableSize)
                            .collect(Collectors.toList());
            if (CollectionUtils.isEmpty(taskGroupQueues)) {
                log.debug("There is no waiting task group queue for task group {}", taskGroup.getName());
                continue;
            }
            for (TaskGroupQueue taskGroupQueue : taskGroupQueues) {
                try {
                    LogUtils.setTaskInstanceIdMDC(taskGroupQueue.getTaskId());
                    // Reduce the taskGroupSize
                    boolean acquireResult = taskGroupDao.acquireTaskGroupSlot(taskGroup.getId());
                    if (!acquireResult) {
                        log.error("Failed to acquire task group slot for task group {}", taskGroup);
                        continue;
                    }
                    // Notify the waiting task instance
                    // We notify first, it notify failed, the taskGroupQueue will be in queue, and then we will retry it
                    // next time.
                    notifyWaitingTaskInstance(taskGroupQueue);

                    // Set the taskGroupQueue status to ACQUIRE_SUCCESS and remove from WAITING queue
                    taskGroupQueue.setInQueue(Flag.YES.getCode());
                    taskGroupQueue.setStatus(TaskGroupQueueStatus.ACQUIRE_SUCCESS);
                    taskGroupQueue.setUpdateTime(new Date());
                    taskGroupQueueDao.updateById(taskGroupQueue);
                    log.info("Success acquire TaskGroupSlot for TaskGroupQueue: {}", taskGroupQueue);
                } catch (UnsupportedOperationException unsupportedOperationException) {
                    deleteTaskGroupQueueSlot(taskGroupQueue);
                    log.info(
                            "Notify the Waiting TaskInstance: {} for taskGroupQueue: {} failed, will release the taskGroupQueue",
                            taskGroupQueue.getTaskName(), taskGroupQueue.getId(), unsupportedOperationException);
                } catch (Throwable throwable) {
                    log.error("Notify Waiting TaskGroupQueue: {} failed", taskGroupQueue, throwable);
                } finally {
                    LogUtils.removeTaskInstanceIdMDC();
                }
            }
        }
    }

    @Override
    public boolean needAcquireTaskGroupSlot(final TaskInstance taskInstance) {
        if (taskInstance == null) {
            throw new IllegalArgumentException("The TaskInstance is null");
        }
        if (!TaskGroupUtils.isUsingTaskGroup(taskInstance)) {
            log.debug("The current TaskInstance doesn't use TaskGroup, no need to acquire TaskGroupSlot");
            return false;
        }
        TaskGroup taskGroup = taskGroupDao.queryById(taskInstance.getTaskGroupId());
        if (taskGroup == null) {
            log.warn("The current TaskGroup: {} does not exist, will not acquire TaskGroupSlot",
                    taskInstance.getTaskGroupId());
            return false;
        }
        return Flag.YES.equals(taskGroup.getStatus());
    }

    @Override
    public void acquireTaskGroupSlot(TaskInstance taskInstance) {
        if (taskInstance == null || taskInstance.getTaskGroupId() <= 0) {
            throw new IllegalArgumentException("The current TaskInstance does not use task group");
        }
        TaskGroup taskGroup = taskGroupDao.queryById(taskInstance.getTaskGroupId());
        if (taskGroup == null) {
            throw new IllegalArgumentException(
                    "The current TaskGroup: " + taskInstance.getTaskGroupId() + " does not exist");
        }
        // Write TaskGroupQueue in db, and then return wait TaskGroupCoordinator to notify it
        // Set the taskGroupQueue status to WAIT_QUEUE and add to queue
        // The queue only contains the taskGroupQueue which status is WAIT_QUEUE or ACQUIRE_SUCCESS
        Date now = new Date();
        TaskGroupQueue taskGroupQueue = TaskGroupQueue
                .builder()
                .taskId(taskInstance.getId())
                .taskName(taskInstance.getName())
                .groupId(taskInstance.getTaskGroupId())
                .workflowInstanceId(taskInstance.getWorkflowInstanceId())
                .priority(taskInstance.getTaskGroupPriority())
                .inQueue(Flag.YES.getCode())
                .forceStart(Flag.NO.getCode())
                .status(TaskGroupQueueStatus.WAIT_QUEUE)
                .createTime(now)
                .updateTime(now)
                .build();
        log.info("Success insert TaskGroupQueue: {} for TaskInstance: {}", taskGroupQueue, taskInstance.getName());
        taskGroupQueueDao.insert(taskGroupQueue);
    }

    @Override
    public boolean needToReleaseTaskGroupSlot(TaskInstance taskInstance) {
        if (taskInstance == null) {
            throw new IllegalArgumentException("The TaskInstance is null");
        }
        if (taskInstance.getTaskGroupId() <= 0) {
            log.debug("The current TaskInstance doesn't use TaskGroup, no need to release TaskGroupSlot");
            return false;
        }
        return true;
    }

    @Override
    public void releaseTaskGroupSlot(TaskInstance taskInstance) {
        if (taskInstance == null) {
            throw new IllegalArgumentException("The TaskInstance is null");
        }
        if (taskInstance.getTaskGroupId() <= 0) {
            log.warn("The task: {} is no need to release TaskGroupSlot", taskInstance.getName());
            return;
        }
        List<TaskGroupQueue> taskGroupQueues = taskGroupQueueDao.queryByTaskInstanceId(taskInstance.getId());
        for (TaskGroupQueue taskGroupQueue : taskGroupQueues) {
            deleteTaskGroupQueueSlot(taskGroupQueue);
        }
    }

    private void notifyWaitingTaskInstance(TaskGroupQueue taskGroupQueue) {
        // Find the related waiting task instance
        // send RPC to notify the waiting task instance
        TaskInstance taskInstance = taskInstanceDao.queryById(taskGroupQueue.getTaskId());
        if (taskInstance == null) {
            throw new UnsupportedOperationException(
                    "The TaskInstance: " + taskGroupQueue.getTaskId() + " is not exist, no need to notify");
        }
        // todo: We may need to add a new status to represent the task instance is waiting for task group slot
        if (taskInstance.getState() != TaskExecutionStatus.SUBMITTED_SUCCESS) {
            throw new UnsupportedOperationException(
                    "The TaskInstance: " + taskInstance.getId() + " state is " + taskInstance.getState()
                            + ", no need to notify");
        }
        WorkflowInstance workflowInstance = workflowInstanceDao.queryById(taskInstance.getWorkflowInstanceId());
        if (workflowInstance == null) {
            throw new UnsupportedOperationException(
                    "The WorkflowInstance: " + taskInstance.getWorkflowInstanceId()
                            + " is not exist, no need to notify");
        }
        if (workflowInstance.getState() != WorkflowExecutionStatus.RUNNING_EXECUTION) {
            throw new UnsupportedOperationException(
                    "The WorkflowInstance: " + workflowInstance.getId() + " state is " + workflowInstance.getState()
                            + ", no need to notify");
        }
        if (workflowInstance.getHost() == null || Constants.NULL.equals(workflowInstance.getHost())) {
            throw new UnsupportedOperationException(
                    "WorkflowInstance host is null, maybe it is in failover: " + workflowInstance);
        }

        TaskGroupSlotAcquireSuccessNotifyRequest taskGroupSlotAcquireSuccessNotifyRequest =
                TaskGroupSlotAcquireSuccessNotifyRequest.builder()
                        .workflowInstanceId(workflowInstance.getId())
                        .taskInstanceId(taskInstance.getId())
                        .build();

        TaskGroupSlotAcquireSuccessNotifyResponse taskGroupSlotAcquireSuccessNotifyResponse =
                Clients
                        .withService(ITaskInstanceController.class)
                        .withHost(workflowInstance.getHost())
                        .notifyTaskGroupSlotAcquireSuccess(taskGroupSlotAcquireSuccessNotifyRequest);
        if (!taskGroupSlotAcquireSuccessNotifyResponse.isSuccess()) {
            throw new UnsupportedOperationException(
                    "Notify TaskInstance: " + taskInstance.getId() + " failed: "
                            + taskGroupSlotAcquireSuccessNotifyResponse);
        }
        log.info("Wake up TaskInstance: {} success", taskInstance.getName());
    }

    private void deleteTaskGroupQueueSlot(TaskGroupQueue taskGroupQueue) {
        taskGroupQueueDao.deleteById(taskGroupQueue);
        log.info("Success release TaskGroupQueue: {}", taskGroupQueue);
    }

    @Override
    public synchronized void close() {
        if (!flag) {
            log.warn("TaskGroupCoordinator is already closed");
            return;
        }
        flag = false;
        try {
            if (internalThread != null) {
                internalThread.interrupt();
            }
        } catch (Exception ex) {
            log.error("Close internalThread failed", ex);
        }
        internalThread = null;
        log.info("TaskGroupCoordinator closed");
    }
}
