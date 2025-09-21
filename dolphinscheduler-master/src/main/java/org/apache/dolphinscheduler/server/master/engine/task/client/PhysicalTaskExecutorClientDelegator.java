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

package org.apache.dolphinscheduler.server.master.engine.task.client;

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.extract.base.client.Clients;
import org.apache.dolphinscheduler.extract.base.utils.Host;
import org.apache.dolphinscheduler.extract.worker.IPhysicalTaskExecutorOperator;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.server.master.cluster.loadbalancer.IWorkerLoadBalancer;
import org.apache.dolphinscheduler.server.master.config.MasterConfig;
import org.apache.dolphinscheduler.server.master.engine.task.runnable.ITaskExecutionRunnable;
import org.apache.dolphinscheduler.server.master.exception.dispatch.TaskDispatchException;
import org.apache.dolphinscheduler.task.executor.eventbus.ITaskExecutorLifecycleEventReporter;
import org.apache.dolphinscheduler.task.executor.operations.TaskExecutorDispatchRequest;
import org.apache.dolphinscheduler.task.executor.operations.TaskExecutorDispatchResponse;
import org.apache.dolphinscheduler.task.executor.operations.TaskExecutorKillRequest;
import org.apache.dolphinscheduler.task.executor.operations.TaskExecutorKillResponse;
import org.apache.dolphinscheduler.task.executor.operations.TaskExecutorPauseRequest;
import org.apache.dolphinscheduler.task.executor.operations.TaskExecutorPauseResponse;
import org.apache.dolphinscheduler.task.executor.operations.TaskExecutorReassignMasterRequest;
import org.apache.dolphinscheduler.task.executor.operations.TaskExecutorReassignMasterResponse;

import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * 物理任务执行器客户端代理
 *
 * 这个类是专门处理物理任务的客户端代理实现。物理任务是指需要在Worker节点上
 * 执行具体代码、脚本或程序的任务，如Shell脚本、SQL查询、Python程序等。
 *
 * 支持的物理任务类型：
 * - Shell任务：执行Linux/Windows命令和脚本
 * - SQL任务：在数据库中执行SQL语句
 * - Python任务：执行Python脚本
 * - Spark任务：提交Spark作业
 * - Flink任务：提交Flink作业
 * - DataX任务：执行数据同步作业
 * - HTTP任务：发送HTTP请求
 * - 等其他需要在Worker节点执行的任务类型
 *
 * 执行特点：
 * - 任务在Worker节点上执行，需要通过负载均衡选择合适的Worker
 * - 支持工作组（WorkerGroup）概念，可以指定任务在特定的Worker集群中执行
 * - 支持主机重新分配，当Master故障转移时可以重新分配任务管理权
 * - 任务执行涉及资源分配、环境准备、代码执行等复杂过程
 *
 * 与逻辑任务的区别：
 * - 逻辑任务：在Master节点执行流程控制和状态判断
 * - 物理任务：在Worker节点执行具体的代码和脚本
 *
 * RPC通信机制：
 * - 使用IPhysicalTaskExecutorOperator接口进行通信
 * - 通信目标是远程Worker节点的物理任务执行器
 * - 需要处理网络延迟、连接超时、重试等复杂情况
 * - 支持任务分发、暂停、杀死和生命周期事件确认
 *
 * 负载均衡策略：
 * - 通过IWorkerLoadBalancer选择最适合的Worker节点
 * - 考虑Worker节点的负载、可用资源、工作组配置等因素
 * - 支持权重轮询、随机选择、最少任务数等多种负载均衡算法
 */
@Slf4j
@Component
public class PhysicalTaskExecutorClientDelegator implements ITaskExecutorClientDelegator {

    /**
     * Master节点配置信息
     * 包含Master节点的地址、端口等配置信息，用于主机重新分配操作
     */
    @Autowired
    private MasterConfig masterConfig;

    /**
     * Worker节点负载均衡器
     * 负责从Worker集群中选择最适合的节点来执行物理任务，考虑负载、资源、工作组等因素
     */
    @Autowired
    private IWorkerLoadBalancer workerLoadBalancer;

    /**
     * 分发物理任务到Worker节点的物理任务执行器
     *
     * 这是物理任务分发的核心方法，负责将任务分发到适合的Worker节点执行。
     * 这个过程比逻辑任务更加复杂，需要考虑网络通信、资源分配等因素。
     *
     * 分发流程：
     * 1. 从Worker集群中选择适合的节点（负载均衡）
     * 2. 设置任务执行上下文的目标Worker地址
     * 3. 通过RPC调用将任务发送到目标Worker节点
     * 4. 检查分发响应结果，处理异常情况
     *
     * 选择Worker的考虑因素：
     * - 工作组（WorkerGroup）限制
     * - Worker节点的当前负载
     * - Worker节点的可用资源（CPU、内存等）
     * - 网络连通性和延迟
     *
     * @param taskExecutionRunnable 要分发的物理任务执行对象
     * @throws TaskDispatchException 分发失败时抛出，可能原因包括无可用Worker、RPC通信失败等
     */
    @Override
    public void dispatch(final ITaskExecutionRunnable taskExecutionRunnable) throws TaskDispatchException {
        final TaskExecutionContext taskExecutionContext = taskExecutionRunnable.getTaskExecutionContext();
        final String taskName = taskExecutionContext.getTaskName();

        // 使用负载均衡器从指定的工作组中选择一个适合的Worker节点
        final String physicalTaskExecutorAddress = workerLoadBalancer
                .select(taskExecutionContext.getWorkerGroup())
                .map(Host::of)
                .map(Host::getAddress)
                .orElseThrow(() -> new TaskDispatchException(
                        String.format("Cannot find the host to dispatch Task[id=%s, name=%s, workerGroup=%s]",
                                taskExecutionContext.getTaskInstanceId(), taskName,
                                taskExecutionContext.getWorkerGroup())));

        // 设置任务执行主机为选定的Worker节点
        taskExecutionContext.setHost(physicalTaskExecutorAddress);
        taskExecutionRunnable.getTaskInstance().setHost(physicalTaskExecutorAddress);

        try {
            // 通过RPC客户端向Worker节点的物理任务执行器发送分发请求
            final TaskExecutorDispatchResponse taskExecutorDispatchResponse = Clients
                    .withService(IPhysicalTaskExecutorOperator.class)
                    .withHost(physicalTaskExecutorAddress)
                    .dispatchTask(TaskExecutorDispatchRequest.of(taskExecutionRunnable.getTaskExecutionContext()));

            // 检查分发是否成功
            if (!taskExecutorDispatchResponse.isDispatchSuccess()) {
                throw new TaskDispatchException(
                        "Dispatch task: " + taskName + " to " + physicalTaskExecutorAddress + " failed: "
                                + taskExecutorDispatchResponse);
            }
        } catch (TaskDispatchException e) {
            // 直接重新抛出任务分发异常
            throw e;
        } catch (Exception e) {
            // 将其他异常包装为任务分发异常
            throw new TaskDispatchException(
                    "Dispatch task: " + taskName + " to " + physicalTaskExecutorAddress + " failed", e);
        }
    }

    /**
     * 重新分配Master主机（物理任务支持）
     *
     * 当Master节点发生故障转移时，需要将正在Worker节点上执行的物理任务的管理权
     * 从故障Master转移到新的Master。这个操作对于物理任务是必要的，因为任务
     * 在Worker节点上执行，需要告知Worker新的Master地址。
     *
     * 重新分配流程：
     * 1. 检查任务实例是否已经初始化
     * 2. 检查任务是否已经分发到Worker节点
     * 3. 构建重新分配请求，包含新Master的地址信息
     * 4. 向Worker节点发送重新分配请求
     * 5. 处理响应结果并记录日志
     *
     * 注意事项：
     * - 只有已经分发到Worker的任务才需要重新分配
     * - 重新分配不会中断任务执行，只是更新Master信息
     * - Worker在接收到重新分配请求后会更新内部的Master地址
     *
     * @param taskExecutionRunnable 需要重新分配的物理任务执行对象
     * @return true-重新分配成功, false-重新分配失败或不需要重新分配
     */
    @Override
    public boolean reassignMasterHost(final ITaskExecutionRunnable taskExecutionRunnable) {
        final String taskName = taskExecutionRunnable.getName();
        // 检查任务实例是否已经初始化
        checkArgument(taskExecutionRunnable.isTaskInstanceInitialized(),
                "Task " + taskName + "is not initialized cannot take-over");

        final TaskInstance taskInstance = taskExecutionRunnable.getTaskInstance();
        final String taskExecutorHost = taskInstance.getHost();
        // 检查任务是否已经分发到Worker节点
        if (StringUtils.isEmpty(taskExecutorHost)) {
            log.debug(
                    "The task executor: {} host is empty, cannot take-over, this might caused by the task hasn't dispatched",
                    taskName);
            return false;
        }

        // 构建重新分配请求，包含新Master的地址信息
        final TaskExecutorReassignMasterRequest taskExecutorReassignMasterRequest =
                TaskExecutorReassignMasterRequest.builder()
                        .taskInstanceId(taskInstance.getId())
                        .workflowHost(masterConfig.getMasterAddress())
                        .build();

        // 向Worker节点发送重新分配请求
        final TaskExecutorReassignMasterResponse taskExecutorReassignMasterResponse =
                Clients
                        .withService(IPhysicalTaskExecutorOperator.class)
                        .withHost(taskInstance.getHost())
                        .reassignWorkflowInstanceHost(taskExecutorReassignMasterRequest);

        // 处理响应结果并记录日志
        boolean success = taskExecutorReassignMasterResponse.isSuccess();
        if (success) {
            log.info("Reassign master host {} to {} successfully", taskExecutorHost, taskName);
        } else {
            log.info("Reassign master host {} on {} failed with response {}",
                    taskExecutorHost,
                    taskName,
                    taskExecutorReassignMasterResponse);
        }
        return success;
    }

    /**
     * 暂停物理任务执行
     *
     * 向Worker节点的物理任务执行器发送暂停指令，要求暂停正在执行的物理任务。
     * 物理任务的暂停操作相对复杂，可能涉及外部进程的暂停、资源的保存等。
     *
     * 物理任务暂停特点：
     * - 需要通过网络通信到远程Worker节点
     * - 可能涉及外部进程的暂停（如Shell进程、数据库连接）
     * - 需要保存中间状态和结果，以便后续恢复
     * - 部分任务类型可能不支持暂停（如一次性执行的脚本）
     * - 暂停操作的成功率取决于任务类型和当前执行状态
     *
     * @param taskExecutionRunnable 要暂停的物理任务执行对象
     */
    @Override
    public void pause(final ITaskExecutionRunnable taskExecutionRunnable) {
        final TaskInstance taskInstance = taskExecutionRunnable.getTaskInstance();
        final String executorHost = taskInstance.getHost();
        final String taskName = taskInstance.getName();
        checkArgument(StringUtils.isNotEmpty(executorHost), "Executor host is empty");

        // 向Worker节点的物理任务执行器发送暂停请求
        final TaskExecutorPauseResponse pauseResponse = Clients
                .withService(IPhysicalTaskExecutorOperator.class)
                .withHost(taskInstance.getHost())
                .pauseTask(TaskExecutorPauseRequest.of(taskInstance.getId()));

        // 记录暂停操作结果
        if (pauseResponse.isSuccess()) {
            log.info("Pause task {} on executor {} successfully", taskName, executorHost);
        } else {
            log.warn("Pause task {} on executor {} failed with response {}", taskName, executorHost, pauseResponse);
        }
    }

    /**
     * 杀死物理任务执行
     *
     * 向Worker节点的物理任务执行器发送杀死指令，强制终止正在执行的物理任务。
     * 物理任务的杀死操作相对复杂，需要处理各种进程和资源清理问题。
     *
     * 物理任务杀死特点：
     * - 需要通过网络通信到远程Worker节点
     * - 涉及外部进程的强制终止（如kill -9）
     * - 需要清理相关资源（文件、网络连接、临时数据等）
     * - 可能需要释放占用的系统资源（内存、CPU、磁盘等）
     * - 杀死操作的耗时取决于任务类型和当前执行状态
     * - 部分情况下可能需要分阶段执行（先正常终止，再强制杀死）
     *
     * @param taskExecutionRunnable 要杀死的物理任务执行对象
     */
    @Override
    public void kill(final ITaskExecutionRunnable taskExecutionRunnable) {
        final TaskInstance taskInstance = taskExecutionRunnable.getTaskInstance();
        final String executorHost = taskInstance.getHost();
        final String taskName = taskInstance.getName();
        checkArgument(StringUtils.isNotEmpty(executorHost), "Executor host is empty");

        // 向Worker节点的物理任务执行器发送杀死请求
        final TaskExecutorKillResponse killResponse = Clients
                .withService(IPhysicalTaskExecutorOperator.class)
                .withHost(executorHost)
                .killTask(TaskExecutorKillRequest.of(taskInstance.getId()));

        // 记录杀死操作结果
        if (killResponse.isSuccess()) {
            log.info("Kill task {} on executor {} successfully", taskName, executorHost);
        } else {
            log.warn("Kill task {} on executor {} failed with response {}", taskName, executorHost, killResponse);
        }
    }

    /**
     * 确认物理任务执行器生命周期事件
     *
     * 向Worker节点的物理任务执行器发送生命周期事件确认消息。由于物理任务
     * 在远程Worker节点上执行，这个确认操作需要通过网络通信完成。
     *
     * 物理任务生命周期事件包括：
     * - 任务开始执行（进程创建、环境准备）
     * - 任务执行中（进度更新、日志输出）
     * - 任务执行成功（正常结束、返回结果）
     * - 任务执行失败（异常终止、错误信息）
     * - 任务被暂停（保存状态、停止执行）
     * - 任务被恢复（恢复状态、继续执行）
     * - 任务被杀死（强制终止、资源清理）
     *
     * 网络通信特点：
     * - 需要处理网络延迟和丢包情况
     * - 支持自动重试机制
     * - 具备超时保护机制
     * - 支持幂等操作，重复确认不会产生副作用
     *
     * @param taskExecutionRunnable 相关的物理任务执行对象
     * @param taskExecutorLifecycleEventAck 生命周期事件确认消息
     */
    @Override
    public void ackTaskExecutorLifecycleEvent(final ITaskExecutionRunnable taskExecutionRunnable,
                                              final ITaskExecutorLifecycleEventReporter.TaskExecutorLifecycleEventAck taskExecutorLifecycleEventAck) {
        final TaskInstance taskInstance = taskExecutionRunnable.getTaskInstance();
        final String executorHost = taskInstance.getHost();
        checkArgument(StringUtils.isNotEmpty(executorHost), "Executor host is empty");

        // 向Worker节点的物理任务执行器发送生命周期事件确认
        Clients
                .withService(IPhysicalTaskExecutorOperator.class)
                .withHost(executorHost)
                .ackPhysicalTaskExecutorLifecycleEvent(taskExecutorLifecycleEventAck);
    }

}
