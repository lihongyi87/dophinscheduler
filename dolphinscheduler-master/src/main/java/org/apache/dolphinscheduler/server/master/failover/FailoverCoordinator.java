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

package org.apache.dolphinscheduler.server.master.failover;

import org.apache.dolphinscheduler.dao.entity.WorkflowInstance;
import org.apache.dolphinscheduler.dao.repository.WorkflowInstanceDao;
import org.apache.dolphinscheduler.plugin.task.api.enums.TaskExecutionStatus;
import org.apache.dolphinscheduler.registry.api.RegistryClient;
import org.apache.dolphinscheduler.registry.api.enums.RegistryNodeType;
import org.apache.dolphinscheduler.registry.api.utils.RegistryUtils;
import org.apache.dolphinscheduler.server.master.cluster.ClusterManager;
import org.apache.dolphinscheduler.server.master.cluster.MasterServerMetadata;
import org.apache.dolphinscheduler.server.master.cluster.WorkerServerMetadata;
import org.apache.dolphinscheduler.server.master.engine.IWorkflowRepository;
import org.apache.dolphinscheduler.server.master.engine.system.event.GlobalMasterFailoverEvent;
import org.apache.dolphinscheduler.server.master.engine.system.event.MasterFailoverEvent;
import org.apache.dolphinscheduler.server.master.engine.system.event.WorkerFailoverEvent;
import org.apache.dolphinscheduler.server.master.engine.task.runnable.ITaskExecutionRunnable;
import org.apache.dolphinscheduler.server.master.engine.workflow.runnable.IWorkflowExecutionRunnable;

import org.apache.commons.lang3.time.StopWatch;

import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * 故障转移协调器
 * 
 * 这个类是DolphinScheduler高可用性的核心组件之一，负责处理节点故障和任务转移。
 * 当Master或Worker节点出现故障时，这个组件会自动发现并处理故障节点的任务。
 * 
 * 主要功能：
 * 1. 全局Master故障转移：处理Master节点崩溃后的任务重新分配
 * 2. Master节点故障转移：将故障 Master 的任务转移到其他 Master
 * 3. Worker节点故障转移：将正在故障Worker上执行的任务重新调度
 * 
 * 简单理解：就像一个智能的“应急指挥中心”，在系统出现问题时自动重新组织工作。
 */
@Slf4j
@Component
public class FailoverCoordinator implements IFailoverCoordinator {

    /**
     * 注册中心客户端 - 用于检测节点状态和获取集群信息
     */
    @Autowired
    private RegistryClient registryClient;

    /**
     * 集群管理器 - 管理所有Master和Worker节点的元数据
     */
    @Autowired
    private ClusterManager clusterManager;

    /**
     * 工作流仓库 - 管理当前正在运行的工作流实例
     */
    @Autowired
    private IWorkflowRepository workflowRepository;

    /**
     * 任务故障转移处理器 - 专门处理单个任务的故障转移
     */
    @Autowired
    private TaskFailover taskFailover;

    /**
     * 工作流实例数据访问对象 - 用于查询数据库中的工作流信息
     */
    @Autowired
    private WorkflowInstanceDao workflowInstanceDao;

    /**
     * 工作流故障转移处理器 - 专门处理整个工作流的故障转移
     */
    @Autowired
    private WorkflowFailover workflowFailover;

    /**
     * 全局Master故障转移处理
     * 
     * 当集群启动或者检测到Master节点故障时，会触发这个方法。
     * 它会扫描所有在数据库中但没有正常Master处理的工作流，
     * 并将它们重新分配给健康的Master节点。
     * 
     * 简单理解：就像公司的“项目重新分配会议”，
     * 当某个项目经理离职后，需要重新分配他的项目给其他经理。
     * 
     * @param globalMasterFailoverEvent 全局Master故障转移事件
     */
    @Override
    public void globalMasterFailover(final GlobalMasterFailoverEvent globalMasterFailoverEvent) {
        // 创建计时器，用于统计故障转移耗时
        final StopWatch failoverTimeCost = StopWatch.createStarted();
        log.info("全局Master故障转移开始执行");
        
        // 从数据库查询所有包含未完成工作流的Master地址
        // 这些Master可能已经故障了，但它们的工作流还在数据库中
        final List<String> masterAddressWhichContainsUnFinishedWorkflow =
                workflowInstanceDao.queryNeedFailoverMasters();
                
        // 遍历每个潜在故障Master的地址
        for (final String masterAddress : masterAddressWhichContainsUnFinishedWorkflow) {
            // 检查这个Master是否还活着（可能只是网络问题而不是真的故障）
            final Optional<MasterServerMetadata> aliveMasterOptional =
                    clusterManager.getMasterClusters().getServer(masterAddress);
                    
            if (aliveMasterOptional.isPresent()) {
                // 如果这个Master还活着，那么使用这个Master的启动时间作为故障转移的截止时间
                final MasterServerMetadata aliveMasterServerMetadata = aliveMasterOptional.get();
                log.info("The master[{}] is alive, do global master failover on it", aliveMasterServerMetadata);
                doMasterFailover(
                        masterAddress,
                        aliveMasterServerMetadata.getServerStartupTime(),
                        RegistryUtils.getGlobalMasterFailoverNodePath(
                                masterAddress));
            } else {
                // If the master is not alive, then we use the event time as the failover deadline.
                log.info("The master[{}] is not alive, do global master failover on it", masterAddress);
                doMasterFailover(
                        masterAddress,
                        globalMasterFailoverEvent.getEventTime().getTime(),
                        RegistryUtils.getGlobalMasterFailoverNodePath(masterAddress));
            }
        }

        failoverTimeCost.stop();
        log.info("Global master failover finished, cost: {}/ms", failoverTimeCost.getTime());
    }

    @Override
    public void failoverMaster(final MasterFailoverEvent masterFailoverEvent) {
        final MasterServerMetadata masterServerMetadata = masterFailoverEvent.getMasterServerMetadata();
        log.info("Master[{}] failover starting", masterServerMetadata);
        final String masterAddress = masterServerMetadata.getAddress();

        final Optional<MasterServerMetadata> aliveMasterOptional =
                clusterManager.getMasterClusters().getServer(masterAddress);
        if (aliveMasterOptional.isPresent()) {
            final MasterServerMetadata aliveMasterServerMetadata = aliveMasterOptional.get();
            if (aliveMasterServerMetadata.getServerStartupTime() == masterServerMetadata.getServerStartupTime()) {
                log.info("The master[{}] is alive, maybe it reconnect to registry skip failover", masterServerMetadata);
                return;
            }
        }
        doMasterFailover(
                masterServerMetadata.getAddress(),
                masterFailoverEvent.getEventTime().getTime(),
                RegistryUtils.getFailoveredNodePath(
                        masterServerMetadata.getAddress(),
                        masterServerMetadata.getServerStartupTime(),
                        masterServerMetadata.getProcessId()));
    }

    /**
     * Do master failover.
     * <p> Will failover the workflow which is scheduled by the master and the workflow's fire time is before the maxWorkflowFireTime.
     */
    private void doMasterFailover(final String masterAddress,
                                  final long workflowFailoverDeadline,
                                  final String masterFailoverNodePath) {
        // We use lock to avoid multiple master failover at the same time.
        // Once the workflow has been failovered, then it's state will be changed to FAILOVER
        // Once the FAILOVER workflow has been refired, then it's host will be changed to the new master and have a new
        // start time.
        // So if a master has been failovered multiple times, there is no problem.
        final StopWatch failoverTimeCost = StopWatch.createStarted();
        registryClient.getLock(RegistryUtils.getMasterFailoverLockPath(masterAddress));
        try {
            // If the master has already been failovered, then we skip the failover.
            if (registryClient.exists(masterFailoverNodePath)
                    && String.valueOf(workflowFailoverDeadline).equals(registryClient.get(masterFailoverNodePath))) {
                log.error("The master[{}/{}] is exist at: {}, means it has already been failovered, skip failover",
                        masterAddress,
                        workflowFailoverDeadline,
                        masterFailoverNodePath);
                return;
            }
            final List<WorkflowInstance> needFailoverWorkflows =
                    getFailoverWorkflowsForMaster(masterAddress, new Date(workflowFailoverDeadline));
            needFailoverWorkflows.forEach(workflowFailover::failoverWorkflow);
            registryClient.persist(masterFailoverNodePath, String.valueOf(workflowFailoverDeadline));
            failoverTimeCost.stop();
            log.info("Master[{}] failover {} workflows finished, cost: {}/ms",
                    masterAddress,
                    needFailoverWorkflows.size(),
                    failoverTimeCost.getTime());
        } finally {
            registryClient.releaseLock(RegistryNodeType.MASTER_FAILOVER_LOCK.getRegistryPath());
        }
    }

    private List<WorkflowInstance> getFailoverWorkflowsForMaster(final String masterAddress,
                                                                 final Date masterCrashTime) {
        // todo: use page query
        final List<WorkflowInstance> workflowInstances =
                workflowInstanceDao.queryNeedFailoverWorkflowInstances(masterAddress);
        return workflowInstances.stream()
                .filter(workflowInstance -> {

                    if (workflowRepository.contains(workflowInstance.getId())) {
                        return false;
                    }

                    // todo: If the first time run workflow have the restartTime, then we can only check this
                    final Date restartTime = workflowInstance.getRestartTime();
                    if (restartTime != null) {
                        return restartTime.before(masterCrashTime);
                    }

                    final Date startTime = workflowInstance.getStartTime();
                    return startTime.before(masterCrashTime);
                })
                .collect(Collectors.toList());
    }

    @Override
    public void failoverWorker(final WorkerFailoverEvent workerFailoverEvent) {
        final WorkerServerMetadata workerServerMetadata = workerFailoverEvent.getWorkerServerMetadata();
        log.info("Worker[{}] failover starting", workerServerMetadata);

        final Optional<WorkerServerMetadata> aliveWorkerOptional =
                clusterManager.getWorkerClusters().getServer(workerServerMetadata.getAddress());
        if (aliveWorkerOptional.isPresent()) {
            final WorkerServerMetadata aliveWorkerServerMetadata = aliveWorkerOptional.get();
            if (aliveWorkerServerMetadata.getServerStartupTime() == workerServerMetadata.getServerStartupTime()) {
                log.info("The worker[{}] is alive, maybe it reconnect to registry skip failover", workerServerMetadata);
                return;
            }
        }
        doWorkerFailover(
                workerServerMetadata.getAddress(),
                System.currentTimeMillis(),
                RegistryUtils.getFailoveredNodePath(
                        workerServerMetadata.getAddress(),
                        workerServerMetadata.getServerStartupTime(),
                        workerServerMetadata.getProcessId()));
    }

    private void doWorkerFailover(final String workerAddress,
                                  final long taskFailoverDeadline,
                                  final String workerFailoverNodePath) {
        final StopWatch failoverTimeCost = StopWatch.createStarted();
        // we don't check the workerFailoverNodePath exist, since the worker may be failovered multiple master

        final List<ITaskExecutionRunnable> needFailoverTasks =
                getFailoverTaskForWorker(workerAddress, new Date(taskFailoverDeadline));
        needFailoverTasks.forEach(taskFailover::failoverTask);

        registryClient.persist(
                workerFailoverNodePath,
                String.valueOf(System.currentTimeMillis()));
        failoverTimeCost.stop();
        log.info("Worker[{}] failover {} tasks finished, cost: {}/ms",
                workerAddress,
                needFailoverTasks.size(),
                failoverTimeCost.getTime());
    }

    private List<ITaskExecutionRunnable> getFailoverTaskForWorker(final String workerAddress,
                                                                  final Date taskFailoverDeadline) {
        return workflowRepository.getAll()
                .stream()
                .map(IWorkflowExecutionRunnable::getWorkflowExecutionGraph)
                .flatMap(workflowExecutionGraph -> workflowExecutionGraph.getActiveTaskExecutionRunnable().stream())
                .filter(ITaskExecutionRunnable::isTaskInstanceInitialized)
                .filter(taskExecutionRunnable -> workerAddress
                        .equals(taskExecutionRunnable.getTaskInstance().getHost()))
                .filter(taskExecutionRunnable -> {
                    final TaskExecutionStatus state = taskExecutionRunnable.getTaskInstance().getState();
                    return state == TaskExecutionStatus.DISPATCH || state == TaskExecutionStatus.RUNNING_EXECUTION;
                })
                .filter(taskExecutionRunnable -> {
                    // The submitTime should not be null.
                    // This is a bad case unless someone manually set the submitTime to null.
                    final Date submitTime = taskExecutionRunnable.getTaskInstance().getSubmitTime();
                    return submitTime != null && submitTime.before(taskFailoverDeadline);
                })
                .collect(Collectors.toList());
    }

}
