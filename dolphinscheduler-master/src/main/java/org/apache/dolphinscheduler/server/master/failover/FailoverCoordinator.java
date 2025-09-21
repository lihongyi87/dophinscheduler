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
        // 性能监控对于大规模集群的故障转移优化很重要
        final StopWatch failoverTimeCost = StopWatch.createStarted();
        log.info("全局Master故障转移开始执行");

        // 从数据库查询所有包含未完成工作流的Master地址
        // 这些Master可能已经故障了，但它们的工作流还在数据库中
        // 查询条件：工作流状态为运行中且分配给了特定的Master
        final List<String> masterAddressWhichContainsUnFinishedWorkflow =
                workflowInstanceDao.queryNeedFailoverMasters();

        // 遍历每个潜在故障Master的地址
        // 每个Master地址代表一个可能需要故障转移的节点
        for (final String masterAddress : masterAddressWhichContainsUnFinishedWorkflow) {
            // 检查这个Master是否还活着（可能只是网络问题而不是真的故障）
            // 通过集群管理器查询当前活跃的Master列表
            final Optional<MasterServerMetadata> aliveMasterOptional =
                    clusterManager.getMasterClusters().getServer(masterAddress);

            if (aliveMasterOptional.isPresent()) {
                // 如果这个Master还活着，那么使用这个Master的启动时间作为故障转移的截止时间
                // 启动时间用于判断哪些工作流是在Master重启前启动的，需要故障转移
                final MasterServerMetadata aliveMasterServerMetadata = aliveMasterOptional.get();
                log.info("Master[{}] 仍然存活，对其执行全局Master故障转移", aliveMasterServerMetadata);
                doMasterFailover(
                        masterAddress,
                        aliveMasterServerMetadata.getServerStartupTime(),     // 使用Master启动时间作为截止时间
                        RegistryUtils.getGlobalMasterFailoverNodePath(        // 构建全局故障转移路径
                                masterAddress));
            } else {
                // 如果Master不存活，则使用事件时间作为故障转移截止时间
                // 事件时间是检测到故障的时间，之前启动的工作流都需要故障转移
                log.info("Master[{}] 不存活，对其执行全局Master故障转移", masterAddress);
                doMasterFailover(
                        masterAddress,
                        globalMasterFailoverEvent.getEventTime().getTime(),   // 使用故障检测时间作为截止时间
                        RegistryUtils.getGlobalMasterFailoverNodePath(masterAddress));
            }
        }

        // 停止计时器并记录总耗时
        failoverTimeCost.stop();
        log.info("全局Master故障转移完成，耗时: {}/ms", failoverTimeCost.getTime());
    }

    /**
     * 执行特定Master节点的故障转移
     *
     * 当接收到Master故障转移事件时，执行以下操作：
     * 1. 验证Master节点是否真的故障（避免网络抖动导致的误判）
     * 2. 调用具体的故障转移逻辑处理该Master的工作流
     *
     * <p>故障验证机制：
     * <ul>
     *   <li>通过集群管理器检查Master节点是否仍然存活</li>
     *   <li>比较服务器启动时间，判断是否为同一个实例</li>
     *   <li>如果节点重新连接到注册中心，则跳过故障转移</li>
     * </ul>
     *
     * @param masterFailoverEvent Master故障转移事件，包含故障节点的元数据信息
     */
    @Override
    public void failoverMaster(final MasterFailoverEvent masterFailoverEvent) {
        // 从事件中获取故障Master的元数据信息
        final MasterServerMetadata masterServerMetadata = masterFailoverEvent.getMasterServerMetadata();
        log.info("Master[{}] 故障转移开始执行", masterServerMetadata);

        // 获取Master节点的网络地址
        final String masterAddress = masterServerMetadata.getAddress();

        // 验证Master节点是否真的故障，避免网络抖动导致的误判
        // 网络抖动可能导致节点临时从注册中心消失，但实际上还在运行
        final Optional<MasterServerMetadata> aliveMasterOptional =
                clusterManager.getMasterClusters().getServer(masterAddress);

        if (aliveMasterOptional.isPresent()) {
            // 如果找到了同地址的活跃Master，需要进一步检查是否为同一实例
            final MasterServerMetadata aliveMasterServerMetadata = aliveMasterOptional.get();

            // 通过启动时间判断是否为同一个Master实例
            // 如果启动时间相同，说明是同一个实例重新连接，不需要故障转移
            if (aliveMasterServerMetadata.getServerStartupTime() == masterServerMetadata.getServerStartupTime()) {
                log.info("Master[{}] 仍然存活，可能重新连接到注册中心，跳过故障转移", masterServerMetadata);
                return;
            }
        }

        // 执行具体的Master故障转移逻辑
        // 传入故障Master的地址、故障时间和在注册中心的标记路径
        doMasterFailover(
                masterServerMetadata.getAddress(),                    // Master地址
                masterFailoverEvent.getEventTime().getTime(),         // 故障发生时间
                RegistryUtils.getFailoveredNodePath(                  // 构建故障转移标记路径
                        masterServerMetadata.getAddress(),
                        masterServerMetadata.getServerStartupTime(),
                        masterServerMetadata.getProcessId()));
    }

    /**
     * 执行Master故障转移的核心逻辑
     *
     * 这是Master故障转移的核心实现方法，负责将故障Master上的工作流转移到其他健康节点。
     * 该方法通过分布式锁确保同一时间只有一个Master执行故障转移，避免重复处理。
     *
     * <p>故障转移流程：
     * <ol>
     *   <li>获取分布式锁，防止多个Master同时处理同一故障节点</li>
     *   <li>检查该Master是否已经被故障转移过，避免重复处理</li>
     *   <li>查找需要故障转移的工作流（根据启动时间筛选）</li>
     *   <li>逐个处理工作流的故障转移</li>
     *   <li>在注册中心记录故障转移完成标记</li>
     * </ol>
     *
     * <p>状态同步机制：
     * <ul>
     *   <li>工作流状态从运行中变更为FAILOVER状态</li>
     *   <li>重新触发的工作流会分配给新的Master并更新启动时间</li>
     *   <li>支持Master被多次故障转移而不产生问题</li>
     * </ul>
     *
     * <p>幂等性保障：
     * <ul>
     *   <li>通过注册中心的路径检查避免重复故障转移</li>
     *   <li>使用故障转移截止时间作为去重标识</li>
     *   <li>分布式锁确保并发安全</li>
     * </ul>
     *
     * @param masterAddress Master节点地址
     * @param workflowFailoverDeadline 工作流故障转移截止时间，早于此时间的工作流才会被转移
     * @param masterFailoverNodePath 注册中心中记录故障转移状态的节点路径
     */
    private void doMasterFailover(final String masterAddress,
                                  final long workflowFailoverDeadline,
                                  final String masterFailoverNodePath) {
        // 使用分布式锁避免多个Master同时执行故障转移
        // 工作流被故障转移后状态会变为FAILOVER
        // FAILOVER状态的工作流重新触发时，会分配给新的Master并有新的启动时间
        // 因此即使Master被多次故障转移也不会有问题
        final StopWatch failoverTimeCost = StopWatch.createStarted();

        // 获取分布式锁，锁路径基于Master地址生成
        // 这确保同一时间只有一个Master能处理特定故障Master的转移
        registryClient.getLock(RegistryUtils.getMasterFailoverLockPath(masterAddress));

        try {
            // 检查该Master是否已经被故障转移过，避免重复处理
            // 通过检查注册中心中的标记节点和时间戳来判断
            if (registryClient.exists(masterFailoverNodePath)
                    && String.valueOf(workflowFailoverDeadline).equals(registryClient.get(masterFailoverNodePath))) {
                log.error("Master[{}/{}] 在路径 {} 已存在，表示已经被故障转移过，跳过故障转移",
                        masterAddress,
                        workflowFailoverDeadline,
                        masterFailoverNodePath);
                return;
            }

            // 查找需要故障转移的工作流实例
            // 只有在故障转移截止时间之前启动的工作流才需要被转移
            final List<WorkflowInstance> needFailoverWorkflows =
                    getFailoverWorkflowsForMaster(masterAddress, new Date(workflowFailoverDeadline));

            // 逐个执行工作流故障转移
            // 每个工作流会被重新分配给当前活跃的Master节点
            needFailoverWorkflows.forEach(workflowFailover::failoverWorkflow);

            // 在注册中心记录故障转移完成标记
            // 存储截止时间作为标记，防止后续重复故障转移
            registryClient.persist(masterFailoverNodePath, String.valueOf(workflowFailoverDeadline));

            // 停止计时器并记录性能数据
            failoverTimeCost.stop();
            log.info("Master[{}] 故障转移完成，处理了 {} 个工作流，耗时: {}/ms",
                    masterAddress,
                    needFailoverWorkflows.size(),
                    failoverTimeCost.getTime());
        } finally {
            // 在finally块中释放分布式锁，确保即使出现异常也能释放锁
            // 避免死锁导致其他Master无法执行故障转移
            registryClient.releaseLock(RegistryNodeType.MASTER_FAILOVER_LOCK.getRegistryPath());
        }
    }

    /**
     * 获取指定Master节点需要故障转移的工作流实例列表
     *
     * 该方法通过时间判断筛选出需要故障转移的工作流实例，确保只转移在Master故障前启动的工作流。
     * 这样可以避免转移那些在Master故障后由其他Master启动的工作流。
     *
     * <p>筛选逻辑：
     * <ul>
     *   <li>排除当前工作流仓库中已存在的工作流（说明已在其他Master上运行）</li>
     *   <li>优先使用重启时间进行判断（如果存在）</li>
     *   <li>否则使用工作流的启动时间进行判断</li>
     *   <li>只有在Master故障时间之前启动的工作流才需要故障转移</li>
     * </ul>
     *
     * <p>时间判断的重要性：
     * <ul>
     *   <li>防止转移已经在其他Master上重新启动的工作流</li>
     *   <li>确保故障转移的精确性和一致性</li>
     *   <li>避免工作流的重复执行</li>
     * </ul>
     *
     * @param masterAddress 故障Master的地址
     * @param masterCrashTime Master故障发生的时间
     * @return 需要故障转移的工作流实例列表
     */
    private List<WorkflowInstance> getFailoverWorkflowsForMaster(final String masterAddress,
                                                                 final Date masterCrashTime) {
        // TODO: 使用分页查询优化性能，避免大量工作流时的内存压力
        // 从数据库查询分配给指定Master且状态为运行中的工作流实例
        final List<WorkflowInstance> workflowInstances =
                workflowInstanceDao.queryNeedFailoverWorkflowInstances(masterAddress);

        // 使用流式处理过滤出真正需要故障转移的工作流
        return workflowInstances.stream()
                .filter(workflowInstance -> {
                    // 如果工作流已经在当前工作流仓库中，说明已经在其他Master上运行，不需要故障转移
                    // 这种情况可能发生在网络分区恢复后
                    if (workflowRepository.contains(workflowInstance.getId())) {
                        return false;
                    }

                    // TODO: 如果首次运行的工作流有重启时间，则只需要检查重启时间
                    // 重启时间表示工作流最后一次启动的时间
                    final Date restartTime = workflowInstance.getRestartTime();
                    if (restartTime != null) {
                        // 重启时间在Master故障时间之前，需要故障转移
                        // 这确保只转移在Master故障前启动的工作流
                        return restartTime.before(masterCrashTime);
                    }

                    // 使用工作流启动时间进行判断
                    // 如果没有重启时间，则使用初始启动时间
                    final Date startTime = workflowInstance.getStartTime();
                    // 只有在Master故障前启动的工作流才需要故障转移
                    return startTime.before(masterCrashTime);
                })
                .collect(Collectors.toList());
    }

    /**
     * 执行Worker节点的故障转移
     *
     * 当接收到Worker故障转移事件时，执行以下操作：
     * 1. 验证Worker节点是否真的故障（避免网络抖动导致的误判）
     * 2. 调用具体的故障转移逻辑处理该Worker上的任务
     *
     * <p>与Master故障转移的区别：
     * <ul>
     *   <li>处理粒度更细：针对单个任务而非整个工作流</li>
     *   <li>影响范围更小：不影响工作流的整体执行流程</li>
     *   <li>恢复速度更快：任务级别的重新调度比工作流级别更快</li>
     * </ul>
     *
     * @param workerFailoverEvent Worker故障转移事件，包含故障Worker节点的元数据信息
     */
    @Override
    public void failoverWorker(final WorkerFailoverEvent workerFailoverEvent) {
        // 从事件中获取故障Worker的元数据信息
        final WorkerServerMetadata workerServerMetadata = workerFailoverEvent.getWorkerServerMetadata();
        log.info("Worker[{}] 故障转移开始执行", workerServerMetadata);

        // 验证Worker节点是否真的故障，避免网络抖动导致的误判
        // 这一步很重要，因为网络瞬断可能导致误判节点故障
        final Optional<WorkerServerMetadata> aliveWorkerOptional =
                clusterManager.getWorkerClusters().getServer(workerServerMetadata.getAddress());

        if (aliveWorkerOptional.isPresent()) {
            // 如果找到了同地址的活跃Worker，需要检查是否为同一实例
            final WorkerServerMetadata aliveWorkerServerMetadata = aliveWorkerOptional.get();

            // 通过启动时间判断是否为同一个Worker实例
            // 启动时间相同意味着是同一个进程，可能只是网络恢复了
            if (aliveWorkerServerMetadata.getServerStartupTime() == workerServerMetadata.getServerStartupTime()) {
                log.info("Worker[{}] 仍然存活，可能重新连接到注册中心，跳过故障转移", workerServerMetadata);
                return;
            }
        }

        // 执行具体的Worker故障转移逻辑
        // 传入Worker地址、当前时间戳和注册中心标记路径
        doWorkerFailover(
                workerServerMetadata.getAddress(),                    // Worker节点地址
                System.currentTimeMillis(),                           // 当前时间作为故障转移截止时间
                RegistryUtils.getFailoveredNodePath(                  // 构建故障转移标记路径
                        workerServerMetadata.getAddress(),
                        workerServerMetadata.getServerStartupTime(),
                        workerServerMetadata.getProcessId()));
    }

    /**
     * 执行Worker故障转移的核心逻辑
     *
     * 该方法负责将故障Worker上正在执行的任务重新调度到其他健康的Worker节点。
     * 与Master故障转移不同，Worker故障转移不使用分布式锁，因为Worker可能被多个Master处理。
     *
     * <p>Worker故障转移流程：
     * <ol>
     *   <li>查找运行在故障Worker上的所有待转移任务</li>
     *   <li>逐个执行任务的故障转移处理</li>
     *   <li>在注册中心记录故障转移完成时间</li>
     * </ol>
     *
     * <p>与Master故障转移的设计差异：
     * <ul>
     *   <li>不检查workerFailoverNodePath是否存在，因为Worker可能被多个Master故障转移</li>
     *   <li>不使用分布式锁，避免多Master环境下的死锁问题</li>
     *   <li>处理的是任务级别的故障转移，粒度更细</li>
     * </ul>
     *
     * <p>任务重新调度机制：
     * <ul>
     *   <li>识别处于DISPATCH或RUNNING状态的任务</li>
     *   <li>根据任务提交时间判断是否需要故障转移</li>
     *   <li>将任务标记为需要重新调度</li>
     *   <li>触发任务重新分配到健康Worker</li>
     * </ul>
     *
     * @param workerAddress 故障Worker的地址
     * @param taskFailoverDeadline 任务故障转移截止时间，早于此时间提交的任务才会被转移
     * @param workerFailoverNodePath 注册中心中记录Worker故障转移状态的节点路径
     */
    private void doWorkerFailover(final String workerAddress,
                                  final long taskFailoverDeadline,
                                  final String workerFailoverNodePath) {
        // 创建计时器来监控Worker故障转移的性能
        final StopWatch failoverTimeCost = StopWatch.createStarted();

        // 我们不检查workerFailoverNodePath是否存在，因为Worker可能被多个Master故障转移
        // 与Master故障转移不同，Worker故障转移允许并发执行，因为影响范围相对较小

        // 查找需要故障转移的任务列表
        // 只有运行在故障Worker上且在截止时间前提交的任务才需要转移
        final List<ITaskExecutionRunnable> needFailoverTasks =
                getFailoverTaskForWorker(workerAddress, new Date(taskFailoverDeadline));

        // 逐个执行任务故障转移
        // 每个任务会被重新标记为需要调度，然后分配给健康的Worker
        needFailoverTasks.forEach(taskFailover::failoverTask);

        // 在注册中心记录故障转移完成时间
        // 记录当前时间戳，表示此Worker的故障转移已完成
        registryClient.persist(
                workerFailoverNodePath,
                String.valueOf(System.currentTimeMillis()));

        // 停止计时器并记录性能数据
        failoverTimeCost.stop();
        log.info("Worker[{}] 故障转移完成，处理了 {} 个任务，耗时: {}/ms",
                workerAddress,
                needFailoverTasks.size(),
                failoverTimeCost.getTime());
    }

    /**
     * 获取指定Worker节点需要故障转移的任务实例列表
     *
     * 该方法通过多层过滤筛选出需要故障转移的任务，确保只转移确实运行在故障Worker上的任务。
     * 这是Worker故障转移的核心筛选逻辑，直接影响故障转移的准确性。
     *
     * <p>筛选条件（按执行顺序）：
     * <ol>
     *   <li>从所有活跃工作流中获取正在执行的任务</li>
     *   <li>过滤出已经初始化的任务实例</li>
     *   <li>过滤出运行在指定Worker地址上的任务</li>
     *   <li>过滤出处于DISPATCH或RUNNING_EXECUTION状态的任务</li>
     *   <li>过滤出提交时间早于故障截止时间的任务</li>
     * </ol>
     *
     * <p>状态过滤的意义：
     * <ul>
     *   <li>DISPATCH状态：任务已分发但可能还未开始执行，需要重新调度</li>
     *   <li>RUNNING_EXECUTION状态：任务正在执行，需要中断并重新调度</li>
     *   <li>其他状态的任务不需要故障转移（如已完成、已失败等）</li>
     * </ul>
     *
     * <p>时间判断的重要性：
     * <ul>
     *   <li>确保只转移在Worker故障前提交的任务</li>
     *   <li>避免转移故障后新提交到其他Worker的任务</li>
     *   <li>保证故障转移的时间一致性</li>
     * </ul>
     *
     * @param workerAddress 故障Worker的地址
     * @param taskFailoverDeadline 任务故障转移截止时间
     * @return 需要故障转移的任务实例列表
     */
    private List<ITaskExecutionRunnable> getFailoverTaskForWorker(final String workerAddress,
                                                                  final Date taskFailoverDeadline) {
        // 从工作流仓库开始，获取所有当前活跃的工作流实例
        return workflowRepository.getAll()
                .stream()
                // 获取每个工作流的执行图
                // 执行图包含了该工作流中所有任务的执行状态和依赖关系
                .map(IWorkflowExecutionRunnable::getWorkflowExecutionGraph)
                // 展开所有活跃的任务执行实例
                // flatMap将多个工作流的任务合并成一个流进行处理
                .flatMap(workflowExecutionGraph -> workflowExecutionGraph.getActiveTaskExecutionRunnable().stream())
                // 过滤出已经初始化的任务实例
                // 只有已初始化的任务才有完整的元数据，包括主机信息
                .filter(ITaskExecutionRunnable::isTaskInstanceInitialized)
                // 过滤出运行在指定Worker地址上的任务
                // 通过任务实例的host字段匹配故障Worker的地址
                .filter(taskExecutionRunnable -> workerAddress
                        .equals(taskExecutionRunnable.getTaskInstance().getHost()))
                // 过滤出处于调度中或运行中状态的任务
                // 只有这些状态的任务需要故障转移，已完成或失败的任务不需要
                .filter(taskExecutionRunnable -> {
                    final TaskExecutionStatus state = taskExecutionRunnable.getTaskInstance().getState();
                    return state == TaskExecutionStatus.DISPATCH || state == TaskExecutionStatus.RUNNING_EXECUTION;
                })
                // 过滤出提交时间早于故障截止时间的任务
                // 确保只转移在Worker故障前提交的任务
                .filter(taskExecutionRunnable -> {
                    // 提交时间不应该为null
                    // 如果为null，这是一个异常情况，除非有人手动将提交时间设置为null
                    final Date submitTime = taskExecutionRunnable.getTaskInstance().getSubmitTime();
                    // 提交时间必须存在且早于故障时间才需要故障转移
                    return submitTime != null && submitTime.before(taskFailoverDeadline);
                })
                // 收集所有符合条件的任务到列表中
                .collect(Collectors.toList());
    }

}
