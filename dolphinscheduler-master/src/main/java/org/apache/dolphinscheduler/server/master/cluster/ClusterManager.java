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

package org.apache.dolphinscheduler.server.master.cluster;

import org.apache.dolphinscheduler.common.model.MasterHeartBeat;
import org.apache.dolphinscheduler.common.model.WorkerHeartBeat;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.registry.api.RegistryClient;
import org.apache.dolphinscheduler.registry.api.enums.RegistryNodeType;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * 集群管理器
 * 
 * 这个类是整个DolphinScheduler集群的核心管理组件，负责管理和协调Master节点和Worker节点。
 * 类比：一个大型工厂的管理部门，负责协调各个车间（Master）和工人组（Worker）的工作。
 * 
 * 主要职责：
 * 1. 管理Master集群：监控Master节点的上下线，进行槽位分配
 * 2. 管理Worker集群：监控Worker节点的状态，管理工作组变化
 * 3. 维护集群拓扑：实时更新集群的拓扑结构信息
 * 4. 处理故障转移：当节点失效时，进行相应的故障处理
 */
@Slf4j
@Component
public class ClusterManager {

    /**
     * Master集群信息
     * 
     * 维护所有Master节点的状态信息，包括节点地址、心跳信息、槽位分配等。
     * 类比：管理层花名册，记录所有管理人员的信息和职责分工。
     */
    @Getter
    private MasterClusters masterClusters;

    /**
     * Worker集群信息
     * 
     * 维护所有Worker节点的状态信息，包括工作组、资源使用情况、可用性等。
     * 类比：工人花名册，记录所有工人的技能、所属班组和工作状态。
     */
    @Getter
    private WorkerClusters workerClusters;

    /**
     * Master槽位管理器
     * 
     * 负责Master节点的槽位分配，确保工作流能够均匀分布到不同Master节点。
     * 类比：工作分配员，根据管理人员的负载情况分配具体的管理任务。
     */
    @Autowired
    private MasterSlotManager masterSlotManager;

    /**
     * Worker组变化通知器
     * 
     * 当Worker组发生变化时，负责通知相关组件进行相应的调整。
     * 类比：人事变动通知员，当有工人调动或离职时及时通知相关部门。
     */
    @Autowired
    private WorkerGroupChangeNotifier workerGroupChangeNotifier;

    /**
     * 注册中心客户端
     * 
     * 与注册中心通信的客户端，用于获取和订阅节点信息变化。
     * 类比：公司的人事系统，记录所有员工的基本信息和状态。
     */
    @Autowired
    private RegistryClient registryClient;

    /**
     * 构造函数
     * 
     * 初始化Master和Worker集群的数据结构。
     * 类比：成立管理部门时，先准备好管理层和工人的花名册。
     */
    public ClusterManager() {
        this.masterClusters = new MasterClusters();
        this.workerClusters = new WorkerClusters();
    }

    /**
     * 启动集群管理器
     * 
     * 初始化Master和Worker集群，开始监听节点变化。
     * 这是集群管理器的入口方法，会依次初始化各个子系统。
     * 
     * 类比：工厂开工时，管理部门开始运作，建立与各车间和工人组的联系。
     */
    public void start() {
        initializeMasterClusters();  // 初始化Master集群管理
        initializeWorkerClusters();  // 初始化Worker集群管理
        log.info("ClusterManager started...");
    }

    /**
     * 初始化Master集群管理
     * 
     * 这个方法负责设置Master集群的管理机制：
     * 1. 注册Master槽位变化监听器，当Master节点变化时重新分配槽位
     * 2. 从注册中心获取当前所有Master节点信息
     * 3. 订阅Master节点变化事件，实时更新集群状态
     * 
     * 类比：建立管理层的沟通机制，了解当前有哪些管理人员，
     *      并建立人事变动的通知渠道。
     *
     * Initialize the master clusters.
     * <p> 1. Register master slot listener once master clusters changed.
     * <p> 2. Fetch master nodes from registry.
     * <p> 3. Subscribe the master change event.
     */
    private void initializeMasterClusters() {
        // ========== 第一步：注册槽位变化监听器 ==========
        // 注册Master槽位变化监听器
        // 当Master集群发生变化时（节点上线/下线），会触发槽位重新分配
        // MasterSlotChangeListenerAdaptor会调用masterSlotManager进行槽位重新平衡
        // 这确保了工作流能够均匀分布到所有可用的Master节点
        this.masterClusters.registerListener(new MasterSlotChangeListenerAdaptor(masterSlotManager, masterClusters));

        // ========== 第二步：加载现有Master节点 ==========
        // 从注册中心获取所有现有的Master节点
        // getServerList返回的是注册中心中所有的Master节点信息
        registryClient.getServerList(RegistryNodeType.MASTER).forEach(server -> {
            // 解析每个Master节点的心跳信息
            // 心跳信息包含：节点地址、CPU使用率、内存使用率、进程ID等
            final MasterHeartBeat masterHeartBeat =
                    JSONUtils.parseObject(server.getHeartBeatInfo(), MasterHeartBeat.class);

            // 将心跳信息转换为Master服务器元数据对象
            // 并添加到Master集群管理器中
            // 这会触发槽位分配等初始化操作
            masterClusters.onServerAdded(MasterServerMetadata.parseFromHeartBeat(masterHeartBeat));
        });

        // 记录初始化后的Master集群状态
        // 便于运维人员了解集群的初始拓扑结构
        log.info("Initialized MasterClusters: {}", JSONUtils.toPrettyJsonString(masterClusters.getServers()));

        // ========== 第三步：订阅Master节点变化事件 ==========
        // 订阅注册中心的Master节点变化事件
        // 当有Master节点上线、下线或更新时，masterClusters会收到通知
        // 实现了集群状态的实时同步
        this.registryClient.subscribe(RegistryNodeType.MASTER.getRegistryPath(), masterClusters);
    }

    /**
     * 初始化Worker集群管理
     * 
     * 这个方法负责设置Worker集群的管理机制：
     * 1. 从注册中心获取当前所有Worker节点信息
     * 2. 订阅Worker节点变化事件，实时更新集群状态
     * 3. 启动Worker组变化通知器，处理工作组变化
     * 
     * 类比：建立工人管理系统，了解当前有哪些工人和班组，
     *      并建立工人调动的通知机制。
     *
     * Initialize the worker clusters.
     * <p> 1. Fetch worker nodes from registry.
     * <p> 2. Register worker group change notifier once worker clusters changed.
     * <p> 3. Subscribe the worker change event.
     */
    private void initializeWorkerClusters() {
        // ========== 第一步：加载现有Worker节点 ==========
        // 从注册中心获取所有现有的Worker节点
        // 这些节点可能属于不同的工作组，具有不同的资源配置
        registryClient.getServerList(RegistryNodeType.WORKER).forEach(server -> {
            // 解析每个Worker节点的心跳信息
            // 心跳信息包含：
            // - 节点地址和端口
            // - 工作组信息（default或自定义组）
            // - 资源信息（CPU核数、内存大小、磁盘空间）
            // - 当前任务执行状态
            final WorkerHeartBeat workerHeartBeat =
                    JSONUtils.parseObject(server.getHeartBeatInfo(), WorkerHeartBeat.class);

            // 将心跳信息转换为Worker服务器元数据对象
            // 并添加到Worker集群管理器中
            // 这会更新工作组的成员列表，影响后续任务分发
            workerClusters.onServerAdded(WorkerServerMetadata.parseFromHeartBeat(workerHeartBeat));
        });

        // 记录初始化后的Worker集群状态
        // 显示每个工作组有哪些Worker节点，便于监控和调试
        log.info("Initialized WorkerClusters: {}", JSONUtils.toPrettyJsonString(workerClusters.getServers()));

        // ========== 第二步：订阅Worker节点变化事件 ==========
        // 订阅注册中心的Worker节点变化事件
        // 当Worker节点发生以下情况时，会收到通知：
        // - 新Worker节点上线（扩容）
        // - Worker节点下线（缩容或故障）
        // - Worker节点信息更新（如工作组变更）
        this.registryClient.subscribe(RegistryNodeType.WORKER.getRegistryPath(), workerClusters);

        // ========== 第三步：启动Worker组变化通知器 ==========
        // 设置Worker组变化通知器，监听工作组的动态变化
        // 工作组可能来源于：
        // 1. 配置文件（静态配置）
        // 2. 数据库（动态配置）
        this.workerGroupChangeNotifier.subscribeWorkerGroupsChange(workerClusters);

        // 启动通知器，开始定期检查工作组变化
        // 当检测到新的工作组或工作组成员变化时，会通知相关组件
        this.workerGroupChangeNotifier.start();
    }

}
