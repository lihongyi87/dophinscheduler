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

import java.util.List;

/**
 * Master槽位变化监听器适配器
 *
 * 这个类是一个适配器模式的实现，它将集群状态变化事件转换为槽位变化事件。
 * 当Master集群发生任何变化（节点增加、删除、更新）时，都会触发槽位重新平衡。
 * 类比：一个自动化调度员，一旦发现管理层人员有任何变动，就立即重新分配工作职责。
 *
 * 适配器模式的优势：
 * 1. 解耦合：MasterSlotManager不需要直接监听集群变化，只需关注槽位平衡逻辑
 * 2. 统一处理：无论是哪种类型的集群变化，都统一触发槽位重新平衡
 * 3. 简化接口：为槽位管理器提供了简化的事件接口
 *
 * 主要职责：
 * 1. 监听Master集群的所有变化事件
 * 2. 将各种集群变化事件统一转换为槽位变化事件
 * 3. 调用槽位管理器进行重新平衡
 */
public class MasterSlotChangeListenerAdaptor
        implements
            IMasterSlotChangeListener,
            IClusters.IClustersChangeListener<MasterServerMetadata> {

    /**
     * Master槽位管理器
     *
     * 负责执行实际的槽位重新平衡逻辑，计算每个Master节点的槽位编号。
     */
    private final MasterSlotManager masterSlotManager;

    /**
     * Master集群管理器
     *
     * 用于获取当前正常状态的Master节点列表，作为槽位重新平衡的数据来源。
     */
    private final MasterClusters masterClusters;

    /**
     * 构造函数
     *
     * 初始化适配器所需的依赖组件。
     *
     * @param masterSlotManager Master槽位管理器，用于执行槽位重新平衡
     * @param masterClusters Master集群管理器，用于获取集群状态信息
     */
    public MasterSlotChangeListenerAdaptor(final MasterSlotManager masterSlotManager,
                                           final MasterClusters masterClusters) {
        // 注入Master槽位管理器，它负责具体的槽位分配算法
        this.masterSlotManager = masterSlotManager;
        // 注入Master集群管理器，用于获取正常状态的Master节点列表
        this.masterClusters = masterClusters;
    }

    /**
     * 处理Master槽位变化事件
     *
     * 实现IMasterSlotChangeListener接口的方法，当Master集群的槽位需要重新分配时调用。
     * 这个方法直接委托给MasterSlotManager来执行槽位重新平衡。
     *
     * @param normalMasterServers 正常状态的Master服务器列表
     */
    @Override
    public void onMasterSlotChanged(final List<MasterServerMetadata> normalMasterServers) {
        // 委托给Master槽位管理器执行实际的重新平衡逻辑
        // 槽位管理器会根据正常Master节点列表重新计算每个节点的槽位编号
        // 确保工作流实例能够均匀分布到不同的Master节点
        masterSlotManager.doReBalance(normalMasterServers);
    }

    /**
     * 处理Master服务器添加事件
     *
     * 实现IClustersChangeListener接口的方法，当有新的Master节点加入集群时调用。
     * 新节点的加入会影响槽位分配，需要重新平衡以包含新节点。
     *
     * 类比：当有新的管理人员入职时，需要重新分配管理职责，让新人也承担一部分工作。
     *
     * @param server 新加入的Master服务器元数据
     */
    @Override
    public void onServerAdded(MasterServerMetadata server) {
        // 获取当前所有正常状态的Master节点（包括新加入的节点）
        // 调用槽位变化处理方法，触发重新平衡
        // 这样新节点就会被分配一个槽位，开始承担工作流调度任务
        onMasterSlotChanged(masterClusters.getNormalServers());
    }

    /**
     * 处理Master服务器移除事件
     *
     * 实现IClustersChangeListener接口的方法，当Master节点从集群中离开时调用。
     * 节点的离开会影响槽位分配，需要重新平衡以排除离开的节点。
     *
     * 类比：当管理人员离职时，需要重新分配其管理职责给剩余的管理人员。
     *
     * @param server 被移除的Master服务器元数据
     */
    @Override
    public void onServerRemove(MasterServerMetadata server) {
        // 获取当前所有正常状态的Master节点（已不包含被移除的节点）
        // 调用槽位变化处理方法，触发重新平衡
        // 这样剩余的节点会重新分配槽位，接管被移除节点的工作
        onMasterSlotChanged(masterClusters.getNormalServers());
    }

    /**
     * 处理Master服务器更新事件
     *
     * 实现IClustersChangeListener接口的方法，当Master节点信息发生变化时调用。
     * 虽然信息更新通常不需要槽位重新分配，但为了保持一致性，仍然触发重新平衡。
     *
     * 类比：当管理人员的状态发生变化时（如从忙碌变为空闲），可能需要调整工作分配。
     *
     * @param server 更新后的Master服务器元数据
     */
    @Override
    public void onServerUpdate(MasterServerMetadata server) {
        // 获取当前所有正常状态的Master节点
        // 调用槽位变化处理方法，可能的情况：
        // 1. 节点状态从异常变为正常，需要重新分配槽位
        // 2. 节点状态从正常变为异常，需要排除该节点
        // 3. 其他属性变化，为保持一致性也执行重新平衡
        onMasterSlotChanged(masterClusters.getNormalServers());
    }
}
