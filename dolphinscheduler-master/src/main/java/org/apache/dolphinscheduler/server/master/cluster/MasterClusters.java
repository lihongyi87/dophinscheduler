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

import org.apache.dolphinscheduler.common.enums.ServerStatus;
import org.apache.dolphinscheduler.common.model.MasterHeartBeat;
import org.apache.dolphinscheduler.common.utils.JSONUtils;

import org.apache.commons.collections4.list.UnmodifiableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

/**
 * Master集群管理器
 *
 * 这个类负责管理DolphinScheduler集群中的所有Master节点，包括节点的增加、删除、更新等操作。
 * 它实现了IClusters接口，并继承自AbstractClusterSubscribeListener来处理注册中心的节点变化事件。
 * 类比：一个管理层的人事部门，负责记录所有管理人员的信息，处理管理人员的入职、离职、调动等事务。
 *
 * 主要功能：
 * 1. 维护Master节点列表：保存所有Master节点的详细信息
 * 2. 处理节点变化：响应Master节点的上线、下线、状态更新事件
 * 3. 提供查询接口：支持查询所有节点、特定节点、正常状态节点等
 * 4. 事件通知机制：当Master集群发生变化时，通知所有注册的监听器
 */
@Slf4j
public class MasterClusters extends AbstractClusterSubscribeListener<MasterServerMetadata>
        implements
            IClusters<MasterServerMetadata> {

    /**
     * Master服务器映射表
     *
     * 键为Master节点的地址（通常是"host:port"格式），值为对应的Master服务器元数据。
     * 使用ConcurrentHashMap确保在多线程环境下的线程安全。
     * 类比：管理层花名册，记录每个管理人员的地址和详细信息。
     */
    private final Map<String, MasterServerMetadata> masterServerMap = new ConcurrentHashMap<>();

    /**
     * Master集群变化监听器列表
     *
     * 存储所有注册的集群变化监听器，当Master集群发生变化时会通知这些监听器。
     * 使用CopyOnWriteArrayList确保在迭代过程中的线程安全，适合读多写少的场景。
     * 类比：通讯录，记录所有需要接收管理层变动通知的部门和人员。
     */
    private final List<IClustersChangeListener<MasterServerMetadata>> masterClusterChangeListeners =
            new CopyOnWriteArrayList<>();

    /**
     * 获取所有Master服务器列表
     *
     * 返回集群中所有Master节点的不可修改列表，确保外部代码无法直接修改内部状态。
     * 类比：提供管理层人员的完整名单，但不允许外部直接修改名单内容。
     *
     * @return 所有Master服务器的不可修改列表
     */
    @Override
    public List<MasterServerMetadata> getServers() {
        // 从并发安全的Map中获取所有Master服务器的元数据值
        // 创建新的ArrayList避免外部直接访问内部集合
        // 返回不可修改的列表，确保外部代码无法修改集群状态
        return UnmodifiableList.unmodifiableList(new ArrayList<>(masterServerMap.values()));
    }

    /**
     * 根据地址获取特定的Master服务器信息
     *
     * @param address Master服务器地址，格式通常为"host:port"
     * @return 包含服务器信息的Optional对象，如果服务器不存在则为空
     */
    @Override
    public Optional<MasterServerMetadata> getServer(final String address) {
        // 从并发哈希表中根据地址查找Master服务器信息
        // 使用Optional.ofNullable确保即使找不到也不会抛出NullPointerException
        // 这种设计遵循了函数式编程的最佳实践
        return Optional.ofNullable(masterServerMap.get(address));
    }

    /**
     * 获取状态正常的Master服务器列表
     *
     * 过滤出所有状态为NORMAL的Master节点，这些节点可以正常处理任务。
     * 类比：从管理层名单中筛选出当前在岗且可以正常工作的管理人员。
     *
     * @return 状态正常的Master服务器的不可修改列表
     */
    public List<MasterServerMetadata> getNormalServers() {
        // 使用Java 8 Stream API进行流式处理
        // 从所有Master服务器中筛选出状态为NORMAL的节点
        List<MasterServerMetadata> normalMasterServers = masterServerMap.values()
                .stream()  // 将集合转换为流
                .filter(masterServer -> masterServer.getServerStatus() == ServerStatus.NORMAL)  // 过滤条件：服务器状态为正常
                .collect(Collectors.toList());  // 将流收集为List
        // 返回不可修改的列表，保护内部状态不被外部修改
        return UnmodifiableList.unmodifiableList(normalMasterServers);
    }

    /**
     * 注册Master集群变化监听器
     *
     * 当Master集群发生变化（节点增加、删除、更新）时，会通知所有注册的监听器。
     * 类比：在人事部门登记，当管理层有人事变动时会收到通知。
     *
     * @param listener 集群变化监听器
     */
    @Override
    public void registerListener(final IClustersChangeListener<MasterServerMetadata> listener) {
        // 将监听器添加到线程安全的CopyOnWriteArrayList中
        // CopyOnWriteArrayList适合读多写少的场景，遍历时不需要额外同步
        // 当集群状态发生变化时，会通知这个监听器
        masterClusterChangeListeners.add(listener);
    }

    /**
     * 从心跳信息解析Master服务器元数据
     *
     * 这是AbstractClusterSubscribeListener的抽象方法实现，用于将注册中心的心跳JSON字符串
     * 转换为MasterServerMetadata对象。
     * 类比：从人事档案的原始记录中提取并整理出标准的员工信息卡。
     *
     * @param masterHeartBeatJson Master心跳信息的JSON字符串
     * @return 解析后的Master服务器元数据，解析失败则返回null
     */
    @Override
    MasterServerMetadata parseServerFromHeartbeat(final String masterHeartBeatJson) {
        // 使用JSONUtils将心跳JSON字符串解析为MasterHeartBeat对象
        // 这里处理的是从注册中心获取的心跳数据
        MasterHeartBeat masterHeartBeat = JSONUtils.parseObject(masterHeartBeatJson, MasterHeartBeat.class);
        // 检查解析结果，如果JSON格式不正确或字段缺失，解析会返回null
        if (masterHeartBeat == null) {
            return null;
        }
        // 将心跳对象转换为Master服务器元数据对象
        // 这个转换过程会提取心跳中的关键信息（如地址、状态、负载等）
        return MasterServerMetadata.parseFromHeartBeat(masterHeartBeat);
    }

    /**
     * 处理Master服务器添加事件
     *
     * 当有新的Master节点加入集群时，将其添加到服务器映射表中，
     * 并通知所有注册的监听器。
     * 类比：当有新的管理人员入职时，将其信息录入花名册，并通知各部门。
     *
     * @param masterServer 新加入的Master服务器元数据
     */
    @Override
    public void onServerAdded(final MasterServerMetadata masterServer) {
        // 将新的Master服务器添加到并发哈希表中
        // 使用服务器地址作为键，确保每个地址只对应一个服务器实例
        masterServerMap.put(masterServer.getAddress(), masterServer);
        // 遍历所有注册的监听器，通知它们有新的Master服务器加入
        // 这种通知机制允许其他组件（如负载均衡器、监控系统）及时响应集群变化
        for (IClustersChangeListener<MasterServerMetadata> listener : masterClusterChangeListeners) {
            listener.onServerAdded(masterServer);
        }
    }

    /**
     * 处理Master服务器移除事件
     *
     * 当Master节点从集群中离开时，将其从服务器映射表中移除，
     * 并通知所有注册的监听器。
     * 类比：当管理人员离职时，将其信息从花名册中删除，并通知各部门。
     *
     * @param masterServer 被移除的Master服务器元数据
     */
    @Override
    public void onServerRemove(final MasterServerMetadata masterServer) {
        // 从并发哈希表中移除指定地址的Master服务器
        // 这个操作是原子的，确保在并发环境下的数据一致性
        masterServerMap.remove(masterServer.getAddress());
        // 遍历所有注册的监听器，通知它们有Master服务器离开集群
        // 监听器可能需要重新平衡负载、触发故障转移等操作
        for (IClustersChangeListener<MasterServerMetadata> listener : masterClusterChangeListeners) {
            listener.onServerRemove(masterServer);
        }
    }

    /**
     * 处理Master服务器更新事件
     *
     * 当Master节点的信息（如状态、负载等）发生变化时，更新服务器映射表中的信息，
     * 并通知所有注册的监听器。
     * 类比：当管理人员的状态或职务发生变化时，更新花名册信息，并通知各部门。
     *
     * @param masterServer 更新后的Master服务器元数据
     */
    @Override
    public void onServerUpdate(final MasterServerMetadata masterServer) {
        // 更新并发哈希表中的Master服务器信息
        // 这里使用put操作，如果地址已存在则覆盖，不存在则添加
        masterServerMap.put(masterServer.getAddress(), masterServer);
        // 遍历所有注册的监听器，通知它们Master服务器信息已更新
        // 更新可能包括状态变化、负载变化、配置变化等
        for (IClustersChangeListener<MasterServerMetadata> listener : masterClusterChangeListeners) {
            listener.onServerUpdate(masterServer);
        }
    }

}
