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
import org.apache.dolphinscheduler.common.model.WorkerHeartBeat;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.dao.entity.WorkerGroup;
import org.apache.dolphinscheduler.dao.utils.WorkerGroupUtils;

import org.apache.commons.collections4.list.UnmodifiableList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * Worker集群管理器
 *
 * 这个类负责管理DolphinScheduler集群中的所有Worker节点，支持工作组(WorkerGroup)的动态管理。
 * 与MasterClusters不同，WorkerClusters需要处理更复杂的工作组分配逻辑。
 * 类比：一个工厂的工人管理部门，不仅要管理所有工人的信息，还要负责不同技能组(工作组)的人员分配。
 *
 * 主要功能：
 * 1. 管理Worker节点：处理Worker节点的增加、删除、更新
 * 2. 工作组管理：支持数据库配置和实时配置两种工作组管理方式
 * 3. 负载查询：按工作组查询可用的Worker节点
 * 4. 动态配置：响应工作组配置的变化
 *
 * 工作组类型：
 * - 数据库工作组(dbWorkerGroupMapping)：在数据库中预先配置的工作组
 * - 配置工作组(configWorkerGroupMapping)：Worker节点启动时通过配置文件指定的工作组
 * - 默认工作组：包含所有Worker节点的特殊工作组
 */
public class WorkerClusters extends AbstractClusterSubscribeListener<WorkerServerMetadata>
        implements
            IClusters<WorkerServerMetadata>,
            WorkerGroupChangeNotifier.WorkerGroupListener {

    /**
     * Worker节点映射表
     *
     * 键为Worker节点地址(workerAddress)，值为对应的Worker服务器元数据。
     * 这是所有Worker节点的主索引表。
     * 类比：工人花名册，记录所有工人的详细信息。
     */
    private final Map<String, WorkerServerMetadata> workerMapping = new ConcurrentHashMap<>();

    /**
     * 数据库工作组映射表
     *
     * 键为工作组名称，值为该工作组中的Worker地址列表。
     * 这些工作组是在数据库中预先配置的，支持动态修改。
     * 类比：正式的班组分配表，由人事部门在系统中统一配置和管理。
     */
    private final Map<String, List<String>> dbWorkerGroupMapping = new ConcurrentHashMap<>();

    /**
     * 配置工作组映射表
     *
     * 键为工作组名称，值为该工作组中的Worker地址列表。
     * 这些工作组是Worker节点启动时通过配置文件自主声明的。
     * 类比：临时技能小组，工人根据自己的技能特长主动加入相应的小组。
     */
    private final Map<String, List<String>> configWorkerGroupMapping = new ConcurrentHashMap<>();

    /**
     * Worker集群变化监听器列表
     *
     * 存储所有注册的集群变化监听器，当Worker集群发生变化时会通知这些监听器。
     */
    private final List<IClustersChangeListener<WorkerServerMetadata>> workerClusterChangeListeners =
            new CopyOnWriteArrayList<>();

    /**
     * 获取所有Worker服务器列表
     *
     * 返回集群中所有Worker节点的不可修改列表。
     *
     * @return 所有Worker服务器的不可修改列表
     */
    @Override
    public List<WorkerServerMetadata> getServers() {
        // 从并发安全的workerMapping中获取所有Worker服务器的元数据值
        // 创建新的ArrayList避免外部直接访问内部集合
        // 返回不可修改的列表，确保外部代码无法修改Worker集群状态
        return UnmodifiableList.unmodifiableList(new ArrayList<>(workerMapping.values()));
    }

    /**
     * 根据地址获取特定的Worker服务器信息
     *
     * @param address Worker服务器地址
     * @return 包含服务器信息的Optional对象，如果服务器不存在则为空
     */
    @Override
    public Optional<WorkerServerMetadata> getServer(final String address) {
        // 从并发哈希表中根据地址查找Worker服务器信息
        // 使用Optional.ofNullable确保即使找不到也不会抛出NullPointerException
        // 这种设计遵循了函数式编程的最佳实践
        return Optional.ofNullable(workerMapping.get(address));
    }

    /**
     * 根据工作组名称获取数据库配置的Worker服务器地址列表
     *
     * 如果指定的是默认工作组，则返回所有Worker节点的地址。
     * 否则返回在数据库中配置的指定工作组的Worker地址列表。
     * 类比：查询指定班组的工人名单，如果是"全体工人"则返回所有工人。
     *
     * @param workerGroup 工作组名称
     * @return Worker服务器地址列表
     */
    public List<String> getDbWorkerServerAddressByGroup(String workerGroup) {
        // 检查是否为默认工作组（通常为"default"）
        // 默认工作组包含所有Worker节点，无论它们属于哪个特定工作组
        if (WorkerGroupUtils.getDefaultWorkerGroup().equals(workerGroup)) {
            // 返回所有Worker节点的地址列表（从主映射表的键集合获取）
            return UnmodifiableList.unmodifiableList(new ArrayList<>(workerMapping.keySet()));
        }
        // 从数据库工作组映射表中获取指定工作组的Worker地址列表
        // 使用getOrDefault避免空指针，如果工作组不存在则返回空列表
        return dbWorkerGroupMapping.getOrDefault(workerGroup, Collections.emptyList());
    }

    /**
     * 根据工作组名称获取配置文件配置的Worker服务器地址列表
     *
     * 如果指定的是默认工作组，则返回所有Worker节点的地址。
     * 否则返回通过配置文件自主声明的指定工作组的Worker地址列表。
     * 类比：查询指定技能小组的工人名单，如果是"全体工人"则返回所有工人。
     *
     * @param workerGroup 工作组名称
     * @return Worker服务器地址列表
     */
    public List<String> getConfigWorkerServerAddressByGroup(String workerGroup) {
        // 检查是否为默认工作组
        if (WorkerGroupUtils.getDefaultWorkerGroup().equals(workerGroup)) {
            // 默认工作组返回所有Worker节点的地址
            return UnmodifiableList.unmodifiableList(new ArrayList<>(workerMapping.keySet()));
        }
        // 从配置文件工作组映射表中获取指定工作组的Worker地址列表
        // 这些是Worker节点启动时通过配置文件声明的工作组
        return configWorkerGroupMapping.getOrDefault(workerGroup, Collections.emptyList());
    }

    /**
     * 根据工作组名称获取正常状态的Worker服务器地址列表
     *
     * 这个方法是任务调度的核心方法，用于获取可以执行任务的Worker节点。
     * 它会同时考虑数据库配置和配置文件配置的工作组，并优先使用配置文件配置。
     *
     * 合并策略：
     * 1. 获取数据库配置的正常Worker节点
     * 2. 获取配置文件配置的正常Worker节点
     * 3. 从数据库配置中移除与配置文件重复的节点（避免重复）
     * 4. 将配置文件配置的节点加入结果（配置文件优先级更高）
     *
     * 类比：为一个生产任务寻找可用的工人，既要考虑正式班组分配，
     *      也要考虑临时技能小组，并且临时小组的优先级更高。
     *
     * @param workerGroup 工作组名称
     * @return 正常状态的Worker服务器地址列表
     */
    public List<String> getNormalWorkerServerAddressByGroup(String workerGroup) {
        // 获取数据库配置的正常Worker地址列表
        List<String> dbWorkerAddresses = getDbWorkerServerAddressByGroup(workerGroup)
                .stream()  // 将地址列表转换为流
                .map(workerMapping::get)  // 根据地址查找Worker元数据对象
                .filter(Objects::nonNull)  // 过滤掉null值（可能数据库中的地址在集群中不存在）
                .filter(workerServer -> workerServer.getServerStatus() == ServerStatus.NORMAL)  // 只保留状态为NORMAL的Worker
                .map(WorkerServerMetadata::getAddress)  // 提取Worker地址
                .collect(Collectors.toList());  // 收集为列表

        // 获取配置文件配置的正常Worker地址列表
        List<String> configWorkerAddresses = getConfigWorkerServerAddressByGroup(workerGroup)
                .stream()  // 将地址列表转换为流
                .map(workerMapping::get)  // 根据地址查找Worker元数据对象
                .filter(Objects::nonNull)  // 过滤掉null值
                .filter(workerServer -> workerServer.getServerStatus() == ServerStatus.NORMAL)  // 只保留正常状态的Worker
                .map(WorkerServerMetadata::getAddress)  // 提取Worker地址
                .collect(Collectors.toList());  // 收集为列表

        // 合并两个列表，优先级规则：配置文件优于数据库配置
        dbWorkerAddresses.removeAll(configWorkerAddresses);  // 从数据库列表中移除与配置文件重复的地址
        dbWorkerAddresses.addAll(configWorkerAddresses);     // 将配置文件的地址添加到结果中
        // 这样确保配置文件中的设置优先级更高，并且每个地址只出现一次
        return UnmodifiableList.unmodifiableList(dbWorkerAddresses);
    }

    /**
     * 检查是否包含指定的工作组
     *
     * 工作组存在的条件：
     * 1. 是默认工作组（总是存在）
     * 2. 在数据库配置中存在
     * 3. 在配置文件配置中存在
     *
     * @param workerGroup 工作组名称
     * @return true表示包含该工作组，false表示不包含
     */
    public boolean containsWorkerGroup(String workerGroup) {
        // 检查工作组是否存在的三个条件：
        // 1. 是否为默认工作组（默认工作组总是存在的）
        // 2. 是否在数据库配置的工作组中
        // 3. 是否在配置文件配置的工作组中
        return WorkerGroupUtils.getDefaultWorkerGroup().equals(workerGroup)     // 条件1：默认工作组
                || dbWorkerGroupMapping.containsKey(workerGroup)                // 条件2：数据库配置
                || configWorkerGroupMapping.containsKey(workerGroup);           // 条件3：配置文件配置
    }

    /**
     * 注册Worker集群变化监听器
     *
     * @param listener 集群变化监听器
     */
    @Override
    public void registerListener(IClustersChangeListener<WorkerServerMetadata> listener) {
        // 将监听器添加到线程安全的CopyOnWriteArrayList中
        // 这个监听器会在Worker集群发生变化时被通知
        // CopyOnWriteArrayList适合读多写少的场景，遍历时不需要额外同步
        workerClusterChangeListeners.add(listener);
    }

    /**
     * 处理工作组删除事件
     *
     * 当数据库中的工作组被删除时，从数据库工作组映射表中移除对应的映射关系。
     * 类比：当某个班组被解散时，从班组名册中删除该班组的信息。
     *
     * @param workerGroups 被删除的工作组列表
     */
    @Override
    public void onWorkerGroupDelete(List<WorkerGroup> workerGroups) {
        // 使用synchronized确保对dbWorkerGroupMapping的修改是线程安全的
        // 防止并发修改导致数据不一致
        synchronized (dbWorkerGroupMapping) {
            // 遍历所有被删除的工作组
            for (WorkerGroup workerGroup : workerGroups) {
                // 从数据库工作组映射表中移除该工作组
                // 这会使该工作组在下次任务调度时无法被使用
                dbWorkerGroupMapping.remove(workerGroup.getName());
            }
        }
    }

    /**
     * 处理工作组添加事件
     *
     * 当数据库中添加新的工作组时，更新数据库工作组映射表。
     * 添加工作组的逻辑与更新工作组相同，都需要将工作组映射更新为最新的配置。
     * 类比：当成立新班组时，在班组名册中登记新班组的信息。
     *
     * @param workerGroups 新添加的工作组列表
     */
    @Override
    public void onWorkerGroupAdd(List<WorkerGroup> workerGroups) {
        // 添加工作组的逻辑与更新工作组相同
        // 无论是新增还是修改，都需要将工作组映射更新为最新的配置
        // 这里直接复用onWorkerGroupChange方法，避免代码重复
        onWorkerGroupChange(workerGroups);
    }

    /**
     * 处理工作组变更事件
     *
     * 当数据库中的工作组配置发生变化时，更新内部的数据库工作组映射表。
     * 从工作组实体中提取Worker地址列表，并更新映射关系。
     * 类比：当班组人员调整时，更新班组名册中的人员列表。
     *
     * @param workerGroups 变更的工作组列表
     */
    @Override
    public void onWorkerGroupChange(List<WorkerGroup> workerGroups) {
        // 遍历所有发生变化的工作组
        for (WorkerGroup workerGroup : workerGroups) {
            // 从工作组实体中提取Worker地址列表
            // WorkerGroup实体中包含了该工作组的所有Worker节点信息
            List<String> workerAddresses = WorkerGroupUtils.getWorkerAddressListFromWorkerGroup(workerGroup);
            // 使用synchronized确保对映射表的修改是线程安全的
            synchronized (dbWorkerGroupMapping) {
                // 更新数据库工作组映射表，将新的Worker地址列表关联到工作组名称
                dbWorkerGroupMapping.put(workerGroup.getName(), workerAddresses);
            }
        }
    }

    /**
     * 从心跳信息解析Worker服务器元数据
     *
     * 这是AbstractClusterSubscribeListener的抽象方法实现，用于将注册中心的心跳JSON字符串
     * 转换为WorkerServerMetadata对象。
     *
     * @param serverHeartBeatJson Worker心跳信息的JSON字符串
     * @return 解析后的Worker服务器元数据，解析失败则返回null
     */
    @Override
    WorkerServerMetadata parseServerFromHeartbeat(String serverHeartBeatJson) {
        // 使用JSONUtils将心跳JSON字符串解析为WorkerHeartBeat对象
        // 这里处理的是从注册中心获取的Worker心跳数据
        WorkerHeartBeat workerHeartBeat = JSONUtils.parseObject(serverHeartBeatJson, WorkerHeartBeat.class);
        // 检查解析结果，防止JSON格式错误或数据损坏
        if (workerHeartBeat == null) {
            return null;
        }
        // 将心跳对象转换为Worker服务器元数据对象
        // 转换过程会提取工作组、地址、状态、负载等关键信息
        return WorkerServerMetadata.parseFromHeartBeat(workerHeartBeat);
    }

    /**
     * 处理Worker服务器添加事件
     *
     * 当有新的Worker节点加入集群时：
     * 1. 将Worker节点添加到主映射表中
     * 2. 更新配置工作组映射表，将该Worker加入其所属的工作组
     * 3. 通知所有注册的监听器
     *
     * 类比：当有新工人入职时，将其信息登记到花名册，
     *      并根据其技能加入相应的技能小组。
     *
     * @param workerServer 新加入的Worker服务器元数据
     */
    @Override
    public void onServerAdded(WorkerServerMetadata workerServer) {
        // 将新的Worker服务器添加到主映射表中
        // 使用Worker地址作为键，确保每个地址只对应一个服务器实例
        workerMapping.put(workerServer.getAddress(), workerServer);

        // 更新配置文件工作组映射表，需要线程同步保证数据一致性
        synchronized (configWorkerGroupMapping) {
            // 获取该Worker所属工作组的地址列表
            List<String> addWorkerGroupAddrList = configWorkerGroupMapping.get(workerServer.getWorkerGroup());
            if (addWorkerGroupAddrList == null) {
                // 如果该工作组还不存在，创建新的工作组并添加当前Worker
                List<String> newWorkerGroupAddrList = new ArrayList<>();
                newWorkerGroupAddrList.add(workerServer.getAddress());
                configWorkerGroupMapping.put(workerServer.getWorkerGroup(), newWorkerGroupAddrList);
            } else if (!addWorkerGroupAddrList.contains(workerServer.getAddress())) {
                // 如果工作组已存在但不包含当前Worker地址，则添加进去
                // 检查contains避免重复添加相同的Worker地址
                addWorkerGroupAddrList.add(workerServer.getAddress());
                configWorkerGroupMapping.put(workerServer.getWorkerGroup(), addWorkerGroupAddrList);
            }
        }

        // 遍历所有注册的监听器，通知它们有新的Worker服务器加入
        // 监听器可能包括负载均衡器、任务调度器等
        for (IClustersChangeListener<WorkerServerMetadata> listener : workerClusterChangeListeners) {
            listener.onServerAdded(workerServer);
        }
    }

    /**
     * 处理Worker服务器移除事件
     *
     * 当Worker节点从集群中离开时：
     * 1. 从主映射表中移除Worker节点
     * 2. 从配置工作组映射表中移除该Worker，如果工作组变空则删除整个工作组
     * 3. 通知所有注册的监听器
     *
     * 类比：当工人离职时，从花名册中删除其信息，
     *      并从所属技能小组中移除，如果小组没人了就解散。
     *
     * @param workerServer 被移除的Worker服务器元数据
     */
    @Override
    public void onServerRemove(WorkerServerMetadata workerServer) {
        // 从主映射表中移除Worker节点
        // 使用remove(key, value)确保只有当键值都匹配时才移除，增强并发安全性
        workerMapping.remove(workerServer.getAddress(), workerServer);

        // 从配置文件工作组映射表中移除该Worker，需要线程同步
        synchronized (configWorkerGroupMapping) {
            // 获取该Worker所属工作组的地址列表
            List<String> removeWorkerGroupAddrList = configWorkerGroupMapping.get(workerServer.getWorkerGroup());
            // 检查工作组是否存在且包含该Worker地址
            if (removeWorkerGroupAddrList != null && removeWorkerGroupAddrList.contains(workerServer.getAddress())) {
                // 从工作组地址列表中移除该Worker地址
                removeWorkerGroupAddrList.remove(workerServer.getAddress());
                // 检查工作组是否变空，如果空了则删除整个工作组
                // 这样可以避免留下空的工作组，保持数据的清洁
                if (removeWorkerGroupAddrList.isEmpty()) {
                    configWorkerGroupMapping.remove(workerServer.getWorkerGroup());
                }
            }
        }

        // 遍历所有注册的监听器，通知它们Worker服务器已离开
        // 监听器可能需要重新分配任务、更新负载均衡等
        for (IClustersChangeListener<WorkerServerMetadata> listener : workerClusterChangeListeners) {
            listener.onServerRemove(workerServer);
        }
    }

    /**
     * 处理Worker服务器更新事件
     *
     * 当Worker节点的信息（如状态、负载等）发生变化时，更新主映射表中的信息，
     * 并通知所有注册的监听器。
     *
     * 注意：这里没有更新配置工作组映射，因为Worker的工作组不会在运行时改变。
     *
     * @param workerServer 更新后的Worker服务器元数据
     */
    @Override
    public void onServerUpdate(WorkerServerMetadata workerServer) {
        // 更新主映射表中的Worker信息
        // Worker的工作组不会在运行时改变，所以这里不更新配置工作组映射
        // 只更新Worker的状态、负载等信息
        workerMapping.put(workerServer.getAddress(), workerServer);

        // 遍历所有注册的监听器，通知它们Worker服务器信息已更新
        // 更新可能包括状态变化（NORMAL/BUSY）、负载变化、配置变化等
        for (IClustersChangeListener<WorkerServerMetadata> listener : workerClusterChangeListeners) {
            listener.onServerUpdate(workerServer);
        }
    }
}
