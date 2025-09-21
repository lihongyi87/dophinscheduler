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

import org.apache.dolphinscheduler.common.utils.MapComparator;
import org.apache.dolphinscheduler.dao.entity.WorkerGroup;
import org.apache.dolphinscheduler.dao.repository.WorkerGroupDao;
import org.apache.dolphinscheduler.server.master.config.MasterConfig;
import org.apache.dolphinscheduler.server.master.utils.MasterThreadFactory;

import org.apache.commons.collections4.CollectionUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Worker组变化通知器
 *
 * 这个类负责监控数据库中WorkerGroup配置的变化，并通知相关的监听器。
 * 它定期轮询数据库，检测WorkerGroup的增加、删除、修改，然后触发相应的事件处理。
 * 类比：一个人事变动监控员，定期检查组织架构的变化，并及时通知相关部门。
 *
 * 主要功能：
 * 1. 定期轮询：按配置的间隔时间检查数据库中的WorkerGroup变化
 * 2. 变化检测：比较当前状态与数据库状态，识别增加、删除、修改的WorkerGroup
 * 3. 事件通知：将变化信息通知给所有注册的监听器
 * 4. 状态同步：维护本地WorkerGroup状态与数据库的同步
 *
 * 设计考虑：
 * - 使用事务确保在主从架构下查询的一致性
 * - 使用MapComparator高效检测集合变化
 * - 支持多个监听器的订阅模式
 */
@Slf4j
@Component
public class WorkerGroupChangeNotifier {

    /**
     * Master配置信息
     *
     * 包含WorkerGroup刷新间隔等配置参数。
     */
    private final MasterConfig masterConfig;

    /**
     * 事务模板
     *
     * 用于确保数据库查询的事务一致性，特别是在主从架构下确保查询路由到主库。
     */
    private final TransactionTemplate transactionTemplate;

    /**
     * WorkerGroup数据访问对象
     *
     * 用于查询数据库中的WorkerGroup配置信息。
     */
    private final WorkerGroupDao workerGroupDao;

    /**
     * WorkerGroup变化监听器列表
     *
     * 存储所有注册的监听器，当WorkerGroup发生变化时会通知这些监听器。
     * 使用CopyOnWriteArrayList确保并发安全。
     */
    private final List<WorkerGroupListener> listeners = new CopyOnWriteArrayList<>();

    /**
     * 当前WorkerGroup映射表
     *
     * 缓存当前已知的WorkerGroup状态，用于与数据库状态进行比较检测变化。
     * 键为WorkerGroup名称，值为WorkerGroup实体。
     */
    private Map<String, WorkerGroup> workerGroupMap = new HashMap<>();

    /**
     * 构造函数
     *
     * @param masterConfig Master配置信息
     * @param workerGroupDao WorkerGroup数据访问对象
     * @param transactionTemplate 事务模板
     */
    public WorkerGroupChangeNotifier(final MasterConfig masterConfig,
                                     final WorkerGroupDao workerGroupDao,
                                     final TransactionTemplate transactionTemplate) {
        this.masterConfig = masterConfig;
        this.workerGroupDao = workerGroupDao;
        this.transactionTemplate = transactionTemplate;
    }

    /**
     * 启动WorkerGroup变化监控
     *
     * 首先执行一次初始化检测，然后按配置的间隔时间定期检测变化。
     * 类比：启动一个定时巡检系统，先做一次全面检查，然后按时巡检。
     */
    public void start() {
        // 首先执行一次初始化检测，获取当前数据库中的WorkerGroup状态
        // 这步是必需的，确保在定时任务启动前已经有了基准数据
        detectWorkerGroupChanges();

        // 从配置中获取WorkerGroup刷新间隔时间（秒）
        // 这个间隔时间决定了系统对数据库配置变化的响应速度
        final long workerGroupRefreshIntervalSeconds = masterConfig.getWorkerGroupRefreshInterval().getSeconds();

        // 启动定时任务，定期检测WorkerGroup变化
        // 使用scheduleWithFixedDelay确保上一次检测完成后再等待指定时间才执行下一次
        // 这种方式比scheduleAtFixedRate更安全，不会在上次检测还未完成时就启动下一次
        MasterThreadFactory.getDefaultSchedulerThreadExecutor().scheduleWithFixedDelay(
                this::detectWorkerGroupChanges,        // 检测方法的方法引用
                workerGroupRefreshIntervalSeconds,     // 初始延迟时间
                workerGroupRefreshIntervalSeconds,     // 每次执行间隔时间
                TimeUnit.SECONDS);                     // 时间单位
    }

    /**
     * 订阅WorkerGroup变化事件
     *
     * 允许外部组件注册监听器，当WorkerGroup发生变化时会收到通知。
     * 类比：在人事部门登记，当组织架构变化时会收到通知。
     *
     * @param listener WorkerGroup变化监听器
     */
    public void subscribeWorkerGroupsChange(WorkerGroupListener listener) {
        // 将监听器添加到线程安全的CopyOnWriteArrayList中
        // CopyOnWriteArrayList在添加元素时会复制整个数组，适合读多写少的场景
        // 这里一般只在系统启动时注册监听器，之后主要是读取操作
        listeners.add(listener);
    }

    /**
     * 检测WorkerGroup变化
     *
     * 这是核心方法，负责：
     * 1. 从数据库获取最新的WorkerGroup配置
     * 2. 与本地缓存比较，检测变化
     * 3. 触发相应的监听器
     * 4. 更新本地缓存
     *
     * 使用synchronized确保同一时间只有一个检测任务在执行。
     */
    public synchronized void detectWorkerGroupChanges() {
        try {
            // 步骤1：检测数据库中的WorkerGroup变化
            // 通过比较本地缓存与数据库的状态，识别出新增、删除、修改的WorkerGroup
            final MapComparator<String, WorkerGroup> mapComparator = detectChangedWorkerGroups();

            // 步骤2：触发监听器，通知所有注册的组件处理变化
            // 监听器可能包括WorkerClusters等组件，它们需要根据变化更新自己的状态
            triggerListeners(mapComparator);

            // 步骤3：更新本地缓存，为下次检测做准备
            // 将新的状态设置为下次比较的基准
            workerGroupMap = mapComparator.getNewMap();
        } catch (Exception ex) {
            // 捕获所有异常，确保定时任务不会因为单次失败而停止
            // 这对于长期运行的后台服务非常重要
            log.error("Detect WorkerGroup changes failed", ex);
        }
    }

    /**
     * 获取当前WorkerGroup映射表
     *
     * 主要用于测试和内部调用，返回当前缓存的WorkerGroup状态。
     *
     * @return 当前WorkerGroup映射表
     */
    Map<String, WorkerGroup> getWorkerGroupMap() {
        // 返回当前缓存的WorkerGroup映射表
        // 这个方法主要用于单元测试和内部调用，不对外部公开
        // 返回的是直接引用，调用者需要注意不要修改返回的Map
        return workerGroupMap;
    }

    /**
     * 检测变化的WorkerGroup
     *
     * 从数据库获取最新的WorkerGroup配置，并与本地缓存进行比较。
     * 使用事务确保在MySQL主从模式下，查询会路由到主库，
     * 避免从从库查询到不是最新的数据。
     *
     * @return MapComparator对象，包含变化的详细信息
     */
    private MapComparator<String, WorkerGroup> detectChangedWorkerGroups() {
        // 使用事务模板确保数据库查询的一致性
        // 在MySQL主从架构下，事务会确保查询路由到主库，获取最新数据
        // 避免因主从延迟导致的数据不一致问题
        return transactionTemplate.execute(status -> {
            // 从数据库查询所有WorkerGroup记录
            // 使用Stream API将列表转换为以WorkerGroup名称为键的映射表
            Map<String, WorkerGroup> tmpWorkerGroupMap = workerGroupDao.queryAll()
                    .stream()  // 将列表转换为流
                    .collect(Collectors.toMap(WorkerGroup::getName, workerGroup -> workerGroup));  // 收集为映射表

            // 创建映射比较器，用于比较本地缓存（旧状态）与数据库（新状态）的差异
            // MapComparator会自动识别出新增、删除、修改的元素
            return new MapComparator<>(workerGroupMap, tmpWorkerGroupMap);
        });
    }

    /**
     * 触发监听器
     *
     * 根据变化类型，分别触发相应的监听器方法：
     * 1. 添加事件：通知所有监听器有新的WorkerGroup被添加
     * 2. 删除事件：通知所有监听器有WorkerGroup被删除
     * 3. 更新事件：通知所有监听器有WorkerGroup被修改
     *
     * @param mapComparator 映射比较器，包含变化的详细信息
     */
    private void triggerListeners(MapComparator<String, WorkerGroup> mapComparator) {
        // 检查是否有注册的监听器，如果没有则直接返回
        // 这种早期返回可以避免不必要的计算
        if (CollectionUtils.isEmpty(listeners)) {
            return;
        }

        // 处理WorkerGroup添加事件
        // 获取所有新增的WorkerGroup，并通知所有监听器
        final List<WorkerGroup> workerGroupsAdded = mapComparator.getValuesToAdd();
        if (CollectionUtils.isNotEmpty(workerGroupsAdded)) {
            // 使用forEach遍历所有监听器，调用它们的onWorkerGroupAdd方法
            listeners.forEach(listener -> listener.onWorkerGroupAdd(workerGroupsAdded));
        }

        // 处理WorkerGroup删除事件
        // 获取所有被删除的WorkerGroup，并通知所有监听器
        final List<WorkerGroup> workerGroupsRemoved = mapComparator.getValuesToRemove();
        if (CollectionUtils.isNotEmpty(workerGroupsRemoved)) {
            // 监听器需要清理与被删除WorkerGroup相关的资源和缓存
            listeners.forEach(listener -> listener.onWorkerGroupDelete(workerGroupsRemoved));
        }

        // 处理WorkerGroup更新事件
        // 获取所有发生变化的WorkerGroup（使用新版本的数据）
        final List<WorkerGroup> workerGroupsUpdated = mapComparator.getNewValuesToUpdate();
        if (CollectionUtils.isNotEmpty(workerGroupsUpdated)) {
            // 监听器需要根据新的WorkerGroup配置更新自己的状态
            listeners.forEach(listener -> listener.onWorkerGroupChange(workerGroupsUpdated));
        }
    }

    /**
     * WorkerGroup监听器接口
     *
     * 定义了处理WorkerGroup变化事件的方法。
     * 实现者需要根据不同的变化类型做出相应的处理。
     */
    public interface WorkerGroupListener {

        /**
         * WorkerGroup删除事件处理
         *
         * @param workerGroups 被删除的WorkerGroup列表
         */
        void onWorkerGroupDelete(List<WorkerGroup> workerGroups);

        /**
         * WorkerGroup添加事件处理
         *
         * @param workerGroups 新添加的WorkerGroup列表
         */
        void onWorkerGroupAdd(List<WorkerGroup> workerGroups);

        /**
         * WorkerGroup变化事件处理
         *
         * @param workerGroups 发生变化的WorkerGroup列表
         */
        void onWorkerGroupChange(List<WorkerGroup> workerGroups);
    }
}
