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

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.dolphinscheduler.registry.api.Registry;
import org.apache.dolphinscheduler.registry.api.enums.RegistryNodeType;
import org.apache.dolphinscheduler.registry.api.ha.AbstractHAServer;
import org.apache.dolphinscheduler.registry.api.ha.AbstractServerStatusChangeListener;
import org.apache.dolphinscheduler.server.master.config.MasterConfig;

import lombok.extern.slf4j.Slf4j;

import org.springframework.stereotype.Component;

/**
 * Master协调器
 * 
 * 这个类是集群中的单例组件，负责协调和管理Master节点的各种控制工作。
 * 它基于高可用性(HA)机制工作，确保在分布式环境中只有一个活跃的协调器。
 * 
 * 主要功能：
 * 1. 管理任务组协调器（TaskGroupCoordinator）
 * 2. 实现Master节点的高可用性选主
 * 3. 处理Master节点状态变化（Active/StandBy切换）
 * 4. 确保关键资源的单点控制和协调
 * 
 * 工作原理：
 * - 使用注册中心进行选主，只有一个Master节点能成为Active状态
 * - Active状态时启动各种协调服务
 * - StandBy状态时停止协调服务，等待接管
 * 
 * 简单理解：就像一个"总指挥中心"，在多个Master节点中选出一个
 * 作为总指挥，负责整个集群的任务组管理和资源协调。
 */
@Slf4j
@Component
public class MasterCoordinator extends AbstractHAServer {

    /**
     * 任务组协调器 - 负责管理和协调任务组的执行
     */
    private final ITaskGroupCoordinator taskGroupCoordinator;

    /**
     * 构造MasterCoordinator
     *
     * @param registry 注册中心客户端，用于选主和状态管理
     * @param masterConfig Master配置信息
     * @param taskGroupCoordinator 任务组协调器实例
     */
    public MasterCoordinator(final Registry registry,
                             final MasterConfig masterConfig,
                             final ITaskGroupCoordinator taskGroupCoordinator) {
        // ========== 第一步：调用父类构造函数，初始化HA基础设施 ==========
        // 传入注册中心客户端，用于进行分布式选举
        // 设置协调器的注册路径，所有Master节点会在该路径下竞争Leader
        // 提供本节点的地址，用于标识参与选举的Master节点
        super(
                registry,
                RegistryNodeType.MASTER_COORDINATOR.getRegistryPath(),
                masterConfig.getMasterAddress());

        // ========== 第二步：保存任务组协调器引用 ==========
        // 任务组协调器是Master协调器管理的核心组件
        // 它负责实际的任务组资源管理和调度工作
        this.taskGroupCoordinator = taskGroupCoordinator;

        // ========== 第三步：注册状态变化监听器 ==========
        // 添加状态监听器，监听本节点的Active/StandBy状态变化
        // 当成为Leader时启动任务组协调器
        // 当失去Leader地位时停止任务组协调器
        // 这确保了整个集群中只有一个活跃的任务组协调器
        addServerStatusChangeListener(new MasterCoordinatorListener(taskGroupCoordinator));
    }

    /**
     * 启动Master协调器
     *
     * 启动高可用性选主机制，开始参与Master节点的选举。
     * 只有被选为Leader的节点才会变为Active状态。
     */
    @Override
    public void start() {
        // ========== 启动HA选主机制 ==========
        // 调用父类的start方法，这会：
        // 1. 在注册中心创建临时顺序节点参与选举
        // 2. 监听Leader节点的变化
        // 3. 如果本节点成为Leader，触发changeToActive回调
        // 4. 如果本节点不是Leader，保持StandBy状态并持续监听
        super.start();

        // 记录启动完成日志
        log.info("MasterCoordinator started...");
    }

    /**
     * 关闭Master协调器
     *
     * 停止所有协调服务，释放相关资源，退出选主竞争。
     */
    @Override
    public void close() {
        // ========== 第一步：关闭任务组协调器 ==========
        // 停止任务组的管理和调度工作
        // 释放任务组相关的资源（如数据库连接、缓存等）
        // 确保正在进行的任务组操作能够优雅结束
        taskGroupCoordinator.close();

        // ========== 第二步：记录关闭日志 ==========
        // 记录关闭完成，便于运维人员了解节点状态
        log.info("MasterCoordinator shutdown...");

        // 注：父类的close方法会在外部调用，负责：
        // - 从注册中心删除选举节点
        // - 停止Leader监听
        // - 清理HA相关资源
    }

    /**
     * Master协调器状态变化监听器
     * 
     * 这个内部类监听Master协调器的状态变化，
     * 当状态从StandBy变为Active时启动任务组协调器，
     * 当状态从Active变为StandBy时停止任务组协调器。
     */
    public static class MasterCoordinatorListener extends AbstractServerStatusChangeListener {

        /**
         * 任务组协调器引用
         */
        private final ITaskGroupCoordinator taskGroupCoordinator;

        public MasterCoordinatorListener(ITaskGroupCoordinator taskGroupCoordinator) {
            this.taskGroupCoordinator = checkNotNull(taskGroupCoordinator);
        }

        /**
         * 变为Active状态时的处理
         *
         * 当此Master节点被选为Leader时，启动任务组协调器，
         * 开始执行任务组的管理和协调工作。
         */
        @Override
        public void changeToActive() {
            // ========== 启动任务组协调器 ==========
            // 当本节点成为Leader时，立即启动任务组协调器
            // 任务组协调器会：
            // 1. 从数据库加载所有活跃的任务组信息
            // 2. 初始化任务组的资源分配和限制
            // 3. 开始监控任务组的使用情况
            // 4. 处理任务组的申请和释放请求
            taskGroupCoordinator.start();
        }

        /**
         * 变为StandBy状态时的处理
         *
         * 当此Master节点失去Leader地位时，停止任务组协调器，
         * 释放资源，等待下次被选为Leader。
         */
        @Override
        public void changeToStandBy() {
            // ========== 停止任务组协调器 ==========
            // 当本节点失去Leader地位时，立即停止任务组协调器
            // 停止过程会：
            // 1. 停止接收新的任务组请求
            // 2. 清理内存中的任务组状态信息
            // 3. 关闭与任务组相关的监控线程
            // 4. 释放所有占用的系统资源
            // 这确保了新的Leader可以接管任务组管理工作
            taskGroupCoordinator.close();
        }
    }

}
