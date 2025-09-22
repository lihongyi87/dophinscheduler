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

package org.apache.dolphinscheduler.server.worker.registry;

import org.apache.dolphinscheduler.common.lifecycle.ServerLifeCycleManager;
import org.apache.dolphinscheduler.registry.api.ConnectionListener;
import org.apache.dolphinscheduler.registry.api.ConnectionState;
import org.apache.dolphinscheduler.registry.api.RegistryClient;

import lombok.extern.slf4j.Slf4j;

/**
 * DolphinScheduler Worker连接状态监听器
 *
 * 这个类是Worker节点与注册中心连接状态的监听器，负责处理连接状态变化事件。
 * 它就像一个"网络连接管家"，时刻监控Worker节点与注册中心的连接情况，
 * 并在连接状态发生变化时采取相应的应对措施。
 *
 * 主要功能：
 * 1. 监听与注册中心的连接状态变化（连接、断开、重连、暂停等）
 * 2. 处理连接断开情况，确保Worker节点的安全关闭
 * 3. 记录连接状态变化的日志，便于故障排查
 * 4. 实现故障检测和自动保护机制
 *
 * 连接状态类型：
 * - CONNECTED: 首次连接成功
 * - RECONNECTED: 重新连接成功
 * - SUSPENDED: 连接暂停（通常是临时网络问题）
 * - DISCONNECTED: 连接完全断开
 */
@Slf4j
public class WorkerConnectionStateListener implements ConnectionListener {

    /**
     * 注册中心客户端引用
     * 用于在连接断开时执行停止操作，确保Worker节点安全退出
     */
    private final RegistryClient registryClient;

    /**
     * 构造函数
     *
     * @param registryClient 注册中心客户端，用于处理连接状态变化
     */
    public WorkerConnectionStateListener(final RegistryClient registryClient) {
        // 保存注册中心客户端引用，用于后续的连接状态处理
        this.registryClient = registryClient;
    }

    /**
     * 处理连接状态更新事件
     *
     * 当Worker节点与注册中心的连接状态发生变化时，这个方法会被自动调用。
     * 它实现了一套完整的连接状态处理机制：
     *
     * 1. CONNECTED（首次连接）: 正常状态，无需特殊处理
     * 2. SUSPENDED（连接暂停）: 网络临时中断，等待恢复
     * 3. RECONNECTED（重新连接）: 网络恢复，记录重连事件
     * 4. DISCONNECTED（连接断开）: 网络完全断开，启动安全关闭流程
     *
     * 安全机制说明：
     * 当连接断开时，Worker会主动停止自己，防止出现"脑裂"现象
     * （即Worker认为自己在工作，但Master无法感知到它的存在）。
     *
     * @param state 新的连接状态
     */
    @Override
    public void onUpdate(ConnectionState state) {
        // 记录状态变化日志，包含当前服务器的生命周期状态
        log.info("Worker received a {} event from registry, the current server state is {}", state,
                ServerLifeCycleManager.getServerStatus());

        // 根据不同的连接状态执行相应的处理逻辑
        switch (state) {
            case CONNECTED:
                // 首次连接成功 - 正常状态，无需特殊处理
                // Worker已经成功注册到集群中，可以接收任务分发
                break;
            case SUSPENDED:
                // 连接暂停 - 通常是临时网络问题
                // 这种状态下Worker暂时无法与注册中心通信，但连接可能很快恢复
                // 不执行停止操作，等待网络恢复
                break;
            case RECONNECTED:
                // 重新连接成功 - 网络从中断状态恢复
                // 记录警告日志，提醒运维人员曾经发生过网络中断
                log.warn("Worker reconnect to registry");
                break;
            case DISCONNECTED:
                // 连接完全断开 - 启动安全关闭流程
                // 这是最严重的情况，Worker必须主动停止以避免任务执行混乱
                // 通过registryClient的stoppable组件来优雅地停止Worker服务
                registryClient.getStoppable().stop("Worker disconnected from registry, will stop myself");
            default:
                // 其他未知状态 - 保持默认行为
        }
    }
}
