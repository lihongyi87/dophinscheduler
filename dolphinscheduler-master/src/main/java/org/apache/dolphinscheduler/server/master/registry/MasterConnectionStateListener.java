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

package org.apache.dolphinscheduler.server.master.registry;

import org.apache.dolphinscheduler.common.lifecycle.ServerLifeCycleManager;
import org.apache.dolphinscheduler.registry.api.ConnectionListener;
import org.apache.dolphinscheduler.registry.api.ConnectionState;
import org.apache.dolphinscheduler.registry.api.RegistryClient;

import lombok.extern.slf4j.Slf4j;

/**
 * Master节点注册中心连接状态监听器
 *
 * 这个监听器负责监控Master节点与注册中心（如ZooKeeper、etcd、Raft等）的连接状态变化，
 * 并根据不同的连接状态执行相应的处理逻辑。
 *
 * 连接状态对集群的影响：
 * - CONNECTED: 连接正常，Master可以正常参与集群协调
 * - SUSPENDED: 连接暂停，可能是网络抖动，等待恢复
 * - RECONNECTED: 重新连接成功，需要重新同步状态
 * - DISCONNECTED: 连接断开，Master必须停止服务防止脑裂
 *
 * 为什么连接断开时要停止服务？
 * 在分布式系统中，如果Master与注册中心失去连接，它就无法获知集群的最新状态，
 * 也无法向其他节点通告自己的状态。这时如果继续提供服务，可能出现"脑裂"问题，
 * 即多个Master认为自己是主节点，导致数据不一致。
 *
 * 所以一旦检测到连接断开，Master会主动停止自己，等待运维人员或自动化系统处理。
 */
@Slf4j
public class MasterConnectionStateListener implements ConnectionListener {

    /**
     * 注册中心客户端引用
     * 用于在需要时停止服务或获取其他注册中心相关信息
     */
    private final RegistryClient registryClient;

    /**
     * 构造函数
     *
     * @param registryClient 注册中心客户端，用于获取停止回调接口
     */
    public MasterConnectionStateListener(final RegistryClient registryClient) {
        this.registryClient = registryClient;
    }

    /**
     * 处理注册中心连接状态变化的回调方法
     *
     * 当Master与注册中心的连接状态发生变化时，会回调这个方法。
     * 根据不同的状态，执行不同的处理逻辑。
     *
     * @param state 新的连接状态
     */
    @Override
    public void onUpdate(ConnectionState state) {
        log.info("Master received a {} event from registry, the current server state is {}", state,
                ServerLifeCycleManager.getServerStatus());
        switch (state) {
            case CONNECTED:
                // 连接建立成功
                // 此时Master可以正常与注册中心通信，进行节点注册、心跳发送等操作
                // 通常在Master首次启动时会收到这个状态
                log.debug("Master successfully connected to registry");
                break;
            case SUSPENDED:
                // 连接暂时挂起
                // 这通常发生在网络抖动或注册中心负载较高时
                // 注册中心客户端会自动尝试重连，Master暂时无法进行注册中心操作
                log.warn("Master connection to registry is suspended, waiting for reconnection");
                break;
            case RECONNECTED:
                // 重新连接成功
                // 在连接中断后重新建立连接时会收到此状态
                // 此时需要重新验证节点状态，可能需要重新注册
                log.warn("Master reconnect to registry");
                break;
            case DISCONNECTED:
                // 连接完全断开
                // 这是最严重的情况，Master无法与注册中心通信
                // 为了防止脑裂问题，必须立即停止Master服务
                log.error("Master disconnected from registry, initiating shutdown to prevent split-brain scenario");
                registryClient.getStoppable().stop("Master disconnected from registry, will stop myself");
                break;
            default:
                // 未知的连接状态
                // 这通常不应该发生，但为了健壮性还是需要处理
                log.warn("Unknown connection state: {}", state);
        }
    }
}
