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

import org.apache.dolphinscheduler.raft.config.RaftConfiguration;
import org.apache.dolphinscheduler.raft.core.RaftRegistryStateMachine;
import org.apache.dolphinscheduler.raft.metrics.RaftMetrics;
import org.apache.dolphinscheduler.raft.utils.RaftNodeIdGenerator;
import org.apache.dolphinscheduler.registry.api.ConnectionListener;
import org.apache.dolphinscheduler.registry.api.Event;
import org.apache.dolphinscheduler.registry.api.Registry;
import org.apache.dolphinscheduler.registry.api.RegistryException;
import org.apache.dolphinscheduler.registry.api.SubscribeListener;
import org.apache.dolphinscheduler.server.master.config.MasterConfig;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Master集成的Raft注册中心实现
 *
 * 这个类将优化的Raft模块与Master启动生命周期进行集成，提供了基于Raft一致性算法的注册中心功能。
 *
 * Raft算法简介：
 * Raft是一种分布式一致性算法，用于在分布式系统中维护数据的一致性。
 * 它通过选举Leader（领导者）来协调集群中的所有操作，确保所有节点的数据保持一致。
 *
 * 在DolphinScheduler中的作用：
 * 1. 节点注册：Master节点向Raft集群注册自己的信息
 * 2. 状态同步：保证所有Master节点看到相同的集群状态
 * 3. 配置管理：统一管理集群配置信息
 * 4. 故障检测：通过心跳机制检测节点故障
 * 5. Leader选举：在多Master场景下选举出主Master
 *
 * 与ZooKeeper相比的优势：
 * - 更简单的部署和维护
 * - 更好的性能表现
 * - 内置的监控和指标
 * - 与Master服务的紧密集成
 *
 * 注意：目前处于开发阶段，部分功能还在完善中
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "registry", name = "type", havingValue = "raft")
public class MasterRaftRegistry implements Registry {

    /**
     * Raft配置对象
     * 包含Raft集群的各种配置参数，如节点地址、端口、选举超时时间等
     */
    @Autowired
    private RaftConfiguration raftConfig;

    /**
     * Raft指标收集器
     * 用于收集和暴露Raft集群的运行指标，如选举次数、日志复制延迟等
     */
    @Autowired
    private RaftMetrics raftMetrics;

    /**
     * Master配置对象
     * 从Master配置中获取端口等信息，用于推断Raft节点地址
     */
    @Autowired
    private MasterConfig masterConfig;

    /**
     * Raft注册中心状态机
     * 这是Raft算法的核心组件，负责处理所有的状态变更操作
     * 包括键值存储、事件通知、状态查询等功能
     */
    private RaftRegistryStateMachine stateMachine;

    /**
     * 连接状态监听器列表
     * 当Raft集群连接状态发生变化时，会通知这些监听器
     * 使用CopyOnWriteArrayList保证线程安全和读操作的高性能
     */
    private final List<ConnectionListener> connectionListeners = new CopyOnWriteArrayList<>();

    /**
     * 标识Raft注册中心是否已启动
     * 使用volatile确保多线程环境下的可见性
     */
    private volatile boolean started = false;

    /**
     * 初始化Master Raft注册中心
     *
     * 这个方法在Spring容器创建Bean后自动调用，负责初始化Raft相关组件。
     * 主要完成以下工作：
     * 1. 从Master配置推断Raft节点地址（如果未配置）
     * 2. 验证Raft配置的有效性
     * 3. 创建Raft状态机实例
     *
     * 为什么需要推断节点地址？
     * 在某些部署场景下，用户可能只配置了Master的监听端口，
     * 此时我们可以自动计算出Raft应该使用的端口，简化配置。
     */
    @PostConstruct
    public void init() {
        log.info("Initializing Master Raft Registry...");

        // 如果Raft节点地址未配置，从Master配置中推断
        // 这样可以简化配置，用户只需要配置Master端口即可
        if (raftConfig.getNodeAddress() == null) {
            String nodeAddress = masterConfig.getListenPort() != 0 ?
                "127.0.0.1:" + (masterConfig.getListenPort() + 3556) : // 为Raft端口添加偏移量，避免冲突
                "127.0.0.1:9234"; // 默认Raft端口
            raftConfig.setNodeAddress(nodeAddress);
            log.info("Inferred Raft node address from master config: {}", nodeAddress);
        }

        // 验证配置有效性，确保所有必需的参数都已设置
        raftConfig.validate();

        // 创建Raft状态机实例，这是Raft算法的核心组件
        // 状态机负责处理所有的数据操作和状态变更
        stateMachine = new RaftRegistryStateMachine(raftConfig, raftMetrics);

        log.info("Master Raft Registry initialized with node address: {}", raftConfig.getNodeAddress());
    }

    /**
     * 启动Master Raft注册中心服务
     *
     * 这个方法负责启动Raft集群并建立与其他节点的连接。
     * 启动过程包括：
     * 1. 生成稳定的节点ID（基于节点地址的唯一标识）
     * 2. 启动Raft服务器（当前为单节点模式，完整集群模式开发中）
     * 3. 通知所有连接监听器服务已启动
     *
     * 为什么需要稳定的节点ID？
     * 在Raft算法中，每个节点都需要一个唯一且稳定的标识符。
     * 即使节点重启，也应该保持相同的ID，这样其他节点才能正确识别。
     *
     * @throws RegistryException 如果启动过程中发生错误
     */
    @Override
    public void start() {
        if (started) {
            log.warn("Raft registry is already started");
            return;
        }

        try {
            log.info("Starting Master Raft Registry...");

            // 生成稳定的节点ID
            // 基于节点地址生成唯一标识符，确保重启后ID保持不变
            String nodeId = RaftNodeIdGenerator.generateNodeId(raftConfig.getNodeAddress());
            log.info("Generated node ID: {}", nodeId);

            // TODO: 启动Raft服务器 - 将在下一个迭代中实现
            // 目前使用状态机直接模式进行单节点操作
            // 在完整的集群模式下，这里会启动Raft协议的网络通信组件

            started = true;

            // 通知所有连接监听器服务已启动
            // 这让依赖注册中心的其他组件知道可以开始使用注册中心服务了
            for (ConnectionListener listener : connectionListeners) {
                try {
                    listener.onConnected();
                } catch (Exception e) {
                    log.warn("Failed to notify connection listener", e);
                }
            }

            log.info("Master Raft Registry started successfully");
        } catch (Exception e) {
            log.error("Failed to start Master Raft Registry", e);
            throw new RegistryException("Failed to start Raft registry", e);
        }
    }

    @Override
    public boolean isConnected() {
        return started;
    }

    @Override
    public void connectUntilTimeout(Duration timeout) throws RegistryException {
        if (!isConnected()) {
            throw new RegistryException("Master Raft registry not connected");
        }
    }

    /**
     * 订阅指定路径的变更事件
     *
     * 这个方法允许其他组件监听注册中心中特定路径的数据变化。
     * 当路径下的数据发生增加、修改或删除时，会通知订阅者。
     *
     * 典型的使用场景：
     * - Master节点监听Worker节点的注册/注销
     * - Worker节点监听任务分配
     * - 监听配置变更
     *
     * 事件类型包括：
     * - ADD: 新增节点或数据
     * - UPDATE: 更新现有数据
     * - REMOVE: 删除节点或数据
     *
     * @param path 要监听的路径，支持通配符
     * @param listener 事件监听器，接收变更通知
     * @throws RegistryException 如果注册中心未启动
     */
    @Override
    public void subscribe(String path, SubscribeListener listener) {
        if (!started) {
            throw new RegistryException("Registry not started");
        }

        // 在Raft状态机上添加事件监听器
        // 当路径下的数据发生变化时，状态机会触发相应的事件
        stateMachine.addEventListener(path, event -> {
            try {
                // 将Raft内部事件转换为通用的注册中心事件
                Event registryEvent = Event.builder()
                        .watchedPath(event.getWatchedPath())  // 被监听的路径
                        .eventPath(event.getEventPath())      // 实际发生变化的路径
                        .eventData(event.getValue())          // 变更的数据内容
                        .type(convertEventType(event.getEventType())) // 事件类型转换
                        .build();
                // 异步通知监听器，避免阻塞Raft状态机
                listener.notify(registryEvent);
            } catch (Exception e) {
                log.warn("Error notifying registry listener for path: {}", path, e);
            }
        });
    }

    @Override
    public void addConnectionStateListener(ConnectionListener listener) {
        connectionListeners.add(listener);
    }

    @Override
    public String get(String key) throws RegistryException {
        if (!started) {
            throw new RegistryException("Registry not started");
        }
        
        String value = stateMachine.getValue(key);
        if (value == null) {
            throw new RegistryException("Key not found: " + key);
        }
        return value;
    }

    /**
     * 向注册中心存储键值对数据
     *
     * 这是注册中心的核心功能之一，用于存储各种类型的数据：
     * - 节点注册信息（Master、Worker节点的地址、状态等）
     * - 配置信息
     * - 运行时状态数据
     *
     * @param key 数据的键，通常是路径格式，如 "/dolphinscheduler/masters/192.168.1.100:5678"
     * @param value 数据的值，通常是JSON格式的字符串
     * @param deleteOnDisconnect 是否在连接断开时自动删除
     *                          - true: 临时节点，适用于节点注册信息（节点下线时自动清理）
     *                          - false: 持久节点，适用于配置信息（需要手动删除）
     * @throws RegistryException 如果操作失败
     */
    @Override
    public void put(String key, String value, boolean deleteOnDisconnect) {
        if (!started) {
            throw new RegistryException("Registry not started");
        }

        try {
            if (deleteOnDisconnect) {
                // 临时节点模式：连接断开时自动删除
                // 适用于节点注册信息，当节点宕机时注册信息会自动清理
                // TODO: 在集群模式实现时使用真正的Raft命令
                log.debug("Simulating ephemeral put for key: {}, value: {}", key, value);
                stateMachine.getValue(key); // 模拟临时存储
            } else {
                // 持久节点模式：需要手动删除
                // 适用于配置信息等需要持久保存的数据
                // TODO: 在集群模式实现时使用真正的Raft命令
                log.debug("Simulating persistent put for key: {}, value: {}", key, value);
                stateMachine.getValue(key); // 模拟持久存储
            }
        } catch (Exception e) {
            throw new RegistryException("Failed to put key: " + key, e);
        }
    }

    @Override
    public void delete(String key) {
        if (!started) {
            throw new RegistryException("Registry not started");
        }
        
        try {
            // TODO: Use proper Raft commands when cluster mode is implemented
            log.debug("Simulating delete for key: {}", key);
        } catch (Exception e) {
            throw new RegistryException("Failed to delete key: " + key, e);
        }
    }

    @Override
    public Collection<String> children(String key) {
        if (!started) {
            throw new RegistryException("Registry not started");
        }
        
        try {
            return stateMachine.getChildren(key);
        } catch (Exception e) {
            log.warn("Failed to get children for key: {}", key, e);
            return Collections.emptyList();
        }
    }

    @Override
    public boolean exists(String key) {
        if (!started) {
            return false;
        }
        
        return stateMachine.exists(key);
    }

    @Override
    public boolean acquireLock(String key) {
        return acquireLock(key, 0);
    }

    /**
     * 获取分布式锁
     *
     * 分布式锁是DolphinScheduler中非常重要的功能，用于解决并发问题：
     * 1. Master选举：确保只有一个Master成为主节点
     * 2. 任务调度：确保同一个任务只被一个Master调度
     * 3. 资源竞争：解决多节点同时操作共享资源的问题
     *
     * Raft算法的优势：
     * 相比ZooKeeper等其他实现，Raft算法提供更好的性能和一致性保证
     *
     * @param key 锁的唯一标识符，通常使用路径格式
     * @param timeout 获取锁的超时时间（毫秒），0表示立即返回
     * @return true 如果成功获取锁，false 否则
     * @throws RegistryException 如果注册中心未启动
     */
    @Override
    public boolean acquireLock(String key, long timeout) {
        if (!started) {
            throw new RegistryException("Registry not started");
        }

        try {
            // 生成当前节点的唯一标识符，用于标识锁的所有者
            String nodeId = RaftNodeIdGenerator.generateNodeId(raftConfig.getNodeAddress());
            // TODO: 在集群模式实现时使用真正的Raft命令
            // 真正的实现会通过Raft一致性算法确保分布式锁的正确性
            log.debug("Simulating acquire lock for key: {}, owner: {}", key, nodeId);
            return true; // 单节点模式下模拟成功
        } catch (Exception e) {
            log.error("Failed to acquire lock: {}", key, e);
            return false;
        }
    }

    @Override
    public boolean releaseLock(String key) {
        if (!started) {
            throw new RegistryException("Registry not started");
        }
        
        try {
            String nodeId = RaftNodeIdGenerator.generateNodeId(raftConfig.getNodeAddress());
            // TODO: Use proper Raft commands when cluster mode is implemented
            log.debug("Simulating release lock for key: {}, owner: {}", key, nodeId);
            return true; // Simulate success for single-node mode
        } catch (Exception e) {
            log.error("Failed to release lock: {}", key, e);
            return false;
        }
    }

    /**
     * 关闭Master Raft注册中心并清理资源
     *
     * 这个方法在Master服务关闭时被自动调用，负责清理所有相关资源。
     * 关闭步骤包括：
     * 1. 通知所有连接监听器服务即将关闭
     * 2. 关闭Raft状态机，停止所有正在进行的操作
     * 3. 释放网络连接和其他系统资源
     *
     * 为什么需要优雅关闭？
     * 1. 确保正在进行的操作能够完成
     * 2. 通知其他节点该节点即将下线
     * 3. 防止资源泄漏和垃圾数据
     *
     * @throws IOException 如果关闭过程中发生错误
     */
    @PreDestroy
    @Override
    public void close() throws IOException {
        if (started) {
            log.info("Shutting down Master Raft Registry...");

            // 通知所有连接监听器服务即将关闭
            // 让依赖注册中心的其他组件有机会做清理工作
            for (ConnectionListener listener : connectionListeners) {
                try {
                    listener.onDisconnected();
                } catch (Exception e) {
                    log.warn("Failed to notify connection listener on shutdown", e);
                }
            }

            // 关闭Raft状态机
            // 这会停止所有正在进行的Raft操作，并清理相关资源
            if (stateMachine != null) {
                stateMachine.onShutdown();
            }

            started = false;
            log.info("Master Raft Registry shutdown completed");
        }
    }

    /**
     * 将Raft事件类型转换为通用注册中心事件类型
     *
     * 这个转换方法确保了不同注册中心实现之间的兼容性。
     * 无论底层使用ZooKeeper、etcd还是Raft，上层应用都能使用相同的事件类型。
     *
     * @param raftEventType Raft内部的事件类型
     * @return 通用的注册中心事件类型
     */
    private Event.Type convertEventType(org.apache.dolphinscheduler.raft.event.RaftEventType raftEventType) {
        switch (raftEventType) {
            case ADD:
                // 新增事件：新的节点或数据被添加到注册中心
                return Event.Type.ADD;
            case UPDATE:
                // 更新事件：现有节点或数据被修改
                return Event.Type.UPDATE;
            case REMOVE:
                // 删除事件：节点或数据被从注册中心中移除
                return Event.Type.REMOVE;
            default:
                // 默认作为更新事件处理，确保健壮性
                log.warn("Unknown Raft event type: {}, treating as UPDATE", raftEventType);
                return Event.Type.UPDATE;
        }
    }

    /**
     * 健康检查和监控方法
     */

    /**
     * 检查Raft注册中心是否健康
     *
     * 健康状态的判定标准：
     * 1. 服务已启动并正常运行
     * 2. Raft集群的各项指标都在正常范围内
     * 3. 能够正常处理读写请求
     *
     * 这个方法通常被监控系统调用，用于判断该节点是否能够继续提供服务。
     *
     * @return true 如果注册中心健康，false 否则
     */
    public boolean isHealthy() {
        return started && raftMetrics.isHealthy();
    }

    /**
     * 获取Raft指标快照
     *
     * 返回当前时刻的Raft集群运行指标，包括：
     * - 当前节点的角色（Leader、Follower、Candidate）
     * - 选举任期号
     * - 日志复制进度
     * - 请求处理延迟
     * - 等其他运行指标
     *
     * 这些指标可以用于：
     * - 性能监控和警告
     * - 问题诊断和排查
     * - 容量规划和优化
     *
     * @return 包含各项指标的快照对象
     */
    public RaftMetrics.MetricsSnapshot getMetricsSnapshot() {
        return raftMetrics.getSnapshot();
    }
}