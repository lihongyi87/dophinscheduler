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

import org.apache.dolphinscheduler.registry.api.Event;
import org.apache.dolphinscheduler.registry.api.SubscribeListener;

import lombok.extern.slf4j.Slf4j;

/**
 * 集群订阅监听器抽象基类
 *
 * 这是一个抽象类，为Master和Worker集群提供统一的注册中心事件处理框架。
 * 它实现了RegistryClient的SubscribeListener接口，处理来自注册中心的节点变化事件。
 * 类比：一个消息中转站，负责接收各种节点变化的消息，然后根据消息类型分发给具体的处理器。
 *
 * 主要职责：
 * 1. 事件解析：将注册中心的原始事件数据解析为服务器元数据对象
 * 2. 事件分发：根据事件类型（ADD、REMOVE、UPDATE）调用相应的处理方法
 * 3. 顺序保证：使用同步机制确保事件按顺序处理，避免并发导致的状态不一致
 * 4. 异常处理：捕获并记录处理过程中的异常，确保系统稳定性
 *
 * 泛型参数：
 * T extends BaseServerMetadata：服务器元数据类型，可以是MasterServerMetadata或WorkerServerMetadata
 *
 * 设计模式：
 * - 模板方法模式：定义事件处理的框架，具体解析和处理逻辑由子类实现
 * - 策略模式：不同类型的集群（Master/Worker）有不同的处理策略
 */
@Slf4j
public abstract class AbstractClusterSubscribeListener<T extends BaseServerMetadata> implements SubscribeListener {

    /**
     * 处理注册中心事件通知
     *
     * 这是SubscribeListener接口的核心方法，当注册中心检测到节点变化时会调用此方法。
     * 方法负责解析事件数据，识别事件类型，并调用相应的处理逻辑。
     *
     * 类比：一个邮件分拣员，收到各种类型的邮件后，根据邮件标识分发到不同的处理部门。
     *
     * 处理流程：
     * 1. 事件数据解析：将JSON格式的心跳数据解析为服务器元数据对象
     * 2. 事件类型识别：根据事件类型（ADD/REMOVE/UPDATE）选择处理策略
     * 3. 委托处理：调用子类实现的具体处理方法
     * 4. 异常保护：确保单个事件处理失败不影响后续事件
     *
     * @param event 注册中心事件，包含事件类型和数据
     */
    @Override
    public void notify(Event event) {
        // ========== 异常保护 ==========
        // 使用try-catch确保事件处理异常不会影响整个监听器的工作
        // 这对于长期运行的服务非常重要，单个事件处理失败不应该影响后续事件
        try {
            // ========== 顺序处理保证 ==========
            // 使用synchronized确保事件按顺序处理，避免并发导致的状态不一致
            // 例如：如果同时收到一个节点的ADD和UPDATE事件，需要确保按正确顺序处理
            synchronized (this) {
                // 获取事件类型（ADD、REMOVE、UPDATE）
                Event.Type type = event.getType();

                // ========== 事件数据解析 ==========
                // 调用子类实现的解析方法，将JSON字符串转换为服务器元数据对象
                // 这是模板方法模式的体现，具体解析逻辑由子类决定
                T server = parseServerFromHeartbeat(event.getEventData());

                // 检查解析结果，如果解析失败则记录错误并跳过处理
                // 可能的失败原因：JSON格式错误、数据缺失、反序列化异常等
                if (server == null) {
                    log.error("Unknown cluster change event: {}", event);
                    return;
                }

                // ========== 事件类型处理 ==========
                // 根据事件类型调用相应的处理方法
                // 每种事件类型对应集群状态的不同变化，需要不同的处理逻辑
                switch (type) {
                    case ADD:
                        // 节点添加事件：有新的服务器加入集群
                        log.info("Server {} added", server);
                        onServerAdded(server);
                        break;
                    case REMOVE:
                        // 节点移除事件：服务器从集群中离开（可能是下线或故障）
                        log.warn("Server {} removed", server);
                        onServerRemove(server);
                        break;
                    case UPDATE:
                        // 节点更新事件：服务器信息发生变化（如状态变更、负载变化）
                        log.debug("Server {} updated", server);
                        onServerUpdate(server);
                        break;
                    default:
                        // 忽略未知的事件类型，确保系统的向前兼容性
                        break;
                }
            }
        } catch (Exception ex) {
            // 记录事件处理失败的详细信息，但不抛出异常
            // 这样可以避免单个事件处理失败导致整个监听器停止工作
            log.error("Notify cluster change event: {} failed", event, ex);
        }
    }

    /**
     * 获取订阅范围
     *
     * 实现SubscribeListener接口的方法，定义了监听器的订阅范围。
     * CHILDREN_ONLY表示只监听指定路径的直接子节点变化，不监听子节点的子节点。
     *
     * 对于集群管理：
     * - Master节点路径：/dolphinscheduler/master/
     * - Worker节点路径：/dolphinscheduler/worker/
     * - 只关心这些路径下直接的服务器节点，不关心更深层的路径
     *
     * @return 订阅范围，只监听直接子节点
     */
    @Override
    public SubscribeScope getSubscribeScope() {
        // 返回CHILDREN_ONLY，表示只监听指定路径的直接子节点变化
        // 这对于集群管理是合适的，因为每个服务器节点都是注册路径的直接子节点
        // 例如：/dolphinscheduler/master/192.168.1.100:5678
        return SubscribeScope.CHILDREN_ONLY;
    }

    /**
     * 解析服务器心跳信息（抽象方法）
     *
     * 子类必须实现此方法，将JSON格式的心跳字符串解析为具体的服务器元数据对象。
     * 不同类型的集群（Master/Worker）有不同的心跳数据格式和解析逻辑。
     *
     * @param serverHeartBeatJson 服务器心跳信息的JSON字符串
     * @return 解析后的服务器元数据对象，解析失败时返回null
     */
    abstract T parseServerFromHeartbeat(String serverHeartBeatJson);

    /**
     * 处理服务器添加事件（抽象方法）
     *
     * 当有新的服务器加入集群时调用。子类需要实现具体的处理逻辑，
     * 如更新集群状态、触发负载重新平衡等。
     *
     * @param serverHeartBeat 新加入的服务器元数据
     */
    public abstract void onServerAdded(T serverHeartBeat);

    /**
     * 处理服务器移除事件（抽象方法）
     *
     * 当服务器从集群中离开时调用。子类需要实现具体的处理逻辑，
     * 如清理相关资源、触发故障转移等。
     *
     * @param serverHeartBeat 被移除的服务器元数据
     */
    public abstract void onServerRemove(T serverHeartBeat);

    /**
     * 处理服务器更新事件（抽象方法）
     *
     * 当服务器信息发生变化时调用。子类需要实现具体的处理逻辑，
     * 如更新缓存信息、调整负载均衡策略等。
     *
     * @param serverHeartBeat 更新后的服务器元数据
     */
    public abstract void onServerUpdate(T serverHeartBeat);

}
