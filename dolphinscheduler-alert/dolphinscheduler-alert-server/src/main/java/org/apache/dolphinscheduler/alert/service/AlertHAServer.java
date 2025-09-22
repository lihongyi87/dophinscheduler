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

package org.apache.dolphinscheduler.alert.service;

import org.apache.dolphinscheduler.alert.config.AlertConfig;
import org.apache.dolphinscheduler.registry.api.Registry;
import org.apache.dolphinscheduler.registry.api.enums.RegistryNodeType;
import org.apache.dolphinscheduler.registry.api.ha.AbstractHAServer;

import lombok.extern.slf4j.Slf4j;

import org.springframework.stereotype.Component;

/**
 * 告警高可用服务器
 *
 * <p>该类实现了告警服务器的高可用功能，通过注册中心实现
 * 主从选举和故障转移。在集群模式下，只有主节点会处理告警，
 * 从节点处于待命状态。</p>
 *
 * <p>主要功能：</p>
 * <ul>
 *   <li>参与主节点选举</li>
 *   <li>监控主节点状态</li>
 *   <li>实现故障转移</li>
 *   <li>维护服务状态</li>
 * </ul>
 */
@Slf4j
@Component
public class AlertHAServer extends AbstractHAServer {

    /**
     * 构造高可用告警服务器
     *
     * @param registry 注册中心客户端
     * @param alertConfig 告警服务配置
     */
    public AlertHAServer(final Registry registry, final AlertConfig alertConfig) {
        // 调用父类构造器，传入注册中心、注册路径和服务器地址
        // 注册路径用于在注册中心创建HA选举节点
        // 服务器地址用于标识当前节点
        super(registry, RegistryNodeType.ALERT_HA_LEADER.getRegistryPath(), alertConfig.getAlertServerAddress());
    }

    /**
     * 启动高可用服务
     * 开始参与主节点选举
     */
    @Override
    public void start() {
        // 调用父类启动方法，开始参与主节点选举
        // 父类会在注册中心创建临时顺序节点，并监听前一个节点的变化
        super.start();
        // 记录HA服务启动成功日志
        log.info("AlertHAServer started...");
    }

    /**
     * 关闭高可用服务
     * 释放主节点锁并退出选举
     */
    @Override
    public void close() {
        // 记录HA服务关闭日志
        // 父类会自动清理注册中心的节点信息并释放主节点锁
        log.info("AlertHAServer shutdown...");
    }
}
