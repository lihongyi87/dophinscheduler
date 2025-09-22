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

package org.apache.dolphinscheduler.server.master.metrics;

import org.apache.dolphinscheduler.server.master.registry.MasterRegistryClient;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

/**
 * Master节点健康状态指示器
 * <p>
 * 该类实现了Spring Boot Actuator的HealthIndicator接口，用于监控Master节点的健康状态。
 * 主要通过检查Master注册客户端的可用性来判断节点是否健康。
 * <p>
 * 健康检查逻辑：
 * - 检查MasterRegistryClient是否可用
 * - 如果可用则返回UP状态
 * - 如果不可用或发生异常则返回DOWN状态
 * <p>
 * 该指标可以通过Spring Boot Actuator的health端点访问，
 * 常用于负载均衡器、监控系统的健康检查
 *
 * @author DolphinScheduler Team
 * @since 1.0.0
 */
@Component
public class MasterHealthIndicator implements HealthIndicator {

    /**
     * Master注册客户端
     * <p>
     * 用于检查Master节点在注册中心（如Zookeeper）的注册状态
     */
    @Autowired
    private MasterRegistryClient masterRegistryClient;

    /**
     * 执行健康检查
     * <p>
     * 通过检查MasterRegistryClient的可用性来确定Master节点的健康状态
     *
     * @return Health对象，包含健康状态信息
     *         - Health.up(): 节点健康，可以正常提供服务
     *         - Health.down(): 节点不健康，可能无法正常提供服务
     */
    @Override
    public Health health() {
        try {
            // 检查Master注册客户端是否可用
            if (masterRegistryClient.isAvailable()) {
                return Health.up().build();
            }
            return Health.down().build();
        } catch (Exception ex) {
            // 发生异常时返回DOWN状态，并包含异常信息
            return Health.down().withException(ex).build();
        }
    }
}
