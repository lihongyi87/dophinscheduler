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

package org.apache.dolphinscheduler.server.worker.metrics;

import org.apache.dolphinscheduler.server.worker.registry.WorkerRegistryClient;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

/**
 * Worker节点健康状态指示器
 *
 * 实现Spring Boot Actuator的HealthIndicator接口，
 * 用于监控Worker节点的健康状态，主要检查Worker是否正常注册到注册中心。
 * 健康检查结果会被暴露给监控端点，便于运维人员和监控系统实时了解Worker节点状态。
 *
 * 健康检查标准：
 * - UP：Worker注册客户端可用且正常工作
 * - DOWN：Worker注册客户端不可用或发生异常
 */
@Component
public class WorkerHealthIndicator implements HealthIndicator {

    /**
     * Worker注册中心客户端
     * 用于检查Worker节点与注册中心的连接状态
     */
    @Autowired
    private WorkerRegistryClient workerRegistryClient;

    /**
     * 执行健康检查
     *
     * 检查逻辑：
     * 1. 检查Worker注册客户端是否可用
     * 2. 如果可用则返回UP状态
     * 3. 如果不可用或发生异常则返回DOWN状态，并包含异常信息
     *
     * @return Health 健康检查结果，包含状态和详细信息
     */
    @Override
    public Health health() {
        try {
            // 检查Worker注册客户端是否正常可用
            if (workerRegistryClient.isAvailable()) {
                return Health.up().build();
            }
            // Worker注册客户端不可用，返回DOWN状态
            return Health.down().build();
        } catch (Exception ex) {
            // 健康检查过程中发生异常，返回DOWN状态并包含异常信息
            return Health.down().withException(ex).build();
        }
    }
}
