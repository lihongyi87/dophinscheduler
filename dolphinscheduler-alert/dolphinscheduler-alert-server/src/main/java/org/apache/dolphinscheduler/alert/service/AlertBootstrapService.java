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

import org.apache.dolphinscheduler.alert.registry.AlertRegistryClient;
import org.apache.dolphinscheduler.alert.rpc.AlertRpcServer;

import lombok.extern.slf4j.Slf4j;

import org.springframework.stereotype.Service;

/**
 * 告警服务器引导服务
 * 负责启动告警服务器所需的所有必要组件
 */
@Slf4j
@Service
public final class AlertBootstrapService implements AutoCloseable {

    /** 告警事件获取器 - 负责从数据库获取待处理的告警事件 */
    private final AlertEventFetcher alertEventFetcher;

    /** 告警事件循环处理器 - 负责持续处理告警事件 */
    private final AlertEventLoop alertEventLoop;

    /**
     * 构造告警引导服务
     * @param alertRpcServer RPC服务器 - 提供远程调用接口
     * @param alertRegistryClient 注册中心客户端 - 负责服务注册与发现
     * @param alertHAServer 高可用服务器 - 提供集群高可用支持
     * @param alertEventFetcher 事件获取器
     * @param alertEventLoop 事件循环处理器
     */
    public AlertBootstrapService(AlertRpcServer alertRpcServer,
                                 AlertRegistryClient alertRegistryClient,
                                 AlertHAServer alertHAServer,
                                 AlertEventFetcher alertEventFetcher,
                                 AlertEventLoop alertEventLoop) {
        // 保存事件获取器引用，用于后续启动和关闭操作
        this.alertEventFetcher = alertEventFetcher;
        // 保存事件循环处理器引用，用于后续启动和关闭操作
        this.alertEventLoop = alertEventLoop;
    }

    /**
     * 启动告警服务
     * 依次启动事件获取器和事件循环处理器
     */
    public void start() {
        // 记录服务启动开始日志
        log.info("AlertBootstrapService starting...");
        // 启动告警事件获取器，开始从数据库获取待处理的告警事件
        alertEventFetcher.start();
        // 启动告警事件循环处理器，开始消费队列中的告警事件
        alertEventLoop.start();
        // 记录服务启动完成日志
        log.info("AlertBootstrapService started...");
    }

    /**
     * 关闭告警服务
     * 优雅关闭事件获取器和事件循环处理器
     */
    @Override
    public void close() {
        // 记录服务关闭开始日志
        log.info("AlertBootstrapService stopping...");
        // 关闭告警事件获取器，停止从数据库获取新的告警事件
        alertEventFetcher.shutdown();
        // 关闭告警事件循环处理器，等待当前处理中的告警完成后停止
        alertEventLoop.shutdown();
        // 记录服务关闭完成日志
        log.info("AlertBootstrapService stopped...");
    }
}
