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

package org.apache.dolphinscheduler.api.service.impl;

import org.apache.dolphinscheduler.api.service.MonitorService;
import org.apache.dolphinscheduler.common.model.Server;
import org.apache.dolphinscheduler.dao.entity.User;
import org.apache.dolphinscheduler.dao.plugin.api.monitor.DatabaseMetrics;
import org.apache.dolphinscheduler.dao.plugin.api.monitor.DatabaseMonitor;
import org.apache.dolphinscheduler.registry.api.RegistryClient;
import org.apache.dolphinscheduler.registry.api.enums.RegistryNodeType;

import java.util.List;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.google.common.collect.Lists;

/**
 * 监控服务实现类
 *
 * <p>该类实现了系统监控功能，提供实时的系统运行状态信息。
 * 通过该服务，管理员可以监控数据库性能、服务器节点状态等
 * 关键指标。</p>
 *
 * <p>主要功能：</p>
 * <ul>
 *   <li>数据库监控 - 获取数据库连接数、性能指标</li>
 *   <li>服务器监控 - 获取Master、Worker、Alert等节点状态</li>
 *   <li>资源监控 - CPU、内存、磁盘使用情况</li>
 * </ul>
 */
@Service
@Slf4j
public class MonitorServiceImpl extends BaseServiceImpl implements MonitorService {

    /** 数据库监控器 */
    @Autowired
    private DatabaseMonitor databaseMonitor;

    /** 注册中心客户端 */
    @Autowired
    private RegistryClient registryClient;

    /**
     * 查询数据库状态
     * 获取数据库的实时性能指标，包括连接数、查询性能等
     *
     * @param loginUser 登录用户
     * @return 数据库性能指标列表
     */
    @Override
    public List<DatabaseMetrics> queryDatabaseState(User loginUser) {
        // 通过数据库监控器获取数据库的实时性能指标
        // 这些指标包括：活跃连接数、最大连接数、查询执行时间等
        DatabaseMetrics metrics = databaseMonitor.getDatabaseMetrics();

        // 将单个数据库指标对象包装为列表返回
        // 使用Guava的Lists.newArrayList创建可变列表，方便后续扩展
        return Lists.newArrayList(metrics);
    }

    /**
     * 查询服务器节点列表
     * 从注册中心获取指定类型的所有在线服务器节点信息
     *
     * @param nodeType 节点类型（Master/Worker/Alert等）
     * @return 服务器节点信息列表
     */
    @Override
    public List<Server> listServer(RegistryNodeType nodeType) {
        // 通过注册中心客户端查询指定类型的服务器列表
        // 注册中心保存了所有在线服务节点的实时信息
        // 包括节点的IP地址、端口、负载状况、最后心跳时间等
        List<Server> serverList = registryClient.getServerList(nodeType);

        // 返回服务器列表，包含每个节点的详细状态信息
        // 这些信息用于系统监控和运维管理
        return serverList;
    }
}
