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

package org.apache.dolphinscheduler.api.service;

import org.apache.dolphinscheduler.common.model.Server;
import org.apache.dolphinscheduler.dao.entity.User;
import org.apache.dolphinscheduler.dao.plugin.api.monitor.DatabaseMetrics;
import org.apache.dolphinscheduler.registry.api.enums.RegistryNodeType;

import java.util.List;

/**
 * 监控服务接口
 *
 * <p>该接口提供系统监控功能，包括数据库状态、服务器节点状态等
 * 监控信息的查询。为系统管理员提供实时的系统运行状态信息。</p>
 *
 * <p>主要功能：</p>
 * <ul>
 *   <li>查询数据库状态和性能指标</li>
 *   <li>查询Master、Worker等服务器节点信息</li>
 *   <li>监控系统资源使用情况</li>
 *   <li>获取集群健康状态</li>
 * </ul>
 */
public interface MonitorService {

    /**
     * 查询数据库状态
     *
     * @param loginUser 登录用户
     * @return 数据库状态和性能指标
     */
    List<DatabaseMetrics> queryDatabaseState(User loginUser);

    /**
     * 查询服务器节点列表
     *
     * @param nodeType 节点类型（Master/Worker/Alert等）
     * @return 服务器信息列表
     */
    List<Server> listServer(RegistryNodeType nodeType);
}
