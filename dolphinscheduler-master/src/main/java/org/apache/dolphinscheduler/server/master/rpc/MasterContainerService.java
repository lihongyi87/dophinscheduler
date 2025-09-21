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

package org.apache.dolphinscheduler.server.master.rpc;

import org.apache.dolphinscheduler.extract.master.IMasterContainerService;
import org.apache.dolphinscheduler.server.master.cluster.WorkerGroupChangeNotifier;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Master容器服务实现类
 *
 * <p>该类是Master节点中负责容器相关操作的RPC服务实现。
 * 主要用于处理与Worker节点群组管理相关的远程调用请求。</p>
 *
 * <p>作为Master节点的容器服务，该类提供了对集群中Worker节点群组的管理功能，
 * 包括刷新Worker群组信息、检测群组变化等操作。这些操作对于维护集群的
 * 动态性和可用性至关重要。</p>
 *
 * <p>主要功能：</p>
 * <ul>
 *   <li>刷新Worker群组配置</li>
 *   <li>检测Worker群组变化</li>
 *   <li>维护集群拓扑信息</li>
 * </ul>
 *
 * @author DolphinScheduler Community
 * @see IMasterContainerService
 * @see WorkerGroupChangeNotifier
 */
@Slf4j
@Component
public class MasterContainerService implements IMasterContainerService {

    /**
     * Worker群组变化通知器
     * 负责检测和通知Worker群组的变化，维护集群拓扑的一致性
     */
    @Autowired
    private WorkerGroupChangeNotifier workerGroupChangeNotifier;

    /**
     * 刷新Worker群组信息
     *
     * <p>该方法通过RPC接口接收刷新Worker群组的请求，并委托给
     * WorkerGroupChangeNotifier检测Worker群组的变化。</p>
     *
     * <p>刷新操作包括：</p>
     * <ul>
     *   <li>检测新加入的Worker节点</li>
     *   <li>识别已离线的Worker节点</li>
     *   <li>更新Worker群组的配置信息</li>
     *   <li>通知相关组件群组变化</li>
     * </ul>
     *
     * <p>该操作通常在以下情况下被触发：</p>
     * <ul>
     *   <li>Worker节点上线或下线</li>
     *   <li>Worker节点配置发生变化</li>
     *   <li>集群拓扑需要更新</li>
     * </ul>
     */
    @Override
    public void refreshWorkerGroup() {
        workerGroupChangeNotifier.detectWorkerGroupChanges();
    }
}
