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

import java.util.List;

/**
 * Master槽位重新平衡器接口
 *
 * 定义了Master槽位重新平衡的契约，当Master集群发生变化时，
 * 实现类需要重新计算和分配槽位，确保工作负载能够均匀分布。
 * 类比：工厂管理层职责重新分配的标准流程接口。
 *
 * 核心职责：
 * 1. 槽位重新计算：根据可用Master节点重新计算槽位分配
 * 2. 一致性保证：确保所有Master节点对槽位分配的理解一致
 * 3. 动态调整：支持Master节点的动态增减
 */
public interface IMasterSlotReBalancer {

    /**
     * 执行Master槽位重新平衡
     *
     * 当Master集群发生变化（节点增加、删除、状态变更）时，调用此方法
     * 重新计算和分配槽位。实现类需要确保：
     * 1. 槽位分配的一致性：所有Master节点计算出相同的结果
     * 2. 负载均衡：槽位尽可能均匀分布
     * 3. 最小化影响：减少不必要的槽位迁移
     *
     * @param masterServerList 当前正常状态的Master服务器列表
     */
    void doReBalance(List<MasterServerMetadata> masterServerList);
}
