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

import org.apache.dolphinscheduler.common.enums.ServerStatus;

import java.util.List;
import java.util.Optional;

/**
 * 集群接口
 *
 * 这是DolphinScheduler集群管理的核心接口，定义了管理服务器集群的基本行为。
 * 类比：一个标准的团队管理框架，规定了如何管理团队成员、监听成员变化等基本规则。
 *
 * 主要功能：
 * 1. 管理服务器列表：获取集群中的所有服务器信息
 * 2. 查询特定服务器：根据地址查找特定的服务器
 * 3. 监听集群变化：注册监听器来响应服务器的增加、删除、更新事件
 *
 * @param <S> 服务器元数据类型，必须实现IServerMetadata接口
 */
public interface IClusters<S extends IClusters.IServerMetadata> {

    /**
     * 获取集群中的所有服务器列表
     *
     * @return 服务器列表，包含集群中所有活跃的服务器信息
     */
    List<S> getServers();

    /**
     * 根据地址获取特定的服务器信息
     *
     * @param address 服务器地址，格式通常为"host:port"
     * @return 服务器信息的Optional对象，如果服务器不存在则为空
     */
    Optional<S> getServer(final String address);

    /**
     * 注册集群变化监听器
     *
     * 当集群中有服务器加入、离开或状态更新时，会通知注册的监听器。
     * 类比：在团队中设置一个通讯员，当有新人加入、老人离开或状态变化时及时通知相关人员。
     *
     * @param listener 集群变化监听器
     */
    void registerListener(IClustersChangeListener<S> listener);

    /**
     * 服务器元数据接口
     *
     * 定义了服务器的基本信息，每个服务器都必须提供这些基础数据。
     * 类比：员工的基本信息卡，包含姓名、联系方式、工作状态等必要信息。
     */
    interface IServerMetadata {

        /**
         * 获取服务器地址
         *
         * @return 服务器的网络地址，通常格式为"host:port"
         */
        String getAddress();

        /**
         * 获取服务器状态
         *
         * @return 服务器当前的运行状态（如：正常、繁忙、不可用等）
         */
        ServerStatus getServerStatus();

    }

    /**
     * 集群变化监听器接口
     *
     * 定义了处理集群变化事件的方法，当集群中的服务器发生变化时会触发相应的回调。
     * 类比：人事变动通知接口，规定了当员工入职、离职、状态变更时应该如何处理。
     *
     * @param <S> 服务器元数据类型
     */
    interface IClustersChangeListener<S extends IServerMetadata> {

        /**
         * 服务器添加事件处理
         *
         * 当有新服务器加入集群时触发此方法。
         *
         * @param server 新加入的服务器信息
         */
        void onServerAdded(S server);

        /**
         * 服务器移除事件处理
         *
         * 当服务器从集群中移除时触发此方法。
         *
         * @param server 被移除的服务器信息
         */
        void onServerRemove(S server);

        /**
         * 服务器更新事件处理
         *
         * 当服务器信息（如状态、负载等）发生更新时触发此方法。
         *
         * @param server 更新后的服务器信息
         */
        void onServerUpdate(S server);

    }

    /**
     * 服务器添加监听器
     *
     * 这是一个专门监听服务器添加事件的监听器，只关心新服务器的加入。
     * 其他事件（移除、更新）使用默认的空实现。
     * 类比：专门负责新员工入职接待的人事专员。
     *
     * @param <S> 服务器元数据类型
     */
    interface ServerAddedListener<S extends IServerMetadata> extends IClustersChangeListener<S> {

        @Override
        default void onServerRemove(S server) {
            // 只关心服务器添加事件，移除事件忽略
        }

        @Override
        default void onServerUpdate(S server) {
            // 只关心服务器添加事件，更新事件忽略
        }

    }

    /**
     * 服务器移除监听器
     *
     * 这是一个专门监听服务器移除事件的监听器，只关心服务器的离开。
     * 其他事件（添加、更新）使用默认的空实现。
     * 类比：专门负责员工离职手续的人事专员。
     *
     * @param <S> 服务器元数据类型
     */
    interface ServerRemovedListener<S extends IServerMetadata> extends IClustersChangeListener<S> {

        @Override
        default void onServerAdded(S server) {
            // 只关心服务器移除事件，添加事件忽略
        }

        /**
         * 处理服务器移除事件
         *
         * @param server 被移除的服务器信息
         */
        @Override
        void onServerRemove(S server);

        @Override
        default void onServerUpdate(S server) {
            // 只关心服务器移除事件，更新事件忽略
        }

    }

}
