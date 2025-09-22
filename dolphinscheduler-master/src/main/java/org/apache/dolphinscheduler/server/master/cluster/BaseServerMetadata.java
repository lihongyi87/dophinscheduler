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

import lombok.Data;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@Data
@ToString
@SuperBuilder
public abstract class BaseServerMetadata implements IClusters.IServerMetadata {

    private final int processId;

    // The server startup time in milliseconds.
    private final long serverStartupTime;

    private final String address;

    private final double cpuUsage;

    private final double memoryUsage;

    private final ServerStatus serverStatus;

    @Override
    public String getAddress() {
        // 返回服务器的网络地址
        // 这个地址是服务器在集群中的唯一标识符，格式通常为"host:port"
        // 用于集群管理、任务分发、节点通信等各种场景
        return address;
    }

    @Override
    public ServerStatus getServerStatus() {
        // 返回服务器当前的运行状态
        // 状态枚举包括：NORMAL（正常）、BUSY（繁忙）、ABNORMAL（异常）等
        // 这个状态决定了服务器是否可以接收新任务、参与负载均衡等
        // 集群管理组件根据这个状态来做调度决策
        return serverStatus;
    }

}
