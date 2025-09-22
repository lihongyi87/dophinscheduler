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

/**
 * 服务器元数据基类
 *
 * 这是所有服务器元数据类的抽象基类，定义了Master和Worker节点共同的基础属性。
 * 实现了IClusters.IServerMetadata接口，为集群管理提供统一的服务器信息访问方式。
 * 类比：员工档案的基础模板，包含所有员工都有的基本信息，如姓名、工作状态、资源使用情况等。
 *
 * 核心属性：
 * 1. 进程标识：用于唯一识别服务器进程
 * 2. 启动时间：记录服务器的启动时间，用于计算运行时长
 * 3. 网络地址：服务器的通信地址
 * 4. 资源使用：CPU和内存使用率，用于负载均衡和监控
 * 5. 运行状态：服务器当前的运行状态
 *
 * 设计模式：
 * - 使用SuperBuilder支持子类的链式构建
 * - 所有字段都是final，确保元数据的不可变性
 * - 抽象类设计，强制子类实现特定的服务器类型逻辑
 */
@Data
@ToString
@SuperBuilder
public abstract class BaseServerMetadata implements IClusters.IServerMetadata {

    /**
     * 进程ID
     *
     * 服务器进程的系统进程标识符，用于唯一识别服务器实例。
     * 在故障排查和进程管理中非常有用。
     */
    private final int processId;

    /**
     * 服务器启动时间（毫秒时间戳）
     *
     * 记录服务器的启动时间，用于：
     * 1. 计算服务器运行时长
     * 2. 故障恢复时的时间戳比较
     * 3. 监控和统计分析
     */
    private final long serverStartupTime;

    /**
     * 服务器地址
     *
     * 服务器的网络通信地址，通常格式为"host:port"。
     * 这是集群中节点之间通信和识别的关键信息。
     */
    private final String address;

    /**
     * CPU使用率
     *
     * 服务器当前的CPU使用率（0.0-1.0），用于：
     * 1. 负载均衡决策
     * 2. 系统监控和报警
     * 3. 资源调度优化
     */
    private final double cpuUsage;

    /**
     * 内存使用率
     *
     * 服务器当前的内存使用率（0.0-1.0），用于：
     * 1. 负载均衡决策
     * 2. 系统监控和报警
     * 3. 资源调度优化
     */
    private final double memoryUsage;

    /**
     * 服务器状态
     *
     * 服务器当前的运行状态（如：NORMAL、BUSY、ABNORMAL等），
     * 用于集群管理决策和任务调度。
     */
    private final ServerStatus serverStatus;

    /**
     * 获取服务器地址
     *
     * 实现IClusters.IServerMetadata接口的方法。
     *
     * @return 服务器网络地址
     */
    @Override
    public String getAddress() {
        // 返回服务器的网络地址
        // 这个地址是服务器在集群中的唯一标识符，格式通常为"host:port"
        // 用于集群管理、任务分发、节点通信等各种场景
        return address;
    }

    /**
     * 获取服务器状态
     *
     * 实现IClusters.IServerMetadata接口的方法。
     *
     * @return 服务器当前运行状态
     */
    @Override
    public ServerStatus getServerStatus() {
        // 返回服务器当前的运行状态
        // 状态枚举包括：NORMAL（正常）、BUSY（繁忙）、ABNORMAL（异常）等
        // 这个状态决定了服务器是否可以接收新任务、参与负载均衡等
        // 集群管理组件根据这个状态来做调度决策
        return serverStatus;
    }

}
