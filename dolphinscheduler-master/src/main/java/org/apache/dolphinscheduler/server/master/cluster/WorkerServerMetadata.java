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

import org.apache.dolphinscheduler.common.constants.Constants;
import org.apache.dolphinscheduler.common.model.WorkerHeartBeat;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.SuperBuilder;

/**
 * Worker服务器元数据
 *
 * 继承自BaseServerMetadata，专门用于描述Worker节点的元数据信息。
 * Worker节点作为集群的执行节点，负责实际的任务执行工作。
 * 类比：工厂车间工人的档案信息，除了基本的员工信息外，还包含技能组、工作权重、任务处理能力等。
 *
 * Worker特有属性：
 * 1. 工作组(workerGroup)：Worker节点所属的逻辑分组，用于任务路由和资源隔离
 * 2. 工作权重(workerWeight)：Worker节点的处理能力权重，用于加权负载均衡
 * 3. 任务线程池使用率：反映Worker当前的任务处理繁忙程度
 *
 * 使用场景：
 * - Worker集群管理和监控
 * - 任务分发时的Worker选择
 * - 负载均衡算法的权重计算
 * - 工作组管理和资源调度
 */
@Data
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
public class WorkerServerMetadata extends BaseServerMetadata {

    /**
     * Worker工作组
     *
     * Worker节点所属的逻辑分组，用于：
     * 1. 任务路由：不同类型的任务可以指定运行在特定的工作组
     * 2. 资源隔离：不同工作组的Worker可以运行不同类型的任务
     * 3. 权限控制：限制某些任务只能在特定工作组中执行
     *
     * 默认值为"default"，表示默认工作组。
     * 类比：工人所属的技能小组或专业班组。
     */
    @Builder.Default
    private final String workerGroup = "default";

    /**
     * Worker权重
     *
     * 用于表示Worker节点的处理能力或优先级，主要在FixedWeightedRoundRobinWorkerLoadBalancer
     * 负载均衡器中使用。权重越高的Worker会被分配更多的任务。
     *
     * 默认值为1.0，表示标准处理能力。
     * 权重可以根据Worker的硬件配置、历史性能等因素进行调整。
     * 类比：工人的技能等级或工作效率评级。
     */
    @Builder.Default
    private final double workerWeight = 1;

    /**
     * 任务线程池使用率
     *
     * 表示Worker节点当前任务线程池的使用率（0.0-1.0），用于：
     * 1. 负载均衡决策：避免向已经繁忙的Worker分配新任务
     * 2. 系统监控：监控Worker的负载情况
     * 3. 容量规划：评估集群的处理能力和扩容需求
     *
     * 使用率越高表示Worker越繁忙，新任务分配的优先级越低。
     * 类比：工人当前的工作饱和度或任务排队情况。
     */
    private final double taskThreadPoolUsage;

    /**
     * 从Worker心跳信息解析创建Worker服务器元数据
     *
     * 这是一个静态工厂方法，用于将Worker节点的心跳信息转换为结构化的元数据对象。
     * 相比Master，Worker的心跳信息包含更多与任务执行相关的状态信息。
     *
     * 类比：从工人的实时工作报告中提取关键信息，创建标准化的工人档案。
     *
     * 解析过程：
     * 1. 提取基础服务器信息（继承自BaseServerMetadata的属性）
     * 2. 获取Worker特有信息（工作组、权重、线程池使用率）
     * 3. 构建完整的Worker元数据对象
     *
     * @param workerHeartBeat Worker心跳信息，包含Worker的实时状态
     * @return 解析后的Worker服务器元数据对象
     */
    public static WorkerServerMetadata parseFromHeartBeat(final WorkerHeartBeat workerHeartBeat) {
        // ========== 构建Worker元数据对象 ==========
        // 使用Lombok的@SuperBuilder模式创建不可变的Worker元数据对象
        // 继承父类BaseServerMetadata的所有属性，并添加Worker特有的属性
        return WorkerServerMetadata.builder()
                // ========== 基础服务器信息（继承自BaseServerMetadata） ==========
                // 设置进程ID，用于标识Worker进程的唯一性
                // Worker进程重启后进程ID会改变，可用于判断Worker是否重启
                .processId(workerHeartBeat.getProcessId())
                // 设置服务器启动时间戳，记录Worker进程的启动时间
                // 用于计算Worker运行时长、故障分析、性能统计等
                .serverStartupTime(workerHeartBeat.getStartupTime())
                // 构建完整的网络地址，格式为"host:port"
                // 这个地址是Worker在集群中的唯一标识符，用于任务分发和通信
                .address(workerHeartBeat.getHost() + Constants.COLON + workerHeartBeat.getPort())
                // 设置CPU使用率，通常范围是0.0到1.0
                // 用于负载监控、性能分析和负载均衡决策
                .cpuUsage(workerHeartBeat.getCpuUsage())
                // 设置内存使用率，通常范围是0.0到1.0
                // 用于监控内存压力、预警和容量规划
                .memoryUsage(workerHeartBeat.getMemoryUsage())
                // 设置服务器状态（NORMAL、BUSY等）
                // 决定该Worker是否可以接收新的任务分配
                .serverStatus(workerHeartBeat.getServerStatus())
                // ========== Worker特有信息 ==========
                // 设置Worker所属的工作组名称
                // 工作组用于任务路由、资源隔离和权限控制
                // 不同的任务可以指定在特定的工作组中执行
                .workerGroup(workerHeartBeat.getWorkerGroup())
                // 设置Worker的权重值
                // 权重反映Worker的处理能力，用于加权负载均衡算法
                // 权重越高的Worker会被分配更多的任务
                .workerWeight(workerHeartBeat.getWorkerHostWeight())
                // 设置任务线程池的使用率
                // 反映Worker当前的任务处理繁忙程度，范围通常是0.0到1.0
                // 高使用率意味着Worker很繁忙，应避免分配更多任务
                .taskThreadPoolUsage(workerHeartBeat.getThreadPoolUsage())
                // 构建最终的不可变Worker元数据对象
                .build();
    }

}
