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

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.dolphinscheduler.common.constants.Constants;
import org.apache.dolphinscheduler.common.model.MasterHeartBeat;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/**
 * Master服务器元数据
 *
 * 继承自BaseServerMetadata，专门用于描述Master节点的元数据信息。
 * Master节点作为集群的管理节点，负责工作流的调度、任务分发、故障处理等核心功能。
 * 类比：工厂管理层的档案信息，除了基本的员工信息外，还有管理层特有的职责和权限。
 *
 * 主要特征：
 * 1. 继承基础属性：进程ID、启动时间、地址、资源使用率、状态等
 * 2. 支持排序比较：基于地址进行排序，用于槽位分配等场景
 * 3. 心跳解析：从Master心跳信息构建元数据对象
 * 4. 不可变性：所有字段在构建后不可修改，确保数据一致性
 *
 * 使用场景：
 * - Master集群管理和监控
 * - 槽位重新平衡时的排序
 * - 故障转移时的Master选举
 * - 负载均衡和资源调度决策
 */
@Data
@ToString(callSuper = true)
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
public class MasterServerMetadata extends BaseServerMetadata implements Comparable<MasterServerMetadata> {

    /**
     * 从Master心跳信息解析创建Master服务器元数据
     *
     * 这是一个静态工厂方法，用于将Master节点的心跳信息转换为结构化的元数据对象。
     * 心跳信息包含了Master节点的实时状态，通过这个方法可以创建不可变的元数据快照。
     *
     * 类比：从员工的实时状态报告中提取关键信息，创建标准化的档案记录。
     *
     * 解析过程：
     * 1. 提取进程基本信息（进程ID、启动时间）
     * 2. 构建网络地址（host:port格式）
     * 3. 获取资源使用状况（CPU、内存使用率）
     * 4. 记录当前运行状态
     *
     * @param masterHeartBeat Master心跳信息，不能为null
     * @return 解析后的Master服务器元数据对象
     * @throws NullPointerException 如果masterHeartBeat为null
     */
    public static MasterServerMetadata parseFromHeartBeat(final MasterHeartBeat masterHeartBeat) {
        // ========== 参数验证 ==========
        // 使用Guava的Preconditions检查参数不能为null
        // 如果为null会抛出NullPointerException，避免后续空指针异常
        checkNotNull(masterHeartBeat);

        // ========== 构建Master元数据对象 ==========
        // 使用Lombok的@SuperBuilder模式创建不可变对象
        // 这种模式确保对象创建后所有字段都不可修改，保证线程安全
        return MasterServerMetadata.builder()
                // 设置进程ID，用于标识Master进程的唯一性
                // 在进程重启后会发生变化，是判断进程是否重启的重要依据
                .processId(masterHeartBeat.getProcessId())
                // 设置服务器启动时间戳，记录Master进程的启动时间
                // 用于计算运行时长、进行故障分析等
                .serverStartupTime(masterHeartBeat.getStartupTime())
                // 构建完整的网络地址，格式为"host:port"
                // 这个地址用作Master节点的唯一标识符，在集群管理中非常重要
                .address(masterHeartBeat.getHost() + Constants.COLON + masterHeartBeat.getPort())
                // 设置CPU使用率，范围通常是0.0到1.0
                // 用于负载监控和资源调度决策
                .cpuUsage(masterHeartBeat.getCpuUsage())
                // 设置内存使用率，范围通常是0.0到1.0
                // 用于监控内存压力和预警
                .memoryUsage(masterHeartBeat.getMemoryUsage())
                // 设置服务器状态（NORMAL、BUSY等）
                // 决定该Master是否可以参与任务调度和槽位分配
                .serverStatus(masterHeartBeat.getServerStatus())
                // 构建最终的不可变对象
                .build();
    }

    /**
     * 比较Master服务器元数据
     *
     * 实现Comparable接口，使用Master地址进行字典序比较。
     * 这个排序在槽位重新平衡时非常重要，确保集群中所有Master节点
     * 对槽位分配的计算结果保持一致。
     *
     * 类比：按员工工号或姓名排序，确保在进行职责分配时所有人的计算结果一致。
     *
     * 排序规则：
     * - 基于Master地址（host:port）进行字典序排序
     * - 保证排序结果的确定性和一致性
     * - 用于槽位分配算法中的Master节点排序
     *
     * @param o 要比较的另一个Master服务器元数据
     * @return 负数、零、正数分别表示小于、等于、大于
     */
    @Override
    public int compareTo(final MasterServerMetadata o) {
        // 使用Master地址进行字典序比较
        // 字符串的compareTo方法按字典序排列，确保排序结果的一致性和可预测性
        // 这个排序在槽位重新平衡算法中至关重要，所有Master节点必须
        // 对Master列表的排序达成一致，才能保证槽位分配结果的一致性
        return this.getAddress().compareTo(o.getAddress());
    }

}
