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

import org.apache.dolphinscheduler.server.master.config.MasterConfig;

import java.util.List;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

import org.springframework.stereotype.Component;

/**
 * Master槽位管理器
 *
 * 这个类负责管理DolphinScheduler集群中Master节点的槽位分配和重新平衡。
 * 槽位机制用于将工作流实例均匀分布到不同的Master节点上，避免单点瓶颈。
 * 类比：一个工厂的生产线调度员，负责将生产任务均匀分配给不同的生产车间。
 *
 * 核心概念：
 * - 槽位(Slot)：每个Master节点占用一个或多个槽位
 * - 重新平衡：当Master集群发生变化时，重新计算槽位分配
 * - 哈希分片：通过工作流实例ID的哈希值映射到不同槽位
 *
 * 工作原理：
 * 1. 根据正常Master节点数量确定总槽位数
 * 2. 按照Master地址排序，确定每个Master的槽位编号
 * 3. 工作流实例通过哈希分片算法分配到对应的Master节点
 */
@Slf4j
@Component
public class MasterSlotManager implements IMasterSlotReBalancer {

    /**
     * Master配置信息
     *
     * 包含当前Master节点的配置信息，特别是Master地址，
     * 用于在槽位重新平衡时确定当前节点的槽位编号。
     */
    private final MasterConfig masterConfig;

    /**
     * 当前Master节点的槽位编号
     *
     * -1表示槽位不可用（Master集群异常或当前节点未在正常节点列表中）
     * >=0表示有效的槽位编号
     * 使用volatile确保多线程可见性
     */
    private volatile int currentSlot = -1;

    /**
     * 总槽位数
     *
     * 等于正常Master节点的数量，当Master集群发生变化时会重新计算。
     * 使用volatile确保多线程可见性
     */
    private volatile int totalSlots = 0;

    /**
     * 构造函数
     *
     * @param masterConfig Master配置信息
     */
    public MasterSlotManager(final MasterConfig masterConfig) {
        this.masterConfig = masterConfig;
    }

    /**
     * 获取当前Master节点的槽位编号
     *
     * 如果槽位为-1，表示Master槽位不可用，可能的原因：
     * 1. Master集群初始化未完成
     * 2. 当前节点不在正常Master节点列表中
     * 3. 集群发生故障
     *
     * @return 当前Master槽位编号，-1表示不可用
     */
    public int getCurrentMasterSlot() {
        // 直接返回当前节点的槽位编号
        // volatile变量确保在多线程环境下的可见性
        // -1表示槽位不可用，>=0表示有效的槽位编号
        return currentSlot;
    }

    /**
     * 获取Master集群的总槽位数
     *
     * 总槽位数等于正常Master节点的数量，用于哈希分片计算。
     *
     * @return 总槽位数
     */
    public int getTotalMasterSlots() {
        // 返回集群中正常Master节点的总数，也就是总槽位数
        // 这个值用于哈希分片计算，将工作流实例分配到不同Master节点
        // volatile变量确保在多线程环境下的可见性
        return totalSlots;
    }

    /**
     * 检查槽位是否有效
     *
     * 槽位有效的条件：
     * 1. 总槽位数大于0（至少有一个正常Master节点）
     * 2. 当前槽位编号大于等于0（当前节点在正常节点列表中）
     *
     * @return true表示槽位有效，false表示无效
     */
    public boolean checkSlotValid() {
        // 检查槽位是否有效的两个条件：
        // 1. totalSlots > 0：至少有一个正常Master节点存在
        // 2. currentSlot >= 0：当前节点在正常节点列表中且已分配槽位
        // 只有两个条件都满足时，当前节点才能处理工作流任务
        return totalSlots > 0 && currentSlot >= 0;
    }

    /**
     * 执行Master槽位重新平衡
     *
     * 这是IMasterSlotReBalancer接口的实现方法，当Master集群发生变化时被调用。
     * 重新平衡的目标是确保每个Master节点都有一个唯一的槽位编号，
     * 以便工作流实例能够通过哈希分片均匀分布到不同的Master节点。
     *
     * 类比：当工厂的生产车间数量发生变化时，重新分配生产线编号，
     *      确保每个车间都有明确的生产任务范围。
     *
     * 重新平衡步骤：
     * 1. 对正常Master节点按地址排序，确保分配结果的一致性
     * 2. 查找当前Master节点在排序列表中的位置，作为槽位编号
     * 3. 检查是否需要重新平衡（集群规模或当前槽位是否变化）
     * 4. 更新槽位信息
     *
     * @param normalMasterServers 正常状态的Master服务器列表
     */
    @Override
    public void doReBalance(List<MasterServerMetadata> normalMasterServers) {

        // 对Master节点按地址进行排序，确保集群中所有节点的槽位分配结果一致
        // 排序是关键的，它保证了在不同Master节点上计算出的槽位分配结果相同
        // 使用Stream API进行排序并收集为新的列表
        normalMasterServers =
                normalMasterServers.stream().sorted(MasterServerMetadata::compareTo).collect(Collectors.toList());

        // 遍历排序后的Master列表，查找当前节点在列表中的位置
        // 这个位置的索引就是当前节点的槽位编号
        int tmpCurrentSlot = -1;
        for (int i = 0; i < normalMasterServers.size(); i++) {
            // 比较地址字符串，查找与当前Master地址匹配的节点
            if (normalMasterServers.get(i).getAddress().equals(masterConfig.getMasterAddress())) {
                tmpCurrentSlot = i;  // 记录当前节点在列表中的位置
                break;  // 找到则立即退出循环
            }
        }

        // 检查是否在正常Master列表中找到了当前节点
        if (tmpCurrentSlot == -1) {
            // 找不到说明当前节点可能处于异常状态或未正常启动
            log.warn(
                    "Do rebalance failed, cannot found the current master: {} in the normal master clusters: {}. Please check the current master server status",
                    masterConfig.getMasterAddress(), normalMasterServers);
            currentSlot = -1;  // 设置为无效槽位，阻止当前节点处理任务
            return;  // 提前返回，不更新槽位信息
        }

        // 优化检查：如果总槽位数和当前槽位都没有变化，则无需重新平衡
        // 这种优化可以减少不必要的日志输出和状态更新
        if (totalSlots == normalMasterServers.size() && currentSlot == tmpCurrentSlot) {
            log.debug("No need to rebalance, the currentSlot: {}, totalSlots: {} doesn't changed", currentSlot,
                    totalSlots);
            return;  // 早期返回，节省计算资源
        }

        // 更新槽位信息，使用volatile变量确保多线程可见性
        totalSlots = normalMasterServers.size();  // 总槽位数等于正常Master节点数
        currentSlot = tmpCurrentSlot;             // 当前节点的槽位编号
        // 记录重新平衡成功的日志，方便运维监控和问题排查
        log.info("Do rebalance success, current master slot: {}, total master slots: {}", currentSlot, totalSlots);
    }
}
