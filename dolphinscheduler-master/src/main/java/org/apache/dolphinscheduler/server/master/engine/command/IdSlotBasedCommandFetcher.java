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

package org.apache.dolphinscheduler.server.master.engine.command;

import org.apache.dolphinscheduler.dao.entity.Command;
import org.apache.dolphinscheduler.dao.repository.CommandDao;
import org.apache.dolphinscheduler.server.master.cluster.MasterSlotManager;
import org.apache.dolphinscheduler.server.master.config.CommandFetchStrategy;
import org.apache.dolphinscheduler.server.master.metrics.WorkflowInstanceMetrics;

import java.util.Collections;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

import org.springframework.transaction.annotation.Transactional;

/**
 * 基于ID槽位的命令获取器
 *
 * 使用ID槽位算法来分配命令给不同的Master节点，确保在多Master环境下
 * 每个命令只被一个Master节点处理，实现负载均衡和避免重复处理。
 *
 * 核心原理：
 * 1. 槽位分配：根据Master节点数量将ID范围分成多个槽位
 * 2. 命令归属：根据命令ID计算其归属的槽位
 * 3. 分布式获取：每个Master只获取属于自己槽位的命令
 *
 * 算法示例：
 * - 假设有3个Master节点，命令ID范围1-100
 * - Master-0处理ID%3=0的命令：3,6,9,12...
 * - Master-1处理ID%3=1的命令：1,4,7,10...
 * - Master-2处理ID%3=2的命令：2,5,8,11...
 *
 * 优势：
 * - 避免重复：同一命令不会被多个Master处理
 * - 负载均衡：命令均匀分布到各个Master
 * - 扩展性好：支持动态增减Master节点
 * - 容错性强：某个Master故障时其他Master可以接管
 *
 * 类比：就像快递分拣中心的分拣规则，根据包裹编号的尾数
 * 将包裹分配给不同的分拣线，确保每个包裹只进入一条分拣线。
 */
@Slf4j
public class IdSlotBasedCommandFetcher implements ICommandFetcher {

    /**
     * ID槽位获取策略配置
     *
     * 包含槽位获取的相关配置参数：
     * - idStep：ID步长，控制每次查询的ID范围
     * - fetchSize：每次获取的命令数量上限
     */
    private final CommandFetchStrategy.IdSlotBasedFetchConfig idSlotBasedFetchConfig;

    /**
     * 命令数据访问对象
     *
     * 提供命令相关的数据库操作接口，包括查询、更新、删除等功能。
     */
    private final CommandDao commandDao;

    /**
     * Master槽位管理器
     *
     * 管理Master节点的槽位分配和状态，包括：
     * - 当前Master的槽位编号
     * - 集群中Master的总槽位数
     * - 槽位有效性验证
     */
    private final MasterSlotManager masterSlotManager;

    /**
     * 构造函数 - 初始化基于ID槽位的命令获取器
     *
     * @param idSlotBasedFetchConfig ID槽位获取策略配置
     * @param masterSlotManager Master槽位管理器
     * @param commandDao 命令数据访问对象
     */
    public IdSlotBasedCommandFetcher(CommandFetchStrategy.IdSlotBasedFetchConfig idSlotBasedFetchConfig,
                                     MasterSlotManager masterSlotManager,
                                     CommandDao commandDao) {
        this.idSlotBasedFetchConfig = idSlotBasedFetchConfig;
        this.masterSlotManager = masterSlotManager;
        this.commandDao = commandDao;
    }

    /**
     * 获取待处理的命令列表
     *
     * 基于ID槽位算法从数据库中获取属于当前Master节点的命令。
     * 使用事务确保在MySQL主从模式下查询会路由到主库，保证数据一致性。
     *
     * 执行流程：
     * 1. 槽位有效性检查：验证当前Master的槽位是否有效
     * 2. 槽位信息获取：获取当前槽位编号和总槽位数
     * 3. 命令查询：根据槽位信息查询属于当前Master的命令
     * 4. 性能监控：记录查询耗时用于性能分析
     *
     * @return 属于当前Master槽位的命令列表
     */
    @Override
    @Transactional // 使用事务确保MySQL主从模式下查询路由到主库
    public List<Command> fetchCommands() {
        // ==========第一步：记录查询开始时间==========
        // 用于计算查询耗时，进行性能监控和优化
        long scheduleStartTime = System.currentTimeMillis();

        // ==========第二步：槽位有效性检查==========
        // 验证当前Master的槽位配置是否有效
        // 槽位无效的情况：Master节点变更、网络分区、配置错误等
        if (!masterSlotManager.checkSlotValid()) {
            // 记录警告日志，包含当前槽位和总槽位信息
            log.warn("MasterSlotManager check slot ({} -> {})is invalidated.",
                    masterSlotManager.getCurrentMasterSlot(), masterSlotManager.getTotalMasterSlots());
            // 返回空列表，避免处理不属于当前Master的命令
            return Collections.emptyList();
        }

        // ==========第三步：获取槽位分配信息==========
        // 获取当前Master在集群中的槽位编号（从0开始）
        int currentSlotIndex = masterSlotManager.getCurrentMasterSlot();
        // 获取集群中Master的总槽位数（即Master节点总数）
        int totalSlot = masterSlotManager.getTotalMasterSlots();

        // ==========第四步：基于槽位查询命令==========
        // 根据槽位算法查询属于当前Master的命令
        // 查询条件：command_id % totalSlot = currentSlotIndex
        List<Command> commands = commandDao.queryCommandByIdSlot(
                currentSlotIndex,                              // 当前Master的槽位编号
                totalSlot,                                     // 总槽位数
                idSlotBasedFetchConfig.getIdStep(),           // ID步长，控制查询范围
                idSlotBasedFetchConfig.getFetchSize());       // 每次获取的最大命令数

        // ==========第五步：性能监控和日志记录==========
        // 计算查询耗时，用于性能分析
        long cost = System.currentTimeMillis() - scheduleStartTime;

        // 记录调试日志，包含槽位信息、命令数量和耗时
        // 格式：[Slot-当前槽位/总槽位] Fetch 命令数量 commands in 耗时ms.
        log.debug("[Slot-{}/{}] Fetch {} commands in {}ms.", currentSlotIndex, totalSlot, commands.size(), cost);

        // 记录查询耗时到监控指标，用于性能告警和分析
        WorkflowInstanceMetrics.recordCommandQueryTime(cost);

        // ==========返回查询结果==========
        return commands;
    }

}
