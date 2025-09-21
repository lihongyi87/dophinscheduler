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

package org.apache.dolphinscheduler.server.master.engine.system.event;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.dolphinscheduler.server.master.cluster.MasterServerMetadata;

import java.util.Date;

import lombok.Getter;

/**
 * Master节点故障转移事件
 *
 * 表示特定Master节点发生故障需要进行故障转移的系统事件。
 * 与全局Master故障转移不同，这个事件针对已经确定失效的特定Master节点，
 * 处理该节点负责的工作流实例和任务。
 *
 * 类比：企业中某个部门主管突然离职，需要将其负责的项目和团队
 *      重新分配给其他在职的主管，确保业务正常进行。
 *
 * 触发场景：
 * - 特定Master节点心跳超时或丢失
 * - Master节点主动下线或退出集群
 * - 网络分区导致Master节点无法通信
 * - 系统检测到Master节点异常状态
 *
 * 处理内容：
 * - 重新分配失效Master节点负责的工作流实例
 * - 恢复正在执行的任务状态和调度
 * - 更新集群中的Master节点状态信息
 * - 重新平衡剩余Master节点的工作负载
 *
 * 与其他故障转移事件的区别：
 * - MasterFailoverEvent：针对特定已知失效的Master节点
 * - GlobalMasterFailoverEvent：全局扫描所有Master节点状态
 * - WorkerFailoverEvent：处理Worker节点的故障转移
 *
 * 延迟处理机制：
 * 支持延迟处理，避免网络抖动等临时问题造成的误判：
 * - 短暂的网络中断不会立即触发故障转移
 * - 给临时故障一定的恢复时间
 * - 减少不必要的系统资源消耗
 *
 * 设计特点：
 * - 不可变对象：确保事件数据的一致性
 * - 包含详细的故障节点信息：便于精确处理
 * - 支持延迟处理：提高系统稳定性
 * - 工厂方法模式：提供参数验证和统一创建接口
 */
@Getter
public class MasterFailoverEvent extends AbstractSystemEvent {

    /**
     * 故障Master节点的元数据信息
     *
     * 包含了发生故障的Master节点的详细信息，包括：
     * - 节点ID和名称
     * - 节点的网络地址和端口
     * - 节点的状态信息
     * - 节点的注册时间和最后心跳时间
     *
     * 这些信息对于故障转移处理非常重要，能够：
     * - 精确定位故障节点
     * - 查找该节点负责的工作流实例
     * - 选择合适的替代节点
     * - 进行故障复盘和分析
     */
    private final MasterServerMetadata masterServerMetadata;

    /**
     * 事件发生时间
     *
     * 记录Master节点故障事件发生的具体时间。
     *
     * 重要说明：
     * 这个时间可能在不同的节点上有所不同，因为：
     * - 网络延迟导致事件传播有时间差
     * - 不同节点的系统时间可能存在小幅偏差
     * - 故障检测机制的响应时间不同
     *
     * 因此在处理故障转移时，需要考虑时间的相对性而不是绝对值。
     */
    private final Date eventTime;

    /**
     * 私有构造方法
     *
     * 通过私有构造方法强制使用静态工厂方法创建实例。
     * 这样可以在创建过程中进行参数验证和预处理。
     *
     * @param masterServerMetadata 故障Master节点的元数据信息，不能为null
     * @param eventTime 事件发生时间，不能为null
     * @param delayTime 延迟处理时间（毫秒），必须大于等于0
     */
    private MasterFailoverEvent(final MasterServerMetadata masterServerMetadata,
                                final Date eventTime,
                                final long delayTime) {
        super(delayTime);
        this.masterServerMetadata = masterServerMetadata;
        this.eventTime = eventTime;
    }

    /**
     * 创建Master故障转移事件实例
     *
     * 静态工厂方法，提供了完整的参数验证和实例创建。
     * 这是创建MasterFailoverEvent实例的推荐方式。
     *
     * 参数说明：
     * - masterServerMetadata: 故障节点的详细信息
     * - eventTime: 事件发生的时间点
     * - delayTime: 延迟处理时间，用于避免网络抖动等临时问题
     *
     * 延迟处理的意义：
     * 在生产环境中，设置适当的延迟时间可以：
     * - 减少网络抖动造成的误报
     * - 给临时故障一定的恢复时间
     * - 减少不必要的系统资源消耗
     *
     * 使用示例：
     * <pre>
     * MasterServerMetadata metadata = // 获取故障节点信息
     * Date now = new Date();
     * long delayTime = 30000; // 30秒延迟
     * MasterFailoverEvent event = MasterFailoverEvent.of(metadata, now, delayTime);
     * </pre>
     *
     * @param masterServerMetadata 故障Master节点的元数据信息，不能为null
     * @param eventTime 事件发生时间，不能为null
     * @param delayTime 延迟处理时间（毫秒），必须大于等于0
     * @return Master故障转移事件实例
     * @throws NullPointerException 如果masterServerMetadata或eventTime为null
     */
    public static MasterFailoverEvent of(final MasterServerMetadata masterServerMetadata,
                                         final Date eventTime,
                                         final long delayTime) {
        checkNotNull(masterServerMetadata);
        checkNotNull(eventTime);
        return new MasterFailoverEvent(masterServerMetadata, eventTime, delayTime);
    }

    /**
     * 获取事件类型
     *
     * 返回Master节点故障转移事件类型。
     * 这个方法用于事件分发和处理器匹配。
     *
     * @return 返回Master故障转移事件类型
     */
    @Override
    public SystemEventType getEventType() {
        return SystemEventType.MASTER_FAILOVER;
    }

    /**
     * 返回事件的字符串表示
     *
     * 提供事件的可读性字符串表示，包含所有关键信息。
     * 用于日志记录、调试和系统监控。
     *
     * 包含信息：
     * - 故障Master节点的详细信息
     * - 事件发生时间
     * - 延迟处理时间
     *
     * @return 包含事件所有关键信息的字符串
     */
    @Override
    public String toString() {
        return "MasterFailoverEvent{" +
                "masterServerMetadata='" + masterServerMetadata + '\'' +
                ", eventTime=" + eventTime +
                ", delayTime=" + delayTime +
                '}';
    }
}
