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

import org.apache.dolphinscheduler.server.master.cluster.WorkerServerMetadata;

import java.util.Date;

import lombok.Getter;

/**
 * Worker节点故障转移事件
 *
 * 表示特定Worker节点发生故障需要进行故障转移的系统事件。
 * 当Worker节点无法正常工作时，需要处理其上正在执行的任务，
 * 并将这些任务重新调度到其他可用的Worker节点上。
 *
 * 类比：工厂中某台生产设备突然故障，需要将正在该设备上
 *      处理的工件转移到其他正常运行的设备上继续生产。
 *
 * 触发场景：
 * - Worker节点心跳丢失或超时
 * - Worker节点资源耗尽无法响应新任务
 * - Worker节点主动退出集群或下线
 * - 网络故障导致Worker节点无法通信
 * - Worker节点进程异常终止
 *
 * 处理内容：
 * - 识别Worker节点上正在执行的任务
 * - 将可重试的任务重新调度到其他Worker节点
 * - 更新任务状态，标记失败或设置为重试状态
 * - 清理Worker节点的相关资源和状态信息
 * - 更新集群中Worker节点的状态记录
 *
 * 与Master故障转移的区别：
 * - Worker故障转移：主要处理任务级别的故障恢复
 * - Master故障转移：主要处理工作流级别的故障恢复
 * - Worker故障影响正在执行的任务
 * - Master故障影响工作流的调度和管理
 *
 * 任务重新调度策略：
 * - 可重试任务：重新调度到其他Worker节点
 * - 不可重试任务：标记为失败状态
 * - 长时间运行任务：根据业务逻辑决定是否重试
 * - 资源依赖任务：检查其他Worker节点的资源可用性
 *
 * 延迟处理的重要性：
 * Worker节点的故障转移特别需要延迟处理机制：
 * - 避免网络抖动导致的误判
 * - 给短暂的资源不足问题恢复时间
 * - 减少不必要的任务重新调度开销
 *
 * 设计特点：
 * - 包含详细的Worker节点信息：便于精确处理
 * - 支持延迟处理：提高系统稳定性
 * - 不可变对象：确保事件数据一致性
 * - 工厂方法模式：提供统一的创建接口
 */
@Getter
public class WorkerFailoverEvent extends AbstractSystemEvent {

    /**
     * 故障Worker节点的元数据信息
     *
     * 包含了发生故障的Worker节点的详细信息，包括：
     * - 节点ID和名称
     * - 节点的网络地址和端口
     * - 节点的资源信息（CPU、内存等）
     * - 节点支持的任务类型和工作组
     * - 节点的注册时间和最后心跳时间
     *
     * 这些信息对于Worker故障转移处理非常重要：
     * - 精确定位故障节点
     * - 查找该节点上正在执行的任务
     * - 选择合适的替代Worker节点
     * - 进行故障复盘和性能分析
     */
    private final WorkerServerMetadata workerServerMetadata;

    /**
     * 事件发生时间
     *
     * 记录Worker节点故障事件发生的具体时间。
     *
     * 对于Worker故障转移，这个时间特别重要：
     * - 用于计算任务的执行时长
     * - 决定任务是否需要重试或标记失败
     * - 进行故障影响范围的评估
     * - SLA指标的统计和分析
     */
    private final Date eventTime;

    /**
     * 私有构造方法
     *
     * 通过私有构造方法强制使用静态工厂方法创建实例。
     * 这样可以在创建过程中进行参数验证和预处理。
     *
     * @param workerServerMetadata 故障Worker节点的元数据信息，不能为null
     * @param eventTime 事件发生时间，不能为null
     * @param delayTime 延迟处理时间（毫秒），必须大于等于0
     */
    private WorkerFailoverEvent(final WorkerServerMetadata workerServerMetadata,
                                final Date eventTime,
                                final long delayTime) {
        super(delayTime);
        this.workerServerMetadata = workerServerMetadata;
        this.eventTime = eventTime;
    }

    /**
     * 创建Worker故障转移事件实例
     *
     * 静态工厂方法，提供了完整的参数验证和实例创建。
     * 这是创建WorkerFailoverEvent实例的推荐方式。
     *
     * 延迟处理在Worker故障转移中的作用：
     * - 给Worker节点一定的恢复时间
     * - 避免网络抖动导致的误判
     * - 减少不必要的任务重新调度
     * - 减轻系统负载和资源消耗
     *
     * 推荐的延迟时间设置：
     * - 短时任务：10-30秒
     * - 长时任务：30-60秒
     * - 资源密集型任务：60-120秒
     *
     * 使用示例：
     * <pre>
     * WorkerServerMetadata metadata = // 获取故障节点信息
     * Date now = new Date();
     * long delayTime = 30000; // 30秒延迟
     * WorkerFailoverEvent event = WorkerFailoverEvent.of(metadata, now, delayTime);
     * </pre>
     *
     * @param workerServerMetadata 故障Worker节点的元数据信息，不能为null
     * @param eventTime 事件发生时间，不能为null
     * @param delayTime 延迟处理时间（毫秒），必须大于等于0
     * @return Worker故障转移事件实例
     * @throws NullPointerException 如果workerServerMetadata或eventTime为null
     */
    public static WorkerFailoverEvent of(final WorkerServerMetadata workerServerMetadata,
                                         final Date eventTime,
                                         final long delayTime) {
        checkNotNull(workerServerMetadata);
        checkNotNull(eventTime);
        return new WorkerFailoverEvent(workerServerMetadata, eventTime, delayTime);
    }

    /**
     * 获取事件类型
     *
     * 返回Worker节点故障转移事件类型。
     * 这个方法用于事件分发和处理器匹配。
     *
     * @return 返回Worker故障转移事件类型
     */
    @Override
    public SystemEventType getEventType() {
        return SystemEventType.WORKER_FAILOVER;
    }

    /**
     * 返回事件的字符串表示
     *
     * 提供事件的可读性字符串表示，包含所有关键信息。
     * 用于日志记录、调试和系统监控。
     *
     * 包含信息：
     * - 故障Worker节点的详细信息和资源状态
     * - 事件发生时间
     * - 延迟处理时间
     *
     * @return 包含事件所有关键信息的字符串
     */
    @Override
    public String toString() {
        return "WorkerFailoverEvent{" +
                "workerServerMetadata='" + workerServerMetadata + '\'' +
                ", eventTime=" + eventTime +
                ", delayTime=" + delayTime +
                '}';
    }
}
