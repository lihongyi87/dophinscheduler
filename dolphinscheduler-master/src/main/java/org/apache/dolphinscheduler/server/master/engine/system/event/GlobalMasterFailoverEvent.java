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

import java.util.Date;

import lombok.Getter;

/**
 * 全局Master故障转移事件
 *
 * 这是触发整个DolphinScheduler集群Master节点故障转移的系统事件。
 * 与针对特定Master节点的故障转移不同，全局故障转移会扫描整个系统，
 * 检查所有Master节点的状态并进行必要的故障恢复操作。
 *
 * 类比：企业的全面应急检查，不是针对某个特定部门，而是对整个企业
 *      进行全面的安全检查和应急响应能力评估。
 *
 * 触发时机：
 * - 系统启动时的故障恢复检查
 * - 定期的系统健康检查
 * - 检测到集群状态异常时的统一处理
 * - 管理员手动触发的全局故障转移
 *
 * 处理范围：
 * - 扫描所有已注册的Master节点
 * - 检查每个Master节点的健康状态
 * - 识别失效的Master节点并进行故障转移
 * - 重新平衡工作流在活跃Master节点间的分配
 *
 * 与单个Master故障转移的区别：
 * - 全局故障转移：主动扫描所有节点，全面检查
 * - 单个故障转移：针对已知的特定失效节点进行处理
 *
 * 性能考虑：
 * - 全局故障转移是相对重量级的操作
 * - 执行频率较低，通常在系统启动或定期维护时执行
 * - 可能会对系统性能产生短暂影响
 *
 * 设计模式：
 * - 不可变对象：一旦创建，状态不可改变
 * - 工厂方法：通过静态方法创建实例，提供参数验证
 */
@Getter
public class GlobalMasterFailoverEvent extends AbstractSystemEvent {

    /**
     * 事件发生时间
     *
     * 记录全局Master故障转移事件发生的具体时间。
     * 这个时间对于故障转移的追踪和分析非常重要。
     *
     * 用途：
     * - 故障转移日志记录
     * - 系统监控和报警
     * - 故障复盘和分析
     * - 性能指标统计
     */
    private final Date eventTime;

    /**
     * 私有构造方法
     *
     * 通过私有构造方法强制使用静态工厂方法创建实例。
     * 这样可以在创建过程中进行参数验证和预处理。
     *
     * @param eventTime 事件发生时间，不能为null
     */
    public GlobalMasterFailoverEvent(Date eventTime) {
        super();
        this.eventTime = eventTime;
    }

    /**
     * 创建全局Master故障转移事件实例
     *
     * 静态工厂方法，提供了参数验证和更好的API设计。
     * 相比直接使用构造方法，静态工厂方法更加安全和易用。
     *
     * 优势：
     * - 参数验证：自动检查参数的有效性
     * - 错误处理：提供清晰的错误信息
     * - API设计：方法名更加语义化，提高可读性
     *
     * 使用示例：
     * <pre>
     * Date now = new Date();
     * GlobalMasterFailoverEvent event = GlobalMasterFailoverEvent.of(now);
     * </pre>
     *
     * 类比：企业中的标准化工作流程，通过统一的申请表格
     *      创建工作任务，确保所有必要信息都已填写。
     *
     * @param eventTime 事件发生时间，不能为null
     * @return 全局Master故障转移事件实例
     * @throws NullPointerException 如果eventTime为null
     */
    public static GlobalMasterFailoverEvent of(final Date eventTime) {
        checkNotNull(eventTime);
        return new GlobalMasterFailoverEvent(eventTime);
    }

    /**
     * 获取事件类型
     *
     * 返回全局Master故障转移事件类型。
     * 这个方法用于事件分发和处理器匹配。
     *
     * @return 返回GLOBAL_MASTER_FAILOVER事件类型
     */
    @Override
    public SystemEventType getEventType() {
        return SystemEventType.GLOBAL_MASTER_FAILOVER;
    }

    /**
     * 返回事件的字符串表示
     *
     * 提供事件的可读性字符串表示，用于日志记录和调试。
     * 包含事件的关键信息，便于问题排查和系统监控。
     *
     * @return 包含事件信息的字符串
     */
    @Override
    public String toString() {
        return "GlobalMasterFailoverEvent{" +
                "eventTime=" + eventTime +
                '}';
    }
}
