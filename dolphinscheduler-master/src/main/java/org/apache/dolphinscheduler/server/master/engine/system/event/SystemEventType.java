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

/**
 * 系统事件类型枚举
 *
 * 定义了DolphinScheduler系统中支持的所有系统级事件类型。
 * 每种事件类型对应不同的系统操作场景，主要用于故障转移和系统维护。
 *
 * 类比：企业中的各种应急响应类型，如火灾预警、设备故障、人员紧急情况等，
 *      每种类型都有对应的处理流程和责任部门。
 *
 * 设计原则：
 * - 枚举值应该语义明确，便于理解和维护
 * - 每个枚举值对应一个具体的业务场景
 * - 支持系统的高可用性和容错能力
 *
 * 事件分类：
 * 目前主要包含故障转移相关的事件，按照影响范围和处理方式分类：
 * 1. 全局性事件：影响整个系统的事件
 * 2. 节点级事件：影响特定节点的事件
 *
 * 扩展性：
 * 当需要添加新的系统事件类型时，只需要：
 * 1. 在此枚举中添加新的类型
 * 2. 创建对应的事件类和处理器
 * 3. 确保处理器正确注册到Spring容器
 */
public enum SystemEventType {

    /**
     * 全局Master故障转移事件
     *
     * 触发整个系统的Master节点扫描和故障转移操作。
     * 这是影响范围最大的系统事件，通常在以下情况下触发：
     * - 系统启动时的故障恢复检查
     * - 检测到多个Master节点异常时的统一处理
     * - 定期的系统健康检查发现问题时
     *
     * 处理特点：
     * - 影响范围：整个DolphinScheduler集群
     * - 处理复杂度：高，需要协调多个节点
     * - 执行频率：相对较低，通常是系统级操作
     *
     * 类比：企业的全面应急演练，需要所有部门配合，
     *      检查整体的应急响应能力和资源配置。
     */
    GLOBAL_MASTER_FAILOVER,

    /**
     * 单个Master节点故障转移事件
     *
     * 针对特定Master节点的故障转移操作。
     * 当检测到某个特定的Master节点失效时触发，处理该节点负责的工作流。
     *
     * 触发场景：
     * - 特定Master节点心跳超时
     * - Master节点主动下线通知
     * - 网络分区导致的节点隔离
     *
     * 处理范围：
     * - 重新分配该Master节点负责的工作流实例
     * - 恢复正在执行的任务状态
     * - 更新集群中的节点状态信息
     *
     * 类比：企业中某个部门主管临时离职，需要将其负责的项目
     *      重新分配给其他主管，确保工作正常进行。
     */
    MASTER_FAILOVER,

    /**
     * Worker节点故障转移事件
     *
     * 处理Worker节点故障时的任务重新调度和状态恢复。
     * 当Worker节点无法正常工作时，需要处理其上正在执行的任务。
     *
     * 触发条件：
     * - Worker节点心跳丢失
     * - Worker节点资源耗尽无法响应
     * - Worker节点主动退出集群
     *
     * 处理内容：
     * - 检查Worker节点上正在执行的任务
     * - 将可重试的任务重新调度到其他Worker
     * - 更新任务状态，标记失败或重试
     * - 清理Worker节点的相关资源和状态
     *
     * 类比：工厂中某台机器故障，需要将正在处理的工件
     *      转移到其他正常的机器上继续生产。
     *
     * 注意：虽然注释中提到"Master failover used to do worker failover"，
     *      但实际上这是Worker故障转移，由Master负责协调处理。
     */
    WORKER_FAILOVER,

}
