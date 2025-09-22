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

package org.apache.dolphinscheduler.server.master.failover;

import org.apache.dolphinscheduler.server.master.engine.system.event.GlobalMasterFailoverEvent;
import org.apache.dolphinscheduler.server.master.engine.system.event.MasterFailoverEvent;
import org.apache.dolphinscheduler.server.master.engine.system.event.WorkerFailoverEvent;

/**
 * 故障转移协调器接口
 *
 * 定义了DolphinScheduler集群中节点故障时的处理策略和方法。
 * 当Master或Worker节点从集群中移除时，负责执行故障转移工作，
 * 确保系统在节点故障后能够继续正常运行。
 *
 * <p>故障转移机制是分布式调度系统高可用性的核心保障，主要包括：
 * <ul>
 *   <li>故障检测：通过注册中心监控节点健康状态</li>
 *   <li>任务恢复：将故障节点的任务重新分配给健康节点</li>
 *   <li>状态同步：确保故障转移过程中数据一致性</li>
 * </ul>
 *
 * <p>三种故障转移场景：
 * <ul>
 *   <li>全局Master故障转移：系统启动时处理历史遗留的未完成任务</li>
 *   <li>Master节点故障转移：特定Master节点故障时的工作流转移</li>
 *   <li>Worker节点故障转移：Worker节点故障时的任务重新调度</li>
 * </ul>
 *
 * @author DolphinScheduler Team
 * @since 1.0.0
 */
public interface IFailoverCoordinator {

    /**
     * 执行全局Master故障转移
     *
     * 在系统启动时或检测到全局故障时执行，扫描整个系统中需要故障转移的工作流并进行转移。
     * 这是一个全量扫描和恢复过程，主要用于处理系统重启后的历史遗留问题。
     *
     * <p>故障检测机制：
     * <ul>
     *   <li>扫描数据库中所有未完成的工作流实例</li>
     *   <li>检查这些工作流对应的Master节点是否还在集群中</li>
     *   <li>对于已故障Master的工作流，重新分配给健康的Master节点</li>
     * </ul>
     *
     * <p>性能考虑：
     * <ul>
     *   <li>该操作需要扫描整个系统的工作流，执行时间较长</li>
     *   <li>仅应在服务器首次启动时调用</li>
     *   <li>不应在主线程中调用，避免长时间阻塞</li>
     * </ul>
     *
     * @param globalMasterFailoverEvent 全局Master故障转移事件，包含事件时间等信息
     */
    void globalMasterFailover(final GlobalMasterFailoverEvent globalMasterFailoverEvent);

    /**
     * 执行特定Master节点的故障转移
     *
     * 当检测到特定Master节点从集群中移除时，查找运行在该故障Master上的工作流，
     * 并将这些工作流转移到其他健康的Master节点继续执行。
     *
     * <p>故障检测流程：
     * <ul>
     *   <li>注册中心监听Master节点的心跳状态</li>
     *   <li>当节点心跳超时或主动下线时触发故障转移事件</li>
     *   <li>验证节点是否真的故障（排除网络抖动等临时问题）</li>
     * </ul>
     *
     * <p>任务恢复策略：
     * <ul>
     *   <li>查询故障Master上正在运行的所有工作流实例</li>
     *   <li>将工作流状态设置为FAILOVER，标记需要恢复</li>
     *   <li>生成恢复命令，由其他Master节点接管执行</li>
     * </ul>
     *
     * <p>与全局故障转移的区别：
     * <ul>
     *   <li>范围更小：仅处理特定Master节点的工作流</li>
     *   <li>实时性更强：响应实时的故障事件</li>
     *   <li>性能更好：不需要全量扫描系统</li>
     * </ul>
     *
     * @param masterFailoverEvent Master故障转移事件，包含故障节点信息和事件时间
     */
    void failoverMaster(final MasterFailoverEvent masterFailoverEvent);

    /**
     * 执行Worker节点的故障转移
     *
     * 当检测到Worker节点从集群中移除时，查找已分发到该故障Worker且正在运行的任务，
     * 并将这些任务重新调度到其他健康的Worker节点执行。
     *
     * <p>故障检测机制：
     * <ul>
     *   <li>Master通过注册中心监控Worker节点的健康状态</li>
     *   <li>当Worker节点心跳超时或主动下线时触发故障转移</li>
     *   <li>确认Worker节点确实不可用（避免网络问题误判）</li>
     * </ul>
     *
     * <p>任务重新调度策略：
     * <ul>
     *   <li>识别故障Worker上处于DISPATCH或RUNNING状态的任务</li>
     *   <li>将这些任务标记为需要重新调度</li>
     *   <li>根据资源和负载情况选择新的Worker节点</li>
     *   <li>重新分发任务并更新任务状态</li>
     * </ul>
     *
     * <p>状态同步保障：
     * <ul>
     *   <li>任务状态的原子性更新，避免状态不一致</li>
     *   <li>任务日志和执行结果的迁移处理</li>
     *   <li>资源清理和释放（如临时文件、进程等）</li>
     * </ul>
     *
     * <p>与Master故障转移的区别：
     * <ul>
     *   <li>粒度更细：处理单个任务级别的故障转移</li>
     *   <li>影响面更小：不影响工作流的整体执行流程</li>
     *   <li>恢复更快：任务级别的重新调度速度更快</li>
     * </ul>
     *
     * @param workerFailoverEvent Worker故障转移事件，包含故障Worker节点信息
     */
    void failoverWorker(final WorkerFailoverEvent workerFailoverEvent);
}
