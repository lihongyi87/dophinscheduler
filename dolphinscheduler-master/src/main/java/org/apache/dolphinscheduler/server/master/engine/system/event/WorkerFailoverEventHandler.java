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

import org.apache.dolphinscheduler.server.master.failover.FailoverCoordinator;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Worker故障转移事件处理器
 *
 * 负责处理Worker节点故障转移事件的具体业务逻辑。
 * 当Worker节点发生故障时，这个处理器会被调用来处理该节点上
 * 正在执行的任务和相关资源的清理工作。
 *
 * 类比：工厂中的设备维修调度员，当某台生产设备出现故障时，
 *      负责将该设备上的工作任务转移到其他可用设备上，
 *      确保生产流程不受影响。
 *
 * 核心职责：
 * - 接收Worker故障转移事件
 * - 委托故障转移协调器执行具体的转移逻辑
 * - 确保Worker故障转移操作的正确执行
 *
 * 处理内容：
 * - 识别故障Worker节点上的活跃任务
 * - 将可重试的任务重新调度到其他Worker节点
 * - 更新失败任务的状态信息
 * - 清理故障节点的相关资源和状态
 *
 * 处理特点：
 * - 无状态设计：不保存任何实例状态，确保线程安全
 * - 委托模式：将具体的故障转移逻辑委托给专门的协调器
 * - 任务级处理：主要关注任务级别的故障恢复
 * - 快速响应：相比Master故障转移，Worker故障转移通常更快
 *
 * 与其他故障转移处理器的差异：
 * - Worker故障转移：处理任务执行层面的故障
 * - Master故障转移：处理工作流调度层面的故障
 * - 影响范围：Worker故障影响单个节点的任务，Master故障影响整个工作流
 *
 * 执行流程：
 * 1. 接收WorkerFailoverEvent事件
 * 2. 解析故障Worker节点的详细信息
 * 3. 调用FailoverCoordinator的failoverWorker方法
 * 4. 故障转移协调器执行具体的任务重新调度
 * 5. 更新集群状态和任务状态信息
 *
 * 性能考虑：
 * - Worker故障转移通常比Master故障转移更频繁
 * - 需要高效的任务重新调度机制
 * - 避免连锁故障的发生
 */
@Slf4j
@Component
public class WorkerFailoverEventHandler implements ISystemEventHandler<WorkerFailoverEvent> {

    /**
     * 故障转移协调器
     *
     * 负责执行具体的Worker故障转移逻辑，包括：
     * - 识别故障Worker节点上的活跃任务
     * - 将可重试的任务重新调度到其他Worker节点
     * - 更新任务状态和集群信息
     * - 清理故障节点的相关资源
     *
     * 类比：专业的设备维修小组，负责执行具体的设备故障处理工作。
     */
    @Autowired
    private FailoverCoordinator failoverCoordinator;

    /**
     * 处理Worker故障转移事件
     *
     * 这是事件处理的入口方法，当Worker节点发生故障需要转移时被调用。
     *
     * 处理步骤：
     * 1. 接收Worker故障转移事件
     * 2. 解析故障节点的详细信息
     * 3. 记录事件处理日志
     * 4. 委托故障转移协调器执行具体操作
     * 5. 等待故障转移操作完成
     *
     * 处理重点：
     * - 任务重新调度：将可重试的任务分配给其他Worker
     * - 状态更新：及时更新任务和节点状态
     * - 资源清理：清理故障节点的相关资源
     * - 负载均衡：重新平衡剩余Worker节点的工作负载
     *
     * 性能优化：
     * - Worker故障转移通常比Master故障转移更快
     * - 主要处理任务级别的故障，影响范围相对较小
     * - 支持并发处理多个Worker节点的故障
     *
     * 异常处理：
     * 如果故障转移过程中发生异常，会向上抛出，
     * 由SystemEventBusFireWorker进行重试处理。
     *
     * 类比：收到设备故障报告后，立即启动应急方案，
     *      将正在故障设备上处理的工件转移到其他设备上。
     *
     * @param workerFailoverEvent Worker故障转移事件，包含故障节点的详细信息
     */
    @Override
    public void handle(final WorkerFailoverEvent workerFailoverEvent) {
        log.info("Processing worker failover event: {}", workerFailoverEvent);

        // 委托故障转移协调器执行Worker故障转移逻辑
        failoverCoordinator.failoverWorker(workerFailoverEvent);

        log.info("Worker failover event processed successfully");
    }

    /**
     * 匹配处理的事件类型
     *
     * 返回这个处理器能够处理的系统事件类型。
     * SystemEventBusFireWorker会根据这个方法的返回值
     * 决定是否将Worker故障转移事件分发给这个处理器。
     *
     * @return 返回WORKER_FAILOVER类型，表示处理Worker故障转移事件
     */
    @Override
    public SystemEventType matchState() {
        return SystemEventType.WORKER_FAILOVER;
    }
}
