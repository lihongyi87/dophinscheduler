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
 * 全局Master故障转移事件处理器
 *
 * 负责处理全局Master故障转移事件的具体业务逻辑。
 * 当系统需要进行全面的Master节点故障检查和转移时，这个处理器会被调用。
 *
 * 类比：企业的总应急指挥官，当收到全面应急检查的指令时，
 *      负责协调各个部门进行全面的安全检查和应急响应。
 *
 * 核心职责：
 * - 接收全局Master故障转移事件
 * - 委托故障转移协调器执行具体的故障转移逻辑
 * - 确保全局故障转移操作的正确执行
 *
 * 处理特点：
 * - 无状态设计：不保存任何实例状态，确保线程安全
 * - 委托模式：将具体的故障转移逻辑委托给专门的协调器
 * - 统一接口：实现标准的系统事件处理器接口
 *
 * 与其他故障转移处理器的关系：
 * - GlobalMasterFailoverEventHandler：处理全局Master故障转移
 * - MasterFailoverEventHandler：处理单个Master节点故障转移
 * - WorkerFailoverEventHandler：处理Worker节点故障转移
 *
 * 执行流程：
 * 1. 接收GlobalMasterFailoverEvent事件
 * 2. 调用FailoverCoordinator的globalMasterFailover方法
 * 3. 故障转移协调器执行具体的故障检查和转移操作
 * 4. 确保系统在故障转移后能够正常运行
 *
 * 依赖关系：
 * - 依赖FailoverCoordinator进行具体的故障转移操作
 * - 作为Spring组件，自动注册到事件处理器列表中
 */
@Slf4j
@Component
public class GlobalMasterFailoverEventHandler implements ISystemEventHandler<GlobalMasterFailoverEvent> {

    /**
     * 故障转移协调器
     *
     * 负责执行具体的故障转移逻辑，包括：
     * - 扫描所有Master节点的状态
     * - 识别失效的Master节点
     * - 执行故障转移操作
     * - 重新分配工作流实例
     *
     * 类比：企业的专业应急响应小组，负责执行具体的应急处理措施。
     */
    @Autowired
    private FailoverCoordinator failoverCoordinator;

    /**
     * 处理全局Master故障转移事件
     *
     * 这是事件处理的入口方法，当系统需要进行全局Master故障转移时被调用。
     *
     * 处理步骤：
     * 1. 接收全局Master故障转移事件
     * 2. 记录事件处理日志（可选）
     * 3. 委托故障转移协调器执行具体操作
     * 4. 等待故障转移操作完成
     *
     * 性能考虑：
     * - 全局故障转移是重量级操作，可能耗时较长
     * - 处理过程中可能会对系统性能产生影响
     * - 需要考虑处理超时和重试机制
     *
     * 异常处理：
     * 如果故障转移过程中发生异常，会向上抛出，
     * 由SystemEventBusFireWorker进行重试处理。
     *
     * 类比：接到全面应急检查指令后，立即启动应急预案，
     *      协调各个部门进行全面的安全检查。
     *
     * @param systemEvent 全局Master故障转移事件，包含事件发生时间等信息
     */
    @Override
    public void handle(final GlobalMasterFailoverEvent systemEvent) {
        log.info("Processing global master failover event: {}", systemEvent);

        // 委托故障转移协调器执行全局Master故障转移逻辑
        failoverCoordinator.globalMasterFailover(systemEvent);

        log.info("Global master failover event processed successfully");
    }

    /**
     * 匹配处理的事件类型
     *
     * 返回这个处理器能够处理的系统事件类型。
     * SystemEventBusFireWorker会根据这个方法的返回值
     * 决定是否将事件分发给这个处理器。
     *
     * 这是策略模式中策略选择的关键方法，确保事件能够被
     * 正确分发到对应的处理器。
     *
     * @return 返回GLOBAL_MASTER_FAILOVER类型，表示处理全局Master故障转移事件
     */
    @Override
    public SystemEventType matchState() {
        return SystemEventType.GLOBAL_MASTER_FAILOVER;
    }
}
