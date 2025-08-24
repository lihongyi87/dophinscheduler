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
 * Master故障转移事件处理器
 * 
 * 当Master节点发生故障时，这个处理器负责协调故障转移过程。
 * 故障转移是分布式系统中保证高可用性的关键机制。
 * 
 * 类比：企业中的应急管理部门，当某个部门主管（Master）出现问题时，
 *      立即启动应急预案，确保工作能够正常继续进行。
 * 
 * 处理流程：
 * 1. 接收Master故障转移事件
 * 2. 调用故障转移协调器进行具体的转移操作
 * 3. 确保正在执行的工作流能够继续运行或妥善处理
 * 
 * 这是系统可靠性的重要保障机制。
 */
@Slf4j
@Component
public class MasterFailoverEventHandler implements ISystemEventHandler<MasterFailoverEvent> {

    /**
     * 故障转移协调器
     * 
     * 负责执行具体的故障转移逻辑，包括：
     * - 识别失效的Master节点
     * - 重新分配该节点负责的工作流
     * - 恢复中断的任务执行
     * 
     * 类比：应急响应小组组长，负责协调和执行具体的应急处理措施。
     */
    @Autowired
    private FailoverCoordinator failoverCoordinator;

    /**
     * 处理Master故障转移事件
     * 
     * 这是事件处理的核心方法，当检测到Master节点故障时会被调用。
     * 
     * 处理步骤：
     * 1. 接收故障转移事件，获取故障节点信息
     * 2. 委托故障转移协调器执行具体的转移操作
     * 3. 确保系统能够在故障节点恢复前正常运行
     * 
     * 类比：接到部门主管出现问题的报告后，立即启动应急预案，
     *      安排其他人员接管该主管的工作职责。
     * 
     * @param masterFailoverEvent Master故障转移事件，包含故障节点的详细信息
     */
    @Override
    public void handle(final MasterFailoverEvent masterFailoverEvent) {
        log.info("Processing master failover event: {}", masterFailoverEvent);
        
        // 委托故障转移协调器处理具体的故障转移逻辑
        // 这包括工作流重新分配、任务状态恢复等复杂操作
        failoverCoordinator.failoverMaster(masterFailoverEvent);
        
        log.info("Master failover event processed successfully");
    }

    /**
     * 匹配处理的事件类型
     * 
     * 返回这个处理器能够处理的系统事件类型。
     * 这是策略模式的实现，确保正确的事件被分发到对应的处理器。
     * 
     * @return 返回MASTER_FAILOVER类型，表示处理Master故障转移事件
     */
    @Override
    public SystemEventType matchState() {
        return SystemEventType.MASTER_FAILOVER;
    }
}
