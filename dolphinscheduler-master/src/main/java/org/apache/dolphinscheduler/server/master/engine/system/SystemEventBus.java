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

package org.apache.dolphinscheduler.server.master.engine.system;

import org.apache.dolphinscheduler.eventbus.AbstractDelayEventBus;
import org.apache.dolphinscheduler.server.master.engine.AbstractLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.system.event.AbstractSystemEvent;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.AbstractTaskLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.workflow.lifecycle.AbstractWorkflowLifecycleLifecycleEvent;

import lombok.extern.slf4j.Slf4j;

import org.springframework.stereotype.Component;

/**
 * 系统事件总线
 *
 * 这是DolphinScheduler Master引擎中用于处理系统级事件的核心组件。
 * 它负责管理和分发各种系统事件，特别是故障转移相关的事件。
 *
 * 类比：企业中的信息传递中心，负责接收、暂存和分发各种重要通知，
 *      确保关键信息能够及时传达到相应的处理部门。
 *
 * 功能特点：
 * - 基于延迟队列机制，支持延迟事件处理
 * - 线程安全的事件发布和消费
 * - 支持系统故障转移事件的统一管理
 *
 * 处理的事件类型包括：
 * - 全局Master故障转移事件 ({@link GlobalMasterFailoverEvent})
 * - 单个Master节点故障转移事件 ({@link MasterFailoverEvent})
 * - Worker节点故障转移事件 ({@link WorkerFailoverEvent})
 *
 * 工作原理：
 * 1. 系统组件通过publish()方法发布事件到队列
 * 2. SystemEventBusFireWorker线程持续从队列中取出事件
 * 3. 事件被分发到对应的处理器进行处理
 *
 * 注意：这个类继承自AbstractDelayEventBus，具备延迟事件处理能力，
 *      这对于故障转移场景特别重要，可以避免网络抖动等临时问题造成的误判。
 */
@Slf4j
@Component
public class SystemEventBus extends AbstractDelayEventBus<AbstractSystemEvent> {

    /**
     * 发布系统事件到事件总线
     *
     * 当系统中发生需要处理的事件时（如节点故障），通过此方法将事件发布到队列中。
     * 事件将被加入到延迟队列，等待SystemEventBusFireWorker消费处理。
     *
     * 类比：向企业内部的通知系统发送重要公告，公告会被统一收集并分发给相关部门。
     *
     * 执行流程：
     * 1. 调用父类的publish方法将事件加入队列
     * 2. 记录事件发布日志，便于追踪和调试
     *
     * @param event 要发布的系统事件，不能为null
     */
    public void publish(final AbstractSystemEvent event) {
        super.publish(event);
        log.info("Published SystemEvent: {}", event);
    }

    /**
     * 从事件队列中取出待处理的系统事件
     *
     * 这是一个阻塞方法，如果队列为空或者队列中的事件尚未到达处理时间，
     * 调用线程将被阻塞直到有可处理的事件为止。
     *
     * 类比：值班人员在通知中心等待处理紧急通知，如果没有通知就一直等待，
     *      直到有新的通知到达或者已有通知到了需要处理的时间。
     *
     * 注意：
     * - 这是一个阻塞操作，适合在专门的事件处理线程中调用
     * - 支持延迟事件，只有到达指定时间的事件才会被返回
     * - 如果线程被中断，会抛出InterruptedException
     *
     * @return 从队列中取出的系统事件
     * @throws InterruptedException 如果等待过程中线程被中断
     */
    public AbstractSystemEvent take() throws InterruptedException {
        return delayEventQueue.take();
    }
}
