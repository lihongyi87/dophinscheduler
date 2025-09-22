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

import org.apache.dolphinscheduler.eventbus.AbstractDelayEvent;

import java.util.Date;

/**
 * 抽象系统事件基类
 *
 * 这是所有系统级事件的基础类，定义了系统事件的通用结构和行为。
 * 系统事件主要用于处理DolphinScheduler集群中的重要系统级操作，
 * 特别是节点故障转移相关的事件。
 *
 * 类比：企业中各种重要通知的统一格式模板，所有通知都必须包含
 *      发生时间、通知类型等基本信息。
 *
 * 设计特点：
 * - 继承自AbstractDelayEvent，支持延迟处理机制
 * - 采用抽象类设计，强制子类实现关键方法
 * - 提供统一的事件时间和类型接口
 *
 * 延迟处理机制：
 * 通过继承AbstractDelayEvent，系统事件具备了延迟处理能力。
 * 这对于故障转移场景特别重要，可以：
 * - 避免网络抖动造成的误判
 * - 给临时故障一定的恢复时间
 * - 减少不必要的故障转移操作
 *
 * 子类实现：
 * - {@link GlobalMasterFailoverEvent} - 全局Master故障转移事件
 * - {@link MasterFailoverEvent} - 单个Master节点故障转移事件
 * - {@link WorkerFailoverEvent} - Worker节点故障转移事件
 *
 * 注意：作为抽象类，不能直接实例化，必须通过具体的子类来创建事件实例。
 */
public abstract class AbstractSystemEvent extends AbstractDelayEvent {

    /**
     * 默认构造方法
     *
     * 创建一个无延迟的系统事件实例。
     * 事件将立即可以被处理，不会在队列中等待。
     *
     * 类比：发送一份紧急通知，需要立即处理。
     */
    public AbstractSystemEvent() {
        super();
    }

    /**
     * 带延迟时间的构造方法
     *
     * 创建一个具有指定延迟时间的系统事件实例。
     * 事件将在指定的延迟时间后才能被从队列中取出处理。
     *
     * 延迟处理的优势：
     * - 避免网络抖动造成的误判
     * - 给临时故障一定的恢复时间
     * - 减少不必要的系统操作
     *
     * 类比：发送一份定时通知，在指定时间后才处理。
     *
     * @param delayTime 延迟时间（毫秒），必须大于等于0
     */
    public AbstractSystemEvent(long delayTime) {
        super(delayTime);
    }

    /**
     * 获取事件发生时间
     *
     * 返回系统事件实际发生的时间。
     * 这个时间对于故障转移、事件排序和问题追踪非常重要。
     *
     * 注意区别：
     * - 事件发生时间：事件实际发生的时刻（此方法返回值）
     * - 事件处理时间：事件被从队列中取出处理的时刻（可能有延迟）
     *
     * 类比：事故报告中的“事故发生时间”，用于确定事件的先后顺序。
     *
     * @return 事件发生的具体时间，不应该为null
     */
    public abstract Date getEventTime();

    /**
     * 获取系统事件类型
     *
     * 返回当前事件的具体类型，用于事件分发和处理器匹配。
     * 这是事件处理机制中的关键信息，决定了事件会被分发给哪个处理器。
     *
     * 支持的事件类型：
     * - GLOBAL_MASTER_FAILOVER: 全局Master故障转移
     * - MASTER_FAILOVER: 单个Master节点故障转移
     * - WORKER_FAILOVER: Worker节点故障转移
     *
     * 类比：邮件上的“类型标签”，邮件分拣员根据这个标签决定
     *      将邮件送到哪个部门。
     *
     * @return 当前事件的类型，不应该为null
     */
    public abstract SystemEventType getEventType();

}
