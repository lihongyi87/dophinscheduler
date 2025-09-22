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

package org.apache.dolphinscheduler.alert.service;

import org.apache.dolphinscheduler.alert.metrics.AlertServerMetrics;
import org.apache.dolphinscheduler.dao.entity.Alert;

import lombok.extern.slf4j.Slf4j;

import org.springframework.stereotype.Component;

/**
 * 告警事件循环处理器
 *
 * <p>该类负责从待处理队列中持续获取告警事件并发送。
 * 作为告警系统的核心消费者，确保告警能够及时、可靠地发送到
 * 目标接收方。</p>
 *
 * <p>主要功能：</p>
 * <ul>
 *   <li>循环消费告警事件队列</li>
 *   <li>使用线程池并发处理告警发送</li>
 *   <li>监控处理中的告警数量</li>
 *   <li>注册告警指标监控</li>
 * </ul>
 */
@Slf4j
@Component
public class AlertEventLoop extends AbstractEventLoop<Alert> {

    /** 告警发送器 */
    private final AlertSender alertSender;

    /**
     * 构造告警事件循环处理器
     *
     * @param alertEventPendingQueue 告警事件待处理队列
     * @param alertSenderThreadPoolFactory 告警发送线程池工厂
     * @param alertSender 告警发送器
     */
    public AlertEventLoop(AlertEventPendingQueue alertEventPendingQueue,
                          AlertSenderThreadPoolFactory alertSenderThreadPoolFactory,
                          AlertSender alertSender) {
        // 调用父类构造器，传入线程名称、线程池和待处理队列
        // 父类负责启动消费者线程从队列中获取事件并提交到线程池处理
        super("AlertEventLoop", alertSenderThreadPoolFactory.getThreadPool(), alertEventPendingQueue);
        // 保存告警发送器引用，用于实际发送告警
        this.alertSender = alertSender;
        // 注册待处理告警数量指标，用于监控系统性能
        // 通过方法引用获取当前正在处理的事件数量
        AlertServerMetrics.registerPendingAlertGauge(this::getHandlingEventCount);
    }

    /**
     * 处理告警事件
     *
     * @param event 待处理的告警事件
     */
    @Override
    public void handleEvent(Alert event) {
        // 委托给告警发送器处理具体的告警发送逻辑
        // 发送器会根据告警类型选择合适的插件进行发送
        alertSender.sendEvent(event);
    }

}
