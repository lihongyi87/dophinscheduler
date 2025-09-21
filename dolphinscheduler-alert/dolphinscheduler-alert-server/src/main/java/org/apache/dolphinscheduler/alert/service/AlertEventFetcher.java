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

import org.apache.dolphinscheduler.dao.AlertDao;
import org.apache.dolphinscheduler.dao.entity.Alert;

import java.util.List;

import lombok.extern.slf4j.Slf4j;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * 告警事件获取器
 *
 * <p>该类负责从数据库中持续获取待处理的告警事件，并将其放入
 * 待处理队列中。作为告警系统的数据源组件，确保所有告警都能
 * 被及时获取和处理。</p>
 *
 * <p>主要职责：</p>
 * <ul>
 *   <li>定期扫描数据库中的待处理告警</li>
 *   <li>将告警事件提交到待处理队列</li>
 *   <li>维护事件偏移量以避免重复处理</li>
 *   <li>支持高可用模式下的主从切换</li>
 * </ul>
 */
@Slf4j
@Component
public class AlertEventFetcher extends AbstractEventFetcher<Alert> {

    /** 告警数据访问对象 */
    private final AlertDao alertDao;

    /**
     * 构造告警事件获取器
     *
     * @param alertHAServer 高可用服务器，用于判断当前节点是否为主节点
     * @param alertDao 告警数据访问对象
     * @param alertEventPendingQueue 告警事件待处理队列
     */
    public AlertEventFetcher(AlertHAServer alertHAServer,
                             AlertDao alertDao,
                             AlertEventPendingQueue alertEventPendingQueue) {
        // 调用父类构造器，设置线程名称、HA服务器和待处理队列
        // 线程名称用于识别，HA服务器用于主从判断，队列用于存放获取的事件
        super("AlertEventFetcher", alertHAServer, alertEventPendingQueue);
        // 保存数据访问对象引用，用于查询数据库中的待处理告警
        this.alertDao = alertDao;
    }

    /**
     * 获取待处理的告警事件
     *
     * <p>使用事务注解确保在MySQL主从模式下，查询会被路由到主库，
     * 避免从从库查询到过期数据。</p>
     *
     * @param eventOffset 事件偏移量，用于增量获取
     * @return 待处理的告警列表
     */
    @Override
    @Transactional
    public List<Alert> fetchPendingEvent(int eventOffset) {
        // 使用事务确保在MySQL主从模式下查询路由到主库
        // 避免从从库查询到非最新数据
        return alertDao.listPendingAlerts(eventOffset);
    }

    /**
     * 获取事件偏移量
     *
     * @param event 告警事件
     * @return 使用告警ID作为偏移量
     */
    @Override
    protected int getEventOffset(Alert event) {
        // 返回告警事件的ID作为偏移量
        // 这样可以确保下次查询时从该ID之后开始，避免重复处理
        return event.getId();
    }
}
