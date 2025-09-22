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

import org.apache.dolphinscheduler.alert.api.AlertData;
import org.apache.dolphinscheduler.alert.api.AlertResult;
import org.apache.dolphinscheduler.alert.config.AlertConfig;
import org.apache.dolphinscheduler.alert.plugin.AlertPluginManager;
import org.apache.dolphinscheduler.common.enums.AlertStatus;
import org.apache.dolphinscheduler.dao.AlertDao;
import org.apache.dolphinscheduler.dao.entity.Alert;
import org.apache.dolphinscheduler.dao.entity.AlertPluginInstance;
import org.apache.dolphinscheduler.extract.alert.request.AlertSendResponse;

import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

import org.springframework.stereotype.Component;

/**
 * 告警发送器
 * 负责处理告警事件的发送，支持同步和异步发送模式
 */
@Slf4j
@Component
public class AlertSender extends AbstractEventSender<Alert> {

    /** 告警数据访问对象 */
    private final AlertDao alertDao;

    public AlertSender(AlertDao alertDao,
                       AlertPluginManager alertPluginManager,
                       AlertConfig alertConfig) {
        // 调用父类构造器，传入插件管理器和等待超时时间
        // 插件管理器用于获取具体的告警发送插件
        // 等待超时时间用于控制发送操作的最大等待时间
        super(alertPluginManager, alertConfig.getWaitTimeout());
        // 保存数据访问对象引用，用于更新告警状态和查询告警组配置
        this.alertDao = alertDao;
    }

    /**
     * 同步发送告警处理器
     * 直接发送告警并等待结果返回
     *
     * @param alertGroupId 告警组ID
     * @param title        告警标题
     * @param content      告警内容
     * @return 告警发送响应结果
     */
    public AlertSendResponse syncHandler(int alertGroupId, String title, String content) {
        // 根据告警组ID查询所有关联的告警插件实例
        List<AlertPluginInstance> alertInstanceList = alertDao.listInstanceByAlertGroupId(alertGroupId);

        // 构建告警数据对象，封装告警内容和标题
        AlertData alertData = AlertData.builder()
                .content(content)
                .title(title)
                .build();

        // 初始化发送状态为成功，如果有任何一个实例发送失败将置为false
        boolean sendResponseStatus = true;
        // 用于收集所有告警实例的发送结果
        List<AlertSendResponse.AlertSendResponseResult> sendResponseResults = new ArrayList<>();

        // 检查是否存在告警实例，如果没有则直接返回失败
        if (CollectionUtils.isEmpty(alertInstanceList)) {
            // 创建失败结果对象
            AlertSendResponse.AlertSendResponseResult alertSendResponseResult =
                    new AlertSendResponse.AlertSendResponseResult();
            // 构建错误信息
            String message = String.format("Alert GroupId %s send error : not found alert instance", alertGroupId);
            alertSendResponseResult.setSuccess(false);
            alertSendResponseResult.setMessage(message);
            sendResponseResults.add(alertSendResponseResult);
            // 记录错误日志
            log.error("告警组 {} 发送失败：未找到告警实例", alertGroupId);
            // 返回失败响应
            return new AlertSendResponse(false, sendResponseResults);
        }

        // 遍历所有告警实例，逐个发送告警
        for (AlertPluginInstance instance : alertInstanceList) {
            // 调用实际的发送方法，通过插件发送告警
            AlertResult alertResult = doSendEvent(instance, alertData);

            // 处理发送结果
            if (alertResult != null) {
                // 创建响应结果对象
                AlertSendResponse.AlertSendResponseResult alertSendResponseResult =
                        new AlertSendResponse.AlertSendResponseResult(
                                alertResult.isSuccess(),
                                alertResult.getMessage());
                // 更新整体发送状态，只要有一个失败就为false
                sendResponseStatus = sendResponseStatus && alertSendResponseResult.isSuccess();
                // 将结果添加到结果列表
                sendResponseResults.add(alertSendResponseResult);
            }
        }

        // 返回最终的发送结果，包含整体状态和每个实例的发送结果
        return new AlertSendResponse(sendResponseStatus, sendResponseResults);
    }

    /**
     * 获取告警插件实例列表
     * @param event 告警事件
     * @return 与告警组关联的插件实例列表
     */
    @Override
    public List<AlertPluginInstance> getAlertPluginInstanceList(Alert event) {
        // 根据告警事件的告警组ID查询所有关联的告警插件实例
        // 每个告警组可以配置多个不同类型的告警插件（如邮件、短信、钉钉等）
        return alertDao.listInstanceByAlertGroupId(event.getAlertGroupId());
    }

    /**
     * 构建告警数据
     * @param event 告警事件
     * @return 用于发送的告警数据对象
     */
    @Override
    public AlertData getAlertData(Alert event) {
        // 使用建造者模式构建告警数据对象
        // 将数据库中的告警实体转换为插件可识别的告警数据格式
        return AlertData.builder()
                // 设置告警ID，用于追踪和标识
                .id(event.getId())
                // 设置告警内容，包含具体的告警信息
                .content(event.getContent())
                // 设置告警日志，包含详细的错误或状态信息
                .log(event.getLog())
                // 设置告警标题，告警的简要描述
                .title(event.getTitle())
                // 设置告警类型代码，用于插件识别处理方式
                .alertType(event.getAlertType().getCode())
                .build();
    }

    /**
     * 获取事件ID
     * @param event 告警事件
     * @return 事件ID
     */
    @Override
    public Integer getEventId(Alert event) {
        // 返回告警事件的唯一标识ID
        // 用于日志记录和状态追踪
        return event.getId();
    }

    /**
     * 处理告警发送错误
     * @param event 告警事件
     * @param log 错误日志
     */
    @Override
    public void onError(Alert event, String log) {
        // 更新告警状态为执行失败，并记录错误日志
        // 这里会将失败信息持久化到数据库，便于后续分析和重试
        alertDao.updateAlert(AlertStatus.EXECUTION_FAILURE, log, event.getId());
    }

    /**
     * 处理告警部分成功
     * @param event 告警事件
     * @param log 执行日志
     */
    @Override
    public void onPartialSuccess(Alert event, String log) {
        // 更新告警状态为部分成功，表示有些插件发送成功，有些失败
        // 这种情况通常发生在配置了多个告警渠道时
        alertDao.updateAlert(AlertStatus.EXECUTION_PARTIAL_SUCCESS, log, event.getId());
    }

    /**
     * 处理告警发送成功
     * @param event 告警事件
     * @param log 执行日志
     */
    @Override
    public void onSuccess(Alert event, String log) {
        // 更新告警状态为执行成功，表示所有配置的告警渠道都发送成功
        // 成功状态意味着告警已经正确送达到接收方
        alertDao.updateAlert(AlertStatus.EXECUTION_SUCCESS, log, event.getId());
    }
}
