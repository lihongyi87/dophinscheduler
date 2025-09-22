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

package org.apache.dolphinscheduler.extract.alert;

import org.apache.dolphinscheduler.extract.alert.request.AlertSendRequest;
import org.apache.dolphinscheduler.extract.alert.request.AlertSendResponse;
import org.apache.dolphinscheduler.extract.alert.request.AlertTestSendRequest;
import org.apache.dolphinscheduler.extract.base.RpcMethod;
import org.apache.dolphinscheduler.extract.base.RpcService;

/**
 * 告警操作接口
 *
 * <p>该接口定义了告警服务的RPC操作方法。</p>
 *
 * <p>主要功能：</p>
 * <ul>
 *   <li>发送正式告警信息</li>
 *   <li>发送测试告警信息</li>
 *   <li>支持多种告警通道(邮件、短信、钉钉、微信等)</li>
 * </ul>
 */
@RpcService
public interface IAlertOperator {

    /**
     * 发送告警信息
     *
     * @param alertSendRequest 告警发送请求，包含告警内容和接收者信息
     * @return 告警发送响应，包含发送结果和状态
     */
    @RpcMethod
    AlertSendResponse sendAlert(AlertSendRequest alertSendRequest);

    /**
     * 发送测试告警
     * 用于验证告警配置是否正确
     *
     * @param alertSendRequest 测试告警发送请求
     * @return 告警发送响应，包含测试结果
     */
    @RpcMethod
    AlertSendResponse sendTestAlert(AlertTestSendRequest alertSendRequest);

}
