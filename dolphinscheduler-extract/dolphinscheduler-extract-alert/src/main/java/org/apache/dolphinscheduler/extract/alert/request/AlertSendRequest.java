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

package org.apache.dolphinscheduler.extract.alert.request;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 告警发送请求
 *
 * <p>该类封装了发送告警所需的所有信息。</p>
 *
 * <p>主要用途：</p>
 * <ul>
 *   <li>携带告警内容和配置信息</li>
 *   <li>指定告警组和告警类型</li>
 *   <li>在RPC调用中传递告警参数</li>
 * </ul>
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AlertSendRequest {

    /**
     * 告警组ID
     * 用于指定告警应该发送给哪个组的成员
     */
    private int groupId;

    /**
     * 告警标题
     * 告警信息的标题或主题
     */
    private String title;

    /**
     * 告警内容
     * 详细的告警信息内容
     */
    private String content;

    /**
     * 告警类型
     * 区分不同的告警级别或类型
     */
    private int warnType;

}
