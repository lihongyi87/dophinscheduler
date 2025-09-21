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

package org.apache.dolphinscheduler.dao.entity.event;

import org.apache.dolphinscheduler.common.enums.ListenerEventType;

import java.util.Date;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 服务器宕机监听事件
 *
 * 当Master或Worker节点宕机或与集群失去连接时触发的事件。
 * 这是系统高可用性和故障转移机制的重要组成部分。
 *
 * 触发时机：
 * - Master节点心跳超时
 * - Worker节点心跳超时
 * - 节点主动退出集群
 * - 网络分区导致节点失联
 * - 节点进程异常崩溃
 *
 * 使用场景：
 * - 触发故障转移流程
 * - 发送紧急告警通知
 * - 记录节点故障日志
 * - 更新集群拓扑信息
 * - 重新分配节点上的任务
 * - 通知运维人员介入
 *
 * 重要性：
 * - 极高优先级事件
 * - 需要立即响应和处理
 * - 可能影响正在运行的任务
 * - 需要触发自动恢复机制
 *
 * 类比：就像数据中心的服务器宕机报警，需要立即通知运维团队，
 *      并自动将服务切换到备用服务器。
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ServerDownListenerEvent implements AbstractListenerEvent {

    /**
     * 服务器类型
     * 标识宕机的服务器类型（Master或Worker）
     */
    private String type;

    /**
     * 主机地址
     * 宕机服务器的主机地址或IP
     */
    private String host;

    /**
     * 事件时间
     * 检测到服务器宕机的时间戳
     */
    private Date eventTime;
    /**
     * 获取事件类型
     *
     * @return 返回SERVER_DOWN类型，表示服务器宕机事件
     */
    @Override
    public ListenerEventType getEventType() {
        return ListenerEventType.SERVER_DOWN;
    }

    /**
     * 获取事件标题
     *
     * 生成描述性的事件标题，包含服务器类型和主机地址。
     * 格式："[Master/Worker] server down: [主机地址]"
     *
     * @return 事件标题字符串
     */
    @Override
    public String getTitle() {
        return String.format("%s server down: %s", type, host);
    }
}
