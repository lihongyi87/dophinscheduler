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

package org.apache.dolphinscheduler.server.worker.executor;

import org.apache.dolphinscheduler.extract.master.TaskExecutorEventRemoteReporterClient;

import org.springframework.stereotype.Component;

/**
 * 物理任务执行器事件远程报告客户端
 *
 * 负责将Worker节点上的物理任务执行事件远程上报给Master节点。
 * 类比：工厂车间的汇报员，定期向总部汇报生产情况和异常状态。
 *
 * 主要功能：
 * 1. 建立与Master节点的通信连接
 * 2. 将任务状态变更事件发送到Master节点
 * 3. 处理网络通信异常和重试逻辑
 * 4. 确保关键事件的可靠传输
 *
 * 上报的事件类型：
 * - 任务启动事件：通知Master任务已开始执行
 * - 任务进度事件：更新任务执行进度
 * - 任务完成事件：通知Master任务成功完成
 * - 任务失败事件：上报任务执行失败信息
 * - 任务取消事件：确认任务已被取消
 * - 运行时状态变更：同步任务运行时信息
 *
 * 通信特点：
 * - 使用异步通信方式，避免阻塞任务执行
 * - 支持事件批量发送，提高网络效率
 * - 具备失败重试机制，确保重要事件不丢失
 * - 兼容Master节点的高可用性架构
 *
 * 架构角色：
 * - 继承父类TaskExecutorEventRemoteReporterClient的通用网络通信功能
 * - 专门适配物理任务执行器的事件报告需求
 * - 作为Worker与Master之间的事件通信桥梁
 */
@Component
public class PhysicalTaskExecutorEventRemoteReporterClient extends TaskExecutorEventRemoteReporterClient {

}
