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

import org.apache.dolphinscheduler.task.executor.eventbus.TaskExecutorLifecycleEventRemoteReporter;

import org.springframework.stereotype.Component;

/**
 * 物理任务执行器生命周期事件报告器
 *
 * 负责将Worker节点上的物理任务生命周期事件报告给Master节点。
 * 类比：企业的信息官，负责将分公司的重要信息及时汇报给总部。
 *
 * 主要功能：
 * 1. 收集物理任务执行器产生的各种生命周期事件
 * 2. 将事件格式化并通过网络发送给Master节点
 * 3. 处理事件发送失败的重试机制
 * 4. 管理事件发送的顺序性和可靠性
 *
 * 报告的事件类型：
 * - 任务启动报告：通知Master任务已开始执行
 * - 任务进度报告：定期更新任务执行进度
 * - 任务状态变更报告：任务状态发生改变时的通知
 * - 任务完成报告：任务成功完成的确认消息
 * - 任务失败报告：任务执行失败的详细信息
 * - 任务取消报告：任务被取消的确认消息
 * - 运行时信息报告：任务运行时环境和资源使用情况
 *
 * 报告机制特点：
 * - 异步发送，不阻塞任务执行
 * - 支持批量发送，提高网络效率
 * - 具备重试机制，确保重要事件送达
 * - 支持Master节点的故障转移
 *
 * 架构角色：
 * - 继承父类TaskExecutorLifecycleEventRemoteReporter的通用报告功能
 * - 专门处理物理任务执行器的事件报告需求
 * - 作为Worker与Master通信的重要桥梁
 */
@Component
public class PhysicalTaskExecutorLifecycleEventReporter extends TaskExecutorLifecycleEventRemoteReporter {

    /**
     * 构造函数
     *
     * 初始化物理任务执行器生命周期事件报告器，设置必要的依赖组件。
     * 报告器将使用远程客户端发送事件，并从仓库获取任务信息。
     *
     * @param physicalTaskExecutorEventRemoteReporterClient 物理任务执行器事件远程报告客户端，处理与Master的网络通信
     * @param taskExecutorRepository 任务执行器仓库，提供任务实例的查询和管理功能
     */
    public PhysicalTaskExecutorLifecycleEventReporter(
                                                      final PhysicalTaskExecutorEventRemoteReporterClient physicalTaskExecutorEventRemoteReporterClient,
                                                      final PhysicalTaskExecutorRepository taskExecutorRepository) {
        // 调用父类构造函数，设置报告器名称、远程客户端和任务仓库
        super("PhysicalTaskExecutorLifecycleEventReporter", physicalTaskExecutorEventRemoteReporterClient,
                taskExecutorRepository);
    }
}
