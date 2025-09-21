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

import org.apache.dolphinscheduler.task.executor.listener.TaskExecutorLifecycleEventListener;

import org.springframework.stereotype.Component;

/**
 * 物理任务执行器生命周期事件监听器
 *
 * 专门监听和处理物理任务执行器在整个生命周期中产生的各种事件。
 * 类比：工厂的质检员，实时监控生产线上每个环节的状态变化。
 *
 * 主要功能：
 * 1. 监听任务执行器的启动、运行、完成、失败等生命周期事件
 * 2. 协调容器提供者进行任务的调度和资源分配
 * 3. 管理任务实例在仓库中的注册和移除
 * 4. 触发事件上报机制，将状态变化通知给Master节点
 *
 * 处理的生命周期事件：
 * - 任务提交事件：新任务被提交到执行队列
 * - 任务启动事件：任务开始实际执行
 * - 任务运行事件：任务正在执行过程中
 * - 任务暂停事件：任务被临时暂停
 * - 任务恢复事件：暂停的任务被恢复执行
 * - 任务完成事件：任务成功完成
 * - 任务失败事件：任务执行失败
 * - 任务取消事件：任务被用户或系统取消
 * - 任务超时事件：任务执行超过预定时间
 *
 * 监听器模式的优势：
 * - 解耦事件的产生和处理逻辑
 * - 支持多个处理器同时监听同一事件
 * - 提供灵活的事件处理机制
 * - 便于系统的扩展和维护
 */
@Component
public class PhysicalTaskExecutorLifecycleEventListener extends TaskExecutorLifecycleEventListener {

    /**
     * 构造函数
     *
     * 初始化物理任务执行器生命周期事件监听器，设置必要的依赖组件。
     * 监听器将协调容器、仓库和事件上报器来完整处理任务生命周期。
     *
     * @param physicalTaskExecutorContainerDelegator 物理任务执行器容器提供者，管理任务执行的线程资源
     * @param physicalTaskExecutorRepository 物理任务执行器仓库，存储和管理任务实例
     * @param physicalTaskExecutorEventReporter 物理任务执行器事件上报器，将事件上报给Master节点
     */
    public PhysicalTaskExecutorLifecycleEventListener(
                                                      final PhysicalTaskExecutorContainerProvider physicalTaskExecutorContainerDelegator,
                                                      final PhysicalTaskExecutorRepository physicalTaskExecutorRepository,
                                                      final PhysicalTaskExecutorLifecycleEventReporter physicalTaskExecutorEventReporter) {
        // 调用父类构造函数，传入必要的组件依赖
        // 这些组件将被用于处理各种生命周期事件
        super(
                physicalTaskExecutorContainerDelegator,
                physicalTaskExecutorRepository,
                physicalTaskExecutorEventReporter);
    }
}
