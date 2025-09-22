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

import org.apache.dolphinscheduler.task.executor.eventbus.TaskExecutorEventBusCoordinator;

import org.springframework.stereotype.Component;

/**
 * 物理任务执行器事件总线协调器
 *
 * 专门处理物理任务执行过程中的各种事件，协调事件的发布、订阅和处理流程。
 * 类比：工厂的调度中心，接收各车间的状态报告并统一协调处理。
 *
 * 主要功能：
 * 1. 协调物理任务执行器的生命周期事件
 * 2. 管理事件监听器的注册和调用
 * 3. 确保事件处理的顺序性和一致性
 * 4. 提供事件总线的统一管理接口
 *
 * 处理的事件类型：
 * - 任务启动事件
 * - 任务状态变更事件
 * - 任务完成事件
 * - 任务失败事件
 * - 任务取消事件
 * - 运行时上下文变更事件
 *
 * 设计模式：
 * - 继承父类TaskExecutorEventBusCoordinator的通用功能
 * - 专门为物理任务执行器定制事件处理逻辑
 * - 使用观察者模式实现事件的发布订阅机制
 */
@Component
public class PhysicalTaskExecutorEventBusCoordinator extends TaskExecutorEventBusCoordinator {

    /**
     * 构造函数
     *
     * 初始化物理任务执行器事件总线协调器，设置必要的依赖组件。
     * 在构造过程中会自动注册物理任务执行器生命周期事件监听器。
     *
     * @param physicalTaskExecutorRepository 物理任务执行器仓库，用于查找和管理任务实例
     * @param physicalTaskExecutorLifecycleEventListener 物理任务执行器生命周期事件监听器，处理具体的事件逻辑
     */
    public PhysicalTaskExecutorEventBusCoordinator(final PhysicalTaskExecutorRepository physicalTaskExecutorRepository,
                                                   final PhysicalTaskExecutorLifecycleEventListener physicalTaskExecutorLifecycleEventListener) {
        // 调用父类构造函数，设置协调器名称和任务执行器仓库
        super("PhysicalTaskExecutorEventBusCoordinator", physicalTaskExecutorRepository);

        // 注册物理任务执行器生命周期事件监听器
        // 这个监听器将处理所有与物理任务生命周期相关的事件
        registerTaskExecutorLifecycleEventListener(physicalTaskExecutorLifecycleEventListener);
    }
}
