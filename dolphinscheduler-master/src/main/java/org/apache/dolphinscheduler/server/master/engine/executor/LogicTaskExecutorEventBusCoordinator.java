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

package org.apache.dolphinscheduler.server.master.engine.executor;

import org.apache.dolphinscheduler.task.executor.eventbus.TaskExecutorEventBusCoordinator;

import org.springframework.stereotype.Component;

/**
 * 逻辑任务执行器事件总线协调器
 *
 * 这是Master节点中专门处理逻辑任务执行事件的协调器。
 * 逻辑任务是指在Master节点本地执行的任务，如条件判断、依赖检查、Switch分支等，
 * 无需调度到Worker节点执行。
 *
 * 核心职责：
 * 1. 管理逻辑任务的生命周期事件
 * 2. 协调事件在Master内部的处理流程
 * 3. 提供逻辑任务事件的统一管理接口
 * 4. 确保逻辑任务事件的有序处理
 *
 * 与物理任务的区别：
 * - 执行位置：逻辑任务在Master执行，物理任务在Worker执行
 * - 任务类型：逻辑任务主要是流程控制，物理任务是实际计算
 * - 资源消耗：逻辑任务轻量级，物理任务可能需要大量资源
 * - 调度方式：逻辑任务直接执行，物理任务需要分发调度
 *
 * 处理的事件类型：
 * - 逻辑任务开始执行
 * - 逻辑任务状态变更
 * - 逻辑任务执行成功
 * - 逻辑任务执行失败
 * - 逻辑任务运行时上下文变更
 *
 * 类比：就像企业内部的决策中心，负责处理不需要外包的内部决策任务，
 * 如审批流程、条件判断等，这些任务在总部直接完成，不需要分配到分公司。
 */
@Component
public class LogicTaskExecutorEventBusCoordinator extends TaskExecutorEventBusCoordinator {

    /**
     * 构造逻辑任务执行器事件总线协调器
     *
     * 初始化协调器并注册逻辑任务生命周期事件监听器。
     * 协调器启动后，所有逻辑任务的事件都会被自动路由到对应的处理器。
     *
     * @param logicTaskExecutorRepository 逻辑任务执行器仓库，管理所有逻辑任务实例
     * @param logicTaskExecutorLifecycleEventListener 逻辑任务生命周期事件监听器，处理各种事件
     */
    public LogicTaskExecutorEventBusCoordinator(final LogicTaskExecutorRepository logicTaskExecutorRepository,
                                                final LogicTaskExecutorLifecycleEventListener logicTaskExecutorLifecycleEventListener) {
        // 调用父类构造函数，设置协调器名称和任务仓库
        super("LogicTaskExecutorEventBusCoordinator", logicTaskExecutorRepository);
        // 注册逻辑任务生命周期事件监听器
        // 该监听器负责处理所有逻辑任务相关的事件
        registerTaskExecutorLifecycleEventListener(logicTaskExecutorLifecycleEventListener);
    }
}
