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

import org.apache.dolphinscheduler.task.executor.TaskEngine;
import org.apache.dolphinscheduler.task.executor.TaskEngineBuilder;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * 物理任务引擎工厂
 *
 * 负责创建和配置Worker节点的物理任务引擎实例。
 * 类比：汽车工厂，按照标准流程组装汽车的各个部件。
 *
 * 主要功能：
 * 1. 集成任务执行器仓库，用于管理正在运行的任务实例
 * 2. 配置任务执行器容器提供者，管理线程池资源
 * 3. 设置事件总线协调器，处理任务生命周期事件
 * 4. 构建完整的物理任务引擎实例
 *
 * 工厂模式的优势：
 * - 封装复杂的对象创建过程
 * - 统一管理依赖注入
 * - 便于后续扩展和配置修改
 */
@Component
public class PhysicalTaskEngineFactory {

    /**
     * 物理任务执行器仓库
     *
     * 用于存储和管理当前Worker节点上所有正在执行的任务实例。
     * 提供任务的注册、查找、移除等功能。
     * 类比：车间的任务看板，记录所有正在进行的工作项目。
     */
    @Autowired
    private PhysicalTaskExecutorRepository physicalTaskExecutorRepository;

    /**
     * 物理任务执行器容器提供者
     *
     * 管理任务执行的线程池容器，控制并发执行的任务数量。
     * 根据Worker配置决定线程池大小和执行策略。
     * 类比：车间的工位管理员，分配工人到不同的工作台。
     */
    @Autowired
    private PhysicalTaskExecutorContainerProvider physicalTaskExecutorContainerDelegator;

    /**
     * 物理任务执行器事件总线协调器
     *
     * 协调任务执行过程中的各种事件处理，如任务启动、完成、失败等。
     * 确保事件的有序处理和状态的正确传递。
     * 类比：车间的广播系统，传递工作状态和重要通知。
     */
    @Autowired
    private PhysicalTaskExecutorEventBusCoordinator physicalTaskExecutorEventBusCoordinator;

    /**
     * 创建物理任务引擎
     *
     * 使用建造者模式组装任务引擎的各个组件，创建完整的任务执行引擎。
     * 该引擎将负责Worker节点上所有物理任务的调度、执行和监控。
     *
     * 类比：按照设计图纸组装一台复杂的生产设备。
     *
     * @return 配置完成的TaskEngine实例，可以直接用于任务执行
     */
    public TaskEngine createTaskEngine() {
        // 使用建造者模式构建任务引擎配置
        final TaskEngineBuilder taskEngineBuilder = TaskEngineBuilder.builder()
                // 设置引擎名称，用于日志记录和监控识别
                .engineName("PhysicalTaskEngine")
                // 配置任务执行器仓库，管理任务生命周期
                .taskExecutorRepository(physicalTaskExecutorRepository)
                // 配置容器提供者，管理执行资源
                .taskExecutorContainerDelegator(physicalTaskExecutorContainerDelegator)
                // 配置事件总线协调器，处理任务事件
                .taskExecutorEventBusCoordinator(physicalTaskExecutorEventBusCoordinator)
                .build();

        // 创建并返回任务引擎实例
        return new TaskEngine(taskEngineBuilder);
    }

}
