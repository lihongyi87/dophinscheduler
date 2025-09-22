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

import org.apache.dolphinscheduler.server.worker.config.WorkerConfig;
import org.apache.dolphinscheduler.task.executor.container.ExclusiveThreadTaskExecutorContainer;
import org.apache.dolphinscheduler.task.executor.container.ITaskExecutorContainer;
import org.apache.dolphinscheduler.task.executor.container.ITaskExecutorContainerProvider;
import org.apache.dolphinscheduler.task.executor.container.TaskExecutorContainerConfig;

import org.springframework.stereotype.Component;

/**
 * 物理任务执行器容器提供者
 *
 * 负责创建和管理Worker节点的任务执行器容器，控制任务的并发执行。
 * 类比：工厂的车间管理员，负责分配工位和管理工人的工作安排。
 *
 * 主要功能：
 * 1. 根据Worker配置创建任务执行器容器
 * 2. 管理任务执行的线程池资源
 * 3. 控制同时执行的任务数量上限
 * 4. 提供任务执行的资源隔离
 *
 * 容器特点：
 * - 使用独占线程模式，每个任务独享一个线程
 * - 线程池大小可配置，避免资源耗尽
 * - 支持任务的优雅关闭和资源清理
 */
@Component
public class PhysicalTaskExecutorContainerProvider implements ITaskExecutorContainerProvider {

    /**
     * 任务执行器容器实例
     *
     * 管理Worker节点上所有物理任务的执行线程，提供线程池和资源管理功能。
     * 一旦创建后，在Worker节点生命周期内保持不变。
     * 类比：车间的工作台集合，每个工作台可以安排一个工人进行操作。
     */
    private final ITaskExecutorContainer taskExecutorContainer;

    /**
     * 构造函数
     *
     * 根据Worker配置初始化任务执行器容器，设置合适的线程池大小和执行策略。
     * 容器将在Worker节点启动时创建，在节点关闭时销毁。
     *
     * @param workerConfig Worker节点配置，包含任务执行相关的各项参数
     */
    public PhysicalTaskExecutorContainerProvider(final WorkerConfig workerConfig) {
        // 构建容器配置，设置容器名称和线程池大小
        final TaskExecutorContainerConfig containerConfig = TaskExecutorContainerConfig.builder()
                // 设置容器名称，用于监控和日志识别
                .containerName("exclusive-task-executor-container")
                // 从Worker配置中获取任务执行器线程池大小
                // 这个值决定了Worker节点能同时执行多少个物理任务
                .taskExecutorThreadPoolSize(workerConfig.getPhysicalTaskConfig().getTaskExecutorThreadSize())
                .build();

        // 创建独占线程任务执行器容器
        // 每个任务都会独占一个线程，确保任务之间的资源隔离
        this.taskExecutorContainer = new ExclusiveThreadTaskExecutorContainer(containerConfig);
    }

    /**
     * 获取任务执行器容器
     *
     * 返回当前Worker节点的任务执行器容器实例。
     * 这个容器将被任务引擎用于管理和调度物理任务的执行。
     *
     * @return 任务执行器容器实例，用于任务的调度和执行
     */
    @Override
    public ITaskExecutorContainer getExecutorContainer() {
        // 返回当前Worker节点的任务执行器容器实例
        // 这个容器将被任务引擎用于管理和调度物理任务的执行
        return taskExecutorContainer;
    }
}
