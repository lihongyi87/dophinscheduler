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

package org.apache.dolphinscheduler.server.master.engine.task.dispatcher;

import org.apache.dolphinscheduler.server.master.engine.task.client.ITaskExecutorClient;
import org.apache.dolphinscheduler.server.master.engine.task.runnable.ITaskExecutionRunnable;

import java.util.concurrent.ConcurrentHashMap;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * 工作组分发器协调者
 * 
 * 这个类负责管理和协调不同工作组的任务分发。它为每个工作组维护一个独立的
 * 任务分发器和优先级延迟队列，确保任务能够正确地分发到对应的工作组。
 * 
 * 主要功能：
 * 1. 管理多个工作组的任务分发器映射关系
 * 2. 支持任务的添加、移除和分发操作
 * 3. 动态创建和启动工作组分发器
 * 4. 支持工作组的启动和停止管理
 * 5. 提供资源清理和关闭功能
 * 
 * 工作原理：
 * - 为每个工作组维护一个WorkerGroupDispatcher实例
 * - 使用ConcurrentHashMap确保线程安全
 * - 支持延迟任务分发机制
 * - 自动管理分发器的生命周期
 * 
 * 简单理解：就像一个"任务分发中心"，根据不同的工作组
 * 把任务分配给对应的专门分发器来处理。
 */
@Component
@Slf4j
public class WorkerGroupDispatcherCoordinator implements AutoCloseable {

    /**
     * 任务执行器客户端，用于与TaskExecutor通信
     */
    @Autowired
    private ITaskExecutorClient taskExecutorClient;

    /**
     * 工作组分发器映射表
     * Key: 工作组名称, Value: 对应的工作组分发器
     */
    private final ConcurrentHashMap<String, WorkerGroupDispatcher> workerGroupDispatcherMap;

    public WorkerGroupDispatcherCoordinator() {
        workerGroupDispatcherMap = new ConcurrentHashMap<>();
    }

    /**
     * 启动工作组分发器协调者
     * 
     * 初始化协调者，准备接受任务分发请求。
     */
    public void start() {
        log.info("WorkerGroupTaskDispatcherManager started...");
    }

    /**
     * 分发任务到指定工作组
     * 
     * 将任务分发到对应的工作组分发器，支持延迟分发机制。
     * 如果目标工作组的分发器不存在，会自动创建并启动。
     * 
     * @param taskExecutionRunnable 要分发的任务执行对象
     * @param delayTimeMills 延迟时间（毫秒）
     */
    public void dispatchTask(final ITaskExecutionRunnable taskExecutionRunnable,
                             final long delayTimeMills) {
        final String workerGroup = taskExecutionRunnable.getTaskInstance().getWorkerGroup();
        // 获取或创建工作组分发器，然后分发任务
        getOrCreateWorkerGroupDispatcher(workerGroup).dispatchTask(taskExecutionRunnable, delayTimeMills);
        log.info("Success add Task[id={}] to WorkerGroupDispatcher[name={}]", taskExecutionRunnable.getId(),
                workerGroup);
    }

    /**
     * 从分发器中移除任务
     * 
     * 尝试从对应工作组的分发器中移除任务。如果任务不存在于分发器中，
     * 返回false，这通常意味着任务可能已经被分发了。
     * 
     * @param taskExecutionRunnable 要移除的任务执行对象
     * @return true-成功移除, false-任务不存在（可能已被分发）
     */
    public boolean removeTask(ITaskExecutionRunnable taskExecutionRunnable) {
        final String workerGroup = taskExecutionRunnable.getTaskInstance().getWorkerGroup();
        boolean removed = getOrCreateWorkerGroupDispatcher(workerGroup).removeTask(taskExecutionRunnable);
        if (removed) {
            log.info("Success removed Task[id={}] from WorkerGroupDispatcher[name={}]",
                    taskExecutionRunnable.getId(), workerGroup);
        } else {
            log.info("Failed to remove Task[id={}] from WorkerGroupDispatcher[name={}], this task has been dispatched",
                    taskExecutionRunnable.getId(), workerGroup);
        }
        return removed;
    }

    /**
     * 检查指定工作组是否存在
     * 
     * @param workerGroup 工作组名称
     * @return true-存在, false-不存在
     */
    public boolean existWorkerGroup(String workerGroup) {
        return workerGroupDispatcherMap.containsKey(workerGroup);
    }

    /**
     * 停止所有工作组分发器并清理资源
     * 
     * 关闭所有工作组的任务分发器，释放相关资源。
     * 这个方法会被Spring容器在销毁Bean时自动调用。
     */
    @Override
    public void close() throws Exception {
        log.info("WorkerGroupDispatcherCoordinator closing");
        // 逐个关闭所有工作组分发器
        for (WorkerGroupDispatcher workerGroupDispatcher : workerGroupDispatcherMap.values()) {
            try {
                workerGroupDispatcher.close();
            } catch (Exception e) {
                log.error("close WorkerGroupDispatcher[name={}] error", workerGroupDispatcher.getName(), e);
            }
        }
        log.info("WorkerGroupDispatcherCoordinator closed...");
    }

    /**
     * 获取或创建工作组分发器
     * 
     * 如果指定工作组的分发器已存在，直接返回；
     * 如果不存在，创建新的分发器并启动，然后返回。
     * 
     * @param workerGroup 工作组名称
     * @return 对应的工作组分发器
     */
    private WorkerGroupDispatcher getOrCreateWorkerGroupDispatcher(String workerGroup) {
        return workerGroupDispatcherMap.computeIfAbsent(workerGroup, wg -> {
            // 创建新的工作组分发器
            WorkerGroupDispatcher workerGroupDispatcher = new WorkerGroupDispatcher(wg, taskExecutorClient);
            // 启动分发器
            workerGroupDispatcher.start();
            return workerGroupDispatcher;
        });
    }
}
