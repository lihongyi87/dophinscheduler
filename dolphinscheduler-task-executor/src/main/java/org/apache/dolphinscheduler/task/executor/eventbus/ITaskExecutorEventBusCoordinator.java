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

package org.apache.dolphinscheduler.task.executor.eventbus;

import org.apache.dolphinscheduler.task.executor.listener.ITaskExecutorLifecycleEventListener;

/**
 * 任务执行器事件总线协调器接口
 *
 * <p>负责协调和触发{@link TaskExecutorEventBus}中的事件。
 * 该协调器管理任务执行器生命周期事件的处理流程，并分发给注册的监听器。
 *
 * <p>主要职责：
 * <ul>
 *   <li>启动和停止事件协调服务</li>
 *   <li>注册事件监听器</li>
 *   <li>定期扫描并处理事件队列</li>
 *   <li>将事件分发给合适的监听器</li>
 * </ul>
 */
public interface ITaskExecutorEventBusCoordinator extends AutoCloseable {

    /**
     * 启动协调器
     *
     * <p>初始化并启动事件协调服务，开始监听和处理任务执行器事件。
     * 通常会启动后台线程定期扫描事件总线。
     */
    void start();

    /**
     * 注册任务执行器生命周期事件监听器
     *
     * <p>将监听器注册到协调器中，当任务执行器生命周期事件发生时，
     * 协调器会调用相应的监听器方法。
     *
     * @param taskExecutorLifecycleEventListener 任务执行器生命周期事件监听器
     */
    void registerTaskExecutorLifecycleEventListener(final ITaskExecutorLifecycleEventListener taskExecutorLifecycleEventListener);

    /**
     * 关闭协调器
     *
     * <p>停止协调器的所有服务，释放相关资源。
     * 该方法实现AutoCloseable接口，支持try-with-resources语法。
     */
    @Override
    void close();

}
