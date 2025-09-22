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

package org.apache.dolphinscheduler.extract.master;

import org.apache.dolphinscheduler.extract.base.RpcMethod;
import org.apache.dolphinscheduler.extract.base.RpcService;
import org.apache.dolphinscheduler.task.executor.events.TaskExecutorDispatchedLifecycleEvent;
import org.apache.dolphinscheduler.task.executor.events.TaskExecutorFailedLifecycleEvent;
import org.apache.dolphinscheduler.task.executor.events.TaskExecutorKilledLifecycleEvent;
import org.apache.dolphinscheduler.task.executor.events.TaskExecutorPausedLifecycleEvent;
import org.apache.dolphinscheduler.task.executor.events.TaskExecutorRuntimeContextChangedLifecycleEvent;
import org.apache.dolphinscheduler.task.executor.events.TaskExecutorStartedLifecycleEvent;
import org.apache.dolphinscheduler.task.executor.events.TaskExecutorSuccessLifecycleEvent;

/**
 * 任务执行器事件监听器接口
 *
 * <p>该接口定义了监听任务执行器生命周期事件的方法。</p>
 *
 * <p>主要功能：</p>
 * <ul>
 *   <li>监听任务分派事件</li>
 *   <li>监听任务运行状态变化</li>
 *   <li>监听任务执行结果(成功、失败、被杀死、暂停)</li>
 *   <li>监听任务运行时上下文变化</li>
 * </ul>
 */
@RpcService
public interface ITaskExecutorEventListener {

    /**
     * 任务执行器已分派事件
     * 当任务被分配给执行器时触发
     *
     * @param taskExecutorDispatchedLifecycleEvent 任务分派事件
     */
    @RpcMethod
    void onTaskExecutorDispatched(final TaskExecutorDispatchedLifecycleEvent taskExecutorDispatchedLifecycleEvent);

    /**
     * 任务执行器开始运行事件
     * 当任务开始执行时触发
     *
     * @param taskExecutorStartedLifecycleEvent 任务启动事件
     */
    @RpcMethod
    void onTaskExecutorRunning(final TaskExecutorStartedLifecycleEvent taskExecutorStartedLifecycleEvent);

    /**
     * 任务执行器运行时上下文变化事件
     * 当任务运行时环境变量或参数变化时触发
     *
     * @param taskExecutorRuntimeContextChangedLifecycleEventr 运行时上下文变化事件
     */
    @RpcMethod
    void onTaskExecutorRuntimeContextChanged(final TaskExecutorRuntimeContextChangedLifecycleEvent taskExecutorRuntimeContextChangedLifecycleEventr);

    /**
     * 任务执行器成功事件
     * 当任务成功完成时触发
     *
     * @param taskExecutorSuccessLifecycleEvent 任务成功事件
     */
    @RpcMethod
    void onTaskExecutorSuccess(final TaskExecutorSuccessLifecycleEvent taskExecutorSuccessLifecycleEvent);

    /**
     * 任务执行器失败事件
     * 当任务执行失败时触发
     *
     * @param taskExecutorFailedLifecycleEvent 任务失败事件
     */
    @RpcMethod
    void onTaskExecutorFailed(final TaskExecutorFailedLifecycleEvent taskExecutorFailedLifecycleEvent);

    /**
     * 任务执行器被杀死事件
     * 当任务被强制终止时触发
     *
     * @param taskExecutorKilledLifecycleEvent 任务被杀死事件
     */
    @RpcMethod
    void onTaskExecutorKilled(final TaskExecutorKilledLifecycleEvent taskExecutorKilledLifecycleEvent);

    /**
     * 任务执行器暂停事件
     * 当任务被暂停时触发
     *
     * @param taskExecutorPausedLifecycleEvent 任务暂停事件
     */
    @RpcMethod
    void onTaskExecutorPaused(final TaskExecutorPausedLifecycleEvent taskExecutorPausedLifecycleEvent);

}
