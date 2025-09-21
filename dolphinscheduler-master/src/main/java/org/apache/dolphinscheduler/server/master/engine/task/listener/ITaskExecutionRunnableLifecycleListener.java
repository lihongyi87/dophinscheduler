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

package org.apache.dolphinscheduler.server.master.engine.task.listener;

import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event.TaskDispatchedLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event.TaskFailedLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event.TaskKilledLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event.TaskPausedLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event.TaskRunningLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event.TaskSuccessLifecycleEvent;

/**
 * 任务执行器生命周期监听器接口
 * <p>
 * 此接口用于监听任务执行器的生命周期事件，基于观察者模式实现事件驱动架构。
 * 通过监听任务状态变化，可以实现告警触发、统计数据收集、审计日志记录等功能。
 * </p>
 *
 * <p>
 * 监听器模式的优势：
 * 1. 解耦：任务执行逻辑与状态处理逻辑分离
 * 2. 扩展性：可灵活添加多个监听器实现不同功能
 * 3. 实时性：状态变化时立即触发相应的处理逻辑
 * </p>
 *
 * @author DolphinScheduler Team
 * @since 3.0.0
 */
public interface ITaskExecutionRunnableLifecycleListener {

    /**
     * 任务已调度事件处理器
     * <p>
     * 当任务成功分发到Worker节点时触发此方法。
     * 此时任务已经离开Master节点，正在等待Worker节点执行。
     * </p>
     *
     * @param taskDispatchedEvent 任务调度事件对象，包含任务调度相关信息
     */
    void onDispatched(TaskDispatchedLifecycleEvent taskDispatchedEvent);

    /**
     * 任务运行中事件处理器
     * <p>
     * 当任务在Worker节点开始执行时触发此方法。
     * 标志着任务从等待状态转为运行状态。
     * </p>
     *
     * @param event 任务运行事件对象，包含任务运行时的相关信息
     */
    void onRunning(TaskRunningLifecycleEvent event);

    /**
     * 任务暂停事件处理器
     * <p>
     * 当任务被用户主动暂停或系统暂停时触发此方法。
     * 暂停的任务可以被恢复执行。
     * </p>
     *
     * @param event 任务暂停事件对象，包含暂停原因等信息
     */
    void onPaused(TaskPausedLifecycleEvent event);

    /**
     * 任务失败事件处理器
     * <p>
     * 当任务执行失败时触发此方法。
     * 可在此处理失败告警、记录失败原因、触发重试逻辑等。
     * </p>
     *
     * @param event 任务失败事件对象，包含失败原因、错误信息等
     */
    void onFailed(TaskFailedLifecycleEvent event);

    /**
     * 任务被杀死事件处理器
     * <p>
     * 当任务被用户手动杀死或系统强制终止时触发此方法。
     * 被杀死的任务不能恢复，需要重新启动。
     * </p>
     *
     * @param event 任务杀死事件对象，包含杀死原因等信息
     */
    void onKilled(TaskKilledLifecycleEvent event);

    /**
     * 任务成功事件处理器
     * <p>
     * 当任务成功完成时触发此方法。
     * 可在此处理成功通知、更新统计信息、触发下游任务等。
     * </p>
     *
     * @param event 任务成功事件对象，包含执行结果等信息
     */
    void onSuccess(TaskSuccessLifecycleEvent event);

}
