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

import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.eventbus.AbstractDelayEventBus;
import org.apache.dolphinscheduler.plugin.task.api.log.TaskLogMarkers;
import org.apache.dolphinscheduler.task.executor.events.AbstractTaskExecutorLifecycleEvent;

import lombok.extern.slf4j.Slf4j;

/**
 * 任务执行器事件总线
 *
 * <p>继承自{@link AbstractDelayEventBus}，专门用于处理任务执行器生命周期事件。
 * 提供了事件发布的日志记录功能，方便跟踪和调试任务执行过程。
 *
 * <p>特性：
 * <ul>
 *   <li>支持延迟事件处理</li>
 *   <li>自动记录事件发布日志</li>
 *   <li>使用美观的JSON格式输出事件内容</li>
 * </ul>
 */
@Slf4j
public class TaskExecutorEventBus extends AbstractDelayEventBus<AbstractTaskExecutorLifecycleEvent> {

    /**
     * 发布任务执行器生命周期事件
     *
     * <p>覆写父类方法，增加了日志记录功能。每次发布事件时，
     * 都会记录事件类型和详细内容，方便问题排查和系统监控。
     *
     * <p>注意：使用TaskLogMarkers.excludeInTaskLog()标记，
     * 避免这些系统日志被包含在任务日志中。
     *
     * @param event 要发布的任务执行器生命周期事件
     */
    public void publish(final AbstractTaskExecutorLifecycleEvent event) {
        super.publish(event);
        log.info(TaskLogMarkers.excludeInTaskLog(), "Publish {}: {}", event.getClass().getSimpleName(),
                JSONUtils.toPrettyJsonString(event));
    }

}
