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

package org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event;

import org.apache.dolphinscheduler.server.master.engine.ILifecycleEventType;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.AbstractTaskLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.TaskLifecycleEventType;
import org.apache.dolphinscheduler.server.master.engine.task.runnable.ITaskExecutionRunnable;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

/**
 * 任务已分发生命周期事件
 *
 * 表示任务实例已成功分发到目标执行器的状态事件。
 * 这是任务分发流程的确认事件，标志着Worker节点已收到任务执行请求。
 *
 * 事件触发时机：
 * - Worker节点收到任务执行请求后
 * - 任务分发成功并获得Worker确认时
 * - 任务在Worker上准备就绪，即将开始执行时
 *
 * 事件信息：
 * 1. 任务执行实例：包含任务的完整上下文
 * 2. 执行器主机：记录任务被分发到的具体Worker节点
 *
 * 生命周期位置：
 * START -> DISPATCH -> DISPATCHED -> RUNNING -> (SUCCEEDED/FAILED)
 *                     ↑
 *                     当前事件位置
 *
 * @see TaskLifecycleEventType#DISPATCHED
 * @see AbstractTaskLifecycleEvent
 * @see ITaskExecutionRunnable
 */
@Getter
@Builder
@AllArgsConstructor
public class TaskDispatchedLifecycleEvent extends AbstractTaskLifecycleEvent {

    /**
     * 关联的任务执行实例
     */
    private ITaskExecutionRunnable taskExecutionRunnable;

    /**
     * 执行器主机地址
     *
     * 记录任务被分发到的Worker节点主机地址。
     * 用于任务追踪、监控和故障排查。
     */
    private String executorHost;

    /**
     * 获取事件类型
     */
    @Override
    public ILifecycleEventType getEventType() {
        return TaskLifecycleEventType.DISPATCHED;
    }

    /**
     * 生成事件的字符串表示
     */
    @Override
    public String toString() {
        return "TaskDispatchedLifecycleEvent{" +
                "task=" + taskExecutionRunnable.getName() +
                ", executorHost='" + executorHost + '\'' +
                '}';
    }
}
