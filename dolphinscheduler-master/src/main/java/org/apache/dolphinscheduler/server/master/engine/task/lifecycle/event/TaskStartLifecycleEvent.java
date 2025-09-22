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
import lombok.Getter;

/**
 * 任务启动生命周期事件
 *
 * 表示任务实例开始执行的生命周期事件。这是任务执行流程的第一个关键事件，
 * 标志着任务从等待状态转入执行准备状态。
 *
 * 事件触发时机：
 * - 工作流执行到该任务节点时
 * - 任务的前置依赖条件全部满足时
 * - 用户手动触发任务执行时
 * - 任务重试时重新启动
 *
 * 事件处理职责：
 * 1. 初始化任务执行上下文
 * 2. 验证任务执行前置条件
 * 3. 准备任务执行所需资源
 * 4. 设置任务初始状态
 * 5. 触发后续的调度分发流程
 *
 * 生命周期位置：
 * START -> DISPATCH -> DISPATCHED -> RUNNING -> (SUCCEEDED/FAILED)
 * ↑
 * 当前事件位置
 *
 * 类比理解：
 * 就像医院的患者开始治疗事件，当患者完成挂号、分诊等准备工作后，
 * 正式开始接受医生的治疗，这个时点就是"治疗启动"事件。
 *
 * 设计特点：
 * - 不可变对象：使用@AllArgsConstructor和@Getter确保对象不可变
 * - 工厂方法：提供of()静态工厂方法简化对象创建
 * - 类型安全：明确指定事件类型为TaskLifecycleEventType.START
 * - 友好输出：重写toString()方法提供清晰的事件描述
 *
 * @see TaskLifecycleEventType#START
 * @see AbstractTaskLifecycleEvent
 * @see ITaskExecutionRunnable
 */
@Getter
@AllArgsConstructor
public class TaskStartLifecycleEvent extends AbstractTaskLifecycleEvent {

    /**
     * 关联的任务执行实例
     *
     * 包含任务的执行上下文、状态信息和操作接口。
     * 通过这个实例可以获取任务的详细信息和执行控制能力。
     */
    private final ITaskExecutionRunnable taskExecutionRunnable;

    /**
     * 静态工厂方法创建任务启动事件
     *
     * 提供一种简洁的方式来创建TaskStartLifecycleEvent实例。
     * 相比直接使用构造函数，工厂方法具有更好的可读性和灵活性。
     *
     * @param taskExecutionRunnable 要启动的任务执行实例
     *                              包含任务的完整执行上下文和控制接口
     * @return 新创建的任务启动生命周期事件实例
     *
     * 使用示例：
     * <pre>{@code
     * ITaskExecutionRunnable task = ...;
     * TaskStartLifecycleEvent event = TaskStartLifecycleEvent.of(task);
     * eventBus.publish(event);
     * }</pre>
     */
    public static TaskStartLifecycleEvent of(ITaskExecutionRunnable taskExecutionRunnable) {
        return new TaskStartLifecycleEvent(taskExecutionRunnable);
    }

    /**
     * 获取事件类型
     *
     * 返回此事件对应的生命周期事件类型。
     * 用于事件分发器识别事件类型并选择相应的处理器。
     *
     * @return TaskLifecycleEventType.START 表示这是任务启动事件
     */
    @Override
    public ILifecycleEventType getEventType() {
        return TaskLifecycleEventType.START;
    }

    /**
     * 生成事件的字符串表示
     *
     * 提供事件的可读性描述，主要用于日志记录和调试。
     * 包含事件类型和关联的任务名称信息。
     *
     * @return 格式化的事件描述字符串，例如：
     *         "TaskStartLifecycleEvent{task=my_task_name}"
     */
    @Override
    public String toString() {
        return "TaskStartLifecycleEvent{" +
                "task=" + taskExecutionRunnable.getName() +
                '}';
    }
}
