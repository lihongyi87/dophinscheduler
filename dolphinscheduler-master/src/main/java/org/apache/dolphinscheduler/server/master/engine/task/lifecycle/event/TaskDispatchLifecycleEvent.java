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
 * 任务分发生命周期事件
 *
 * 表示将任务实例分发到目标执行器的生命周期事件。这是任务执行流程中的关键调度事件，
 * 负责将准备好的任务分发到合适的Worker节点进行实际执行。
 *
 * 事件触发时机：
 * - 任务启动事件处理完成后
 * - 任务执行前置条件验证通过
 * - 找到可用的执行器资源时
 * - 任务重试时重新分发
 *
 * 分发策略要素：
 * 1. Worker组选择：根据任务配置的Worker组筛选可用节点
 * 2. 负载均衡：在可用Worker中选择负载较低的节点
 * 3. 资源匹配：确保目标Worker具备执行该任务的资源
 * 4. 地理位置：考虑数据本地性和网络延迟
 * 5. 容错能力：选择稳定性较高的Worker节点
 *
 * 事件处理职责：
 * 1. 构建任务执行上下文和参数
 * 2. 选择最优的目标Worker节点
 * 3. 序列化任务执行请求
 * 4. 发送任务执行请求到目标Worker
 * 5. 设置任务分发超时监控
 *
 * 生命周期位置：
 * START -> DISPATCH -> DISPATCHED -> RUNNING -> (SUCCEEDED/FAILED)
 *          ↑
 *          当前事件位置
 *
 * 类比理解：
 * 就像医院的患者分配事件，当确定患者需要治疗后，医院调度中心
 * 根据医生专长、科室负载、设备可用性等因素，将患者分配给最合适的医生。
 *
 * 设计特点：
 * - 不可变对象：确保事件对象的线程安全性
 * - 工厂方法：提供语义清晰的对象创建方式
 * - 类型安全：明确指定为DISPATCH事件类型
 * - 调试友好：提供清晰的字符串表示
 *
 * @see TaskLifecycleEventType#DISPATCH
 * @see AbstractTaskLifecycleEvent
 * @see ITaskExecutionRunnable
 */
@Getter
@AllArgsConstructor
public class TaskDispatchLifecycleEvent extends AbstractTaskLifecycleEvent {

    /**
     * 关联的任务执行实例
     *
     * 包含待分发任务的完整执行上下文、配置信息和状态数据。
     * 分发处理器将基于这些信息进行Worker选择和任务分发。
     */
    private final ITaskExecutionRunnable taskExecutionRunnable;

    /**
     * 静态工厂方法创建任务分发事件
     *
     * 提供一种简洁且语义明确的方式来创建TaskDispatchLifecycleEvent实例。
     * 封装了对象创建的复杂性，提供更好的API设计。
     *
     * @param taskExecutionRunnable 要分发的任务执行实例
     *                              包含任务的配置、依赖和执行要求等信息
     * @return 新创建的任务分发生命周期事件实例
     *
     * 使用示例：
     * <pre>{@code
     * ITaskExecutionRunnable task = ...;
     * TaskDispatchLifecycleEvent event = TaskDispatchLifecycleEvent.of(task);
     * workflowEventBus.publish(event);
     * }</pre>
     */
    public static TaskDispatchLifecycleEvent of(final ITaskExecutionRunnable taskExecutionRunnable) {
        return new TaskDispatchLifecycleEvent(taskExecutionRunnable);
    }

    /**
     * 获取事件类型
     *
     * 返回此事件对应的生命周期事件类型。
     * 事件分发系统使用此类型来路由事件到相应的处理器。
     *
     * @return TaskLifecycleEventType.DISPATCH 表示这是任务分发事件
     */
    @Override
    public ILifecycleEventType getEventType() {
        return TaskLifecycleEventType.DISPATCH;
    }

    /**
     * 生成事件的字符串表示
     *
     * 提供事件的可读性描述，主要用于日志记录、监控和调试。
     * 包含事件类型和关联的任务名称，便于追踪任务执行流程。
     *
     * @return 格式化的事件描述字符串，例如：
     *         "TaskDispatchLifecycleEvent{task=data_process_task}"
     */
    @Override
    public String toString() {
        return "TaskDispatchLifecycleEvent{" +
                "task=" + taskExecutionRunnable.getName() +
                '}';
    }
}
