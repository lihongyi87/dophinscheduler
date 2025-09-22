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

import org.apache.dolphinscheduler.task.executor.events.IReportableTaskExecutorLifecycleEvent;
import org.apache.dolphinscheduler.task.executor.events.TaskExecutorLifecycleEventType;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 任务执行器生命周期事件报告器接口
 *
 * <p>用于向Master节点报告任务执行事件。
 * 当任务执行器的生命周期发生变化时，需要及时将事件报告给Master。
 *
 * <p>主要职责：
 * <ul>
 *   <li>管理事件报告队列</li>
 *   <li>异步发送事件到Master</li>
 *   <li>处理Master的ACK确认</li>
 *   <li>实现重试机制</li>
 * </ul>
 */
public interface ITaskExecutorLifecycleEventReporter extends AutoCloseable {

    /**
     * 启动报告器
     *
     * <p>初始化并启动事件报告服务，开始监听和发送事件。
     */
    void start();

    /**
     * 接收需要报告给Master的任务执行器事件
     *
     * <p>该方法是异步的，仅将事件放入待处理队列，
     * 实际的发送操作将在另一个线程中执行。
     *
     * @param reportableTaskExecutorLifecycleEvent 可报告的任务执行器生命周期事件
     */
    void reportTaskExecutorLifecycleEvent(final IReportableTaskExecutorLifecycleEvent reportableTaskExecutorLifecycleEvent);

    /**
     * 接收任务执行事件的ACK确认
     *
     * <p>当Master成功接收到事件后，会发送ACK确认。
     * 接收到ACK后，报告器会从待处理队列中移除相应事件。
     * 如果未接收到ACK，将会定期重试发送事件。
     *
     * @param taskExecutorLifecycleEventAck 任务执行器生命周期事件ACK
     */
    void receiveTaskExecutorLifecycleEventACK(final TaskExecutorLifecycleEventAck taskExecutorLifecycleEventAck);

    /**
     * 工作流实例主机变更时重置事件
     *
     * <p>当工作流实例的主机发生变更时（如Master故障转移），
     * 重置通道中的事件，使其能够立即被重新报告。
     *
     * @param taskInstanceId 任务实例ID
     */
    void onWorkflowInstanceHostChanged(int taskInstanceId);

    /**
     * 关闭报告器
     *
     * <p>停止报告器的所有服务，释放相关资源。
     * 实现AutoCloseable接口，支持try-with-resources语法。
     */
    @Override
    void close();

    /**
     * 任务执行器生命周期事件ACK
     *
     * <p>用于确认Master已成功接收特定的任务执行器事件。
     * 包含任务执行器ID和事件类型，用于匹配待确认的事件。
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    class TaskExecutorLifecycleEventAck {

        /**
         * 任务执行器ID
         */
        private int taskExecutorId;

        /**
         * 任务执行器生命周期事件类型
         */
        private TaskExecutorLifecycleEventType taskExecutorLifecycleEventType;
    }

}
