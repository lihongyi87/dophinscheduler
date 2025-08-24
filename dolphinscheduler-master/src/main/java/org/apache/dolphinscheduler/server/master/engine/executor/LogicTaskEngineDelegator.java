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

package org.apache.dolphinscheduler.server.master.engine.executor;

import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.task.executor.ITaskExecutor;
import org.apache.dolphinscheduler.task.executor.TaskEngine;
import org.apache.dolphinscheduler.task.executor.eventbus.ITaskExecutorLifecycleEventReporter;

import lombok.extern.slf4j.Slf4j;

import org.springframework.stereotype.Component;

@Slf4j
@Component
public class LogicTaskEngineDelegator implements AutoCloseable {

    /**
     * 任务引擎 - 负责管理任务的生命周期和执行
     */
    private final TaskEngine taskEngine;

    /**
     * 逻辑任务执行器工厂 - 根据任务类型创建对应的执行器
     */
    private final LogicTaskExecutorFactory logicTaskExecutorFactory;

    /**
     * 逻辑任务执行器生命周期事件上报器 - 向Master上报任务状态变更
     */
    private final LogicTaskExecutorLifecycleEventReporter logicTaskExecutorEventReporter;

    /**
     * LogicTaskEngineDelegator构造函数
     * 初始化Master端逻辑任务引擎委托器，负责处理不需要分发到Worker的任务
     * 
     * @param logicTaskEngineFactory 逻辑任务引擎工厂
     * @param logicTaskExecutorFactory 逻辑任务执行器工厂
     * @param logicTaskExecutorEventReporter 事件上报器
     */
    public LogicTaskEngineDelegator(final LogicTaskEngineFactory logicTaskEngineFactory,
                                    final LogicTaskExecutorFactory logicTaskExecutorFactory,
                                    final LogicTaskExecutorLifecycleEventReporter logicTaskExecutorEventReporter) {
        this.logicTaskExecutorFactory = logicTaskExecutorFactory;
        // 创建任务引擎实例，配置线程池和执行策略
        this.taskEngine = logicTaskEngineFactory.createTaskEngine();
        this.logicTaskExecutorEventReporter = logicTaskExecutorEventReporter;
    }

    /**
     * 启动逻辑任务引擎委托器
     * 启动底层的TaskEngine和事件上报器
     */
    public void start() {
        // 启动任务引擎 - 初始化线程池和任务调度器
        taskEngine.start();
        // 启动事件上报器 - 开始监听任务状态变更事件
        logicTaskExecutorEventReporter.start();
        log.info("LogicTaskEngineDelegator started");
    }

    /**
     * 分发逻辑任务 - 在Master端执行的任务
     * 这些任务不需要分发到Worker节点，而是在Master端直接执行
     * 如：条件判断任务、子工作流任务、Switch任务等
     * 
     * @param taskExecutionContext 任务执行上下文，包含任务的所有信息
     */
    public void dispatchLogicTask(final TaskExecutionContext taskExecutionContext) {
        // 使用工厂模式根据任务类型创建对应的执行器
        final ITaskExecutor taskExecutor = logicTaskExecutorFactory.createTaskExecutor(taskExecutionContext);
        // 将任务提交给任务引擎进行调度执行
        taskEngine.submitTask(taskExecutor);
    }

    /**
     * 杀死逻辑任务
     * 强制终止指定的任务实例，通常用于用户手动取消或系统异常处理
     * 
     * @param taskInstanceId 任务实例ID
     */
    public void killLogicTask(final int taskInstanceId) {
        taskEngine.killTask(taskInstanceId);
    }

    /**
     * 暂停逻辑任务
     * 暂停指定的任务实例，任务可以在后续被恢复执行
     * 
     * @param taskInstanceId 任务实例ID
     */
    public void pauseLogicTask(final int taskInstanceId) {
        taskEngine.pauseTask(taskInstanceId);
    }

    /**
     * 确认逻辑任务执行事件
     * 处理任务执行过程中的状态反馈和确认消息
     * 
     * @param taskExecutorLifecycleEventAck 任务执行器生命周期事件确认
     */
    public void ackLogicTaskExecutionEvent(final ITaskExecutorLifecycleEventReporter.TaskExecutorLifecycleEventAck taskExecutorLifecycleEventAck) {
        logicTaskExecutorEventReporter.receiveTaskExecutorLifecycleEventACK(taskExecutorLifecycleEventAck);
    }

    /**
     * 关闭逻辑任务引擎委托器
     * 优雅关闭所有组件，确保正在执行的任务能够正常完成
     */
    @Override
    public void close() {
        // 使用try-with-resources确保所有资源都被正确释放
        try (
                // 先关闭任务引擎，停止接收新任务并等待当前任务完成
                final TaskEngine ignore1 = taskEngine;
                // 再关闭事件上报器，停止上报任务状态变更
                final LogicTaskExecutorLifecycleEventReporter ignore2 = logicTaskExecutorEventReporter) {
            log.info("LogicTaskEngineDelegator closed");
        }
    }
}
