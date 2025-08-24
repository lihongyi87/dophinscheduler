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

package org.apache.dolphinscheduler.server.worker.executor;

import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.task.executor.ITaskExecutor;
import org.apache.dolphinscheduler.task.executor.TaskEngine;
import org.apache.dolphinscheduler.task.executor.eventbus.ITaskExecutorLifecycleEventReporter;
import org.apache.dolphinscheduler.task.executor.operations.TaskExecutorReassignMasterRequest;

import java.util.Optional;

import lombok.extern.slf4j.Slf4j;

import org.springframework.stereotype.Component;

@Slf4j
@Component
public class PhysicalTaskEngineDelegator implements AutoCloseable {

    /**
     * 任务引擎 - 负责管理Worker端任务的生命周期和执行
     */
    private final TaskEngine taskEngine;

    /**
     * 物理任务执行器工厂 - 根据任务类型创建对应的执行器
     */
    private final PhysicalTaskExecutorFactory physicalTaskExecutorFactory;

    /**
     * 物理任务执行器生命周期事件上报器 - 向Master上报任务状态
     */
    private final PhysicalTaskExecutorLifecycleEventReporter physicalTaskExecutorEventReporter;

    /**
     * 物理任务执行器仓库 - 存储和查找正在执行的任务
     */
    private final PhysicalTaskExecutorRepository physicalTaskExecutorRepository;

    /**
     * PhysicalTaskEngineDelegator构造函数
     * 初始化Worker端物理任务引擎委托器，负责处理从 Master 分发过来的具体任务
     * 
     * @param physicalTaskEngineFactory 物理任务引擎工厂
     * @param physicalTaskExecutorFactory 物理任务执行器工厂
     * @param physicalTaskExecutorRepository 任务执行器仓库
     * @param physicalTaskExecutorEventReporter 事件上报器
     */
    public PhysicalTaskEngineDelegator(final PhysicalTaskEngineFactory physicalTaskEngineFactory,
                                       final PhysicalTaskExecutorFactory physicalTaskExecutorFactory,
                                       final PhysicalTaskExecutorRepository physicalTaskExecutorRepository,
                                       final PhysicalTaskExecutorLifecycleEventReporter physicalTaskExecutorEventReporter) {
        this.physicalTaskExecutorFactory = physicalTaskExecutorFactory;
        // 创建任务引擎实例，配置线程池大小和任务执行策略
        this.taskEngine = physicalTaskEngineFactory.createTaskEngine();
        this.physicalTaskExecutorRepository = physicalTaskExecutorRepository;
        this.physicalTaskExecutorEventReporter = physicalTaskExecutorEventReporter;
    }

    /**
     * 启动物理任务引擎委托器
     * 启动底层的TaskEngine和事件上报器，开始接收和执行任务
     */
    public void start() {
        // 启动任务引擎 - 初始化线程池和任务调度器
        taskEngine.start();
        // 启动事件上报器 - 开始向Master上报任务状态变更
        physicalTaskExecutorEventReporter.start();
        log.info("PhysicalTaskEngineDelegator started");
    }

    /**
     * 分发逻辑任务到Worker端执行
     * 接收Master分发的任务，在Worker节点上实际执行
     * 如：Shell脚本、SQL查询、Spark作业、Python脚本等
     * 
     * @param taskExecutionContext 任务执行上下文，包含任务参数和环境信息
     */
    public void dispatchLogicTask(final TaskExecutionContext taskExecutionContext) {
        // 使用工厂模式根据任务类型创建对应的执行器
        final ITaskExecutor taskExecutor = physicalTaskExecutorFactory.createTaskExecutor(taskExecutionContext);
        // 将任务提交给任务引擎进行调度执行
        taskEngine.submitTask(taskExecutor);
    }

    /**
     * 杀死物理任务
     * 强制终止指定的任务实例，包括正在运行的进程和子进程
     * 
     * @param taskInstanceId 任务实例ID
     */
    public void killLogicTask(final int taskInstanceId) {
        taskEngine.killTask(taskInstanceId);
    }

    /**
     * 暂停物理任务
     * 暂停指定的任务实例执行，任务可以在后续被恢复
     * 
     * @param taskInstanceId 任务实例ID
     */
    public void pauseLogicTask(final int taskInstanceId) {
        taskEngine.pauseTask(taskInstanceId);
    }

    /**
     * 确认物理任务执行器生命周期事件
     * 处理任务执行过程中的状态反馈和确认消息
     * 
     * @param taskExecutorLifecycleEventAck 任务执行器生命周期事件确认
     */
    public void ackPhysicalTaskExecutorLifecycleEventACK(final ITaskExecutorLifecycleEventReporter.TaskExecutorLifecycleEventAck taskExecutorLifecycleEventAck) {
        physicalTaskExecutorEventReporter.receiveTaskExecutorLifecycleEventACK(taskExecutorLifecycleEventAck);
    }

    /**
     * 重新分配工作流实例主机
     * 当Master节点发生故障转移时，更新任务的目标Master地址
     * 这对于高可用性和故障恢复非常重要
     * 
     * @param taskExecutorReassignMasterRequest 重新分配请求，包含新的Master地址
     * @return true 如果重新分配成功，false 如果任务不存在
     */
    public boolean reassignWorkflowInstanceHost(final TaskExecutorReassignMasterRequest taskExecutorReassignMasterRequest) {
        final int taskInstanceId = taskExecutorReassignMasterRequest.getTaskInstanceId();
        final String workflowHost = taskExecutorReassignMasterRequest.getWorkflowHost();
        // 从任务执行器仓库中查找指定的任务
        final Optional<ITaskExecutor> taskExecutorOptional = physicalTaskExecutorRepository.get(taskInstanceId);
        if (taskExecutorOptional.isPresent()) {
            // 更新任务上下文中的工作流实例主机地址
            taskExecutorOptional.get().getTaskExecutionContext().setWorkflowInstanceHost(workflowHost);
            // 通知事件上报器主机地址发生变更
            physicalTaskExecutorEventReporter.onWorkflowInstanceHostChanged(taskInstanceId);
            return true;
        }
        return false;
    }

    /**
     * 关闭物理任务引擎委托器
     * 优雅关闭任务引擎，等待所有正在执行的任务完成
     * 这对于Worker节点的正常关闭非常重要
     */
    @Override
    public void close() {
        // 关闭任务引擎，停止接收新任务并等待当前任务完成
        taskEngine.close();
    }
}
