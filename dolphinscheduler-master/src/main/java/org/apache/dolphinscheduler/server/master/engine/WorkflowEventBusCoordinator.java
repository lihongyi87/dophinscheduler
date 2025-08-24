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

package org.apache.dolphinscheduler.server.master.engine;

import org.apache.dolphinscheduler.dao.entity.WorkflowInstance;
import org.apache.dolphinscheduler.server.master.engine.workflow.runnable.IWorkflowExecutionRunnable;
import org.apache.dolphinscheduler.server.master.runner.IWorkflowExecuteContext;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

/**
 * 工作流事件总线协调器
 * 
 * 这是DolphinScheduler工作流引擎中的核心协调组件，负责管理工作流事件的分发和处理。
 * 类比：一个高效的邮政分拣中心，负责将不同的工作流事件分配给合适的处理工作者。
 * 
 * 核心职责：
 * 1. 事件总线管理：为每个工作流实例分配专属的事件处理工作者
 * 2. 负载均衡：通过哈希算法将工作流均匀分布到不同的处理工作者
 * 3. 注册管理：处理工作流事件总线的注册和注销操作
 * 4. 生命周期管理：管理事件处理工作者的启动和关闭
 * 
 * 设计理念：
 * - 分治策略：将工作流事件处理任务分配给多个工作者，提高并发处理能力
 * - 哈希分片：使用工作流实例ID进行哈希分片，确保同一工作流的事件由同一工作者处理
 * - 异步处理：事件处理采用异步方式，避免阻塞主线程
 * - 资源隔离：不同工作流的事件处理相互独立，避免相互影响
 * 
 * 性能特点：
 * - 高并发：支持多个工作者并行处理不同工作流的事件
 * - 低延迟：事件可以快速分发到对应的处理工作者
 * - 可扩展：工作者数量可配置，支持根据业务负载调整
 * - 容错性：单个工作者故障不影响其他工作流的处理
 * 
 * 类比理解：
 * 就像一个智能的快递分拣系统：
 * - 收到包裹（工作流事件）
 * - 根据地址（工作流ID）计算分拣区域
 * - 分配给对应的配送员（事件处理工作者）
 * - 配送员专门处理自己区域的包裹（保证事件处理的有序性）
 */
@Slf4j
@Component
public class WorkflowEventBusCoordinator implements AutoCloseable {

    /**
     * 工作流事件总线处理工作者集合
     * 
     * 这是一个工作者池，包含多个事件处理工作者。
     * 每个工作者负责处理分配给它的工作流事件，实现并行处理。
     * 使用@Lazy注解延迟初始化，避免循环依赖问题。
     * 
     * 类比：快递公司的配送团队，每个配送员负责特定区域的包裹配送。
     */
    @Lazy
    @Autowired
    private WorkflowEventBusFireWorkers workflowEventBusFireWorkers;

    /**
     * 启动工作流事件总线协调器
     * 
     * 初始化并启动所有的事件处理工作者。
     * 这是协调器生命周期的开始，所有工作者准备就绪后可以接收和处理工作流事件。
     * 
     * 类比：快递分拣中心开始营业，所有配送员到岗准备开始工作。
     */
    public void start() {
        workflowEventBusFireWorkers.start();
        log.info("WorkflowEventBusCoordinator started");
    }

    /**
     * 注册工作流事件总线
     * 
     * 将工作流执行实例注册到对应的事件处理工作者。
     * 一旦注册成功，该工作流的所有事件都会由指定的工作者自动处理。
     * 
     * 核心流程：
     * 1. 根据工作流实例ID计算应该分配到哪个工作者（哈希分片）
     * 2. 获取对应的事件处理工作者
     * 3. 将工作流事件总线注册到该工作者
     * 
     * 类比：新客户搬到某个区域，快递公司将其地址分配给对应区域的配送员。
     * 
     * @param workflowExecutionRunnable 工作流执行实例，包含工作流的完整上下文信息
     */
    public void registerWorkflowEventBus(IWorkflowExecutionRunnable workflowExecutionRunnable) {
        final int workerSlot = calculateWorkflowEventBusFireWorkerSlot(workflowExecutionRunnable);
        final WorkflowEventBusFireWorker workflowEventBusFireWorker = workflowEventBusFireWorkers.getWorker(workerSlot);
        workflowEventBusFireWorker.registerWorkflowEventBus(workflowExecutionRunnable);
    }

    /**
     * UeRegister a WorkflowExecuteRunnable to the corresponding WorkflowEventBusFireWorker, once the WorkflowExecuteRunnable has been deregistered,
     * then the EventBus will be removed from the WorkflowEventBusFireWorker.
     */
    public void unRegisterWorkflowEventBus(IWorkflowExecutionRunnable workflowExecutionRunnable) {
        final int workerSlot = calculateWorkflowEventBusFireWorkerSlot(workflowExecutionRunnable);
        final WorkflowEventBusFireWorker workflowEventBusFireWorker = workflowEventBusFireWorkers.getWorker(workerSlot);
        workflowEventBusFireWorker.unRegisterWorkflowEventBus(workflowExecutionRunnable);
    }

    /**
     * Calculate the slot of the WorkflowEventBusFireWorker which the WorkflowExecuteRunnable should be registered.
     * <p> The slot is calculated by the workflowInstanceId % workerSize.
     * <p> e.g. If the workflowInstanceId is 1, and the workerSize is 3, then the slot is 1, the workflow will be registered to the worker[1].
     * <p> If the workflowInstanceIds are not consecutive numbers, these will cause some worker busy.
     */
    private int calculateWorkflowEventBusFireWorkerSlot(IWorkflowExecutionRunnable workflowExecutionRunnable) {
        final IWorkflowExecuteContext workflowExecuteContext = workflowExecutionRunnable.getWorkflowExecuteContext();
        final WorkflowInstance workflowInstance = workflowExecuteContext.getWorkflowInstance();
        final Integer workflowInstanceId = workflowInstance.getId();
        return workflowInstanceId % workflowEventBusFireWorkers.getWorkerSize();
    }

    @Override
    public void close() throws Exception {
        workflowEventBusFireWorkers.close();
    }
}
