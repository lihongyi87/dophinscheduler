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
        // ========== 第一步：计算工作者槽位 ==========
        // 根据工作流ID计算该工作流应该分配到哪个工作者
        // 使用哈希算法确保同一工作流始终分配到同一工作者
        final int workerSlot = calculateWorkflowEventBusFireWorkerSlot(workflowExecutionRunnable);

        // ========== 第二步：获取目标工作者 ==========
        // 从工作者池中获取指定槽位的事件处理工作者
        // 每个工作者负责处理分配给它的工作流事件
        final WorkflowEventBusFireWorker workflowEventBusFireWorker = workflowEventBusFireWorkers.getWorker(workerSlot);

        // ========== 第三步：注册工作流到工作者 ==========
        // 将工作流的事件总线注册到对应的工作者
        // 注册后，该工作流产生的所有事件都会路由到这个工作者处理
        workflowEventBusFireWorker.registerWorkflowEventBus(workflowExecutionRunnable);
    }

    /**
     * 注销工作流事件总线
     *
     * 从对应的事件处理工作者中移除工作流执行实例。
     * 注销后，该工作流的事件将不再被处理，相关资源会被释放。
     *
     * 使用场景：
     * - 工作流执行完成后的清理
     * - 工作流被强制终止时的资源释放
     * - 系统关闭时的优雅清理
     *
     * 类比：客户搬离某个区域，快递公司将其从配送员的服务列表中移除。
     *
     * @param workflowExecutionRunnable 需要注销的工作流执行实例
     */
    public void unRegisterWorkflowEventBus(IWorkflowExecutionRunnable workflowExecutionRunnable) {
        // ========== 第一步：计算工作者槽位 ==========
        // 使用相同的哈希算法确定工作流当前注册在哪个工作者
        final int workerSlot = calculateWorkflowEventBusFireWorkerSlot(workflowExecutionRunnable);

        // ========== 第二步：获取目标工作者 ==========
        // 获取当前持有该工作流的工作者
        final WorkflowEventBusFireWorker workflowEventBusFireWorker = workflowEventBusFireWorkers.getWorker(workerSlot);

        // ========== 第三步：从工作者中注销工作流 ==========
        // 从工作者的管理列表中移除该工作流
        // 释放相关资源，停止事件处理
        workflowEventBusFireWorker.unRegisterWorkflowEventBus(workflowExecutionRunnable);
    }

    /**
     * 计算工作流事件处理工作者的槽位
     *
     * 通过哈希算法将工作流分配到特定的工作者，确保负载均衡和处理有序性。
     *
     * 算法原理：
     * - 使用工作流实例ID对工作者总数取模
     * - 保证同一工作流始终分配到同一工作者（会话亲和性）
     * - 理论上实现均匀分布（假设ID是均匀分布的）
     *
     * 示例：
     * - 工作流ID=1, 工作者数=3, 槽位=1%3=1, 分配到worker[1]
     * - 工作流ID=4, 工作者数=3, 槽位=4%3=1, 分配到worker[1]
     * - 工作流ID=5, 工作者数=3, 槽位=5%3=2, 分配到worker[2]
     *
     * 注意事项：
     * - 如果工作流ID不是连续的，可能导致某些工作者负载较重
     * - 工作者数量固定后不宜频繁变更，否则会导致重新分配
     *
     * @param workflowExecutionRunnable 工作流执行实例
     * @return 工作者槽位索引（0到workerSize-1）
     */
    private int calculateWorkflowEventBusFireWorkerSlot(IWorkflowExecutionRunnable workflowExecutionRunnable) {
        // ========== 第一步：获取工作流上下文 ==========
        // 从执行实例中提取工作流执行上下文
        final IWorkflowExecuteContext workflowExecuteContext = workflowExecutionRunnable.getWorkflowExecuteContext();

        // ========== 第二步：获取工作流实例 ==========
        // 从上下文中获取工作流实例对象
        final WorkflowInstance workflowInstance = workflowExecuteContext.getWorkflowInstance();

        // ========== 第三步：提取工作流ID ==========
        // 获取工作流实例的唯一标识符
        final Integer workflowInstanceId = workflowInstance.getId();

        // ========== 第四步：计算槽位 ==========
        // 使用取模运算将工作流ID映射到工作者槽位
        // 这确保了相同ID的工作流总是分配到同一个工作者
        return workflowInstanceId % workflowEventBusFireWorkers.getWorkerSize();
    }

    /**
     * 关闭工作流事件总线协调器
     *
     * 实现AutoCloseable接口，确保资源的正确释放。
     * 关闭所有事件处理工作者，停止接收和处理新的工作流事件。
     *
     * 关闭流程：
     * 1. 停止接收新的事件注册请求
     * 2. 等待正在处理的事件完成
     * 3. 关闭所有工作者线程
     * 4. 释放相关资源
     *
     * @throws Exception 关闭过程中可能抛出的异常
     *
     * 类比：快递分拣中心下班时关闭所有操作台，等配送员完成手头工作。
     */
    @Override
    public void close() throws Exception {
        // ========== 关闭所有事件处理工作者 ==========
        // 这会触发工作者的优雅关闭流程：
        // 1. 停止接收新的工作流注册
        // 2. 完成正在处理的事件
        // 3. 关闭事件处理线程
        // 4. 清理相关资源
        workflowEventBusFireWorkers.close();
        log.info("WorkflowEventBusCoordinator closed");
    }
}
