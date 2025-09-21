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

import org.apache.dolphinscheduler.server.master.engine.command.CommandEngine;
import org.apache.dolphinscheduler.server.master.engine.executor.LogicTaskEngineDelegator;
import org.apache.dolphinscheduler.server.master.engine.task.dispatcher.WorkerGroupDispatcherCoordinator;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * 工作流引擎
 *
 * 这是DolphinScheduler Master节点的核心组件，负责工作流的整体调度和执行控制。
 * 类比：一个大型工厂的中央控制系统，协调所有生产线的运行。
 *
 * 核心职责：
 * 1. 组件协调：统一管理和协调工作流相关的所有子系统
 * 2. 生命周期管理：控制各个子系统的启动和关闭顺序
 * 3. 资源整合：将分散的功能模块整合成完整的工作流处理系统
 * 4. 统一入口：为外部提供工作流引擎的统一操作接口
 *
 * 子系统组成：
 * - WorkflowEventBusCoordinator：工作流事件总线协调器
 * - CommandEngine：命令引擎，处理工作流启动命令
 * - WorkerGroupDispatcherCoordinator：Worker组调度协调器
 * - LogicTaskEngineDelegator：逻辑任务引擎委托器
 *
 * 启动顺序：
 * 1. 工作流事件总线 → 2. 命令引擎 → 3. 任务分发器 → 4. 逻辑任务引擎
 *
 * 关闭顺序（与启动相反）：
 * 1. 逻辑任务引擎 → 2. 任务分发器 → 3. 事件总线 → 4. 命令引擎
 *
 * 类比理解：
 * 就像一个智能制造工厂的总控制台：
 * - 统一管理所有生产线（子系统）
 * - 按顺序启动各个工段（组件启动）
 * - 协调不同工序之间的配合（事件协调）
 * - 在关闭时确保所有设备安全停机（资源释放）
 */
@Slf4j
@Component
public class WorkflowEngine implements AutoCloseable {

    /**
     * 工作流事件总线协调器
     * 负责工作流生命周期事件的统一管理和分发
     * 类比：工厂的信息发布系统，负责传达各种生产指令和状态更新
     */
    @Autowired
    private WorkflowEventBusCoordinator workflowEventBusCoordinator;

    /**
     * 命令引擎
     * 负责处理工作流启动命令，将命令转换为可执行的工作流实例
     * 类比：工厂的生产计划部门，负责将生产订单转换为具体的生产任务
     */
    @Autowired
    private CommandEngine commandEngine;

    /**
     * Worker组调度协调器
     * 负责将任务按照负载均衡策略分发给合适的Worker节点
     * 类比：工厂的作业调度员，负责将具体任务分配给合适的工作站
     */
    @Autowired
    private WorkerGroupDispatcherCoordinator workerGroupDispatcherCoordinator;

    /**
     * 逻辑任务引擎委托器
     * 负责处理在Master端执行的逻辑任务，如条件判断、子工作流等
     * 类比：工厂的质量控制部门，负责流程控制和决策判断
     */
    @Autowired
    private LogicTaskEngineDelegator logicTaskEngineDelegator;

    /**
     * 工作流引擎启动方法
     *
     * 按照特定顺序启动所有子系统，确保系统间的依赖关系得到正确处理。
     * 启动顺序非常重要，必须确保基础设施先启动，业务逻辑后启动。
     *
     * 启动流程：
     * 1. 事件总线（基础通信） → 2. 命令引擎（任务创建） →
     * 3. 任务分发器（任务执行） → 4. 逻辑任务引擎（Master端任务）
     *
     * 类比：启动一个制造工厂的完整流程
     */
    public void start() {
        // ==========第一阶段：启动基础通信设施==========
        // 启动工作流事件总线协调器，建立事件通信基础设施
        // 必须最先启动，因为后续组件都依赖事件总线进行通信
        // 功能：处理工作流启动、暂停、恢复、完成等生命周期事件
        // 类比：工厂的内部通信系统，所有部门都通过它传递信息
        workflowEventBusCoordinator.start();

        // ==========第二阶段：启动任务创建引擎==========
        // 启动命令引擎，开始处理工作流启动命令
        // 从数据库轮询获取待执行的命令，解析命令参数，创建工作流实例
        // 功能：命令解析、工作流实例创建、DAG图构建
        // 类比：工厂的生产计划部门，将订单转换为生产任务
        commandEngine.start();

        // ==========第三阶段：启动任务分发机制==========
        // 启动Worker组调度协调器，开始管理任务分发
        // 监听待分发的任务，根据负载均衡策略选择合适的Worker节点
        // 功能：任务队列管理、Worker负载监控、任务分发
        // 类比：工厂的作业调度中心，将任务分配给各个工作站
        workerGroupDispatcherCoordinator.start();

        // ==========第四阶段：启动Master端任务处理==========
        // 启动逻辑任务引擎委托器，处理Master端的特殊任务
        // 处理不需要发送到Worker的任务，如条件节点、子工作流、开关节点等
        // 功能：条件判断、依赖检查、子工作流管理、数据传递
        // 类比：工厂的质量控制和流程管理部门，负责决策和控制
        logicTaskEngineDelegator.start();

        // ==========启动完成==========
        // 记录启动成功日志，标志整个工作流引擎已准备就绪
        // 此时系统可以接收和处理工作流调度请求
        log.info("WorkflowEngine started");
    }

    /**
     * 工作流引擎关闭方法
     *
     * 实现AutoCloseable接口，按照与启动相反的顺序优雅关闭各个组件。
     * 关闭顺序确保正在处理的任务能够正常完成，新任务不再被接受。
     *
     * 关闭流程：
     * 1. 逻辑任务引擎（停止Master端任务） → 2. 任务分发器（停止新任务分发） →
     * 3. 事件总线（停止事件处理） → 4. 命令引擎（停止命令处理）
     *
     * @throws Exception 关闭过程中可能抛出的异常
     *
     * 类比：工厂关闭时的安全停机流程
     */
    @Override
    public void close() throws Exception {
        // ==========使用try-with-resources确保资源安全释放==========
        // 这种方式确保即使某个组件关闭异常，其他组件也会被正确关闭
        // 每个组件的close方法会被自动调用，不需要手动调用
        try (
                // ==========第一步：停止命令处理==========
                // 关闭命令引擎，停止接受新的工作流启动命令
                // 正在处理的命令会完成，但不再从数据库拉取新命令
                // 类比：关闭工厂的订单接收部门，不再接受新订单
                final CommandEngine ignore1 = commandEngine;

                // ==========第二步：停止事件处理==========
                // 关闭工作流事件总线协调器，停止事件的分发和处理
                // 正在处理的事件会完成，但不再接受新的事件
                // 类比：关闭工厂的通信系统，停止内部信息传递
                final WorkflowEventBusCoordinator ignore2 = workflowEventBusCoordinator;

                // ==========第三步：停止任务分发==========
                // 关闭Worker组调度协调器，停止向Worker节点分发新任务
                // 已分发的任务继续执行，但不再分发新任务
                // 类比：关闭工厂的调度中心，停止向工作站分配新任务
                final WorkerGroupDispatcherCoordinator ignore3 = workerGroupDispatcherCoordinator;

                // ==========第四步：停止Master端任务处理==========
                // 关闭逻辑任务引擎委托器，停止Master端逻辑任务的处理
                // 正在执行的逻辑任务会完成，但不再处理新的逻辑任务
                // 类比：关闭工厂的质量控制部门，停止新的质量检查工作
                final LogicTaskEngineDelegator ignore5 = logicTaskEngineDelegator) {

            // ==========资源释放说明==========
            // try-with-resources语法会自动调用每个组件的close()方法
            // 调用顺序与声明顺序相反：ignore5 → ignore3 → ignore2 → ignore1
            // 如果某个组件关闭时抛出异常，其他组件仍会被关闭
            // 这确保了即使部分组件异常，整个引擎也能正确关闭
        }
        // ==========关闭完成==========
        // 所有组件都已关闭，工作流引擎停止运行
        // 系统资源已释放，可以安全退出
    }
}
