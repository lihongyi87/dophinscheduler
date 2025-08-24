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

@Slf4j
@Component
public class WorkflowEngine implements AutoCloseable {

    @Autowired
    private WorkflowEventBusCoordinator workflowEventBusCoordinator;

    @Autowired
    private CommandEngine commandEngine;

    @Autowired
    private WorkerGroupDispatcherCoordinator workerGroupDispatcherCoordinator;

    @Autowired
    private LogicTaskEngineDelegator logicTaskEngineDelegator;

    /**
     * 工作流引擎启动方法 - 按顺序启动所有子系统
     * 这是整个调度系统的核心，负责DAG工作流的调度和执行
     */
    public void start() {

        // 第一步：启动工作流事件总线协调器 - 处理工作流相关事件
        // 负责工作流生命周期事件的分发和处理
        workflowEventBusCoordinator.start();

        // 第二步：启动命令引擎 - 处理工作流启动命令
        // 从数据库拉取待执行的工作流命令，解析并转换为工作流实例
        commandEngine.start();

        // 第三步：启动Worker组调度协调器 - 管理任务分发
        // 负责将任务按照负载均衡策略分发给适合的Worker节点
        workerGroupDispatcherCoordinator.start();

        // 第四步：启动逻辑任务引擎委托器 - 处理Master端逻辑任务
        // 处理不需要分发到Worker的任务，如条件判断、子工作流等
        logicTaskEngineDelegator.start();

        // 记录启动成功日志
        log.info("WorkflowEngine started");
    }

    /**
     * 工作流引擎关闭方法 - AutoCloseable接口实现
     * 按照与启动相反的顺序关闭各个组件，确保资源正确释放
     * 
     * @throws Exception 关闭过程中可能抛出的异常
     */
    @Override
    public void close() throws Exception {
        // 使用try-with-resources确保所有组件都被正确关闭
        try (
                // 1. 关闭命令引擎 - 停止新命令的处理
                final CommandEngine ignore1 = commandEngine;
                // 2. 关闭工作流事件总线协调器 - 停止事件处理
                final WorkflowEventBusCoordinator ignore2 = workflowEventBusCoordinator;
                // 3. 关闭Worker组调度协调器 - 停止任务分发
                final WorkerGroupDispatcherCoordinator ignore3 = workerGroupDispatcherCoordinator;
                // 4. 关闭逻辑任务引擎委托器 - 停止逻辑任务处理
                final LogicTaskEngineDelegator ignore5 = logicTaskEngineDelegator) {
            // AutoCloseable会自动调用每个组件的close方法
            // 即使某个组件关闭失败，也会继续关闭其他组件
        }
    }
}
