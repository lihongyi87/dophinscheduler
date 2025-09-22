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

package org.apache.dolphinscheduler.server.master.engine.executor.plugin.condition;

import org.apache.dolphinscheduler.dao.repository.TaskInstanceDao;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.task.ConditionsLogicTaskChannelFactory;
import org.apache.dolphinscheduler.server.master.engine.IWorkflowRepository;
import org.apache.dolphinscheduler.server.master.engine.executor.plugin.ILogicTaskPluginFactory;
import org.apache.dolphinscheduler.server.master.engine.workflow.runnable.IWorkflowExecutionRunnable;
import org.apache.dolphinscheduler.task.executor.ITaskExecutor;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * 条件逻辑任务插件工厂
 * 负责创建条件逻辑任务实例，实现ILogicTaskPluginFactory接口
 *
 * 工厂功能：
 * 1. 创建ConditionLogicTask实例
 * 2. 注入必要的依赖组件（任务实例DAO、工作流仓库等）
 * 3. 提供任务类型标识符用于工厂注册
 *
 * 作为Spring组件，该工厂会被自动注册到LogicTaskPluginFactoryBuilder中，
 * 当需要执行条件任务时，系统会通过任务类型找到该工厂并创建对应的任务实例。
 *
 * @author DolphinScheduler Team
 */
@Slf4j
@Component
public class ConditionLogicTaskPluginFactory implements ILogicTaskPluginFactory<ConditionLogicTask> {

    /**
     * 任务实例数据访问对象
     * 用于查询工作流中任务实例的状态信息
     */
    @Autowired
    private TaskInstanceDao taskInstanceDao;

    /**
     * 工作流执行运行时仓库
     * 用于获取当前工作流的执行上下文和状态信息
     */
    @Autowired
    private IWorkflowRepository workflowExecutionRunnableMemoryRepository;

    /**
     * 创建条件逻辑任务实例
     * 根据任务执行器提供的上下文信息，创建配置好的条件任务实例
     *
     * @param taskExecutor 任务执行器，包含任务执行上下文
     * @return 配置完成的条件逻辑任务实例
     */
    @Override
    public ConditionLogicTask createLogicTask(final ITaskExecutor taskExecutor) {
        // 从任务执行器中获取任务执行上下文
        final TaskExecutionContext taskExecutionContext = taskExecutor.getTaskExecutionContext();

        // 从工作流仓库中获取当前工作流的执行运行时对象
        final IWorkflowExecutionRunnable workflowExecutionRunnable =
                workflowExecutionRunnableMemoryRepository.get(taskExecutionContext.getWorkflowInstanceId());

        log.debug("Creating ConditionLogicTask for workflow instance: {}, task instance: {}",
                taskExecutionContext.getWorkflowInstanceId(), taskExecutionContext.getTaskInstanceId());

        // 创建并返回条件逻辑任务实例
        return new ConditionLogicTask(workflowExecutionRunnable, taskExecutionContext, taskInstanceDao);
    }

    /**
     * 获取支持的任务类型
     * 返回条件任务的类型标识符，用于任务类型路由
     *
     * @return 条件任务的类型标识符
     */
    @Override
    public String getTaskType() {
        return ConditionsLogicTaskChannelFactory.NAME;
    }
}
