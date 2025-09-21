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

package org.apache.dolphinscheduler.server.master.engine.executor.plugin.dependent;

import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.dao.repository.ProjectDao;
import org.apache.dolphinscheduler.dao.repository.TaskDefinitionDao;
import org.apache.dolphinscheduler.dao.repository.TaskInstanceContextDao;
import org.apache.dolphinscheduler.dao.repository.TaskInstanceDao;
import org.apache.dolphinscheduler.dao.repository.WorkflowDefinitionDao;
import org.apache.dolphinscheduler.dao.repository.WorkflowInstanceDao;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.enums.TaskExecutionStatus;
import org.apache.dolphinscheduler.plugin.task.api.parameters.DependentParameters;
import org.apache.dolphinscheduler.server.master.engine.executor.plugin.AbstractLogicTask;
import org.apache.dolphinscheduler.server.master.engine.executor.plugin.ITaskParameterDeserializer;
import org.apache.dolphinscheduler.server.master.engine.workflow.runnable.IWorkflowExecutionRunnable;
import org.apache.dolphinscheduler.server.master.exception.MasterTaskExecuteException;

import lombok.extern.slf4j.Slf4j;

import com.fasterxml.jackson.core.type.TypeReference;

/**
 * 依赖逻辑任务实现
 * 用于检查其他工作流实例或任务实例的执行状态，实现跨工作流的依赖控制
 *
 * 依赖任务的核心功能：
 * 1. 监控指定的外部工作流实例或任务实例的状态
 * 2. 支持跨项目、跨工作流的依赖关系检查
 * 3. 根据依赖的执行状态决定当前任务的执行结果
 * 4. 支持复杂的依赖条件组合（AND/OR逻辑）
 *
 * 使用场景：
 * - 等待其他工作流完成后再执行当前工作流
 * - 检查历史执行实例的状态作为执行条件
 * - 实现工作流间的顺序依赖和数据依赖
 * - 支持定时调度中的依赖等待机制
 *
 * 注意：依赖任务会持续运行直到依赖条件满足或超时
 *
 * @author DolphinScheduler Team
 */
@Slf4j
public class DependentLogicTask extends AbstractLogicTask<DependentParameters> {

    /**
     * 任务执行上下文
     * 存储任务运行时的环境信息和配置参数
     */
    private final TaskExecutionContext taskExecutionContext;

    /**
     * 依赖任务跟踪器
     * 负责实际的依赖状态检查和监控逻辑
     */
    private final DependentTaskTracker dependentTaskTracker;

    /**
     * 构造函数 - 初始化依赖逻辑任务
     * 创建依赖任务跟踪器并配置必要的数据访问组件
     *
     * @param taskExecutionContext 任务执行上下文
     * @param projectDao 项目数据访问对象
     * @param workflowDefinitionDao 工作流定义数据访问对象
     * @param taskDefinitionDao 任务定义数据访问对象
     * @param taskInstanceDao 任务实例数据访问对象
     * @param workflowInstanceDao 工作流实例数据访问对象
     * @param workflowExecutionRunnable 工作流执行运行时对象
     * @param taskInstanceContextDao 任务实例上下文数据访问对象
     */
    public DependentLogicTask(TaskExecutionContext taskExecutionContext,
                              ProjectDao projectDao,
                              WorkflowDefinitionDao workflowDefinitionDao,
                              TaskDefinitionDao taskDefinitionDao,
                              TaskInstanceDao taskInstanceDao,
                              WorkflowInstanceDao workflowInstanceDao,
                              IWorkflowExecutionRunnable workflowExecutionRunnable,
                              TaskInstanceContextDao taskInstanceContextDao) {
        super(taskExecutionContext);
        this.taskExecutionContext = taskExecutionContext;

        // 创建依赖任务跟踪器，注入所有必要的数据访问组件
        this.dependentTaskTracker = new DependentTaskTracker(
                taskExecutionContext,
                taskParameters,
                projectDao,
                workflowDefinitionDao,
                taskDefinitionDao,
                taskInstanceDao,
                workflowInstanceDao,
                taskInstanceContextDao);

        // 将任务状态设置为运行中
        onTaskRunning();
    }

    /**
     * 启动依赖任务执行
     * 初始化依赖检查逻辑，任务将进入持续监控状态
     */
    @Override
    public void start() throws MasterTaskExecuteException {
        log.info("Dependent task: {} started", taskExecutionContext.getTaskName());
        // 依赖任务的具体逻辑由getTaskExecutionState方法中的dependentTaskTracker处理
        // 这里只是记录任务启动日志，实际的依赖检查会在状态查询时进行
    }

    /**
     * 获取任务执行状态
     * 通过依赖任务跟踪器检查依赖条件，返回当前任务的执行状态
     * 这是依赖任务的核心方法，会被工作流引擎周期性调用
     *
     * @return 当前任务的执行状态
     */
    @Override
    public TaskExecutionStatus getTaskExecutionState() {
        if (isRunning()) {
            // 如果任务正在运行，通过跟踪器检查依赖状态
            // 跟踪器会检查所有配置的依赖条件，并返回相应的状态
            taskExecutionStatus = dependentTaskTracker.getDependentTaskStatus();
            return taskExecutionStatus;
        }
        // 如果任务不在运行状态，直接返回当前状态
        return taskExecutionStatus;
    }

    /**
     * 暂停依赖任务
     * 将任务设置为暂停状态，停止依赖检查
     */
    @Override
    public void pause() throws MasterTaskExecuteException {
        onTaskPaused();
        log.info("Pause task : {} success", taskExecutionContext.getTaskName());
    }

    /**
     * 终止依赖任务
     * 强制结束任务执行，停止所有依赖检查
     */
    @Override
    public void kill() throws MasterTaskExecuteException {
        onTaskKilled();
        log.info("Kill task : {} success", taskExecutionContext.getTaskName());
    }

    /**
     * 获取任务参数反序列化器
     * 用于将JSON格式的任务参数转换为DependentParameters对象
     *
     * @return 依赖任务参数的反序列化器
     */
    @Override
    public ITaskParameterDeserializer<DependentParameters> getTaskParameterDeserializer() {
        return taskParamsJson -> JSONUtils.parseObject(taskParamsJson, new TypeReference<DependentParameters>() {
        });
    }

}
