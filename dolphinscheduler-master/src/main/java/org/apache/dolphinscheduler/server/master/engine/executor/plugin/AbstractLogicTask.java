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

package org.apache.dolphinscheduler.server.master.engine.executor.plugin;

import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.enums.TaskExecutionStatus;
import org.apache.dolphinscheduler.plugin.task.api.parameters.AbstractParameters;

import lombok.extern.slf4j.Slf4j;

import com.google.common.base.Preconditions;

/**
 * 抽象逻辑任务基类
 * 为所有逻辑任务实现提供通用的基础功能和状态管理
 *
 * 该基类的职责：
 * 1. 统一任务参数初始化和反序列化流程
 * 2. 提供标准的任务状态管理机制
 * 3. 定义任务生命周期的状态转换方法
 * 4. 封装通用的任务执行上下文处理逻辑
 *
 * 继承该类的具体任务实现需要：
 * - 实现start()、pause()、kill()方法
 * - 提供对应的ITaskParameterDeserializer实现
 * - 在执行过程中调用状态转换方法更新任务状态
 *
 * @param <T> 任务参数类型，继承自AbstractParameters
 *
 * @author DolphinScheduler Team
 */
@Slf4j
public abstract class AbstractLogicTask<T extends AbstractParameters> implements ILogicTask<T> {

    /**
     * 任务执行上下文
     * 包含任务运行所需的环境信息和配置参数
     */
    protected final TaskExecutionContext taskExecutionContext;

    /**
     * 任务参数对象
     * 从任务定义的JSON参数反序列化而来的强类型参数对象
     */
    protected final T taskParameters;

    /**
     * 当前任务执行状态
     * 记录任务的实时执行状态，用于状态查询和流程控制
     */
    protected TaskExecutionStatus taskExecutionStatus;

    /**
     * 构造函数 - 初始化逻辑任务
     * 完成任务参数反序列化和基础状态设置
     *
     * @param taskExecutionContext 任务执行上下文，包含任务定义和运行环境信息
     */
    public AbstractLogicTask(final TaskExecutionContext taskExecutionContext) {
        this.taskExecutionContext = taskExecutionContext;
        // 使用对应的反序列化器将JSON参数转换为强类型对象
        this.taskParameters = getTaskParameterDeserializer().deserialize(taskExecutionContext.getTaskParams());
        // 验证参数反序列化结果
        Preconditions.checkNotNull(taskParameters,
                "Deserialize task parameters: " + taskExecutionContext.getTaskParams());
        log.info("Success initialize parameters: \n{}", JSONUtils.toPrettyJsonString(taskParameters));
    }

    /**
     * 获取任务当前执行状态
     *
     * @return 任务执行状态枚举值
     */
    @Override
    public TaskExecutionStatus getTaskExecutionState() {
        return taskExecutionStatus;
    }

    /**
     * 判断任务是否正在运行
     * 用于子类在执行逻辑中进行状态检查
     *
     * @return true 如果任务正在运行，false 否则
     */
    protected boolean isRunning() {
        return taskExecutionStatus == TaskExecutionStatus.RUNNING_EXECUTION;
    }

    /**
     * 将任务状态设置为运行中
     * 在任务开始执行时调用
     */
    protected void onTaskRunning() {
        taskExecutionStatus = TaskExecutionStatus.RUNNING_EXECUTION;
    }

    /**
     * 将任务状态设置为成功
     * 在任务执行完成且结果成功时调用
     */
    protected void onTaskSuccess() {
        taskExecutionStatus = TaskExecutionStatus.SUCCESS;
    }

    /**
     * 将任务状态设置为失败
     * 在任务执行过程中发生错误时调用
     */
    protected void onTaskFailed() {
        taskExecutionStatus = TaskExecutionStatus.FAILURE;
    }

    /**
     * 将任务状态设置为已终止
     * 在任务被强制终止时调用
     */
    protected void onTaskKilled() {
        taskExecutionStatus = TaskExecutionStatus.KILL;
    }

    /**
     * 将任务状态设置为已暂停
     * 在任务被暂停时调用
     */
    protected void onTaskPaused() {
        taskExecutionStatus = TaskExecutionStatus.PAUSE;
    }
}
