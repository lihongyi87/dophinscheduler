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

import org.apache.dolphinscheduler.plugin.task.api.enums.TaskExecutionStatus;
import org.apache.dolphinscheduler.plugin.task.api.parameters.AbstractParameters;
import org.apache.dolphinscheduler.server.master.exception.MasterTaskExecuteException;

/**
 * 逻辑任务接口
 * 定义了Master节点上执行的逻辑任务的基本操作规范
 * 逻辑任务是在Master节点本地执行的任务，区别于需要在Worker节点执行的物理任务
 *
 * 常见的逻辑任务类型包括：
 * - 条件任务(Condition)：根据上游任务状态进行条件判断
 * - 依赖任务(Dependent)：检查其他工作流实例的执行状态
 * - 子工作流任务(SubWorkflow)：触发并管理子工作流的执行
 * - 开关任务(Switch)：根据条件选择不同的执行分支
 *
 * @param <T> 任务参数类型，继承自AbstractParameters
 *
 * @author DolphinScheduler Team
 */
public interface ILogicTask<T extends AbstractParameters> {

    /**
     * 启动任务执行
     * 该方法会初始化任务执行环境并开始任务的具体逻辑处理
     *
     * @throws MasterTaskExecuteException 当任务启动失败时抛出异常
     */
    void start() throws MasterTaskExecuteException;

    /**
     * 暂停任务执行
     * 将正在运行的任务暂停，保留当前执行状态以便后续恢复
     * 注意：并非所有逻辑任务都支持暂停操作
     *
     * @throws MasterTaskExecuteException 当任务暂停失败时抛出异常
     */
    void pause() throws MasterTaskExecuteException;

    /**
     * 强制终止任务执行
     * 立即停止任务执行，清理相关资源
     *
     * @throws MasterTaskExecuteException 当任务终止失败时抛出异常
     */
    void kill() throws MasterTaskExecuteException;

    /**
     * 获取任务当前执行状态
     * 返回任务的实时执行状态，用于工作流引擎进行状态判断和流程控制
     *
     * @return 任务执行状态枚举值
     */
    TaskExecutionStatus getTaskExecutionState();

    /**
     * 获取任务参数反序列化器
     * 用于将任务定义中的参数字符串反序列化为具体的参数对象
     *
     * @return 任务参数反序列化器实例
     */
    ITaskParameterDeserializer<T> getTaskParameterDeserializer();

}
