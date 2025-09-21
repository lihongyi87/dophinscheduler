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

import org.apache.dolphinscheduler.plugin.task.api.AbstractTask;
import org.apache.dolphinscheduler.plugin.task.api.TaskChannel;
import org.apache.dolphinscheduler.plugin.task.api.TaskPluginManager;

import org.springframework.stereotype.Component;

/**
 * 物理任务插件工厂
 *
 * 负责根据任务类型创建对应的物理任务插件实例，是DolphinScheduler插件化架构的核心组件。
 * 类比：万能工具箱管理员，根据不同的工作需求提供对应的专业工具。
 *
 * 主要功能：
 * 1. 根据任务类型识别并创建对应的任务插件
 * 2. 提供任务通道（TaskChannel）的获取接口
 * 3. 管理各种任务类型的插件实例化过程
 * 4. 支持动态扩展新的任务类型
 *
 * 支持的任务类型示例：
 * - Shell：执行Shell脚本任务
 * - SQL：执行数据库查询任务
 * - Python：执行Python脚本任务
 * - Spark：执行Spark计算任务
 * - Flink：执行Flink流处理任务
 * - HTTP：执行HTTP请求任务
 * - MapReduce：执行Hadoop MapReduce任务
 * - Procedure：执行存储过程任务
 * - SubProcess：执行子流程任务
 * - DataX：执行数据同步任务
 * - 以及通过插件扩展的自定义任务类型
 *
 * 插件化优势：
 * - 松耦合：各任务类型独立开发和维护
 * - 可扩展：支持第三方任务插件
 * - 标准化：统一的任务接口和生命周期
 * - 模块化：便于测试和部署
 *
 * 工厂模式特点：
 * - 封装对象创建逻辑
 * - 统一管理插件实例化
 * - 支持运行时类型识别
 * - 提供类型安全的接口
 */
@Component
public class PhysicalTaskPluginFactory {

    /**
     * 创建物理任务实例
     *
     * 根据物理任务执行器的上下文信息，创建对应类型的物理任务插件实例。
     * 这是工厂的核心方法，实现了从任务类型到具体实现的映射。
     *
     * @param physicalTaskExecutor 物理任务执行器，包含任务执行的完整上下文信息
     * @return 创建好的物理任务实例，可以直接用于任务执行
     */
    public AbstractTask createPhysicalTask(final PhysicalTaskExecutor physicalTaskExecutor) {
        // 首先获取任务类型对应的任务通道
        TaskChannel taskChannel = getTaskChannel(physicalTaskExecutor);

        // 通过任务通道创建具体的任务实例
        // 任务实例将包含特定任务类型的执行逻辑
        return taskChannel.createTask(physicalTaskExecutor.getTaskExecutionContext());
    }

    /**
     * 获取任务通道
     *
     * 根据任务类型从任务插件管理器中获取对应的任务通道。
     * 任务通道是任务插件的入口，提供任务创建和管理的接口。
     *
     * @param physicalTaskExecutor 物理任务执行器，包含任务类型信息
     * @return 对应任务类型的任务通道实例
     */
    public TaskChannel getTaskChannel(final PhysicalTaskExecutor physicalTaskExecutor) {
        // 从任务执行上下文中获取任务类型
        final String taskType = physicalTaskExecutor.getTaskExecutionContext().getTaskType();

        // 通过任务插件管理器获取对应的任务通道
        // 如果任务类型不存在，插件管理器会抛出相应的异常
        return TaskPluginManager.getTaskChannel(taskType);
    }
}
