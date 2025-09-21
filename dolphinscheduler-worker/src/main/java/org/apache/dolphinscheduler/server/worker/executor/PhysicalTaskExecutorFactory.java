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

import org.apache.dolphinscheduler.plugin.storage.api.StorageOperator;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.utils.LogUtils;
import org.apache.dolphinscheduler.server.worker.config.WorkerConfig;
import org.apache.dolphinscheduler.task.executor.ITaskExecutor;
import org.apache.dolphinscheduler.task.executor.ITaskExecutorFactory;

import lombok.extern.slf4j.Slf4j;

import org.springframework.stereotype.Component;

/**
 * 物理任务执行器工厂
 *
 * 专门负责创建PhysicalTaskExecutor实例的工厂类，实现了标准的任务执行器工厂接口。
 * 类比：专业生产线，按照标准流程组装特定类型的产品。
 *
 * 主要功能：
 * 1. 根据任务执行上下文创建物理任务执行器
 * 2. 配置执行器所需的各种依赖组件
 * 3. 设置任务日志路径和存储配置
 * 4. 使用建造者模式简化对象创建过程
 *
 * 工厂职责：
 * - 封装复杂的PhysicalTaskExecutor创建逻辑
 * - 确保所有必需的依赖都正确注入
 * - 提供统一的任务执行器创建接口
 * - 支持不同类型任务的执行器定制
 *
 * 设计模式：
 * - 工厂模式：标准化对象创建流程
 * - 依赖注入：管理组件间的依赖关系
 * - 建造者模式：简化复杂对象的构建
 */
@Slf4j
@Component
public class PhysicalTaskExecutorFactory implements ITaskExecutorFactory {

    /**
     * Worker节点配置
     *
     * 包含Worker的各项运行时配置，如资源限制、超时设置、工作目录等。
     * 这些配置将影响任务执行器的行为和性能表现。
     * 类比：工厂的操作手册，规定了生产标准和质量要求。
     */
    private final WorkerConfig workerConfig;

    /**
     * 物理任务插件工厂
     *
     * 根据任务类型创建对应的任务插件实例，支持插件化的任务扩展。
     * 是DolphinScheduler支持多种任务类型的核心机制。
     * 类比：专业工具库，根据不同的工作需求提供合适的工具。
     */
    private final PhysicalTaskPluginFactory physicalTaskPluginFactory;

    /**
     * 存储操作器
     *
     * 提供与外部存储系统的交互能力，处理文件上传下载、日志存储等操作。
     * 支持多种存储后端，如本地文件系统、HDFS、S3等。
     * 类比：仓库管理系统，负责原料和成品的存储管理。
     */
    private final StorageOperator storageOperator;

    /**
     * 构造函数
     *
     * 通过依赖注入初始化工厂所需的各种组件。
     * 确保工厂具备创建完整功能的物理任务执行器的能力。
     *
     * @param workerConfig Worker节点配置，提供执行环境相关的参数
     * @param physicalTaskPluginFactory 任务插件工厂，用于创建特定类型的任务插件
     * @param storageOperator 存储操作器，处理文件和日志相关的存储操作
     */
    public PhysicalTaskExecutorFactory(final WorkerConfig workerConfig,
                                       final PhysicalTaskPluginFactory physicalTaskPluginFactory,
                                       final StorageOperator storageOperator) {
        // 保存Worker节点配置，用于后续任务执行器创建
        this.workerConfig = workerConfig;
        // 保存任务插件工厂，用于创建特定类型的任务插件
        this.physicalTaskPluginFactory = physicalTaskPluginFactory;
        // 保存存储操作器，用于处理文件和日志相关的存储操作
        this.storageOperator = storageOperator;
    }

    /**
     * 创建任务执行器
     *
     * 根据提供的任务执行上下文创建配置完整的物理任务执行器实例。
     * 这是工厂的核心方法，负责组装所有必需的组件。
     *
     * @param taskExecutionContext 任务执行上下文，包含任务的所有执行信息
     * @return 配置完整的物理任务执行器实例，可以直接用于任务执行
     */
    @Override
    public ITaskExecutor createTaskExecutor(final TaskExecutionContext taskExecutionContext) {
        // 首先设置任务的日志路径，确保日志能正确记录和存储
        assemblyTaskLogPath(taskExecutionContext);

        // 使用建造者模式创建物理任务执行器
        final PhysicalTaskExecutorBuilder physicalTaskExecutorBuilder = PhysicalTaskExecutorBuilder.builder()
                // 设置任务执行上下文，包含任务的所有执行信息
                .taskExecutionContext(taskExecutionContext)
                // 注入Worker配置，提供执行环境参数
                .workerConfig(workerConfig)
                // 注入存储操作器，处理文件和日志存储
                .storageOperator(storageOperator)
                // 注入任务插件工厂，支持多种任务类型
                .physicalTaskPluginFactory(physicalTaskPluginFactory)
                .build();

        // 创建并返回物理任务执行器实例
        return new PhysicalTaskExecutor(physicalTaskExecutorBuilder);
    }

    /**
     * 组装任务日志路径
     *
     * 根据任务执行上下文生成完整的日志文件路径，确保日志能正确存储和检索。
     * 日志路径通常包含项目信息、工作流信息、任务信息等层次结构。
     *
     * @param taskExecutionContext 任务执行上下文，包含生成日志路径所需的信息
     */
    private void assemblyTaskLogPath(final TaskExecutionContext taskExecutionContext) {
        // 使用日志工具类生成标准化的任务实例日志完整路径
        taskExecutionContext.setLogPath(LogUtils.getTaskInstanceLogFullPath(taskExecutionContext));
    }

}
