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
import org.apache.dolphinscheduler.server.worker.config.WorkerConfig;
import org.apache.dolphinscheduler.task.executor.eventbus.TaskExecutorEventBus;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

/**
 * 物理任务执行器构建器
 *
 * 使用建造者模式为PhysicalTaskExecutor的创建提供灵活的配置方式。
 * 类比：定制汽车的配置清单，客户可以选择不同的配置组合。
 *
 * 主要作用：
 * 1. 收集创建PhysicalTaskExecutor所需的各种依赖和配置
 * 2. 提供类型安全的对象构建过程
 * 3. 简化复杂对象的创建，提高代码可读性
 * 4. 支持链式调用，使配置过程更加直观
 *
 * 建造者模式的优势：
 * - 参数验证集中化
 * - 对象创建过程清晰
 * - 支持可选参数和默认值
 * - 避免了构造函数参数过多的问题
 */
@Data
@Builder
@AllArgsConstructor
public class PhysicalTaskExecutorBuilder {

    /**
     * 任务执行上下文
     *
     * 包含任务执行所需的所有信息，如任务参数、环境变量、资源文件等。
     * 这是任务执行的核心数据结构，贯穿整个任务生命周期。
     * 类比：工作任务单，包含完成工作所需的所有指示和材料清单。
     */
    private TaskExecutionContext taskExecutionContext;

    /**
     * Worker节点配置
     *
     * Worker节点的运行时配置，包括资源限制、超时设置、工作目录等。
     * 这些配置决定了任务执行的环境和约束条件。
     * 类比：工作场所的规章制度，规定了工作环境和操作规范。
     */
    private WorkerConfig workerConfig;

    /**
     * 存储操作器
     *
     * 提供与外部存储系统交互的接口，支持文件上传下载、日志存储等操作。
     * 可以对接多种存储后端：本地文件系统、HDFS、S3、OSS等。
     * 类比：仓库管理系统，负责物料的存取和管理。
     */
    private StorageOperator storageOperator;

    /**
     * 任务执行器事件总线
     *
     * 处理任务执行过程中的各种事件，如状态变更、进度更新等。
     * 使用默认值确保即使没有显式配置也能正常工作。
     * 类比：工厂的通信系统，传递各种工作状态和通知。
     */
    @Builder.Default
    private TaskExecutorEventBus taskExecutorEventBus = new TaskExecutorEventBus();

    /**
     * 物理任务插件工厂
     *
     * 根据任务类型创建对应的任务处理器，支持插件化的任务类型扩展。
     * 是DolphinScheduler插件化架构的重要组成部分。
     * 类比：工具库管理员，根据工作需要提供合适的专业工具。
     */
    private PhysicalTaskPluginFactory physicalTaskPluginFactory;

}
