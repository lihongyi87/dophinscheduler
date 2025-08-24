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

import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.plugin.storage.api.StorageOperator;
import org.apache.dolphinscheduler.plugin.task.api.AbstractTask;
import org.apache.dolphinscheduler.plugin.task.api.TaskCallBack;
import org.apache.dolphinscheduler.plugin.task.api.log.TaskLogMarkers;
import org.apache.dolphinscheduler.plugin.task.api.model.ApplicationInfo;
import org.apache.dolphinscheduler.plugin.task.api.resource.ResourceContext;
import org.apache.dolphinscheduler.server.worker.config.WorkerConfig;
import org.apache.dolphinscheduler.server.worker.utils.TaskExecutionContextUtils;
import org.apache.dolphinscheduler.server.worker.utils.TenantUtils;
import org.apache.dolphinscheduler.task.executor.AbstractTaskExecutor;
import org.apache.dolphinscheduler.task.executor.ITaskExecutor;
import org.apache.dolphinscheduler.task.executor.TaskExecutorState;
import org.apache.dolphinscheduler.task.executor.TaskExecutorStateMappings;
import org.apache.dolphinscheduler.task.executor.events.TaskExecutorRuntimeContextChangedLifecycleEvent;

import java.util.ArrayList;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * 物理任务执行器
 * 
 * 这是Worker节点中真正执行具体任务的核心组件，负责执行各种类型的物理任务。
 * 类比：工厂中的专业技术工人，根据具体的工作指令执行实际的操作任务。
 * 
 * 主要功能：
 * 1. 根据任务类型创建对应的任务插件实例
 * 2. 管理任务的生命周期（初始化、运行、完成、取消）
 * 3. 处理任务执行过程中的资源分配和清理
 * 4. 将任务状态变化上报给Master节点
 * 5. 处理任务日志的收集和上传
 * 
 * 支持的任务类型：
 * - Shell脚本任务
 * - SQL查询任务  
 * - Python脚本任务
 * - Spark计算任务
 * - MapReduce任务
 * - HTTP请求任务
 * - 存储过程任务
 * - 以及其他插件化任务类型
 */
@Slf4j
public class PhysicalTaskExecutor extends AbstractTaskExecutor {

    /**
     * Worker节点配置信息
     * 
     * 包含Worker的基本配置参数，如工作目录、资源限制、超时设置等。
     * 类比：工人的工作规范手册，规定了工作环境和操作标准。
     */
    private final WorkerConfig workerConfig;

    /**
     * 存储操作器
     * 
     * 用于处理文件上传下载、日志存储等与外部存储系统的交互。
     * 支持多种存储后端：本地文件系统、HDFS、S3、OSS等。
     * 类比：仓库管理员，负责物料的存取和管理。
     */
    private final StorageOperator storageOperator;

    /**
     * 物理任务实例
     * 
     * 根据任务类型动态创建的具体任务实例，封装了特定任务类型的执行逻辑。
     * 类比：具体的工作工具，比如电焊工用的焊枪、程序员用的IDE。
     */
    @Getter
    private AbstractTask physicalTask;

    /**
     * 物理任务插件工厂
     * 
     * 负责根据任务类型创建对应的任务插件实例。
     * 这是插件化架构的核心，支持动态扩展新的任务类型。
     * 类比：工具箱管理员，根据工作需要提供合适的工具。
     */
    private final PhysicalTaskPluginFactory physicalTaskPluginFactory;

    /**
     * 构造函数
     * 
     * 通过建造者模式初始化物理任务执行器。
     * 建造者模式简化了复杂对象的创建过程，使配置更加灵活。
     * 
     * @param physicalTaskExecutorBuilder 物理任务执行器建造者，包含所有必需的配置信息
     */
    public PhysicalTaskExecutor(final PhysicalTaskExecutorBuilder physicalTaskExecutorBuilder) {
        // 调用父类构造函数，传入任务执行上下文和事件总线
        super(physicalTaskExecutorBuilder.getTaskExecutionContext(),
                physicalTaskExecutorBuilder.getTaskExecutorEventBus());
        
        // 初始化各个组件
        this.workerConfig = physicalTaskExecutorBuilder.getWorkerConfig();
        this.storageOperator = physicalTaskExecutorBuilder.getStorageOperator();
        this.physicalTaskPluginFactory = physicalTaskExecutorBuilder.getPhysicalTaskPluginFactory();
    }

    /**
     * 初始化任务插件
     * 
     * 根据任务类型创建对应的任务插件实例，并进行必要的初始化配置。
     * 这是任务执行流程的第一步，为后续的任务执行做准备。
     * 
     * 类比：工人接到新任务后，先准备好对应的工具和材料。
     */
    @Override
    protected void initializeTaskPlugin() {
        // 使用工厂模式创建具体的物理任务实例
        // 根据任务类型（Shell、SQL、Python等）创建对应的处理器
        this.physicalTask = physicalTaskPluginFactory.createPhysicalTask(this);
        log.info("Initialized physicalTask: {} successfully", taskExecutionContext.getTaskType());

        // 初始化任务插件，设置运行环境和参数
        this.physicalTask.init();

        // 初始化变量池，用于任务执行过程中的变量传递
        // 变量池允许任务之间共享和传递数据
        this.physicalTask.getParameters().setVarPool(new ArrayList<>());
        log.info("Set taskVarPool: {} successfully", taskExecutionContext.getVarPool());
    }

    /**
     * 触发任务插件执行
     * 
     * 启动实际的任务执行过程，并设置回调处理机制。
     * 这是任务执行的核心逻辑，负责协调任务执行和状态上报。
     * 
     * 类比：工人开始具体的工作，并定期向项目经理汇报进度。
     */
    @Override
    protected void doTriggerTaskPlugin() {
        final ITaskExecutor taskExecutor = this;
        
        // 启动任务执行，并提供回调接口用于状态更新
        physicalTask.handle(new TaskCallBack() {

            /**
             * 更新远程应用信息回调
             * 
             * 当任务产生外部应用（如Spark Job、Yarn Application）时调用此方法。
             * 用于追踪和管理任务产生的外部应用实例。
             * 
             * @param taskInstanceId 任务实例ID
             * @param applicationInfo 应用信息，包含应用ID等
             */
            @Override
            public void updateRemoteApplicationInfo(final int taskInstanceId, final ApplicationInfo applicationInfo) {
                // 更新任务上下文中的应用ID列表
                taskExecutionContext.setAppIds(applicationInfo.getAppIds());
                // 发布运行时上下文变化事件，通知Master节点
                taskExecutorEventBus.publish(TaskExecutorRuntimeContextChangedLifecycleEvent.of(taskExecutor));
            }

            /**
             * 更新任务实例信息回调
             * 
             * 当任务实例的关键信息发生变化时调用此方法。
             * 用于及时将任务状态变化同步给Master节点。
             * 
             * @param taskInstanceId 任务实例ID
             */
            @Override
            public void updateTaskInstanceInfo(final int taskInstanceId) {
                // 发布运行时上下文变化事件
                taskExecutorEventBus.publish(TaskExecutorRuntimeContextChangedLifecycleEvent.of(taskExecutor));
            }
        });
    }

    /**
     * 跟踪任务插件执行状态
     * 
     * 检查物理任务的当前执行状态，并将其映射为统一的任务执行器状态。
     * 这个方法会被定期调用，用于监控任务的执行进度。
     * 
     * 类比：项目经理定期检查工人的工作进度和完成情况。
     * 
     * @return 映射后的任务执行器状态
     */
    @Override
    protected TaskExecutorState doTrackTaskPluginStatus() {
        // 使用状态映射器将物理任务的退出状态转换为标准的执行器状态
        return TaskExecutorStateMappings.mapState(physicalTask.getExitStatus());
    }

    /**
     * 暂停任务
     * 
     * 物理任务执行器不支持暂停操作，因为大多数物理任务（如Shell、SQL）
     * 一旦开始执行就难以暂停和恢复。
     * 
     * 类比：某些工作（如化学反应）一旦开始就不能暂停。
     */
    @Override
    public void pause() {
        log.warn("The physical task doesn't support pause operation");
    }

    /**
     * 终止任务
     * 
     * 强制终止正在执行的物理任务，释放相关资源。
     * 这通常在任务超时、用户取消或系统异常时调用。
     * 
     * 类比：紧急情况下叫停正在进行的工作。
     */
    @Override
    public void kill() {
        if (physicalTask != null) {
            log.info("Killing physical task: {}", taskExecutionContext.getTaskName());
            physicalTask.cancel();
        }
    }

    /**
     * 初始化任务上下文
     * 
     * 设置任务执行所需的各种环境和资源，包括：
     * 1. 租户信息配置
     * 2. 工作目录创建
     * 3. 资源文件下载
     * 4. 执行环境准备
     * 
     * 类比：工人开始工作前的准备工作，包括准备工作场地、获取材料等。
     */
    @Override
    protected void initializeTaskContext() {
        // 调用父类方法，执行基础的上下文初始化
        super.initializeTaskContext();

        // 设置任务应用ID，用于唯一标识这个任务实例
        taskExecutionContext.setTaskAppId(String.valueOf(taskExecutionContext.getTaskInstanceId()));

        // 设置租户信息，用于多租户环境下的资源隔离和权限控制
        // 如果租户不存在，会自动创建一个实际的租户
        taskExecutionContext.setTenantCode(TenantUtils.getOrCreateActualTenant(workerConfig, taskExecutionContext));
        log.info("TenantCode: {} check successfully", taskExecutionContext.getTenantCode());

        // 为任务实例创建专用的工作目录
        // 每个任务实例都有独立的工作目录，避免文件冲突
        TaskExecutionContextUtils.createTaskInstanceWorkingDirectory(taskExecutionContext);
        log.info("TaskInstance working directory: {} create successfully", taskExecutionContext.getExecutePath());

        // 下载任务执行所需的资源文件
        // 包括脚本文件、数据文件、配置文件等
        final ResourceContext resourceContext = TaskExecutionContextUtils.downloadResourcesIfNeeded(
                physicalTaskPluginFactory.getTaskChannel(this),
                storageOperator,
                taskExecutionContext);
        taskExecutionContext.setResourceContext(resourceContext);
        log.info("Download resources successfully: \n{}", taskExecutionContext.getResourceContext());

        // 记录完整的任务上下文信息，用于调试和问题诊断
        log.info(TaskLogMarkers.excludeInTaskLog(), "Initialized Task Context{}",
                JSONUtils.toPrettyJsonString(taskExecutionContext));

    }

    /**
     * 获取任务执行器的字符串表示
     * 
     * 用于日志记录和调试，提供任务的关键信息概览。
     * 
     * @return 包含任务ID、名称和状态的字符串表示
     */
    @Override
    public String toString() {
        return "PhysicalTaskExecutor{" +
                "id=" + taskExecutionContext.getTaskInstanceId() +
                ", name=" + taskExecutionContext.getTaskName() +
                ", state=" + taskExecutorState.get() +
                '}';
    }

}
