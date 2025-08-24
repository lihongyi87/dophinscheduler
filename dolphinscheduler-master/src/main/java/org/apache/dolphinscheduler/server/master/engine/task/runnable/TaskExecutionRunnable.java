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

package org.apache.dolphinscheduler.server.master.engine.task.runnable;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import org.apache.dolphinscheduler.dao.entity.Project;
import org.apache.dolphinscheduler.dao.entity.TaskDefinition;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.dao.entity.WorkflowDefinition;
import org.apache.dolphinscheduler.dao.entity.WorkflowInstance;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.server.master.engine.WorkflowEventBus;
import org.apache.dolphinscheduler.server.master.engine.graph.IWorkflowExecutionGraph;
import org.apache.dolphinscheduler.server.master.engine.task.client.ITaskExecutorClient;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event.TaskKillLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event.TaskPauseLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event.TaskStartLifecycleEvent;
import org.apache.dolphinscheduler.server.master.runner.TaskExecutionContextFactory;

import javax.annotation.Nullable;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.springframework.context.ApplicationContext;

/**
 * 任务执行运行器
 * 
 * 这是Master节点中单个任务执行的核心管理器，负责任务的完整生命周期管理。
 * 类比：一个专业的项目任务执行负责人，从任务分配到完成交付的全程管理。
 * 
 * 核心职责：
 * 1. 任务实例管理：创建、初始化和管理任务实例的生命周期
 * 2. 执行状态跟踪：监控任务执行状态，处理各种生命周期事件
 * 3. 重试机制：支持任务失败后的自动重试，提高任务成功率
 * 4. 故障恢复：处理任务执行过程中的各种异常情况
 * 5. 资源协调：协调工作流、任务定义、执行上下文等资源
 * 
 * 设计特点：
 * - 状态驱动：基于任务状态变化驱动执行流程
 * - 事件响应：通过生命周期事件响应各种执行场景
 * - 构建器模式：使用建造者模式简化复杂对象构造
 * - 空值安全：严格的非空检查，避免空指针异常
 * 
 * 生命周期阶段：
 * 1. 初始化阶段：创建任务实例，设置执行上下文
 * 2. 启动阶段：发送启动事件，开始任务执行
 * 3. 执行阶段：监控执行进度，处理状态变化
 * 4. 完成阶段：处理成功/失败结果，清理资源
 * 5. 重试阶段：必要时进行任务重试
 * 
 * 类比理解：
 * 就像一个项目任务的专属管家：
 * - 接收任务指令（任务定义）
 * - 准备执行环境（初始化上下文）
 * - 跟踪执行进度（状态监控）
 * - 处理突发情况（异常处理）
 * - 确保任务完成（重试机制）
 * - 汇报执行结果（事件上报）
 */
@Slf4j
public class TaskExecutionRunnable implements ITaskExecutionRunnable {

    /**
     * Spring应用上下文
     * 
     * 用于获取Spring管理的各种Bean和服务，实现依赖注入。
     * 这里主要用于获取任务实例工厂等组件。
     * 
     * 类比：项目管家的资源调配中心，可以获取各种专业服务。
     */
    private final ApplicationContext applicationContext;

    /**
     * 工作流执行图
     * 
     * 包含工作流中所有任务的依赖关系和执行顺序信息。
     * 任务执行时需要根据执行图确定前置依赖是否满足。
     * 
     * 类比：项目的甘特图，显示所有任务的依赖关系和执行顺序。
     */
    @Getter
    private final IWorkflowExecutionGraph workflowExecutionGraph;

    /**
     * 工作流事件总线
     * 
     * 用于发布和处理工作流相关的各种事件。
     * 任务执行过程中的状态变化都会通过事件总线进行通知。
     * 
     * 类比：项目通信系统，用于各个任务之间的信息传递。
     */
    @Getter
    private final WorkflowEventBus workflowEventBus;

    /**
     * 工作流定义
     * 
     * 工作流的元数据定义，包含工作流的基本信息和配置。
     * 提供任务执行所需的工作流级别的配置信息。
     * 
     * 类比：项目的总体规划文档。
     */
    @Getter
    private final WorkflowDefinition workflowDefinition;

    /**
     * 项目信息
     * 
     * 任务所属的项目信息，用于权限控制和资源隔离。
     * 不同项目的任务在执行时会有不同的权限和配置。
     * 
     * 类比：任务所属的部门或业务单元信息。
     */
    @Getter
    private final Project project;

    /**
     * 工作流实例
     * 
     * 工作流的运行时实例，包含本次执行的具体信息。
     * 每次工作流运行都会创建一个新的工作流实例。
     * 
     * 类比：项目的具体执行批次，每次执行都有唯一的批次号。
     */
    @Getter
    private final WorkflowInstance workflowInstance;

    /**
     * 任务实例
     * 
     * 任务的运行时实例，包含任务的执行状态和结果。
     * 使用@Nullable注解表示初始时可能为空，需要后续初始化。
     * 
     * 类比：具体任务的执行记录卡，记录执行过程和结果。
     */
    @Getter
    private @Nullable TaskInstance taskInstance;

    /**
     * 任务定义
     * 
     * 任务的元数据定义，包含任务的类型、参数、配置等信息。
     * 这是任务执行的模板，定义了任务应该如何执行。
     * 
     * 类比：任务的标准作业指导书，规定了任务的执行方法。
     */
    @Getter
    private final TaskDefinition taskDefinition;

    /**
     * 任务执行上下文
     * 
     * 包含任务执行所需的所有运行时信息，如参数、资源、环境配置等。
     * 这是任务执行器执行任务时的重要输入信息。
     * 
     * 类比：任务执行时的工作台，包含所有必需的工具和材料。
     */
    @Getter
    private TaskExecutionContext taskExecutionContext;

    public TaskExecutionRunnable(TaskExecutionRunnableBuilder taskExecutionRunnableBuilder) {
        this.applicationContext = taskExecutionRunnableBuilder.getApplicationContext();
        this.workflowExecutionGraph = checkNotNull(taskExecutionRunnableBuilder.getWorkflowExecutionGraph());
        this.workflowEventBus = checkNotNull(taskExecutionRunnableBuilder.getWorkflowEventBus());
        this.workflowDefinition = checkNotNull(taskExecutionRunnableBuilder.getWorkflowDefinition());
        this.project = checkNotNull(taskExecutionRunnableBuilder.getProject());
        this.workflowInstance = checkNotNull(taskExecutionRunnableBuilder.getWorkflowInstance());
        this.taskDefinition = checkNotNull(taskExecutionRunnableBuilder.getTaskDefinition());
        this.taskInstance = taskExecutionRunnableBuilder.getTaskInstance();
    }

    @Override
    public boolean isTaskInstanceInitialized() {
        return taskInstance != null;
    }

    @Override
    public void initializeFirstRunTaskInstance() {
        checkState(!isTaskInstanceInitialized(),
                "The task instance is already initialized, can't initialize first run task.");
        this.taskInstance = applicationContext.getBean(TaskInstanceFactories.class)
                .firstRunTaskInstanceFactory()
                .builder()
                .withTaskDefinition(taskDefinition)
                .withWorkflowInstance(workflowInstance)
                .build();
    }

    @Override
    public boolean isTaskInstanceCanRetry() {
        return taskInstance.getRetryTimes() < taskInstance.getMaxRetryTimes();
    }

    @Override
    public void retry() {
        checkState(isTaskInstanceInitialized(), "The task instance is not initialized, can't initialize retry task.");
        this.taskInstance = applicationContext.getBean(TaskInstanceFactories.class)
                .retryTaskInstanceFactory()
                .builder()
                .withTaskInstance(taskInstance)
                .build();
        getWorkflowEventBus().publish(TaskStartLifecycleEvent.of(this));
    }

    @Override
    public void failover() {
        checkState(isTaskInstanceInitialized(), "The task instance is not initialized, can't failover.");
        if (takeOverTaskFromExecutor()) {
            log.info("Failover task success, the task {} has been taken-over from executor", taskInstance.getName());
            return;
        }
        this.taskInstance = applicationContext.getBean(TaskInstanceFactories.class)
                .failoverTaskInstanceFactory()
                .builder()
                .withTaskInstance(taskInstance)
                .build();
        getWorkflowEventBus().publish(TaskStartLifecycleEvent.of(this));
    }

    @Override
    public void pause() {
        getWorkflowEventBus().publish(TaskPauseLifecycleEvent.of(this));
    }

    @Override
    public void kill() {
        getWorkflowEventBus().publish(TaskKillLifecycleEvent.of(this));
    }

    @Override
    public void initializeTaskExecutionContext() {
        checkState(isTaskInstanceInitialized(),
                "The task instance is not initialized, can't initialize TaskExecutionContext.");
        final TaskExecutionContextCreateRequest request = TaskExecutionContextCreateRequest.builder()
                .workflowExecutionGraph(workflowExecutionGraph)
                .workflowDefinition(workflowDefinition)
                .project(project)
                .workflowInstance(workflowInstance)
                .taskDefinition(taskDefinition)
                .taskInstance(taskInstance)
                .build();
        this.taskExecutionContext =
                applicationContext.getBean(TaskExecutionContextFactory.class).createTaskExecutionContext(request);
    }

    private boolean takeOverTaskFromExecutor() {
        checkState(isTaskInstanceInitialized(), "The task instance is null, can't take over from executor.");
        try {
            return applicationContext.getBean(ITaskExecutorClient.class).reassignWorkflowInstanceHost(this);
        } catch (Exception ex) {
            log.warn("Take over task: {} failed", taskInstance.getName(), ex);
            return false;
        }
    }

    @Override
    public int compareTo(ITaskExecutionRunnable other) {
        if (other == null) {
            return 1;
        }
        int workflowInstancePriorityCompareResult = workflowInstance.getWorkflowInstancePriority().getCode() -
                other.getWorkflowInstance().getWorkflowInstancePriority().getCode();
        if (workflowInstancePriorityCompareResult != 0) {
            return workflowInstancePriorityCompareResult;
        }

        // smaller number, higher priority
        int taskInstancePriorityCompareResult = taskInstance.getTaskInstancePriority().getCode()
                - other.getTaskInstance().getTaskInstancePriority().getCode();
        if (taskInstancePriorityCompareResult != 0) {
            return taskInstancePriorityCompareResult;
        }

        // larger number, higher priority
        int taskGroupPriorityCompareResult =
                taskInstance.getTaskGroupPriority() - other.getTaskInstance().getTaskGroupPriority();
        if (taskGroupPriorityCompareResult != 0) {
            return -taskGroupPriorityCompareResult;
        }
        // earlier submit time, higher priority
        return taskInstance.getFirstSubmitTime().compareTo(other.getTaskInstance().getFirstSubmitTime());
    }

    @Override
    public String toString() {
        if (taskInstance != null) {
            return "TaskExecutionRunnable{" + "name=" + getName() + ", state=" + taskInstance.getState() + '}';
        }
        return "TaskExecutionRunnable{" + "name=" + getName() + '}';
    }
}
