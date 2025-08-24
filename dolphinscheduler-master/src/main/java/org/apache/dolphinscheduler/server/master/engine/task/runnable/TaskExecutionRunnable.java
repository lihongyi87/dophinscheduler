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

    /**
     * 构造函数 - 使用建造者模式创建任务执行器
     * 
     * 通过TaskExecutionRunnableBuilder建造者模式创建任务执行器实例。
     * 这种设计避免了构造函数参数过多的问题，提供了灵活的对象创建方式。
     * 
     * 严格的非空检查：
     * 对于核心依赖组件（工作流图、事件总线、定义等）使用checkNotNull进行严格检查。
     * 只有taskInstance允许为空，因为它需要在运行时根据情况进行初始化。
     * 
     * 类比：就像装配一台精密设备，每个核心部件都必须到位，
     * 只有某些可选配件（任务实例）可以后续安装。
     * 
     * @param taskExecutionRunnableBuilder 任务执行器建造者，包含所有必需的组件
     */
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

    /**
     * 检查任务实例是否已初始化
     * 
     * 这是一个简单但重要的状态检查方法，判断当前任务执行器中的任务实例是否已经创建。
     * 任务实例的初始化状态决定了后续可以执行哪些操作。
     * 
     * 使用场景：
     * - 在执行重试操作前检查任务实例状态
     * - 在故障转移前验证任务实例是否存在
     * - 在创建执行上下文前确认任务实例已准备就绪
     * 
     * 类比：检查工人是否已经到岗，只有工人到位了才能开始分配具体的工作任务。
     * 
     * @return true表示任务实例已初始化，false表示尚未初始化
     */
    @Override
    public boolean isTaskInstanceInitialized() {
        return taskInstance != null;
    }

    /**
     * 初始化首次运行的任务实例
     * 
     * 这是任务生命周期的第一个关键方法，负责为首次执行的任务创建任务实例。
     * 方法使用工厂模式通过Spring容器获取任务实例工厂，然后使用建造者模式创建任务实例。
     * 
     * 执行流程：
     * 1. 状态检查：确保当前没有已初始化的任务实例，避免重复初始化
     * 2. 获取工厂：从Spring容器中获取任务实例工厂集合
     * 3. 选择工厂：选择专门用于首次运行的任务实例工厂
     * 4. 构建实例：使用建造者模式，基于任务定义和工作流实例创建新的任务实例
     * 
     * 设计模式应用：
     * - 工厂模式：TaskInstanceFactories提供不同类型的任务实例创建策略
     * - 建造者模式：通过builder().withXxx().build()链式调用构建复杂对象
     * - 模板方法：不同的工厂实现不同的任务实例创建逻辑
     * 
     * 异常安全：
     * - 使用checkState进行前置条件验证，确保方法调用的正确性
     * - 如果任务实例已存在，会抛出IllegalStateException防止状态混乱
     * 
     * 类比：就像为新员工办理入职手续，创建员工档案和工号，
     * 只有第一次入职的员工需要走完整的入职流程。
     */
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

    /**
     * 检查任务实例是否可以重试
     * 
     * 判断当前任务是否还有重试机会，这是任务容错机制的核心判断逻辑。
     * 每个任务都有最大重试次数限制，只有在未达到最大重试次数时才允许重试。
     * 
     * 重试机制意义：
     * - 提高任务成功率：网络抖动、资源临时不足等临时问题可通过重试解决
     * - 避免无限重试：通过最大重试次数限制避免无效的死循环重试
     * - 提升系统稳定性：合理的重试策略能显著提升分布式系统的容错能力
     * 
     * 判断逻辑：
     * 当前重试次数 < 最大重试次数 = 可以重试
     * 例如：maxRetryTimes=3时，retryTimes为0、1、2都可以重试，到达3时不能再重试
     * 
     * 类比：就像考试补考机会，学校给每门课程3次补考机会，
     * 只要还没用完3次机会，就还可以申请补考。
     * 
     * @return true表示可以重试，false表示已达到最大重试次数
     */
    @Override
    public boolean isTaskInstanceCanRetry() {
        return taskInstance.getRetryTimes() < taskInstance.getMaxRetryTimes();
    }

    /**
     * 执行任务重试
     * 
     * 当任务执行失败但仍有重试机会时，通过此方法进行重试处理。
     * 这是任务容错机制的核心实现，能够自动恢复临时性的任务执行失败。
     * 
     * 重试执行流程：
     * 1. 前置检查：确保任务实例已经初始化，避免在无效状态下重试
     * 2. 创建重试实例：使用重试任务实例工厂基于现有任务实例创建新的重试实例
     * 3. 更新重试计数：重试实例会自动递增重试次数并重置执行状态
     * 4. 触发启动事件：发布任务启动生命周期事件，启动重试执行
     * 
     * 重试策略特点：
     * - 保留历史信息：重试实例继承原任务实例的配置和历史信息
     * - 递增重试次数：自动更新retryTimes字段，用于后续重试判断
     * - 重置执行状态：将任务状态重置为可执行状态，清除之前的失败标记
     * - 事件驱动：通过事件总线异步启动重试执行，避免阻塞当前线程
     * 
     * 异常安全：
     * - 状态检查：确保任务实例已初始化才能执行重试
     * - 工厂模式：通过专门的重试工厂确保重试实例的正确创建
     * - 事件解耦：通过事件总线解耦重试触发和执行逻辑
     * 
     * 类比：就像学生重考，保留学号和课程信息，但清除之前的不及格记录，
     * 重新安排考试时间，给学生新的考试机会。
     */
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

    /**
     * 执行故障转移处理
     * 
     * 当Master节点故障重启或任务执行器异常时，通过故障转移机制恢复任务执行。
     * 这是分布式系统高可用性的核心保障机制，确保任务能在节点故障后继续执行。
     * 
     * 故障转移执行流程：
     * 1. 前置检查：确保任务实例已初始化，避免在无效状态下执行故障转移
     * 2. 任务接管尝试：首先尝试从原执行器直接接管任务，保持执行连续性
     * 3. 接管成功处理：如果成功接管，直接返回，任务继续在新节点执行
     * 4. 接管失败处理：如果接管失败，创建新的故障转移任务实例
     * 5. 重新启动：发布任务启动事件，在新节点重新开始执行
     * 
     * 两种故障转移策略：
     * - 热故障转移：直接接管正在运行的任务，保持执行状态和进度
     * - 冷故障转移：重新创建任务实例，从头开始执行（保留历史信息）
     * 
     * 故障转移优势：
     * - 无缝恢复：尽可能保持任务执行的连续性，减少故障影响
     * - 状态保持：保留任务的历史信息和配置，避免重复初始化
     * - 自动化：无需人工干预，系统自动检测和处理故障转移
     * - 容错性：即使接管失败也能通过重新创建实例继续执行
     * 
     * 类比：就像接力赛中棒的传递，前一棒选手受伤时，
     * 先尝试在原地交接（热故障转移），如果不行就回到起点重新开始（冷故障转移）。
     */
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

    /**
     * 暂停任务执行
     * 
     * 通过发布任务暂停生命周期事件来暂停正在执行的任务。
     * 这是任务生命周期管理的重要功能，允许用户在任务执行过程中进行人工干预。
     * 
     * 暂停机制特点：
     * - 事件驱动：通过事件总线发布暂停事件，实现异步处理
     * - 优雅暂停：不是强制终止，而是通知执行器进行优雅暂停
     * - 状态保持：暂停时保留任务的执行状态和进度信息
     * - 可恢复性：暂停后的任务可以通过恢复操作继续执行
     * 
     * 使用场景：
     * - 维护窗口：在系统维护期间暂停任务执行
     * - 资源调度：在资源紧张时暂停低优先级任务
     * - 问题排查：发现异常时暂停任务进行问题定位
     * - 人工干预：需要人工确认或调整时暂停等待处理
     * 
     * 实现原理：
     * 暂停事件会被对应的事件监听器捕获，监听器会调用任务执行器的暂停接口，
     * 执行器收到暂停信号后会在合适的时机停止执行并保存当前状态。
     * 
     * 类比：就像DVD播放器的暂停按钮，按下后视频停止播放但记住了当前位置，
     * 再次播放时可以从暂停的地方继续。
     */
    @Override
    public void pause() {
        getWorkflowEventBus().publish(TaskPauseLifecycleEvent.of(this));
    }

    /**
     * 终止任务执行
     * 
     * 通过发布任务终止生命周期事件来强制终止正在执行的任务。
     * 这是任务生命周期管理的最终手段，用于处理无法通过暂停解决的紧急情况。
     * 
     * 终止机制特点：
     * - 强制终止：与暂停不同，终止是不可恢复的强制停止操作
     * - 事件驱动：通过事件总线发布终止事件，实现异步处理
     * - 资源清理：终止时会清理任务占用的资源和临时数据
     * - 状态更新：将任务状态标记为已终止，记录终止时间和原因
     * 
     * 使用场景：
     * - 紧急停止：发现任务行为异常需要立即停止时
     * - 资源回收：任务占用过多资源影响系统稳定性时
     * - 错误恢复：任务进入死锁或无限循环状态时
     * - 手动干预：用户明确要求停止任务执行时
     * - 系统关闭：系统关闭前需要停止所有运行中的任务
     * 
     * 终止与暂停的区别：
     * - 暂停：可恢复的临时停止，保留执行上下文和进度
     * - 终止：不可恢复的永久停止，清理资源并标记为最终状态
     * 
     * 实现原理：
     * 终止事件会被监听器捕获，监听器会调用任务执行器的终止接口，
     * 执行器收到终止信号后会立即停止执行，清理资源，并更新任务状态。
     * 
     * 类比：就像电脑的强制关机按钮，按下后立即断电停止所有程序，
     * 不会保存当前工作状态，下次开机需要重新开始。
     */
    @Override
    public void kill() {
        getWorkflowEventBus().publish(TaskKillLifecycleEvent.of(this));
    }

    /**
     * 初始化任务执行上下文
     * 
     * 创建任务执行时所需的完整上下文信息，这是任务真正开始执行前的最后一步准备工作。
     * 执行上下文包含任务执行器执行任务时需要的所有信息，如参数、资源配置、环境变量等。
     * 
     * 上下文创建流程：
     * 1. 前置检查：确保任务实例已经初始化，避免在无效状态下创建上下文
     * 2. 构建请求：使用建造者模式创建执行上下文创建请求，包含所有必需的组件信息
     * 3. 工厂创建：通过任务执行上下文工厂根据请求创建完整的执行上下文
     * 4. 上下文赋值：将创建的执行上下文赋值给当前实例，供后续使用
     * 
     * 执行上下文内容：
     * - 工作流信息：工作流定义、实例、执行图等完整工作流上下文
     * - 任务信息：任务定义、任务实例、参数配置等任务级别信息  
     * - 项目信息：项目配置、权限、资源等项目级别信息
     * - 执行环境：主机信息、资源配置、环境变量等运行时环境
     * - 依赖关系：前置任务、数据依赖等任务间依赖信息
     * 
     * 设计特点：
     * - 延迟创建：上下文在真正需要时才创建，避免不必要的资源消耗
     * - 完整信息：包含任务执行所需的所有信息，避免执行时缺少关键数据
     * - 工厂模式：通过专门的工厂类创建，确保上下文的正确性和一致性
     * - 状态检查：严格的前置条件检查，确保创建时机的正确性
     * 
     * 类比：就像演员上台前的最后准备，包括换装、化妆、背台词、了解其他角色，
     * 确保一上台就能完美地演出自己的角色。
     */
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

    /**
     * 从执行器接管任务
     * 
     * 这是故障转移机制中的核心方法，尝试从原执行器直接接管正在运行的任务。
     * 接管成功可以避免任务重启，保持执行的连续性，是热故障转移的关键实现。
     * 
     * 接管执行流程：
     * 1. 前置检查：确保任务实例已初始化，避免在无效状态下进行接管操作
     * 2. 调用接管接口：通过任务执行器客户端调用重新分配主机接口
     * 3. 结果处理：根据接管结果返回成功或失败状态
     * 4. 异常处理：捕获接管过程中的异常，记录日志并返回失败
     * 
     * 接管机制原理：
     * - 主机重分配：将任务的执行主机从原节点重新分配到当前节点
     * - 状态同步：同步任务的执行状态和进度信息到新节点
     * - 连接转移：将任务执行的网络连接转移到新的执行器
     * - 资源迁移：将任务占用的临时资源迁移到新节点
     * 
     * 接管成功条件：
     * - 原执行器仍可访问且响应正常
     * - 任务状态允许进行主机重分配
     * - 新节点有足够资源接管任务执行
     * - 网络连接和权限配置正确
     * 
     * 异常处理：
     * - 网络异常：原执行器不可达或网络超时
     * - 权限异常：当前节点没有接管权限
     * - 资源异常：新节点资源不足无法接管
     * - 状态异常：任务状态不允许进行接管
     * 
     * 类比：就像高速路上汽车故障时的道路救援，
     * 救援车先尝试现场修复继续行驶（接管成功），
     * 如果修不好就拖到修理厂重新开始（接管失败，冷故障转移）。
     * 
     * @return true表示接管成功，false表示接管失败
     */
    private boolean takeOverTaskFromExecutor() {
        checkState(isTaskInstanceInitialized(), "The task instance is null, can't take over from executor.");
        try {
            return applicationContext.getBean(ITaskExecutorClient.class).reassignWorkflowInstanceHost(this);
        } catch (Exception ex) {
            log.warn("Take over task: {} failed", taskInstance.getName(), ex);
            return false;
        }
    }

    /**
     * 任务执行优先级比较方法
     * 
     * 实现Comparable接口，用于任务执行队列中的优先级排序。
     * 通过多级优先级比较确保重要任务优先执行，提高系统整体执行效率。
     * 
     * 优先级比较层次（按重要性排序）：
     * 1. 工作流实例优先级：不同工作流实例间的优先级比较（数字越小优先级越高）
     * 2. 任务实例优先级：同一工作流内任务间的优先级比较（数字越小优先级越高）  
     * 3. 任务组优先级：任务组级别的优先级比较（数字越大优先级越高）
     * 4. 首次提交时间：相同优先级下按提交时间排序（越早提交优先级越高）
     * 
     * 比较规则说明：
     * - 工作流和任务优先级：数字越小优先级越高，符合一般优先级概念
     * - 任务组优先级：数字越大优先级越高，用负号转换保持一致性
     * - 提交时间：越早提交的任务越优先执行，保证公平性
     * 
     * 优先级设计意义：
     * - 资源分配：高优先级任务优先获得执行资源
     * - 用户体验：重要任务能快速响应，提升用户满意度
     * - 系统性能：合理的优先级能提高系统整体吞吐量
     * - 业务保障：关键业务流程能得到优先保障
     * 
     * 边界处理：
     * - 空值处理：如果比较对象为null，当前对象优先级更高
     * - 相等处理：只有所有比较维度都相等时，两个任务才被视为相等优先级
     * - 溢出处理：使用int类型进行比较，避免long型溢出问题
     * 
     * 类比：就像医院的分诊系统，急诊优先于普通门诊（工作流优先级），
     * 同是急诊时重症优先于轻症（任务优先级），
     * 同等病情下VIP优先于普通患者（任务组优先级），
     * 最后按挂号时间排序（提交时间）。
     * 
     * @param other 要比较的另一个任务执行实例
     * @return 负数表示当前任务优先级更高，正数表示对方优先级更高，0表示优先级相同
     */
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

    /**
     * 任务执行器的字符串表示方法
     * 
     * 提供任务执行器的可读字符串表示，主要用于日志输出、调试和监控。
     * 根据任务实例的初始化状态提供不同详细程度的信息展示。
     * 
     * 展示信息策略：
     * - 任务实例已初始化：显示任务名称和当前执行状态，便于跟踪任务进度
     * - 任务实例未初始化：仅显示任务名称，避免访问未初始化字段导致异常
     * 
     * 信息展示意义：
     * - 日志友好：在日志中能快速识别是哪个任务的日志信息
     * - 调试方便：调试时能直观看到任务名称和状态，便于问题定位
     * - 监控支持：监控系统能通过toString获取任务基本信息
     * - 状态跟踪：通过状态信息能快速了解任务执行进展
     * 
     * 实现细节：
     * - 空值安全：先检查taskInstance是否为null，避免空指针异常
     * - 状态展示：已初始化的任务实例显示详细的名称和状态信息
     * - 格式统一：使用统一的字符串格式，便于日志解析和监控
     * - 信息精简：只显示关键信息，避免输出过多内容影响日志可读性
     * 
     * 使用场景：
     * - 日志输出：在各种日志中标识当前操作的任务
     * - 异常信息：异常堆栈中显示相关的任务信息
     * - 调试输出：IDE调试时查看对象信息
     * - 监控指标：将任务信息作为监控标签
     * 
     * 类比：就像员工的工牌，上面显示姓名和当前工作状态，
     * 让别人一眼就知道这是谁，现在在做什么。
     * 
     * @return 任务执行器的字符串表示
     */
    @Override
    public String toString() {
        if (taskInstance != null) {
            return "TaskExecutionRunnable{" + "name=" + getName() + ", state=" + taskInstance.getState() + '}';
        }
        return "TaskExecutionRunnable{" + "name=" + getName() + '}';
    }
}
