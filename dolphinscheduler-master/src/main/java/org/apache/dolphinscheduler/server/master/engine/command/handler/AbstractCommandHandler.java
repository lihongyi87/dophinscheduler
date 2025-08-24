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

package org.apache.dolphinscheduler.server.master.engine.command.handler;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.dolphinscheduler.common.utils.JSONUtils.parseObject;

import org.apache.dolphinscheduler.dao.entity.Command;
import org.apache.dolphinscheduler.dao.entity.Project;
import org.apache.dolphinscheduler.dao.entity.TaskDefinition;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.dao.entity.WorkflowDefinition;
import org.apache.dolphinscheduler.dao.entity.WorkflowInstance;
import org.apache.dolphinscheduler.dao.repository.ProjectDao;
import org.apache.dolphinscheduler.dao.repository.TaskInstanceDao;
import org.apache.dolphinscheduler.dao.repository.WorkflowDefinitionLogDao;
import org.apache.dolphinscheduler.extract.master.command.ICommandParam;
import org.apache.dolphinscheduler.server.master.engine.WorkflowEventBus;
import org.apache.dolphinscheduler.server.master.engine.command.ICommandHandler;
import org.apache.dolphinscheduler.server.master.engine.graph.IWorkflowGraph;
import org.apache.dolphinscheduler.server.master.engine.graph.WorkflowGraphFactory;
import org.apache.dolphinscheduler.server.master.engine.workflow.listener.IWorkflowLifecycleListener;
import org.apache.dolphinscheduler.server.master.engine.workflow.runnable.WorkflowExecutionRunnable;
import org.apache.dolphinscheduler.server.master.engine.workflow.runnable.WorkflowExecutionRunnableBuilder;
import org.apache.dolphinscheduler.server.master.runner.WorkflowExecuteContext;
import org.apache.dolphinscheduler.server.master.runner.WorkflowExecuteContext.WorkflowExecuteContextBuilder;

import org.apache.commons.collections4.CollectionUtils;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

/**
 * 抽象命令处理器
 * 
 * 这是DolphinScheduler命令处理机制的抽象基类，提供命令处理的通用流程和基础服务。
 * 类比：就像一个工厂的生产线主管，负责协调各个工序，组装完整的工作流执行实例。
 * 
 * 核心职责：
 * 1. 命令处理框架：定义标准的命令处理流程和模板方法
 * 2. 上下文构建：组装工作流执行所需的完整上下文信息
 * 3. 依赖注入：管理和注入各种必要的服务组件
 * 4. 公共服务：提供子类通用的工具方法和数据访问能力
 * 5. 模板方法：定义可扩展的抽象方法供子类特化实现
 * 
 * 处理流程：
 * 1. 接收命令：处理来自调度系统的各种命令请求
 * 2. 组装定义：根据命令参数获取工作流定义信息
 * 3. 构建图结构：创建工作流的依赖关系图
 * 4. 创建实例：构建工作流执行实例
 * 5. 配置监听：设置生命周期监听器
 * 6. 初始化事件总线：创建事件通信机制
 * 7. 生成执行对象：最终生成可执行的工作流运行实例
 * 
 * 设计模式：
 * - 模板方法模式：定义算法骨架，具体步骤由子类实现
 * - 建造者模式：使用Builder模式构建复杂的执行上下文
 * - 依赖注入模式：通过Spring自动注入各种服务组件
 * - 工厂模式：使用工厂创建工作流图和其他对象
 * 
 * 子类职责：
 * - assembleWorkflowInstance：创建特定类型的工作流实例
 * - assembleWorkflowExecutionGraph：构建执行图结构
 * 不同的命令类型（首次执行、重跑、恢复等）有不同的实现策略
 * 
 * 异常处理：
 * - 参数验证：使用checkArgument验证关键参数的有效性
 * - 数据完整性：确保所有必需的数据都能正确获取
 * - 空值检查：防止关键组件为空导致的运行时异常
 * 
 * 类比理解：
 * 就像一个汽车生产线的总装车间：
 * - 接收生产订单（命令）
 * - 准备生产图纸（工作流定义）
 * - 组装各个部件（上下文组件）
 * - 安装质检系统（监听器）
 * - 连接通信系统（事件总线）
 * - 最终下线完整的汽车（工作流执行实例）
 */
public abstract class AbstractCommandHandler implements ICommandHandler {

    /**
     * 工作流定义日志数据访问对象
     * 
     * 用于查询工作流定义的历史版本信息，支持版本化的工作流管理。
     * 类比：产品图纸档案馆，可以查到任意版本的设计图纸。
     */
    @Autowired
    protected WorkflowDefinitionLogDao workflowDefinitionLogDao;

    /**
     * 工作流图工厂
     * 
     * 负责根据工作流定义创建依赖关系图，将定义转换为可执行的图结构。
     * 类比：根据设计图纸生产工序流程图的专业部门。
     */
    @Autowired
    protected WorkflowGraphFactory workflowGraphFactory;

    /**
     * Spring应用上下文
     * 
     * 提供访问Spring容器中各种Bean的能力，用于依赖查找和服务获取。
     * 类比：工厂的资源调配中心，可以获取各种专业服务和工具。
     */
    @Autowired
    protected ApplicationContext applicationContext;

    /**
     * 任务实例数据访问对象
     * 
     * 用于查询和操作任务实例的数据库记录，获取任务的执行历史和状态信息。
     * 类比：生产记录档案，记录每道工序的执行情况。
     */
    @Autowired
    protected TaskInstanceDao taskInstanceDao;

    /**
     * 工作流生命周期监听器列表
     * 
     * 监听工作流执行过程中的各种生命周期事件，用于状态跟踪、日志记录、通知等。
     * 类比：生产线的质检员和记录员，监控生产过程并记录关键事件。
     */
    @Autowired
    protected List<IWorkflowLifecycleListener> workflowLifecycleListeners;

    /**
     * 项目数据访问对象
     * 
     * 用于查询项目信息，提供工作流执行的项目上下文。
     * 类比：项目资料库，包含项目的基本信息和权限配置。
     */
    @Autowired
    protected ProjectDao projectDao;

    /**
     * 处理工作流命令的主入口方法
     * 
     * 这是命令处理的核心模板方法，按照固定的流程组装工作流执行所需的所有组件。
     * 每个子类都会调用这个统一的处理流程，但在具体的组装步骤中有不同的实现。
     * 
     * 处理流程（标准7步组装法）：
     * 1. 组装工作流定义：获取工作流的元数据和版本信息
     * 2. 组装项目信息：获取工作流所属项目的上下文信息
     * 3. 组装工作流图：构建任务依赖关系的DAG图结构
     * 4. 组装工作流实例：创建本次执行的工作流实例（子类特化）
     * 5. 组装生命周期监听器：注册状态变化的监听回调
     * 6. 组装事件总线：创建工作流内部的事件通信机制
     * 7. 组装执行图：构建实际执行时的任务图结构（子类特化）
     * 
     * 设计特点：
     * - 模板方法模式：定义了固定的处理骨架，具体步骤由子类实现
     * - 建造者模式：使用Builder模式逐步构建复杂的执行上下文
     * - 依赖注入：通过Spring容器注入各种服务组件
     * - 上下文传递：所有组装步骤共享同一个上下文建造者
     * 
     * 扩展点：
     * - assembleWorkflowInstance：不同命令类型创建不同的工作流实例
     * - assembleWorkflowExecutionGraph：不同场景构建不同的执行图
     * 
     * @param command 需要处理的工作流命令，包含执行参数和上下文信息
     * @return 完全组装好的工作流执行运行实例，可直接提交给执行引擎
     * 
     * 类比：就像汽车生产线的总装流程，每种车型都要经过相同的7个装配工站，
     * 但在某些工站会根据车型安装不同的部件，最终下线完整的汽车。
     */
    @Override
    public WorkflowExecutionRunnable handleCommand(final Command command) {
        final WorkflowExecuteContextBuilder workflowExecuteContextBuilder = WorkflowExecuteContext.builder()
                .withCommand(command);

        assembleWorkflowDefinition(workflowExecuteContextBuilder);
        assembleProject(workflowExecuteContextBuilder);
        assembleWorkflowGraph(workflowExecuteContextBuilder);
        assembleWorkflowInstance(workflowExecuteContextBuilder);
        assembleWorkflowInstanceLifecycleListeners(workflowExecuteContextBuilder);
        assembleWorkflowEventBus(workflowExecuteContextBuilder);
        assembleWorkflowExecutionGraph(workflowExecuteContextBuilder);

        final WorkflowExecutionRunnableBuilder workflowExecutionRunnableBuilder = WorkflowExecutionRunnableBuilder
                .builder()
                .workflowExecuteContextBuilder(workflowExecuteContextBuilder)
                .applicationContext(applicationContext)
                .build();
        return new WorkflowExecutionRunnable(workflowExecutionRunnableBuilder);
    }

    /**
     * 组装工作流事件总线
     * 
     * 为工作流执行创建独立的事件总线，用于工作流内部各组件间的事件通信。
     * 每个工作流实例都有自己的事件总线，确保事件的隔离性和独立性。
     * 
     * 事件总线的作用：
     * - 任务状态变化事件：任务开始、完成、失败等状态通知
     * - 工作流状态事件：工作流暂停、恢复、终止等控制事件
     * - 资源分配事件：任务组资源获取和释放的通知
     * - 异常处理事件：执行过程中各种异常情况的广播
     * 
     * 通信特点：
     * - 异步通信：事件发布和处理采用异步模式，提高响应性能
     * - 松耦合：发布者和订阅者之间没有直接依赖关系
     * - 广播机制：一个事件可以被多个监听器同时接收
     * - 类型安全：基于Java泛型确保事件类型安全
     * 
     * @param workflowExecuteContextBuilder 工作流执行上下文建造者
     * 
     * 类比：就像为每个项目配备一个内部对讲系统，项目组成员可以通过对讲机
     * 实时沟通项目进展、问题报告和资源需求，确保信息及时传达。
     */
    protected void assembleWorkflowEventBus(
                                            final WorkflowExecuteContextBuilder workflowExecuteContextBuilder) {
        workflowExecuteContextBuilder.setWorkflowEventBus(new WorkflowEventBus());
    }

    /**
     * 组装工作流实例生命周期监听器
     * 
     * 将系统中注册的所有工作流生命周期监听器添加到执行上下文中。
     * 这些监听器会在工作流执行的各个关键节点被触发，执行相应的处理逻辑。
     * 
     * 监听器类型和职责：
     * - 状态监听器：记录工作流状态变化，更新数据库状态
     * - 通知监听器：发送邮件、短信、webhook等外部通知
     * - 审计监听器：记录执行日志，追踪操作轨迹
     * - 性能监听器：统计执行时间，分析性能指标
     * - 资源监听器：监控资源使用，触发资源回收
     * 
     * 生命周期事件：
     * - 工作流启动：onWorkflowStart
     * - 工作流完成：onWorkflowFinished
     * - 工作流失败：onWorkflowFailed
     * - 工作流暂停：onWorkflowPaused
     * - 工作流恢复：onWorkflowResumed
     * - 工作流终止：onWorkflowKilled
     * 
     * 执行特点：
     * - 顺序执行：按监听器注册顺序依次执行
     * - 异常隔离：单个监听器异常不影响其他监听器
     * - 异步处理：部分监听器可配置为异步执行
     * 
     * @param workflowExecuteContextBuilder 工作流执行上下文建造者
     * 
     * 类比：就像为项目配置各种职能部门的联络人，当项目状态发生变化时，
     * 自动通知财务、人事、采购、质检等部门，让他们及时了解项目进展。
     */
    protected void assembleWorkflowInstanceLifecycleListeners(
                                                              final WorkflowExecuteContextBuilder workflowExecuteContextBuilder) {
        workflowExecuteContextBuilder.setWorkflowInstanceLifecycleListeners(workflowLifecycleListeners);
    }

    /**
     * 组装工作流定义
     * 
     * 根据命令中的工作流定义编码和版本号，从数据库中查询对应的工作流定义信息。
     * 工作流定义包含了工作流的元数据，是执行工作流的基础蓝图。
     * 
     * 查询逻辑：
     * 1. 从命令中提取工作流定义编码和版本号
     * 2. 通过工作流定义日志DAO查询指定版本的定义
     * 3. 验证查询结果的有效性，防止引用不存在的定义
     * 4. 将查询到的定义设置到执行上下文中
     * 
     * 工作流定义内容：
     * - 基本信息：名称、描述、创建者、项目归属
     * - 执行配置：超时时间、失败策略、通知设置
     * - 任务列表：包含的所有任务定义及其配置
     * - 依赖关系：任务之间的前后依赖关系
     * - 全局参数：工作流级别的参数定义
     * - 调度设置：定时调度的配置信息
     * 
     * 版本管理：
     * - 支持多版本：同一工作流可以有多个版本
     * - 版本隔离：不同版本的执行互不影响
     * - 版本回退：可以执行历史版本的工作流
     * - 版本追踪：记录每次执行使用的具体版本
     * 
     * 异常处理：
     * - 定义不存在：抛出IllegalArgumentException
     * - 版本不匹配：确保指定版本的定义存在
     * - 数据完整性：验证定义数据的完整性
     * 
     * @param workflowExecuteContextBuilder 工作流执行上下文建造者
     * @throws IllegalArgumentException 当指定的工作流定义不存在时
     * 
     * 类比：就像建筑工程开工前要先获取建筑图纸，图纸包含了建筑的
     * 设计方案、材料清单、施工要求等关键信息，是施工的根本依据。
     */
    protected void assembleWorkflowDefinition(
                                              final WorkflowExecuteContextBuilder workflowExecuteContextBuilder) {
        final Command command = workflowExecuteContextBuilder.getCommand();
        final long workflowDefinitionCode = command.getWorkflowDefinitionCode();
        final int workflowDefinitionVersion = command.getWorkflowDefinitionVersion();

        final WorkflowDefinition workflowDefinition = workflowDefinitionLogDao.queryByDefinitionCodeAndVersion(
                workflowDefinitionCode,
                workflowDefinitionVersion);
        checkArgument(workflowDefinition != null,
                "Cannot find the WorkflowDefinition: [" + workflowDefinitionCode + ":" + workflowDefinitionVersion
                        + "]");
        workflowExecuteContextBuilder.setWorkflowDefinition(workflowDefinition);

    }

    /**
     * 组装工作流图
     * 
     * 基于工作流定义创建工作流图对象，将定义中的任务和依赖关系转换为可遍历的图结构。
     * 工作流图是执行引擎进行任务调度和状态管理的核心数据结构。
     * 
     * 图构建过程：
     * 1. 解析工作流定义：提取任务定义和任务关系信息
     * 2. 创建图节点：为每个任务定义创建对应的图节点
     * 3. 建立图边：根据任务关系建立有向边，表示依赖关系
     * 4. 验证图结构：检查是否存在循环依赖等异常情况
     * 5. 优化图结构：对图进行拓扑排序等优化操作
     * 
     * 图的特性：
     * - 有向无环图(DAG)：确保工作流能够正常执行完成
     * - 节点表示任务：每个节点包含任务的完整定义信息
     * - 边表示依赖：有向边表示任务间的前后依赖关系
     * - 支持并行：没有依赖关系的任务可以并行执行
     * - 起始节点：没有前驱的节点作为工作流的入口点
     * 
     * 图的应用：
     * - 执行调度：确定任务的执行顺序和并发策略
     * - 状态传播：根据依赖关系传播任务状态变化
     * - 路径分析：分析任务执行的关键路径和影响范围
     * - 故障恢复：基于图结构进行智能的故障恢复
     * 
     * 工厂模式：
     * - 使用WorkflowGraphFactory创建图实例
     * - 封装复杂的图构建逻辑
     * - 支持不同类型的图实现
     * - 便于扩展和测试
     * 
     * @param workflowExecuteContextBuilder 工作流执行上下文建造者
     * 
     * 类比：就像根据建筑图纸绘制施工网络图，将各个施工任务和它们的
     * 先后顺序用网络图表示，为项目经理安排施工计划提供直观的依据。
     */
    protected void assembleWorkflowGraph(
                                         final WorkflowExecuteContextBuilder workflowExecuteContextBuilder) {
        final WorkflowDefinition workflowDefinition = workflowExecuteContextBuilder.getWorkflowDefinition();
        workflowExecuteContextBuilder.setWorkflowGraph(workflowGraphFactory.createWorkflowGraph(workflowDefinition));
    }

    /**
     * 组装工作流实例（抽象方法）
     * 
     * 这是一个抽象方法，由具体的命令处理器子类实现。不同类型的命令需要创建
     * 不同类型的工作流实例，体现了模板方法模式中的变化点。
     * 
     * 不同命令类型的实例创建策略：
     * - 首次执行命令：创建全新的工作流实例，所有任务都处于待执行状态
     * - 重跑命令：基于历史实例创建新实例，可能保留部分任务状态
     * - 恢复命令：恢复已暂停的工作流实例，保持原有执行状态
     * - 补数据命令：创建指定时间范围的多个工作流实例
     * - 重试命令：基于失败实例创建重试实例，重置失败任务状态
     * 
     * 实例包含的信息：
     * - 基本信息：实例ID、名称、状态、开始时间等
     * - 执行参数：全局参数、用户参数、系统参数等
     * - 调度信息：调度时间、执行节点、优先级等
     * - 关联关系：关联的工作流定义、项目、租户等
     * - 执行配置：超时设置、失败策略、通知配置等
     * 
     * 状态管理：
     * - 初始状态：根据命令类型设置合适的初始状态
     * - 状态转换：定义实例在执行过程中的状态流转
     * - 状态持久化：确保状态变化及时保存到数据库
     * - 状态一致性：保证内存和数据库状态的一致性
     * 
     * 子类职责：
     * - 创建合适的工作流实例对象
     * - 设置实例的初始状态和参数
     * - 处理特定命令类型的业务逻辑
     * - 确保实例数据的完整性和正确性
     * 
     * @param workflowExecuteContextBuilder 工作流执行上下文建造者
     * 
     * 类比：就像不同类型的项目需要创建不同的项目实例卡，新项目创建
     * 空白项目卡，重启项目要复制原项目卡，恢复项目要找回暂停的项目卡。
     */
    protected abstract void assembleWorkflowInstance(
                                                     final WorkflowExecuteContextBuilder workflowExecuteContextBuilder);

    /**
     * 组装工作流执行图（抽象方法）
     * 
     * 这是另一个关键的抽象方法，用于创建工作流的执行图结构。执行图是在基础
     * 工作流图的基础上，结合具体的执行场景和任务实例状态构建的执行计划。
     * 
     * 执行图与基础图的区别：
     * - 基础图：静态的任务定义和依赖关系，来源于工作流定义
     * - 执行图：动态的执行计划，结合实例状态和执行策略
     * - 基础图不变：一个定义版本对应一个固定的基础图
     * - 执行图可变：同样的基础图在不同执行场景下有不同的执行图
     * 
     * 不同命令的执行图构建策略：
     * - 首次执行：基于基础图创建完整的执行图，所有任务待执行
     * - 部分重跑：只包含需要重新执行的任务及其依赖任务
     * - 故障恢复：从失败点开始构建执行图，跳过已成功的任务
     * - 条件执行：基于条件判断动态裁剪执行图
     * - 并行优化：根据资源情况调整任务的并行度
     * 
     * 执行图的构成：
     * - 任务实例节点：包含任务实例的状态和执行信息
     * - 依赖关系边：表示任务实例间的依赖关系
     * - 执行策略：每个节点的执行策略和参数
     * - 资源需求：任务执行所需的资源配置
     * - 执行顺序：基于依赖关系的执行拓扑序列
     * 
     * 图的优化：
     * - 并行优化：识别可并行执行的任务集合
     * - 路径优化：优化关键路径的执行效率
     * - 资源优化：根据资源约束调整执行计划
     * - 缓存优化：利用已有结果避免重复计算
     * 
     * 动态调整：
     * - 执行过程中根据实际情况动态调整图结构
     * - 支持任务的动态添加、删除、修改
     * - 处理执行过程中的异常和中断情况
     * - 实现执行图的实时重构和优化
     * 
     * @param workflowExecuteContextBuilder 工作流执行上下文建造者
     * 
     * 类比：就像制定具体的施工作业计划，基础图是施工网络图，执行图是
     * 考虑了工人数量、设备情况、天气条件后制定的具体施工时间表。
     */
    protected abstract void assembleWorkflowExecutionGraph(
                                                           final WorkflowExecuteContextBuilder workflowExecuteContextBuilder);

    /**
     * 从工作流实例中解析起始节点列表
     * 
     * 从工作流实例的命令参数中提取起始节点信息，并转换为任务名称列表。
     * 起始节点决定了工作流执行的入口点，不同的执行场景可能有不同的起始节点。
     * 
     * 解析流程：
     * 1. 从工作流实例中获取命令参数JSON字符串
     * 2. 将JSON字符串解析为命令参数对象
     * 3. 验证命令参数的有效性，确保不为空
     * 4. 提取起始节点的编码列表
     * 5. 通过工作流图将编码转换为任务名称
     * 6. 返回任务名称的列表
     * 
     * 起始节点的应用场景：
     * - 全量执行：起始节点为空，表示从图的自然起始点开始
     * - 部分执行：指定特定任务作为起始点，只执行其后的任务
     * - 重跑特定任务：以失败任务作为起始点进行重新执行
     * - 跳过前置任务：从中间某个任务开始执行后续流程
     * - 条件分支：根据条件选择不同的起始执行路径
     * 
     * 数据转换：
     * - 编码到名称：任务编码是系统内部标识，名称是用户可读标识
     * - 批量转换：使用Stream API进行高效的批量数据转换
     * - 空值处理：妥善处理空列表和无效编码的情况
     * - 类型安全：确保转换过程中的数据类型正确性
     * 
     * 异常处理：
     * - 参数无效：命令参数为空或格式错误时抛出异常
     * - 编码无效：起始节点编码在图中不存在时的处理
     * - JSON解析失败：命令参数JSON格式错误时的异常处理
     * 
     * @param workflowExecuteContextBuilder 工作流执行上下文建造者
     * @return 起始节点的任务名称列表，如果没有指定则返回空列表
     * @throws IllegalArgumentException 当命令参数无效时
     * 
     * 类比：就像解读项目启动指令，从项目计划书中找出需要首先开工的
     * 任务清单，确定项目的具体启动点和执行范围。
     */
    protected List<String> parseStartNodesFromWorkflowInstance(
                                                               final WorkflowExecuteContextBuilder workflowExecuteContextBuilder) {
        final WorkflowInstance workflowInstance = workflowExecuteContextBuilder.getWorkflowInstance();
        final ICommandParam commandParam = parseObject(workflowInstance.getCommandParam(), ICommandParam.class);
        checkArgument(commandParam != null, "Invalid command param : " + workflowInstance.getCommandParam());
        List<Long> startCodes = commandParam.getStartNodes();
        if (CollectionUtils.isEmpty(startCodes)) {
            return Collections.emptyList();
        }
        final IWorkflowGraph workflowGraph = workflowExecuteContextBuilder.getWorkflowGraph();
        return startCodes
                .stream()
                .map(workflowGraph::getTaskNodeByCode)
                .map(TaskDefinition::getName)
                .collect(Collectors.toList());

    }

    /**
     * 获取有效的任务实例列表
     * 
     * 查询指定工作流实例下所有有效的任务实例。有效的任务实例是指状态正常、
     * 数据完整且可以参与执行流程的任务实例，排除已删除或损坏的记录。
     * 
     * 有效性判断标准：
     * - 数据完整性：任务实例的基本字段都有有效值
     * - 状态有效性：任务状态在预期的状态范围内
     * - 关联完整性：与工作流实例、任务定义的关联关系正常
     * - 逻辑一致性：任务的执行逻辑和依赖关系保持一致
     * - 时间有效性：任务的时间戳信息合理且连续
     * 
     * 使用场景：
     * - 状态恢复：系统重启后恢复工作流的执行状态
     * - 依赖分析：分析任务间的依赖关系和执行顺序
     * - 进度统计：计算工作流的整体执行进度
     * - 错误排查：查找和定位执行过程中的异常任务
     * - 重试机制：确定哪些任务需要重新执行
     * 
     * 查询优化：
     * - 索引利用：基于工作流实例ID的索引查询
     * - 批量获取：一次性获取所有相关任务实例
     * - 内存友好：通过DAO层控制结果集大小
     * - 缓存考虑：对于频繁查询的数据考虑缓存策略
     * 
     * 数据过滤：
     * - DAO层过滤：在数据库层面过滤掉无效记录
     * - 业务层验证：在业务逻辑层进一步验证数据有效性
     * - 状态筛选：根据任务状态进行筛选
     * - 类型筛选：根据任务类型进行分类处理
     * 
     * @param workflowInstance 工作流实例对象
     * @return 该工作流实例下的所有有效任务实例列表
     * 
     * 类比：就像清点项目中所有正在进行和已完成的有效任务，
     * 排除已取消、已删除或数据异常的任务记录。
     */
    protected List<TaskInstance> getValidTaskInstance(final WorkflowInstance workflowInstance) {
        return taskInstanceDao.queryValidTaskListByWorkflowInstanceId(
                workflowInstance.getId());
    }

    /**
     * 组装项目信息
     * 
     * 根据工作流定义中的项目编码，查询并设置项目上下文信息。
     * 项目信息提供了工作流执行所需的权限、资源、配置等上下文环境。
     * 
     * 查询流程：
     * 1. 从工作流定义中提取项目编码
     * 2. 通过项目DAO查询项目详细信息
     * 3. 验证项目的存在性和有效性
     * 4. 将项目信息设置到执行上下文中
     * 
     * 项目上下文的作用：
     * - 权限控制：确定用户对工作流的操作权限
     * - 资源分配：获取项目可用的计算资源配额
     * - 配置继承：继承项目级别的全局配置参数
     * - 安全策略：应用项目的安全策略和访问控制
     * - 租户隔离：实现多租户环境下的数据隔离
     * 
     * 项目包含的信息：
     * - 基本信息：项目名称、描述、创建者、创建时间
     * - 权限信息：项目成员、角色权限、访问控制列表
     * - 资源配置：CPU配额、内存限制、存储空间
     * - 环境配置：数据源、队列、告警组等环境参数
     * - 安全配置：加密策略、审计规则、合规要求
     * 
     * 业务关联：
     * - 一个项目可以包含多个工作流定义
     * - 项目是权限管理的基本单位
     * - 项目提供资源隔离和配额管理
     * - 项目支持团队协作和角色分工
     * 
     * 异常处理：
     * - 项目不存在：抛出IllegalArgumentException
     * - 项目已禁用：检查项目状态的有效性
     * - 权限不足：验证当前用户的项目访问权限
     * - 配额限制：检查项目资源配额是否充足
     * 
     * @param workflowExecuteContextBuilder 工作流执行上下文建造者
     * @throws IllegalArgumentException 当项目不存在时
     * 
     * 类比：就像确认工程项目的归属部门，获取该部门的预算额度、
     * 人员配置、设备资源等信息，为工程执行提供必要的支持保障。
     */
    protected void assembleProject(
                                   final WorkflowExecuteContextBuilder workflowExecuteContextBuilder) {
        final WorkflowDefinition workflowDefinition = workflowExecuteContextBuilder.getWorkflowDefinition();
        final Project project = projectDao.queryByCode(workflowDefinition.getProjectCode());
        checkArgument(project != null, "Cannot find the project code: " + workflowDefinition.getProjectCode());
        workflowExecuteContextBuilder.setProject(project);
    }

}
