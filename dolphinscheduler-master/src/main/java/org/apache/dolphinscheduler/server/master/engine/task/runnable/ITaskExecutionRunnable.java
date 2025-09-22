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

import org.apache.dolphinscheduler.dao.entity.TaskDefinition;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.dao.entity.WorkflowInstance;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.server.master.engine.WorkflowEventBus;
import org.apache.dolphinscheduler.server.master.engine.graph.IWorkflowExecutionGraph;
import org.apache.dolphinscheduler.server.master.engine.workflow.runnable.WorkflowExecutionRunnable;

/**
 * 任务执行运行器接口
 *
 * 定义了任务执行过程中的核心操作接口，是任务执行管理的核心抽象。
 * 该接口代表工作流执行器({@link WorkflowExecutionRunnable})中的一个正在运行的任务实例。
 *
 * 设计思想：
 * - 生命周期管理：定义任务从初始化到完成的完整生命周期操作
 * - 状态控制：提供任务执行状态的查询和控制方法
 * - 容错处理：支持重试、故障转移等容错机制
 * - 可比较性：实现Comparable接口，支持任务优先级排序
 *
 * 核心功能：
 * 1. 任务实例初始化：创建和配置任务执行实例
 * 2. 执行上下文管理：构建任务执行所需的完整上下文
 * 3. 重试机制：支持任务失败后的自动重试
 * 4. 故障转移：处理执行器故障时的任务迁移
 * 5. 生命周期控制：暂停、终止等执行控制操作
 *
 * 类比理解：
 * 就像一个项目经理的岗位职责描述，定义了项目管理的各种标准操作：
 * - 项目启动（初始化）
 * - 进度跟踪（状态查询）
 * - 问题处理（重试、故障转移）
 * - 项目控制（暂停、终止）
 *
 * 实现类需要提供这些操作的具体实现逻辑。
 */
public interface ITaskExecutionRunnable
        extends
            Comparable<ITaskExecutionRunnable> {

    /**
     * 获取任务实例ID
     *
     * 返回当前任务实例的唯一标识符。需要注意的是，由于任务实例可能会在重试、
     * 故障转移等场景下重新生成，因此ID可能会发生变化。
     *
     * 使用场景：
     * - 日志记录：在日志中标识具体的任务实例
     * - 状态跟踪：通过ID查询任务的执行状态
     * - 数据库操作：用于数据库记录的更新和查询
     * - 监控告警：作为监控指标的维度标识
     *
     * 注意事项：
     * - ID可变性：任务重试或故障转移时会生成新的实例ID
     * - 唯一性：在同一时刻，ID在系统中是唯一的
     * - 生命周期：ID的有效期与任务实例的生命周期一致
     *
     * @return 任务实例的唯一标识符
     */
    default int getId() {
        return getTaskInstance().getId();
    }

    /**
     * 获取任务名称
     *
     * 返回任务定义中指定的任务名称，用于标识任务的业务含义。
     * 任务名称在工作流定义中是唯一的，便于用户识别和管理。
     *
     * 使用场景：
     * - 用户界面：在UI中显示任务的可读名称
     * - 日志输出：在日志中使用友好的任务名称
     * - 监控告警：使用任务名称作为告警消息的标识
     * - 调试定位：通过任务名称快速定位问题任务
     *
     * @return 任务的业务名称
     */
    default String getName() {
        return getTaskDefinition().getName();
    }

    /**
     * 检查任务实例是否已初始化
     *
     * 判断当前任务执行器中的任务实例是否已经创建和初始化完成。
     * 任务实例的初始化状态决定了可以执行哪些后续操作。
     *
     * 初始化状态说明：
     * - 未初始化：任务执行器刚创建，还没有触发过任务实例创建
     * - 已初始化：任务实例已创建，包括首次运行、故障转移、恢复等场景
     *
     * 使用场景：
     * - 前置检查：在执行重试、故障转移等操作前的状态验证
     * - 流程控制：根据初始化状态决定后续的执行路径
     * - 异常处理：避免在未初始化状态下执行需要任务实例的操作
     * - 状态同步：确保任务实例与执行器状态的一致性
     *
     * 类比：检查员工是否已经入职报到，只有入职的员工才能分配具体工作。
     *
     * @return true表示任务实例已初始化，false表示尚未初始化
     */
    boolean isTaskInstanceInitialized();

    /**
     * 初始化首次运行的任务实例
     *
     * 为首次执行的任务创建和初始化任务实例。这是任务生命周期的起始操作，
     * 将使用{@link FirstRunTaskInstanceFactory}工厂来创建全新的任务实例。
     *
     * 执行条件：
     * - 任务执行器已创建但任务实例尚未初始化
     * - 这是任务的第一次执行（非重试、非故障转移）
     *
     * 初始化内容：
     * - 创建新的任务实例记录
     * - 设置任务的基本属性和配置
     * - 初始化任务状态为提交成功
     * - 记录首次提交时间
     *
     * 异常情况：
     * - 如果任务实例已初始化，调用此方法会抛出异常
     * - 如果缺少必要的任务定义或工作流实例信息，创建会失败
     *
     * 类比：为新员工办理入职手续，创建员工档案和工号。
     */
    void initializeFirstRunTaskInstance();

    /**
     * 初始化任务执行上下文
     *
     * 创建任务执行时所需的完整上下文信息({@link TaskExecutionContext})。
     * 执行上下文包含了任务执行器执行任务时需要的所有运行时信息。
     *
     * 执行时机：
     * - 必须在任务分发(dispatch)阶段之前完成
     * - 任务实例已初始化后才能创建执行上下文
     *
     * 上下文内容：
     * - 任务基本信息：任务ID、名称、类型、参数等
     * - 工作流信息：工作流实例、定义、全局参数等
     * - 执行环境：主机信息、资源配置、环境变量等
     * - 依赖关系：前置任务、数据依赖等信息
     * - 运行时参数：合并后的完整参数信息
     *
     * 重要性：
     * - 执行上下文是任务执行的"工作台"，包含所有必需的工具和材料
     * - 缺少执行上下文，任务执行器无法正确执行任务
     * - 上下文的完整性直接影响任务执行的成功率
     *
     * 类比：为演员准备完整的表演道具和背景资料，确保能完美演出。
     */
    void initializeTaskExecutionContext();

    /**
     * 检查任务实例是否可以重试
     *
     * 判断当前任务是否还有重试机会，这是任务容错机制的核心判断逻辑。
     * 重试机制能够自动恢复由于临时性问题导致的任务执行失败。
     *
     * 判断依据：
     * - 当前重试次数 < 最大重试次数 = 可以重试
     * - 每次重试会递增重试计数器，直到达到上限
     *
     * 重试机制的意义：
     * - 提高成功率：自动处理网络抖动、资源临时不足等问题
     * - 避免死循环：通过重试次数限制防止无效的无限重试
     * - 提升稳定性：显著提升分布式系统的容错能力
     *
     * 示例：
     * - maxRetryTimes=3时，retryTimes为0、1、2都可以重试
     * - 当retryTimes达到3时，不能再重试
     *
     * 类比：考试补考机会，只要还没用完规定的补考次数，就还可以申请补考。
     *
     * @return true表示可以重试，false表示已达到最大重试次数限制
     */
    boolean isTaskInstanceCanRetry();

    /**
     * 执行任务重试
     *
     * 当任务执行失败但仍有重试机会时，通过此方法进行重试处理。
     * 这是任务容错机制的核心实现，能够自动恢复临时性的执行失败。
     *
     * 重试流程：
     * 1. 使用重试任务实例工厂创建新的重试实例
     * 2. 继承原任务实例的配置和历史信息
     * 3. 递增重试次数并重置执行状态
     * 4. 发布任务启动事件触发重新执行
     *
     * 重试特点：
     * - 保留历史：重试实例保留原任务的配置和历史记录
     * - 状态重置：清除之前的失败状态，重新开始执行
     * - 计数递增：自动更新重试次数，用于后续重试判断
     * - 异步启动：通过事件机制异步启动重试执行
     *
     * 前置条件：
     * - 任务实例必须已初始化
     * - 任务必须还有重试机会（未达到最大重试次数）
     *
     * 类比：学生重考，保留学号和课程信息，清除不及格记录，重新安排考试。
     */
    void retry();

    /**
     * 执行故障转移处理
     *
     * 当Master节点故障重启或任务执行器异常时，通过故障转移机制恢复任务执行。
     * 这是分布式系统高可用性的核心保障，确保任务能在节点故障后继续执行。
     *
     * 故障转移策略：
     * 1. 热故障转移：尝试直接接管正在运行的任务，保持执行连续性
     * 2. 冷故障转移：如果接管失败，创建新实例重新开始执行
     *
     * 执行流程：
     * 1. 尝试从原执行器接管任务
     * 2. 如果接管成功，直接返回继续执行
     * 3. 如果接管失败，使用故障转移工厂创建新实例
     * 4. 发布启动事件，在新节点重新开始执行
     *
     * 故障转移优势：
     * - 无缝恢复：尽可能保持任务执行的连续性
     * - 状态保持：保留任务的历史信息和配置
     * - 自动化：无需人工干预，系统自动处理
     * - 双重保障：接管和重建两种策略确保高可用性
     *
     * 前置条件：
     * - 任务实例必须已初始化
     * - 系统检测到需要进行故障转移
     *
     * 类比：接力赛中的棒次传递，前一棒受伤时先尝试就地交接，
     * 不行就回到起点重新开始。
     */
    void failover();

    /**
     * 暂停任务执行
     *
     * 通过发布任务暂停生命周期事件来暂停正在执行的任务。
     * 这是任务生命周期管理的重要功能，允许在执行过程中进行人工干预。
     *
     * 暂停特点：
     * - 优雅暂停：不是强制终止，而是通知执行器进行优雅暂停
     * - 状态保持：暂停时保留任务的执行状态和进度
     * - 可恢复性：暂停后的任务可以通过恢复操作继续执行
     * - 事件驱动：通过事件总线异步处理暂停逻辑
     *
     * 使用场景：
     * - 维护窗口：系统维护期间暂停任务执行
     * - 资源调度：资源紧张时暂停低优先级任务
     * - 问题排查：发现异常时暂停任务进行调查
     * - 人工干预：需要人工确认或调整时暂停等待
     *
     * 实现机制：
     * - 暂停事件会被相应的事件监听器捕获
     * - 监听器调用任务执行器的暂停接口
     * - 执行器在合适时机停止执行并保存状态
     *
     * 类比：DVD播放器的暂停按钮，停止播放但记住当前位置，
     * 可以从暂停的地方继续播放。
     */
    void pause();

    /**
     * 终止任务执行
     *
     * 通过发布任务终止生命周期事件来强制终止正在执行的任务。
     * 这是任务生命周期管理的最终手段，用于处理无法通过暂停解决的紧急情况。
     *
     * 终止特点：
     * - 强制性：与暂停不同，终止是不可恢复的强制停止
     * - 最终性：终止后任务进入最终状态，无法继续执行
     * - 资源清理：终止时会清理任务占用的资源和临时数据
     * - 状态记录：记录终止时间和原因，用于后续分析
     *
     * 使用场景：
     * - 紧急停止：任务行为异常需要立即停止
     * - 资源回收：任务占用过多资源影响系统稳定性
     * - 错误恢复：任务进入死锁或无限循环状态
     * - 手动干预：用户明确要求停止任务执行
     * - 系统关闭：系统关闭前停止所有运行中的任务
     *
     * 与暂停的区别：
     * - 暂停：可恢复的临时停止，保留执行上下文
     * - 终止：不可恢复的永久停止，清理资源和状态
     *
     * 实现机制：
     * - 终止事件被监听器捕获后立即执行
     * - 执行器收到终止信号后立即停止并清理
     * - 任务状态被标记为已终止的最终状态
     *
     * 类比：电脑的强制关机，立即断电停止所有程序，
     * 不保存工作状态，下次开机需要重新开始。
     */
    void kill();

    /**
     * 获取工作流事件总线
     *
     * 返回工作流事件总线的引用，用于发布和订阅工作流相关的各种事件。
     * 事件总线是系统内部通信的核心机制，实现组件间的解耦通信。
     *
     * @return 工作流事件总线实例
     */
    WorkflowEventBus getWorkflowEventBus();

    /**
     * 获取工作流执行图
     *
     * 返回工作流执行图的引用，包含工作流中所有任务的依赖关系和执行顺序信息。
     * 执行图用于判断任务的前置依赖是否满足，以及确定任务的执行顺序。
     *
     * @return 工作流执行图实例
     */
    IWorkflowExecutionGraph getWorkflowExecutionGraph();

    /**
     * 获取工作流实例
     *
     * 返回当前任务所属的工作流实例，包含工作流的运行时信息。
     * 工作流实例提供了任务执行所需的工作流级别的上下文信息。
     *
     * @return 工作流实例
     */
    WorkflowInstance getWorkflowInstance();

    /**
     * 获取任务实例
     *
     * 返回当前的任务实例，包含任务的运行时状态和执行信息。
     * 任务实例是任务执行的核心数据载体，记录了任务的完整执行过程。
     *
     * @return 任务实例，可能为null（如果尚未初始化）
     */
    TaskInstance getTaskInstance();

    /**
     * 获取任务定义
     *
     * 返回任务的元数据定义，包含任务的类型、参数、配置等静态信息。
     * 任务定义是任务执行的模板，定义了任务应该如何执行。
     *
     * @return 任务定义
     */
    TaskDefinition getTaskDefinition();

    /**
     * 获取任务执行上下文
     *
     * 返回任务执行上下文，包含任务执行时所需的所有运行时信息。
     * 执行上下文是任务执行器执行任务的重要输入信息。
     *
     * @return 任务执行上下文，可能为null（如果尚未初始化）
     */
    TaskExecutionContext getTaskExecutionContext();
}
