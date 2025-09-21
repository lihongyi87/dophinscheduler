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

package org.apache.dolphinscheduler.server.master.engine.task.lifecycle.handler;

import org.apache.dolphinscheduler.plugin.task.api.enums.TaskExecutionStatus;
import org.apache.dolphinscheduler.server.master.engine.ILifecycleEventHandler;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.AbstractTaskLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.task.runnable.ITaskExecutionRunnable;
import org.apache.dolphinscheduler.server.master.engine.task.statemachine.ITaskStateAction;
import org.apache.dolphinscheduler.server.master.engine.task.statemachine.TaskStateActionFactory;
import org.apache.dolphinscheduler.server.master.engine.workflow.runnable.IWorkflowExecutionRunnable;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;

/**
 * 抽象任务生命周期事件处理器
 * 
 * 这是所有任务生命周期事件处理器的抽象基类，提供了事件处理的通用框架和模板方法。
 * 类比：就像一个专业的项目事件处理中心的总管，定义了处理各类项目事件的标准流程。
 * 
 * 核心职责：
 * 1. 事件分发：接收并分发各种任务生命周期事件
 * 2. 状态管理：根据当前任务状态选择合适的处理策略
 * 3. 行为调度：协调任务状态动作和事件处理逻辑
 * 4. 日志记录：统一记录事件处理过程和结果
 * 5. 模板定义：为具体事件处理器提供标准化的处理模板
 * 
 * 设计模式应用：
 * - 模板方法模式：定义事件处理的通用流程，子类实现具体处理逻辑
 * - 策略模式：根据任务状态选择不同的处理策略
 * - 工厂模式：通过TaskStateActionFactory获取状态对应的处理动作
 * - 依赖注入：通过Spring注入TaskStateActionFactory等依赖组件
 * 
 * 处理流程：
 * 1. 事件接收：接收工作流执行器传来的任务生命周期事件
 * 2. 状态获取：从任务执行器中获取当前任务的执行状态
 * 3. 动作选择：通过状态动作工厂获取对应状态的处理动作
 * 4. 委托处理：将具体处理逻辑委托给子类实现
 * 5. 日志记录：记录事件处理的完成情况
 * 
 * 扩展机制：
 * - 泛型支持：支持不同类型的任务生命周期事件
 * - 抽象方法：子类实现具体的事件处理逻辑
 * - 状态感知：能够感知任务的当前状态并做相应处理
 * - 统一日志：提供统一的日志记录格式
 * 
 * 使用场景：
 * - 任务启动：处理任务启动事件，初始化执行环境
 * - 任务分发：处理任务分发事件，选择执行器并发送任务
 * - 状态变更：处理任务状态变更事件，更新任务状态
 * - 异常处理：处理任务异常事件，执行容错和恢复逻辑
 * - 完成处理：处理任务完成事件，清理资源并通知后续任务
 * 
 * 类比理解：
 * 就像医院的急诊处理中心：
 * - 接收患者：接收各种紧急情况的患者（事件）
 * - 判断病情：根据患者当前状态判断病情严重程度（状态判断）
 * - 分配科室：根据病情选择合适的专科医生（选择处理动作）
 * - 委托治疗：将患者交给专科医生进行具体治疗（委托处理）
 * - 记录档案：记录整个处理过程和结果（日志记录）
 * 
 * @param <T> 具体的任务生命周期事件类型，必须继承自AbstractTaskLifecycleEvent
 */
@Slf4j
public abstract class AbstractTaskLifecycleEventHandler<T extends AbstractTaskLifecycleEvent>
        implements
            ILifecycleEventHandler<T> {

    /**
     * 任务状态动作工厂
     * 
     * 用于根据任务的当前状态获取相应的处理动作。
     * 这是连接任务状态和处理逻辑的桥梁，实现状态驱动的任务处理。
     * 
     * 工厂作用：
     * - 状态映射：将任务执行状态映射为具体的处理动作
     * - 策略选择：根据不同状态选择不同的处理策略
     * - 解耦设计：将状态判断逻辑与具体处理逻辑分离
     * - 统一管理：集中管理所有状态对应的处理动作
     * 
     * 类比：就像医院的诊疗指南，根据患者症状快速找到对应的治疗方案。
     */
    @Autowired
    protected TaskStateActionFactory taskStateActionFactory;

    /**
     * 处理任务生命周期事件的模板方法
     * 
     * 这是事件处理的核心模板方法，定义了处理任务生命周期事件的标准流程。
     * 所有具体的事件处理都会经过这个统一的处理管道。
     * 
     * 处理流程：
     * 1. 事件解析：从事件中提取任务执行器和相关信息
     * 2. 状态获取：获取任务实例的当前执行状态
     * 3. 动作选择：通过状态动作工厂获取对应的处理动作
     * 4. 委托处理：调用抽象方法，让子类实现具体处理逻辑
     * 5. 日志记录：记录事件处理的完成情况和结果
     * 
     * 设计优势：
     * - 标准化流程：所有事件处理都遵循相同的标准流程
     * - 状态感知：根据任务当前状态选择合适的处理策略
     * - 日志统一：提供统一格式的处理日志，便于监控和调试
     * - 扩展性：通过抽象方法支持不同类型事件的个性化处理
     * 
     * 日志信息：
     * - 记录任务名称、事件类型和当前状态
     * - 便于跟踪任务执行过程和问题排查
     * - 提供统一的日志格式，支持日志分析工具
     * 
     * 类比：就像医院急诊科的标准处理流程，每个病人都要经过
     * 挂号、分诊、诊断、治疗、记录的标准化流程。
     * 
     * @param workflowExecutionRunnable 工作流执行器，提供工作流级别的上下文信息
     * @param event 要处理的任务生命周期事件，包含事件相关的所有信息
     */
    @Override
    public void handle(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                       final T event) {
        final ITaskExecutionRunnable taskExecutionRunnable = event.getTaskExecutionRunnable();
        final TaskExecutionStatus state = taskExecutionRunnable.getTaskInstance().getState();
        final ITaskStateAction taskStateAction = taskStateActionFactory.getTaskStateAction(state);
        handle(taskStateAction, workflowExecutionRunnable, taskExecutionRunnable, event);
        log.info("Fired task {} {} with state {}",
                taskExecutionRunnable.getName(),
                event,
                state.name());
    }

    /**
     * 具体事件处理的抽象方法
     * 
     * 这是留给子类实现的抽象方法，用于实现具体类型事件的处理逻辑。
     * 每个具体的事件处理器都需要实现这个方法来定义自己的处理行为。
     * 
     * 方法参数：
     * - taskStateAction：根据任务当前状态选择的处理动作策略
     * - workflowExecutionRunnable：工作流执行器，提供工作流上下文
     * - taskExecutionRunnable：任务执行器，提供任务级别的操作接口
     * - event：具体的生命周期事件，包含事件特定的信息
     * 
     * 实现要求：
     * - 状态感知：根据taskStateAction执行相应的状态处理逻辑
     * - 上下文利用：充分利用工作流和任务执行器提供的上下文信息
     * - 事件特化：根据具体事件类型实现相应的处理逻辑
     * - 异常处理：妥善处理执行过程中可能出现的异常情况
     * 
     * 设计意图：
     * - 关注分离：将通用处理流程与具体处理逻辑分离
     * - 多态支持：支持不同类型事件的多态处理
     * - 策略模式：结合状态动作实现策略模式
     * - 模板方法：作为模板方法模式的具体实现点
     * 
     * 类比：就像医院不同科室的专科医生，都遵循医院的标准流程，
     * 但在具体诊疗环节会根据自己的专业特长实施不同的治疗方案。
     * 
     * @param taskStateAction 任务状态对应的处理动作策略
     * @param workflowExecutionRunnable 工作流执行器
     * @param taskExecutionRunnable 任务执行器
     * @param event 要处理的具体生命周期事件
     */
    public abstract void handle(final ITaskStateAction taskStateAction,
                                final IWorkflowExecutionRunnable workflowExecutionRunnable,
                                final ITaskExecutionRunnable taskExecutionRunnable,
                                final T event);

}
