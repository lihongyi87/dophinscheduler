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

import org.apache.dolphinscheduler.dao.entity.TaskDefinition;
import org.apache.dolphinscheduler.server.master.engine.ILifecycleEventType;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.TaskLifecycleEventType;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event.TaskStartLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event.TaskTimeoutLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.task.runnable.ITaskExecutionRunnable;
import org.apache.dolphinscheduler.server.master.engine.task.statemachine.ITaskStateAction;
import org.apache.dolphinscheduler.server.master.engine.workflow.runnable.IWorkflowExecutionRunnable;

import lombok.extern.slf4j.Slf4j;

import org.springframework.stereotype.Component;

/**
 * 任务启动生命周期事件处理器
 * 
 * 这是专门处理任务启动事件的处理器，负责任务启动时的初始化和监控工作。
 * 类比：就像项目启动时的项目经理，负责确保项目顺利启动并建立监控机制。
 * 
 * 核心职责：
 * 1. 任务初始化：确保任务实例在启动前已正确初始化
 * 2. 启动处理：协调任务状态动作执行实际的启动逻辑
 * 3. 超时监控：为任务建立超时监控机制，防止任务无限运行
 * 4. 事件匹配：识别并处理任务启动类型的生命周期事件
 * 5. 状态转换：将任务状态从待启动转换为运行中
 * 
 * 处理特点：
 * - 初始化保证：确保任务实例在启动前已完成必要的初始化
 * - 状态驱动：根据任务当前状态选择合适的启动策略
 * - 监控建立：为任务建立超时监控，保证任务不会无限运行
 * - 事件委托：将具体启动逻辑委托给任务状态动作执行
 * 
 * 启动流程：
 * 1. 事件接收：接收任务启动生命周期事件
 * 2. 初始化检查：检查任务实例是否已初始化，未初始化则执行初始化
 * 3. 监控建立：根据任务配置建立超时监控机制
 * 4. 状态处理：调用父类的标准处理流程
 * 5. 动作执行：通过状态动作执行具体的启动逻辑
 * 
 * 初始化策略：
 * - 延迟初始化：任务实例在首次启动时才进行初始化
 * - 状态检查：通过isTaskInstanceInitialized()检查初始化状态
 * - 首次运行：调用initializeFirstRunTaskInstance()进行初始化
 * - 状态机准备：确保状态机能够正确识别任务状态
 * 
 * 超时监控：
 * - 配置检查：检查任务定义中是否配置了有效的超时时间
 * - 监控启动：为配置了超时的任务启动超时监控事件
 * - 防护机制：避免任务无限运行影响系统资源
 * - 异常处理：超时后会触发相应的超时处理逻辑
 * 
 * 类比理解：
 * 就像工厂生产线的启动控制室：
 * - 设备检查：确保所有生产设备已准备就绪（任务初始化）
 * - 启动生产：按下启动按钮开始生产流程（状态动作执行）
 * - 监控建立：设置生产监控系统防止异常（超时监控）
 * - 状态跟踪：跟踪生产线从停机到运行的状态变化（状态转换）
 */
@Slf4j
@Component
public class TaskStartLifecycleEventHandler extends AbstractTaskLifecycleEventHandler<TaskStartLifecycleEvent> {

    /**
     * 处理任务启动生命周期事件的重写方法
     * 
     * 这是任务启动处理的核心方法，在标准处理流程之前执行特定的启动准备工作。
     * 方法会确保任务实例已正确初始化，并为任务建立必要的监控机制。
     * 
     * 启动准备流程：
     * 1. 任务实例检查：检查任务执行器的任务实例是否已初始化
     * 2. 延迟初始化：如果未初始化则执行首次运行的任务实例初始化
     * 3. 监控建立：根据任务配置建立超时监控机制
     * 4. 标准处理：调用父类的标准事件处理流程
     * 
     * 初始化逻辑：
     * - 由于任务执行器在首次启动时可能未初始化，需要在这里进行初始化
     * - 初始化是状态机正常工作的前提，没有任务实例就无法根据状态查找状态机
     * - 使用initializeFirstRunTaskInstance()创建首次运行的任务实例
     * 
     * 设计考虑：
     * - 延迟初始化：只有在真正启动时才进行初始化，避免不必要的资源消耗
     * - 状态依赖：状态机需要依赖任务实例的状态，所以必须先初始化
     * - 幂等性：多次调用initializeFirstRunTaskInstance()是安全的
     * - 监控及时性：超时监控必须在任务启动时立即建立
     * 
     * 类比：就像汽车启动前的准备工作，要检查油量、预热引擎、调整座椅，
     * 确保一切就绪后才能真正启动并开始行驶监控。
     * 
     * @param workflowExecutionRunnable 工作流执行器，提供工作流上下文
     * @param taskStartLifecycleEvent 任务启动生命周期事件，包含启动相关信息
     */
    @Override
    public void handle(final IWorkflowExecutionRunnable workflowExecutionRunnable,
                       final TaskStartLifecycleEvent taskStartLifecycleEvent) {
        final ITaskExecutionRunnable taskExecutionRunnable = taskStartLifecycleEvent.getTaskExecutionRunnable();
        
        // 由于任务执行器在首次启动时可能未初始化，需要在这里进行初始化
        // 这样才能确保状态机能够通过任务实例状态找到对应的处理逻辑
        if (!taskExecutionRunnable.isTaskInstanceInitialized()) {
            taskExecutionRunnable.initializeFirstRunTaskInstance();
        }
        taskTimeoutMonitor(taskExecutionRunnable);
        super.handle(workflowExecutionRunnable, taskStartLifecycleEvent);
    }

    /**
     * 执行任务启动的具体处理逻辑
     * 
     * 这是抽象方法的具体实现，负责将任务启动事件委托给相应的状态动作进行处理。
     * 方法通过状态动作的onStartEvent方法执行实际的任务启动逻辑。
     * 
     * 处理机制：
     * - 委托模式：将具体的启动逻辑委托给状态动作执行
     * - 状态感知：根据任务当前状态选择合适的启动处理策略
     * - 上下文传递：传递完整的执行上下文给状态动作
     * - 事件驱动：基于事件驱动的启动处理模式
     * 
     * 状态动作职责：
     * - 根据任务当前状态执行相应的启动逻辑
     * - 可能的状态包括：待提交、重试、故障转移等
     * - 不同状态下的启动逻辑可能有所不同
     * - 状态动作会处理任务的实际分发和执行
     * 
     * 设计优势：
     * - 职责分离：启动事件处理器专注于启动准备，状态动作专注于启动执行
     * - 策略模式：不同状态对应不同的启动策略
     * - 可扩展性：新增状态时只需实现相应的状态动作
     * - 统一接口：所有状态动作都提供统一的onStartEvent接口
     * 
     * 类比：就像项目启动仪式上，主持人宣布项目开始后，
     * 各个部门负责人根据自己部门的当前状态执行相应的启动工作。
     * 
     * @param taskStateAction 任务状态对应的处理动作，负责执行具体启动逻辑
     * @param workflowExecutionRunnable 工作流执行器，提供工作流级别的上下文
     * @param taskExecutionRunnable 任务执行器，提供任务级别的操作接口
     * @param event 任务启动生命周期事件，包含启动相关的详细信息
     */
    @Override
    public void handle(final ITaskStateAction taskStateAction,
                       final IWorkflowExecutionRunnable workflowExecutionRunnable,
                       final ITaskExecutionRunnable taskExecutionRunnable,
                       final TaskStartLifecycleEvent event) {
        taskStateAction.onStartEvent(workflowExecutionRunnable, taskExecutionRunnable, event);
    }

    /**
     * 匹配任务启动事件类型
     * 
     * 返回当前处理器能够处理的生命周期事件类型。
     * 这是事件路由机制的重要组成部分，确保事件能够被正确的处理器处理。
     * 
     * 事件匹配机制：
     * - 类型标识：每个处理器都有唯一的事件类型标识
     * - 路由基础：事件总线根据这个标识进行事件路由
     * - 处理器注册：Spring容器根据这个标识注册处理器
     * - 多态支持：支持不同类型事件的多态处理
     * 
     * 设计意图：
     * - 类型安全：确保事件类型和处理器类型的匹配
     * - 自动路由：支持事件总线的自动事件路由
     * - 解耦设计：事件发布者不需要知道具体的处理器
     * - 扩展性：新增事件类型时只需实现相应的处理器
     * 
     * 类比：就像医院科室的专业标识牌，告诉患者这个科室专门治疗什么疾病，
     * 分诊台根据患者病情将其引导到对应的科室。
     * 
     * @return TaskLifecycleEventType.START，表示处理任务启动类型的事件
     */
    @Override
    public ILifecycleEventType matchEventType() {
        return TaskLifecycleEventType.START;
    }

    /**
     * 建立任务超时监控机制
     * 
     * 根据任务定义中的超时配置为任务建立超时监控机制。
     * 如果任务配置了有效的超时时间，会发布超时监控事件来跟踪任务执行时间。
     * 
     * 监控逻辑：
     * 1. 配置检查：从任务定义中获取超时配置
     * 2. 有效性验证：检查超时时间是否大于0（有效）
     * 3. 监控启动：发布超时监控事件到事件总线
     * 4. 日志记录：记录监控建立或跳过的信息
     * 
     * 超时配置：
     * - 配置来源：从TaskDefinition.getTimeout()获取超时时间
     * - 时间单位：通常以秒或毫秒为单位
     * - 无效值处理：小于等于0的值被视为无效，不启动监控
     * - 灵活配置：不同任务可以配置不同的超时时间
     * 
     * 监控机制：
     * - 事件驱动：通过发布TaskTimeoutLifecycleEvent启动监控
     * - 异步处理：超时监控事件会被相应的处理器异步处理
     * - 定时检查：监控器会定时检查任务是否超时
     * - 超时处理：超时后会触发相应的超时处理逻辑
     * 
     * 防护价值：
     * - 资源保护：防止任务无限运行占用系统资源
     * - 及时发现：及时发现任务执行异常或卡死
     * - 自动恢复：超时后可以触发重试或故障转移
     * - 系统稳定：保证系统的整体稳定性和可用性
     * 
     * 类比：就像给烤箱设置定时器，防止食物烤糊或浪费电力，
     * 定时器到时会自动提醒或关闭设备。
     * 
     * @param taskExecutionRunnable 任务执行器，用于获取任务定义和发布超时事件
     */
    private void taskTimeoutMonitor(final ITaskExecutionRunnable taskExecutionRunnable) {
        final TaskDefinition taskDefinition = taskExecutionRunnable.getTaskDefinition();
        if (taskDefinition.getTimeout() <= 0) {
            log.debug("The task {} timeout {} is invalided, so the timeout monitor will not be started.",
                    taskDefinition.getName(),
                    taskDefinition.getTimeout());
            return;
        }
        taskExecutionRunnable.getWorkflowEventBus().publish(TaskTimeoutLifecycleEvent.of(taskExecutionRunnable));
    }

}
