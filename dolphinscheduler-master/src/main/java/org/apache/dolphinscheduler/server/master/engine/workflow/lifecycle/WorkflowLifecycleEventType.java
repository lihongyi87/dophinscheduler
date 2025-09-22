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

package org.apache.dolphinscheduler.server.master.engine.workflow.lifecycle;

import org.apache.dolphinscheduler.server.master.engine.ILifecycleEventType;

/**
 * 工作流生命周期事件类型枚举
 *
 * 定义了工作流在整个生命周期中可能发生的各种事件类型。
 * 这些事件类型用于驱动工作流的状态转换和执行流程控制。
 *
 * 工作流的生命周期包括：启动、任务完成触发、暂停、停止、成功、失败、最终化等阶段。
 * 每个事件类型都对应特定的业务逻辑和状态转换。
 *
 * 简单理解：这就像一套标准的工作流操作指令集，告诉系统该做什么动作。
 *
 * @author DolphinScheduler
 */
public enum WorkflowLifecycleEventType implements ILifecycleEventType {

    /**
     * 启动工作流实例
     *
     * 当工作流需要开始执行时触发此事件。
     * 这是工作流生命周期的起始点，会初始化工作流的执行环境。
     */
    START,

    /**
     * 任务完成后触发拓扑逻辑转换事件
     *
     * 当工作流中的某个任务执行完成时触发此事件。
     * 系统会根据DAG（有向无环图）的拓扑结构，判断是否需要触发下游任务。
     * 这是工作流任务调度的核心事件，负责任务间的依赖关系处理。
     */
    TOPOLOGY_LOGICAL_TRANSACTION_WITH_TASK_FINISH,

    /**
     * 暂停工作流实例
     *
     * 用户请求暂停工作流时触发此事件。
     * 会停止新任务的调度，但不会中断正在执行的任务。
     */
    PAUSE,

    /**
     * 工作流实例已暂停
     *
     * 工作流成功暂停后触发此事件。
     * 表示工作流已进入暂停状态，不会再调度新的任务。
     */
    PAUSED,

    /**
     * 停止工作流实例
     *
     * 用户请求停止工作流时触发此事件。
     * 会尝试中断正在执行的任务并停止整个工作流。
     */
    STOP,

    /**
     * 工作流实例已停止
     *
     * 工作流成功停止后触发此事件。
     * 表示工作流已完全停止，所有任务都已中断或完成。
     */
    STOPPED,

    /**
     * 工作流实例执行成功
     *
     * 当工作流中的所有任务都成功完成时触发此事件。
     * 表示整个工作流按预期完成了所有业务逻辑。
     */
    SUCCEED,

    /**
     * 工作流实例执行失败
     *
     * 当工作流中有任务失败且不可恢复时触发此事件。
     * 表示工作流无法继续执行，需要人工干预或重新启动。
     */
    FAILED,

    /**
     * 最终化工作流实例
     *
     * 工作流生命周期的最后阶段，进行资源清理和状态确认。
     * 无论工作流是成功、失败还是被停止，都会触发此事件来做最后的收尾工作。
     */
    FINALIZE,

}
