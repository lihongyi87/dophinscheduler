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

package org.apache.dolphinscheduler.server.master.engine.task.lifecycle.event;

import org.apache.dolphinscheduler.plugin.task.api.model.Property;
import org.apache.dolphinscheduler.server.master.engine.ILifecycleEventType;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.AbstractTaskLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.TaskLifecycleEventType;
import org.apache.dolphinscheduler.server.master.engine.task.runnable.ITaskExecutionRunnable;

import java.util.Date;
import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

/**
 * 任务成功生命周期事件
 *
 * 表示任务实例成功完成执行的结果事件。这是任务正常执行流程的成功终点，
 * 标志着任务已按预期完成并产生了正确的结果。
 *
 * 事件触发条件：
 * - 任务程序正常退出（返回码为0）
 * - 任务执行没有抛出异常
 * - 任务产生了预期的输出结果
 * - 任务在超时时间内完成执行
 * - 任务通过了结果验证检查
 *
 * 事件携带的核心信息：
 * 1. 任务执行实例：完整的任务上下文和状态信息
 * 2. 结束时间：任务完成的精确时间戳，用于性能分析
 * 3. 变量池：任务执行过程中产生的输出变量，可供后续任务使用
 *
 * 事件处理职责：
 * 1. 更新任务实例的最终状态为SUCCESS
 * 2. 记录任务的执行时间和性能指标
 * 3. 保存任务产生的输出变量到上下文
 * 4. 触发依赖此任务的后续任务开始执行
 * 5. 发送任务成功完成的通知
 * 6. 清理任务执行过程中的临时资源
 *
 * 生命周期位置：
 * START -> DISPATCH -> DISPATCHED -> RUNNING -> SUCCEEDED
 *                                               ↑
 *                                               当前事件位置
 *
 * 变量池机制：
 * 任务可以在执行过程中设置输出变量，这些变量会被收集到varPool中，
 * 供后续的任务作为输入参数使用，实现任务间的数据传递。
 *
 * 类比理解：
 * 就像医院的治疗成功事件，当患者治疗成功后，医院需要：
 * - 记录治疗完成时间和效果
 * - 保存治疗过程中的重要数据（检查结果、用药记录等）
 * - 通知相关科室和人员
 * - 安排后续的康复或随访计划
 *
 * 设计特点：
 * - 构建器模式：使用@Builder支持灵活的对象构建
 * - 不可变对象：所有字段都是final，确保线程安全
 * - 丰富信息：携带任务成功所需的完整信息
 * - 详细日志：toString方法提供详细的事件描述
 *
 * @see TaskLifecycleEventType#SUCCEEDED
 * @see AbstractTaskLifecycleEvent
 * @see ITaskExecutionRunnable
 * @see Property 任务输出变量
 */
@Getter
@Builder
@AllArgsConstructor
public class TaskSuccessLifecycleEvent extends AbstractTaskLifecycleEvent {

    /**
     * 关联的任务执行实例
     *
     * 包含成功完成的任务的完整执行上下文、状态信息和元数据。
     * 用于任务状态更新和后续处理流程。
     */
    private final ITaskExecutionRunnable taskExecutionRunnable;

    /**
     * 任务结束时间
     *
     * 任务完成执行的精确时间戳。
     * 用于计算任务执行时长、性能分析和审计日志记录。
     *
     * 时间计算：
     * - 执行时长 = endTime - startTime
     * - 用于SLA监控和性能优化
     * - 作为调度决策的参考依据
     */
    private final Date endTime;

    /**
     * 任务输出变量池
     *
     * 包含任务执行过程中产生的所有输出变量。
     * 这些变量可以被后续任务作为输入参数使用，实现任务间的数据传递。
     *
     * 变量类型：
     * - 程序输出：任务程序显式设置的输出变量
     * - 系统变量：系统自动收集的执行环境信息
     * - 结果数据：任务执行产生的关键结果数据
     *
     * 使用场景：
     * - 数据ETL流程中的数据传递
     * - 机器学习管道中的模型参数传递
     * - 动态配置的运行时传递
     * - 条件分支的判断依据
     *
     * 注意事项：
     * - 变量值应该是可序列化的
     * - 避免传递过大的数据对象
     * - 变量名应该具有明确的语义
     */
    private final List<Property> varPool;

    /**
     * 获取事件类型
     *
     * 返回此事件对应的生命周期事件类型。
     * 用于事件路由和处理器选择。
     *
     * @return TaskLifecycleEventType.SUCCEEDED 表示这是任务成功事件
     */
    @Override
    public ILifecycleEventType getEventType() {
        return TaskLifecycleEventType.SUCCEEDED;
    }

    /**
     * 生成事件的字符串表示
     *
     * 提供详细的事件描述，包含任务名称、完成时间和输出变量信息。
     * 主要用于日志记录、监控展示和问题排查。
     *
     * @return 格式化的事件描述字符串，包含：
     *         - 事件类型
     *         - 任务名称
     *         - 结束时间
     *         - 输出变量池内容
     *
     * 示例输出：
     * "TaskSuccessLifecycleEvent{task=data_process, endTime=2023-10-01 12:30:45, varPool='[{name=result_count, value=1000}]'}"
     */
    @Override
    public String toString() {
        return "TaskSuccessLifecycleEvent{" +
                "task=" + taskExecutionRunnable.getName() +
                ", endTime=" + endTime +
                ", varPool='" + varPool + '\'' +
                '}';
    }
}
