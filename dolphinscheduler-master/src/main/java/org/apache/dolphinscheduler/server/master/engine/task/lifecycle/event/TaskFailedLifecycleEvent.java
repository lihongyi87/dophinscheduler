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

import org.apache.dolphinscheduler.server.master.engine.ILifecycleEventType;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.AbstractTaskLifecycleEvent;
import org.apache.dolphinscheduler.server.master.engine.task.lifecycle.TaskLifecycleEventType;
import org.apache.dolphinscheduler.server.master.engine.task.runnable.ITaskExecutionRunnable;

import java.util.Date;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

/**
 * 任务失败生命周期事件
 *
 * 表示任务实例执行失败的结果事件。当任务在执行过程中出现错误、异常或
 * 无法正常完成时触发此事件，标志着任务执行流程的异常终点。
 *
 * 失败触发条件：
 * - 任务程序异常退出（返回码非0）
 * - 任务执行过程中抛出未捕获的异常
 * - 任务执行超时且策略为失败
 * - 系统资源不足导致任务无法完成
 * - 外部依赖服务不可用
 * - 任务输出验证失败
 *
 * 事件处理职责：
 * 1. 更新任务实例状态为FAILED
 * 2. 记录失败时间和错误信息
 * 3. 保存失败日志和诊断信息
 * 4. 评估是否满足重试条件
 * 5. 可能触发工作流失败或继续执行策略
 * 6. 发送失败通知和告警
 * 7. 清理任务占用的资源
 *
 * 生命周期位置：
 * START -> DISPATCH -> DISPATCHED -> RUNNING -> FAILED
 *                                               ↑
 *                                               当前事件位置
 *
 * 后续处理策略：
 * - 重试策略：如果配置了重试，可能触发TaskRetryLifecycleEvent
 * - 失败策略：根据工作流配置决定是否继续执行后续任务
 * - 通知策略：发送失败通知给相关人员
 * - 恢复策略：记录失败信息用于故障排查和系统优化
 *
 * 类比理解：
 * 就像医院的治疗失败事件，当患者治疗失败时，医院需要：
 * - 记录失败时间和原因
 * - 分析失败的具体原因
 * - 评估是否需要尝试其他治疗方案
 * - 通知患者家属和相关医护人员
 * - 总结经验以避免类似失败
 *
 * @see TaskLifecycleEventType#FAILED
 * @see AbstractTaskLifecycleEvent
 * @see ITaskExecutionRunnable
 */
@Data
@Builder
@AllArgsConstructor
public class TaskFailedLifecycleEvent extends AbstractTaskLifecycleEvent {

    /**
     * 关联的任务执行实例
     *
     * 包含失败任务的完整执行上下文、状态信息和错误详情。
     * 用于失败原因分析和后续处理决策。
     */
    private final ITaskExecutionRunnable taskExecutionRunnable;

    /**
     * 任务失败时间
     *
     * 任务失败的精确时间戳，用于故障分析、性能监控和审计记录。
     *
     * 时间用途：
     * - 计算任务执行时长（失败前的执行时间）
     * - 故障时间点分析和关联
     * - SLA违反统计和报告
     * - 系统性能趋势分析
     */
    private final Date endTime;

    /**
     * 获取事件类型
     *
     * 返回此事件对应的生命周期事件类型。
     * 用于事件路由系统识别和分发到相应的失败处理器。
     *
     * @return TaskLifecycleEventType.FAILED 表示这是任务失败事件
     */
    @Override
    public ILifecycleEventType getEventType() {
        return TaskLifecycleEventType.FAILED;
    }

    /**
     * 生成事件的字符串表示
     *
     * 提供失败事件的可读性描述，包含任务名称和失败时间。
     * 主要用于日志记录、监控展示和故障排查。
     *
     * @return 格式化的失败事件描述字符串，例如：
     *         "TaskFailedLifecycleEvent{task=data_process_task, endTime=2023-10-01 12:30:45}"
     *
     * 日志用途：
     * - 快速识别失败的任务
     * - 故障时间点记录
     * - 失败事件的搜索和过滤
     * - 运维监控和告警
     */
    @Override
    public String toString() {
        return "TaskFailedLifecycleEvent{" +
                "task=" + taskExecutionRunnable.getName() +
                ", endTime=" + endTime +
                '}';
    }
}
