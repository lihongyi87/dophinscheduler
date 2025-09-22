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

package org.apache.dolphinscheduler.dao.entity.event;

import org.apache.dolphinscheduler.common.enums.ListenerEventType;
import org.apache.dolphinscheduler.plugin.task.api.enums.TaskExecutionStatus;

import java.util.Date;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 任务失败监听事件
 *
 * 当任务实例执行失败时触发的事件，包含失败时的详细上下文信息。
 * 这是任务异常处理和告警机制的重要组成部分。
 *
 * 触发时机：
 * - 任务执行过程中抛出异常
 * - 任务执行返回非零退出码
 * - 任务执行超时
 * - 资源不足导致失败
 * - 任务被强制终止
 *
 * 使用场景：
 * - 发送失败告警通知
 * - 触发失败重试机制
 * - 记录失败日志和原因
 * - 更新失败统计信息
 * - 通知相关负责人
 * - 触发失败后的补偿流程
 *
 * 重要性：
 * - 高优先级事件，通常需要立即处理
 * - 可能影响整个工作流的执行
 * - 需要记录详细的失败信息以便排查
 *
 * 类比：就像生产线工位出现质量问题，需要立即停线检查，
 *      记录问题位置、时间、原因，通知质检部门。
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TaskFailListenerEvent implements AbstractListenerEvent {

    /**
     * 项目代码
     * 任务所属项目的唯一标识符
     */
    private long projectCode;

    /**
     * 项目名称
     * 任务所属项目的名称
     */
    private String projectName;

    /**
     * 负责人
     * 任务的负责人，失败时的主要通知对象
     */
    private String owner;

    /**
     * 工作流实例ID
     * 任务所属工作流实例的唯一标识符
     */
    private long processId;

    /**
     * 工作流定义代码
     * 任务所属工作流定义的唯一标识符
     */
    private long processDefinitionCode;

    /**
     * 工作流实例名称
     * 任务所属工作流实例的名称
     */
    private String processName;

    /**
     * 任务实例ID
     * 失败的任务实例的唯一标识符
     */
    private int taskInstanceId;

    /**
     * 任务代码
     * 任务定义的唯一标识符
     */
    private long taskCode;

    /**
     * 任务名称
     * 失败的任务实例的名称
     */
    private String taskName;

    /**
     * 任务类型
     * 标识任务的具体类型，用于失败原因分析
     */
    private String taskType;

    /**
     * 任务状态
     * 通常为FAILED状态
     */
    private TaskExecutionStatus taskState;

    /**
     * 任务开始时间
     * 任务实例开始执行的时间戳
     */
    private Date taskStartTime;

    /**
     * 任务结束时间
     * 任务实例失败的时间戳
     */
    private Date taskEndTime;

    /**
     * 执行主机
     * 执行该任务的Worker节点地址，用于故障定位
     */
    private String taskHost;

    /**
     * 日志路径
     * 任务执行日志的存储路径，重要的失败原因查找依据
     */
    private String logPath;

    /**
     * 获取事件类型
     *
     * @return 返回TASK_FAIL类型，表示任务失败事件
     */
    @Override
    public ListenerEventType getEventType() {
        return ListenerEventType.TASK_FAIL;
    }

    /**
     * 获取事件标题
     *
     * 生成描述性的事件标题，包含失败的任务名称。
     * 格式："task fail: [任务名称]"
     *
     * @return 事件标题字符串
     */
    @Override
    public String getTitle() {
        return String.format("task fail: %s", taskName);
    }
}
