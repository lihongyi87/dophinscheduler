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
 * 任务开始监听事件
 *
 * 当任务实例开始执行时触发的事件，包含任务启动时的详细信息。
 * 这是任务生命周期中的第一个重要事件。
 *
 * 触发时机：
 * - 任务被调度并开始执行
 * - 任务从暂停状态恢复执行
 * - 任务重试开始执行
 *
 * 使用场景：
 * - 记录任务执行开始时间
 * - 更新任务执行状态展示
 * - 触发任务级监控
 * - 初始化任务执行环境
 * - 发送任务开始通知
 *
 * 任务信息：
 * - 任务基本信息：任务ID、任务代码、任务名称、任务类型
 * - 所属工作流信息：工作流ID、工作流定义代码、工作流名称
 * - 执行信息：开始时间、执行主机、日志路径
 *
 * 类比：就像流水线上的工位开始作业，记录工位编号、开始时间、
 *      操作员、产品信息等关键数据。
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TaskStartListenerEvent implements AbstractListenerEvent {

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
     * 任务的负责人或创建者
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
     * 任务实例的唯一标识符
     */
    private int taskInstanceId;

    /**
     * 任务代码
     * 任务定义的唯一标识符
     */
    private long taskCode;

    /**
     * 任务名称
     * 任务实例的名称
     */
    private String taskName;

    /**
     * 任务类型
     * 标识任务的具体类型（如：Shell、SQL、Python、Spark等）
     */
    private String taskType;

    /**
     * 任务状态
     * 当前任务的执行状态，通常为RUNNING
     */
    private TaskExecutionStatus taskState;

    /**
     * 任务开始时间
     * 任务实例开始执行的时间戳
     */
    private Date taskStartTime;

    /**
     * 任务结束时间
     * 在开始事件中通常为null，仅在任务结束时填充
     */
    private Date taskEndTime;

    /**
     * 执行主机
     * 执行该任务的Worker节点地址
     */
    private String taskHost;

    /**
     * 日志路径
     * 任务执行日志的存储路径，用于日志查看和问题排查
     */
    private String logPath;

    /**
     * 获取事件类型
     *
     * @return 返回TASK_START类型，表示任务开始事件
     */
    @Override
    public ListenerEventType getEventType() {
        return ListenerEventType.TASK_START;
    }

    /**
     * 获取事件标题
     *
     * 生成描述性的事件标题，包含任务名称。
     * 格式："task start: [任务名称]"
     *
     * @return 事件标题字符串
     */
    @Override
    public String getTitle() {
        return String.format("task start: %s", taskName);
    }
}
