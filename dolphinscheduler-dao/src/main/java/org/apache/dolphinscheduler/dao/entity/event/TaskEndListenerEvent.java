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
 * 任务结束监听事件
 *
 * 当任务实例执行结束时触发的事件，无论是成功完成还是失败终止。
 * 包含任务执行的完整生命周期信息和最终状态。
 *
 * 触发时机：
 * - 任务成功完成
 * - 任务执行失败
 * - 任务被手动停止
 * - 任务超时终止
 *
 * 使用场景：
 * - 记录任务执行结果
 * - 更新任务执行统计
 * - 触发下游依赖任务
 * - 清理任务执行资源
 * - 发送任务完成通知
 *
 * 事件内容：
 * - 任务基本信息：任务ID、任务代码、任务名称、任务类型
 * - 所属工作流信息：工作流ID、工作流定义代码、工作流名称
 * - 执行信息：开始时间、结束时间、最终状态
 * - 执行环境：执行主机、日志路径
 *
 * 类比：就像流水线工位完成作业，记录完成时间、耗时、
 *      质量结果（合格/不合格）等完整信息。
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TaskEndListenerEvent implements AbstractListenerEvent {

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
     * 标识任务的具体类型
     */
    private String taskType;

    /**
     * 任务最终状态
     * 任务结束时的最终执行状态（成功、失败、停止等）
     */
    private TaskExecutionStatus taskState;

    /**
     * 任务开始时间
     * 任务实例开始执行的时间戳
     */
    private Date taskStartTime;

    /**
     * 任务结束时间
     * 任务实例结束执行的时间戳
     */
    private Date taskEndTime;

    /**
     * 执行主机
     * 执行该任务的Worker节点地址
     */
    private String taskHost;

    /**
     * 日志路径
     * 任务执行日志的存储路径
     */
    private String logPath;

    /**
     * 获取事件类型
     *
     * @return 返回TASK_END类型，表示任务结束事件
     */
    @Override
    public ListenerEventType getEventType() {
        return ListenerEventType.TASK_END;
    }

    /**
     * 获取事件标题
     *
     * 生成描述性的事件标题，包含任务名称。
     * 格式："task end: [任务名称]"
     *
     * @return 事件标题字符串
     */
    @Override
    public String getTitle() {
        return String.format("task end: %s", taskName);
    }
}
