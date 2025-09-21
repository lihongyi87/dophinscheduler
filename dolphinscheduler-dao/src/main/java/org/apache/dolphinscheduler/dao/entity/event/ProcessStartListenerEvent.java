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

import org.apache.dolphinscheduler.common.enums.CommandType;
import org.apache.dolphinscheduler.common.enums.Flag;
import org.apache.dolphinscheduler.common.enums.ListenerEventType;
import org.apache.dolphinscheduler.common.enums.WorkflowExecutionStatus;

import java.util.Date;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 工作流开始监听事件
 *
 * 当工作流实例开始执行时触发的事件，包含工作流启动时的完整上下文信息。
 * 用于通知相关系统组件和外部监听器工作流已经开始执行。
 *
 * 触发时机：
 * - 工作流实例被成功创建并开始执行
 * - 工作流从暂停状态恢复执行
 * - 工作流重试开始执行
 *
 * 使用场景：
 * - 发送工作流开始通知给相关人员
 * - 记录工作流执行审计日志
 * - 触发相关的监控和告警机制
 * - 更新外部系统的工作流状态
 *
 * 事件内容：
 * - 项目信息：项目代码、项目名称
 * - 工作流定义信息：定义代码、定义名称
 * - 工作流实例信息：实例ID、实例名称、执行类型
 * - 执行信息：执行次数、是否恢复执行、开始时间
 *
 * 类比：就像马拉松比赛的起跑枪声，通知所有相关人员比赛已经开始，
 *      记录选手的号码、姓名、起跑时间等关键信息。
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ProcessStartListenerEvent implements AbstractListenerEvent {

    /**
     * 项目代码
     * 工作流所属项目的唯一标识符
     */
    private Long projectCode;

    /**
     * 项目名称
     * 工作流所属项目的名称，用于显示和日志记录
     */
    private String projectName;

    /**
     * 负责人
     * 工作流的负责人或创建者，用于权限控制和通知
     */
    private String owner;

    /**
     * 工作流定义代码
     * 工作流定义的唯一标识符
     */
    private Long processDefinitionCode;

    /**
     * 工作流定义名称
     * 工作流定义的名称，用于显示和识别
     */
    private String processDefinitionName;

    /**
     * 工作流实例ID
     * 当前执行的工作流实例的唯一标识符
     */
    private Integer processId;

    /**
     * 工作流实例名称
     * 当前执行的工作流实例的名称
     */
    private String processName;

    /**
     * 执行类型
     * 标识工作流的执行方式（如：手动启动、调度触发、补数等）
     */
    private CommandType processType;

    /**
     * 工作流状态
     * 当前工作流的执行状态
     */
    private WorkflowExecutionStatus processState;

    /**
     * 执行次数
     * 工作流实例的执行次数，用于重试计数
     */
    private Integer runTimes;

    /**
     * 恢复标记
     * 标识工作流是否是从故障恢复执行
     */
    private Flag recovery;

    /**
     * 工作流开始时间
     * 工作流实例开始执行的时间戳
     */
    private Date processStartTime;

    /**
     * 获取事件类型
     *
     * @return 返回PROCESS_START类型，表示工作流开始事件
     */
    @Override
    public ListenerEventType getEventType() {
        return ListenerEventType.PROCESS_START;
    }

    /**
     * 获取事件标题
     *
     * 生成描述性的事件标题，包含工作流名称。
     * 格式："process start: [工作流名称]"
     *
     * @return 事件标题字符串
     */
    @Override
    public String getTitle() {
        return String.format("process start: %s", processName);
    }
}
