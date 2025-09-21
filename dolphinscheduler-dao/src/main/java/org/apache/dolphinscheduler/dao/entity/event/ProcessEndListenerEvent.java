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
 * 工作流结束监听事件
 *
 * 当工作流实例执行结束时触发的事件，无论是成功完成还是失败终止。
 * 包含工作流执行的完整生命周期信息和最终状态。
 *
 * 触发时机：
 * - 工作流成功完成所有任务
 * - 工作流执行失败
 * - 工作流被手动停止
 * - 工作流超时终止
 *
 * 使用场景：
 * - 发送工作流完成通知
 * - 记录工作流执行结果
 * - 更新执行统计信息
 * - 触发后续依赖工作流
 * - 清理执行过程中的临时资源
 *
 * 事件内容：
 * - 基本信息：项目、工作流定义、实例信息
 * - 执行信息：执行类型、最终状态、执行次数
 * - 时间信息：开始时间、结束时间
 * - 执行主机：Master节点信息
 *
 * 类比：就像马拉松选手冲过终点线，记录最终成绩、完成时间、
 *      比赛状态（正常完成或退赛）等关键信息。
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ProcessEndListenerEvent implements AbstractListenerEvent {

    /**
     * 项目代码
     * 工作流所属项目的唯一标识符
     */
    private Long projectCode;

    /**
     * 项目名称
     * 工作流所属项目的名称
     */
    private String projectName;

    /**
     * 负责人
     * 工作流的负责人或创建者
     */
    private String owner;

    /**
     * 工作流实例ID
     * 执行的工作流实例的唯一标识符
     */
    private Integer processId;

    /**
     * 工作流定义代码
     * 工作流定义的唯一标识符
     */
    private Long processDefinitionCode;

    /**
     * 工作流实例名称
     * 执行的工作流实例的名称
     */
    private String processName;

    /**
     * 执行类型
     * 标识工作流的执行方式
     */
    private CommandType processType;

    /**
     * 工作流最终状态
     * 工作流结束时的最终执行状态（成功、失败、停止等）
     */
    private WorkflowExecutionStatus processState;

    /**
     * 恢复标记
     * 标识工作流是否是从故障恢复执行
     */
    private Flag recovery;

    /**
     * 执行次数
     * 工作流实例的总执行次数
     */
    private Integer runTimes;

    /**
     * 工作流开始时间
     * 工作流实例开始执行的时间戳
     */
    private Date processStartTime;

    /**
     * 工作流结束时间
     * 工作流实例结束执行的时间戳
     */
    private Date processEndTime;

    /**
     * 执行主机
     * 执行该工作流的Master节点地址
     */
    private String processHost;

    /**
     * 获取事件类型
     *
     * @return 返回PROCESS_END类型，表示工作流结束事件
     */
    @Override
    public ListenerEventType getEventType() {
        return ListenerEventType.PROCESS_END;
    }

    /**
     * 获取事件标题
     *
     * 生成描述性的事件标题，包含工作流名称。
     * 格式："process end: [工作流名称]"
     *
     * @return 事件标题字符串
     */
    @Override
    public String getTitle() {
        return String.format("process end: %s", processName);
    }
}
