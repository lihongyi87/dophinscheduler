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
 * 工作流失败监听事件
 *
 * 当工作流实例执行失败时触发的事件，包含失败时的详细上下文信息。
 * 这个事件是系统异常处理和告警机制的重要组成部分。
 *
 * 触发时机：
 * - 任务执行失败导致工作流失败
 * - 工作流执行超时
 * - 资源不足导致失败
 * - 依赖条件不满足
 * - 系统异常导致失败
 *
 * 使用场景：
 * - 发送失败告警通知
 * - 触发故障恢复机制
 * - 记录失败日志和原因
 * - 更新失败统计信息
 * - 通知相关运维人员
 * - 触发失败后的补偿流程
 *
 * 重要性：
 * - 高优先级事件，通常需要立即处理
 * - 可能需要人工介入
 * - 影响下游依赖任务
 *
 * 类比：就像生产线上的故障报警，需要立即通知相关人员，
 *      记录故障时间、位置、原因等关键信息。
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ProcessFailListenerEvent implements AbstractListenerEvent {

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
     * 工作流的负责人，失败时的主要通知对象
     */
    private String owner;

    /**
     * 工作流实例ID
     * 失败的工作流实例的唯一标识符
     */
    private Integer processId;

    /**
     * 工作流定义代码
     * 工作流定义的唯一标识符
     */
    private Long processDefinitionCode;

    /**
     * 工作流实例名称
     * 失败的工作流实例的名称
     */
    private String processName;

    /**
     * 执行类型
     * 标识工作流的执行方式
     */
    private CommandType processType;

    /**
     * 工作流状态
     * 通常为FAILED状态
     */
    private WorkflowExecutionStatus processState;

    /**
     * 恢复标记
     * 标识工作流是否是从故障恢复执行后失败
     */
    private Flag recovery;

    /**
     * 执行次数
     * 工作流实例的总执行次数，用于判断重试情况
     */
    private Integer runTimes;

    /**
     * 工作流开始时间
     * 工作流实例开始执行的时间戳
     */
    private Date processStartTime;

    /**
     * 工作流结束时间
     * 工作流实例失败的时间戳
     */
    private Date processEndTime;

    /**
     * 执行主机
     * 执行该工作流的Master节点地址，用于故障定位
     */
    private String processHost;

    /**
     * 获取事件类型
     *
     * @return 返回PROCESS_FAIL类型，表示工作流失败事件
     */
    @Override
    public ListenerEventType getEventType() {
        return ListenerEventType.PROCESS_FAIL;
    }

    /**
     * 获取事件标题
     *
     * 生成描述性的事件标题，包含失败的工作流名称。
     * 格式："process fail: [工作流名称]"
     *
     * @return 事件标题字符串
     */
    @Override
    public String getTitle() {
        return String.format("process fail: %s", processName);
    }
}
