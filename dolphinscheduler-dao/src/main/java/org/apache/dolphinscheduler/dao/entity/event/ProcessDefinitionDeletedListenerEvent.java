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

import java.util.Date;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 工作流定义删除监听事件
 *
 * 当工作流定义被删除时触发的事件，保留删除前的关键信息。
 * 这是工作流生命周期的最后一个事件。
 *
 * 触发时机：
 * - 用户手动删除工作流定义
 * - 批量删除工作流定义
 * - 项目删除时级联删除
 * - API调用删除
 *
 * 使用场景：
 * - 记录工作流删除审计日志
 * - 清理相关联的资源
 * - 通知外部系统删除关联数据
 * - 停止相关调度任务
 * - 归档历史数据
 * - 更新统计信息
 *
 * 重要性：
 * - 不可逆操作，需要谨慎处理
 * - 可能影响正在运行的实例
 * - 需要保存删除记录以便审计
 *
 * 类比：就像一个生产流程被废弃，需要记录废弃时间、
 *      操作人、流程信息，并清理相关设备和资源。
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ProcessDefinitionDeletedListenerEvent implements AbstractListenerEvent {

    /**
     * 项目ID
     * 工作流所属项目的数据库ID
     */
    private Integer projectId;

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
     * 工作流定义ID
     * 被删除的工作流定义的数据库ID
     */
    private Integer id;

    /**
     * 工作流定义代码
     * 被删除的工作流定义的唯一标识符
     */
    private Long code;

    /**
     * 工作流定义名称
     * 被删除的工作流定义的名称
     */
    private String name;

    /**
     * 创建用户ID
     * 创建该工作流的用户ID
     */
    private Integer userId;

    /**
     * 删除操作人
     * 执行删除操作的用户名
     */
    private String modifiedBy;

    /**
     * 删除时间
     * 工作流定义被删除的时间戳
     */
    private Date eventTime;
    /**
     * 获取事件类型
     *
     * @return 返回PROCESS_DEFINITION_DELETED类型，表示工作流定义删除事件
     */
    @Override
    public ListenerEventType getEventType() {
        return ListenerEventType.PROCESS_DEFINITION_DELETED;
    }

    /**
     * 获取事件标题
     *
     * 生成描述性的事件标题，包含被删除的工作流名称。
     * 格式："process definition deleted:[工作流名称]"
     *
     * @return 事件标题字符串
     */
    @Override
    public String getTitle() {
        return String.format("process definition deleted:%s", this.name);
    }
}
