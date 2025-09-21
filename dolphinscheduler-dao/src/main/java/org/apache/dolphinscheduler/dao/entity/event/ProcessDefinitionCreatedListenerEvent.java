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

import org.apache.dolphinscheduler.common.enums.Flag;
import org.apache.dolphinscheduler.common.enums.ListenerEventType;
import org.apache.dolphinscheduler.common.enums.ReleaseState;
import org.apache.dolphinscheduler.common.enums.WorkflowExecutionTypeEnum;
import org.apache.dolphinscheduler.dao.entity.TaskDefinitionLog;
import org.apache.dolphinscheduler.dao.entity.WorkflowDefinition;
import org.apache.dolphinscheduler.dao.entity.WorkflowTaskRelationLog;
import org.apache.dolphinscheduler.plugin.task.api.model.Property;

import java.util.Date;
import java.util.List;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 工作流定义创建监听事件
 *
 * 当新的工作流定义被创建时触发的事件，包含工作流定义的完整信息。
 * 这个事件对于审计、权限管理和工作流版本控制非常重要。
 *
 * 触发时机：
 * - 用户创建新的工作流定义
 * - 通过API创建工作流定义
 * - 导入工作流定义
 * - 复制现有工作流定义
 *
 * 使用场景：
 * - 记录工作流定义创建审计日志
 * - 同步工作流定义到外部系统
 * - 触发工作流定义审批流程
 * - 更新工作流定义统计信息
 * - 初始化工作流相关资源
 *
 * 事件内容：
 * - 工作流基本信息：ID、代码、名称、版本、描述
 * - 项目信息：项目代码、项目名称
 * - 用户信息：创建者、修改者
 * - 配置信息：全局参数、超时设置、告警组
 * - 任务信息：任务定义、任务关系
 * - 状态信息：发布状态、调度状态
 *
 * 类比：就像工厂创建新的生产流程图，需要记录流程名称、
 *      创建人、创建时间、各个工序步骤等完整信息。
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ProcessDefinitionCreatedListenerEvent implements AbstractListenerEvent {

    /**
     * 工作流定义ID
     * 数据库中的主键
     */
    private Integer id;

    /**
     * 工作流定义代码
     * 工作流定义的唯一标识符
     */
    private long code;

    /**
     * 工作流定义名称
     * 用户自定义的工作流名称
     */
    private String name;

    /**
     * 工作流版本
     * 用于版本控制和回滚
     */
    private int version;

    /**
     * 发布状态
     * 上线(ONLINE)/下线(OFFLINE)
     */
    private ReleaseState releaseState;

    /**
     * 项目代码
     * 工作流所属项目的唯一标识符
     */
    private long projectCode;

    /**
     * 工作流描述
     * 详细说明工作流的用途和功能
     */
    private String description;

    /**
     * 全局参数
     * JSON格式的全局参数字符串
     */
    private String globalParams;

    /**
     * 全局参数列表
     * 解析后的全局参数对象列表
     */
    private List<Property> globalParamList;

    /**
     * 全局参数映射
     * 键值对形式的全局参数
     */
    private Map<String, String> globalParamMap;

    /**
     * 创建时间
     * 工作流定义的创建时间戳
     */
    private Date createTime;

    /**
     * 更新时间
     * 工作流定义的最后更新时间
     */
    private Date updateTime;

    /**
     * 有效标记
     * 标识工作流是否有效
     */
    private Flag flag;

    /**
     * 创建用户ID
     * 创建该工作流的用户ID
     */
    private int userId;

    /**
     * 创建用户名
     * 创建该工作流的用户名称
     */
    private String userName;

    /**
     * 项目名称
     * 工作流所属项目的名称
     */
    private String projectName;

    /**
     * 位置信息
     * Web界面中任务节点的位置坐标
     */
    private String locations;

    /**
     * 调度发布状态
     * 调度任务的上线/下线状态
     */
    private ReleaseState scheduleReleaseState;

    /**
     * 超时时间
     * 工作流执行超时阈值，单位：分钟
     */
    private int timeout;

    /**
     * 修改人
     * 最后修改该工作流的用户名
     */
    private String modifyBy;

    /**
     * 告警组ID
     * 工作流失败时的告警组
     */
    private Integer warningGroupId;

    /**
     * 执行类型
     * 串行/并行执行策略
     */
    private WorkflowExecutionTypeEnum executionType;

    /**
     * 任务定义日志
     * 包含所有任务定义的详细信息
     */
    List<TaskDefinitionLog> taskDefinitionLogs;

    /**
     * 任务关系列表
     * 描述任务之间的依赖和执行顺序
     */
    List<WorkflowTaskRelationLog> taskRelationList;

    /**
     * 构造函数
     *
     * 从工作流定义对象创建事件，复制所有必要的属性。
     *
     * @param workflowDefinition 工作流定义对象
     */
    public ProcessDefinitionCreatedListenerEvent(WorkflowDefinition workflowDefinition) {
        this.setId(workflowDefinition.getId());
        this.setCode(workflowDefinition.getCode());
        this.setName(workflowDefinition.getName());
        this.setVersion(workflowDefinition.getVersion());
        this.setReleaseState(workflowDefinition.getReleaseState());
        this.setProjectCode(workflowDefinition.getProjectCode());
        this.setDescription(workflowDefinition.getDescription());
        this.setGlobalParams(workflowDefinition.getGlobalParams());
        this.setGlobalParamList(workflowDefinition.getGlobalParamList());
        this.setGlobalParamMap(workflowDefinition.getGlobalParamMap());
        this.setCreateTime(workflowDefinition.getCreateTime());
        this.setUpdateTime(workflowDefinition.getUpdateTime());
        this.setFlag(workflowDefinition.getFlag());
        this.setUserId(workflowDefinition.getUserId());
        this.setUserName(workflowDefinition.getUserName());
        this.setProjectName(workflowDefinition.getProjectName());
        this.setLocations(workflowDefinition.getLocations());
        this.setScheduleReleaseState(workflowDefinition.getScheduleReleaseState());
        this.setTimeout(workflowDefinition.getTimeout());
        this.setModifyBy(workflowDefinition.getModifyBy());
        this.setWarningGroupId(workflowDefinition.getWarningGroupId());
        this.setExecutionType(workflowDefinition.getExecutionType());
    }
    /**
     * 获取事件类型
     *
     * @return 返回PROCESS_DEFINITION_CREATED类型，表示工作流定义创建事件
     */
    @Override
    public ListenerEventType getEventType() {
        return ListenerEventType.PROCESS_DEFINITION_CREATED;
    }

    /**
     * 获取事件标题
     *
     * 生成描述性的事件标题，包含工作流定义名称。
     * 格式："process definition created:[工作流名称]"
     *
     * @return 事件标题字符串
     */
    @Override
    public String getTitle() {
        return String.format("process definition created:%s", this.name);
    }
}
