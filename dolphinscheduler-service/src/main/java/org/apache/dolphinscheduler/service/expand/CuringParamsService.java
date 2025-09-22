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

package org.apache.dolphinscheduler.service.expand;

import org.apache.dolphinscheduler.common.enums.CommandType;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.dao.entity.WorkflowInstance;
import org.apache.dolphinscheduler.plugin.task.api.model.Property;
import org.apache.dolphinscheduler.plugin.task.api.parameters.AbstractParameters;

import java.util.Date;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import lombok.NonNull;

/**
 * 参数固化服务接口
 *
 * 负责工作流和任务参数的解析、替换和固化处理。
 * 在任务执行前将各种参数占位符替换为实际值。
 *
 * 参数类型：
 * - 内置参数：系统预定义的参数，如${system.datetime}
 * - 全局参数：工作流级别的参数
 * - 本地参数：任务级别的参数
 * - 项目参数：项目级别的参数
 * - 父工作流参数：从父工作流传递的参数
 *
 * 参数优先级（从高到低）：
 * 本地参数 > 工作流参数 > 项目参数 > 全局参数
 *
 * 应用场景：
 * - 动态配置：根据环境、时间等动态设置参数
 * - 参数传递：在任务间传递参数
 * - 模板化配置：使用参数模板实现灵活配置
 *
 * 类比：就像模板引擎，将模板中的变量占位符替换为实际值。
 */
public interface CuringParamsService {

    /**
     * 转换参数占位符
     *
     * 将字符串中的参数占位符替换为实际值
     *
     * @param val 包含占位符的字符串
     * @param allParamMap 所有参数映射
     * @return 替换后的字符串
     */
    String convertParameterPlaceholders(String val, Map<String, Property> allParamMap);

    /**
     * 固化全局参数
     *
     * 将全局参数中的占位符替换为实际值，处理内置参数
     *
     * @param workflowInstanceId 工作流实例ID
     * @param globalParamMap 全局参数映射
     * @param globalParamList 全局参数列表
     * @param commandType 命令类型
     * @param scheduleTime 调度时间
     * @param timezone 时区
     * @return 固化后的参数JSON字符串
     */
    String curingGlobalParams(Integer workflowInstanceId, Map<String, String> globalParamMap,
                              List<Property> globalParamList, CommandType commandType, Date scheduleTime,
                              String timezone);

    /**
     * 参数解析准备
     *
     * 为任务执行准备所有需要的参数，包括全局参数、本地参数等
     *
     * @param taskInstance 任务实例
     * @param parameters 任务参数
     * @param workflowInstance 工作流实例
     * @param projectName 项目名称
     * @param workflowDefinitionName 工作流定义名称
     * @return 所有参数的映射
     */
    Map<String, Property> paramParsingPreparation(@NonNull TaskInstance taskInstance,
                                                  @NonNull AbstractParameters parameters,
                                                  @NonNull WorkflowInstance workflowInstance,
                                                  String projectName,
                                                  String workflowDefinitionName);

    /**
     * 解析工作流启动参数
     *
     * @param cmdParam 命令参数
     * @return 启动参数映射
     */
    Map<String, Property> parseWorkflowStartParam(@Nullable Map<String, String> cmdParam);

    /**
     * 解析父工作流参数
     *
     * @param cmdParam 命令参数
     * @return 父工作流参数映射
     */
    Map<String, Property> parseWorkflowFatherParam(@Nullable Map<String, String> cmdParam);

    /**
     * 预构建业务参数
     *
     * 构建工作流执行所需的内置业务参数
     *
     * @param workflowInstance 工作流实例
     * @return 业务参数映射
     */
    Map<String, Property> preBuildBusinessParams(WorkflowInstance workflowInstance);

    /**
     * 获取项目参数映射
     *
     * @param projectCode 项目代码
     * @return 项目参数映射
     */
    Map<String, Property> getProjectParameterMap(long projectCode);
}
