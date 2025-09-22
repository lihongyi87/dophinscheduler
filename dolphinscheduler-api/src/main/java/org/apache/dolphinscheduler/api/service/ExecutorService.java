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

package org.apache.dolphinscheduler.api.service;

import org.apache.dolphinscheduler.api.dto.workflow.WorkflowBackFillRequest;
import org.apache.dolphinscheduler.api.dto.workflow.WorkflowTriggerRequest;
import org.apache.dolphinscheduler.api.dto.workflowInstance.WorkflowExecuteResponse;
import org.apache.dolphinscheduler.api.enums.ExecuteType;
import org.apache.dolphinscheduler.common.enums.TaskDependType;
import org.apache.dolphinscheduler.dao.entity.User;
import org.apache.dolphinscheduler.dao.entity.WorkflowDefinition;

import java.util.List;
import java.util.Map;

/**
 * 工作流执行服务接口
 *
 * <p>该接口提供工作流实例的执行管理功能，包括启动、停止、暂停、
 * 重跑等操作。作为工作流调度的核心服务，处理所有与工作流
 * 执行相关的请求。</p>
 *
 * <p>主要功能：</p>
 * <ul>
 *   <li>触发工作流执行</li>
 *   <li>回填执行工作流</li>
 *   <li>控制工作流实例状态</li>
 *   <li>管理任务节点执行</li>
 * </ul>
 */
public interface ExecutorService {

    /**
     * 触发工作流执行并返回工作流实例 ID
     * 根据触发请求创建并启动工作流实例
     *
     * <p>业务逻辑：</p>
     * <ul>
     *   <li>验证工作流定义有效性</li>
     *   <li>检查用户执行权限</li>
     *   <li>验证全局参数格式</li>
     *   <li>创建工作流实例</li>
     *   <li>提交到执行队列</li>
     * </ul>
     *
     * @param workflowTriggerRequest 工作流触发请求，包含项目、工作流定义、参数等信息
     * @return 新创建的工作流实例 ID
     * @throws ServiceException 当工作流定义不存在或权限不足时抛出
     */
    Integer triggerWorkflowDefinition(final WorkflowTriggerRequest workflowTriggerRequest);

    /**
     * 回填执行工作流并返回工作流实例 ID列表
     * 用于批量补跑历史数据，按指定时间范围生成多个工作流实例
     *
     * <p>业务逻辑：</p>
     * <ul>
     *   <li>验证工作流定义有效性</li>
     *   <li>检查用户执行权限</li>
     *   <li>按调度周期计算执行时间节点</li>
     *   <li>批量创建工作流实例</li>
     *   <li>提交到执行队列</li>
     * </ul>
     *
     * <p>使用场景：数据补全、历史数据重跑等</p>
     *
     * @param workflowBackFillRequest 工作流回填请求，包含时间范围、调度参数等
     * @return 创建的所有工作流实例 ID列表
     * @throws ServiceException 当时间范围无效或权限不足时抛出
     */
    List<Integer> backfillWorkflowDefinition(final WorkflowBackFillRequest workflowBackFillRequest);

    /**
     * 检查工作流定义是否可以执行
     * 验证工作流定义的完整性和有效性
     *
     * <p>校验项目：</p>
     * <ul>
     *   <li>工作流定义是否存在且可用</li>
     *   <li>任务节点配置是否完整</li>
     *   <li>依赖关系是否有效</li>
     *   <li>子工作流定义是否可用</li>
     *   <li>资源文件是否存在</li>
     * </ul>
     *
     * @param projectCode 项目编码，用于权限校验
     * @param workflowDefinition 工作流定义对象，可为空
     * @param workflowDefinitionCode 工作流定义编码，用于查找工作流定义
     * @param version 工作流定义版本，可为空使用最新版本
     * @throws ServiceException 当工作流定义无效或不可执行时抛出
     */
    void checkWorkflowDefinitionValid(long projectCode, WorkflowDefinition workflowDefinition,
                                      long workflowDefinitionCode,
                                      Integer version);

    /**
     * 对工作流实例中的指定任务执行操作
     * 从指定的节点开始执行或重新执行任务
     *
     * <p>业务逻辑：</p>
     * <ul>
     *   <li>验证工作流实例是否存在</li>
     *   <li>检查用户执行权限</li>
     *   <li>验证起始节点有效性</li>
     *   <li>计算任务依赖关系</li>
     *   <li>更新任务状态并提交执行</li>
     * </ul>
     *
     * <p>使用场景：任务失败重跑、部分任务执行等</p>
     *
     * @param loginUser 登录用户，需要有工作流执行权限
     * @param projectCode 项目编码，用于权限校验
     * @param workflowInstanceId 工作流实例 ID，必须存在
     * @param startNodeList 起始任务节点列表，逗号分隔的任务名称
     * @param taskDependType 任务依赖类型，决定如何处理任务间的依赖关系
     * @return 执行结果，包含执行状态和相关信息
     * @throws ServiceException 当工作流实例不存在或权限不足时抛出
     */
    WorkflowExecuteResponse executeTask(User loginUser,
                                        long projectCode,
                                        Integer workflowInstanceId,
                                        String startNodeList,
                                        TaskDependType taskDependType);

    /**
     * 控制工作流实例的执行状态
     * 可以暂停、停止、重复、恢复工作流实例的执行
     *
     * <p>支持的操作类型：</p>
     * <ul>
     *   <li>PAUSE - 暂停正在运行的工作流实例</li>
     *   <li>STOP - 停止正在运行的工作流实例</li>
     *   <li>REPEAT_RUNNING - 重新运行已完成的工作流实例</li>
     *   <li>RECOVER_WAITTING_THREAD - 恢复暂停的工作流实例</li>
     * </ul>
     *
     * @param loginUser 登录用户，需要有工作流控制权限
     * @param workflowInstanceId 工作流实例 ID，必须存在
     * @param executeType 执行类型，决定要执行的具体操作
     * @throws ServiceException 当工作流实例不存在、状态不允许或权限不足时抛出
     */
    void controlWorkflowInstance(User loginUser, Integer workflowInstanceId, ExecuteType executeType);

    /**
     * 检查当前工作流是否包含子工作流且所有子工作流都有效
     * 递归验证子工作流定义的存在性和可用性
     *
     * <p>校验内容：</p>
     * <ul>
     *   <li>检查是否存在子工作流任务节点</li>
     *   <li>验证子工作流定义是否存在</li>
     *   <li>递归检查子工作流的子工作流</li>
     *   <li>验证子工作流的有效性</li>
     * </ul>
     *
     * @param workflowDefinition 工作流定义对象，包含任务节点信息
     * @return true表示所有子工作流都有效，false表示存在无效的子工作流
     */
    boolean checkSubWorkflowDefinitionValid(WorkflowDefinition workflowDefinition);

    /**
     * 强制启动任务实例
     * 忽略任务依赖关系，强制启动指定的任务实例
     *
     * <p>使用场景：任务被错误地设置为依赖等待状态，需要管理员干预</p>
     *
     * @param loginUser 登录用户，需要有管理员权限
     * @param queueId 任务队列ID，指定要强制启动的任务
     * @return 操作结果信息，包含执行状态
     * @throws ServiceException 当任务不存在或权限不足时抛出
     */
    Map<String, Object> forceStartTaskInstance(User loginUser, int queueId);

    /**
     * 执行流处理任务实例
     * 启动一个流处理任务，用于实时数据处理
     *
     * <p>业务逻辑：</p>
     * <ul>
     *   <li>验证任务定义有效性</li>
     *   <li>检查用户执行权限</li>
     *   <li>验证资源配置（Worker组、租户、环境）</li>
     *   <li>创建流任务实例</li>
     *   <li>提交到指定Worker组执行</li>
     * </ul>
     *
     * <p>与正常任务的区别：流任务不属于任何工作流实例，独立运行</p>
     *
     * @param loginUser 登录用户，需要有任务执行权限
     * @param projectCode 项目编码，用于权限校验
     * @param taskDefinitionCode 任务定义编码
     * @param taskDefinitionVersion 任务定义版本
     * @param warningGroupId 告警组ID，用于任务异常通知
     * @param workerGroup Worker组名称，指定任务执行的目标机器组
     * @param tenantCode 租户编码，决定任务的执行用户和资源隔离
     * @param environmentCode 环境编码，指定任务执行环境
     * @param startParams 启动参数，传递给任务的全局参数值
     * @param dryRun 是否为试运行模式，1表示试运行，0表示正常运行
     * @throws ServiceException 当任务定义不存在或资源配置无效时抛出
     */
    void execStreamTaskInstance(User loginUser, long projectCode,
                                long taskDefinitionCode, int taskDefinitionVersion,
                                int warningGroupId,
                                String workerGroup,
                                String tenantCode,
                                Long environmentCode,
                                Map<String, String> startParams,
                                int dryRun);
}
