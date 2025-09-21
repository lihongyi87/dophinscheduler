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

package org.apache.dolphinscheduler.service.process;

import org.apache.dolphinscheduler.common.enums.AuthorizationType;
import org.apache.dolphinscheduler.common.graph.DAG;
import org.apache.dolphinscheduler.common.model.TaskNodeRelation;
import org.apache.dolphinscheduler.dao.entity.DagData;
import org.apache.dolphinscheduler.dao.entity.DataSource;
import org.apache.dolphinscheduler.dao.entity.Schedule;
import org.apache.dolphinscheduler.dao.entity.TaskDefinitionLog;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.dao.entity.User;
import org.apache.dolphinscheduler.dao.entity.WorkflowDefinition;
import org.apache.dolphinscheduler.dao.entity.WorkflowDefinitionLog;
import org.apache.dolphinscheduler.dao.entity.WorkflowInstance;
import org.apache.dolphinscheduler.dao.entity.WorkflowTaskRelation;
import org.apache.dolphinscheduler.dao.entity.WorkflowTaskRelationLog;
import org.apache.dolphinscheduler.service.model.TaskNode;

import java.util.List;
import java.util.Optional;

/**
 * 工作流服务接口
 *
 * 提供工作流相关的核心业务逻辑，是DolphinScheduler的核心服务接口。
 * 该接口统一管理工作流定义、工作流实例、任务定义、任务关系等核心数据。
 *
 * 主要功能模块：
 * 1. 工作流实例管理：查询、删除、查找父子工作流关系
 * 2. 工作流定义管理：查询、保存、版本切换
 * 3. 任务定义管理：保存、版本切换、状态查询
 * 4. 任务关系管理：保存、查询、DAG图生成
 * 5. 权限管理：用户授权查询
 * 6. 调度管理：调度信息查询
 * 7. 数据源管理：数据源查询
 * 8. 集群配置管理：配置文件查询
 *
 * 设计特点：
 * - 抽象化：将工作流相关的业务逻辑抽象成统一接口
 * - 原子性：多个操作支持事务，保证数据一致性
 * - 版本控制：支持工作流和任务的版本管理
 * - DAG支持：提供有向无环图的生成和解析
 *
 * 类比：就像生产线的控制中心，管理整个生产流程的定义、
 *      执行、监控和调度，确保生产任务有序进行。
 */
public interface ProcessService {

    /**
     * 根据ID查询工作流实例详情
     *
     * @param workflowInstanceId 工作流实例ID
     * @return 工作流实例详情，可能为空
     */
    Optional<WorkflowInstance> findWorkflowInstanceDetailById(int workflowInstanceId);

    /**
     * 根据ID查询工作流实例
     *
     * @param workflowInstanceId 工作流实例ID
     * @return 工作流实例对象
     */
    WorkflowInstance findWorkflowInstanceById(int workflowInstanceId);

    /**
     * 查找工作流定义
     *
     * @param workflowDefinitionCode 工作流定义代码
     * @param workflowDefinitionVersion 工作流定义版本
     * @return 工作流定义对象
     */
    WorkflowDefinition findWorkflowDefinition(Long workflowDefinitionCode, int workflowDefinitionVersion);

    /**
     * 删除工作流实例
     *
     * @param workflowInstanceId 工作流实例ID
     * @return 删除结果，成功返回1
     */
    int deleteWorkflowInstanceById(int workflowInstanceId);

    /**
     * 查找所有子工作流定义代码
     *
     * 递归查找指定工作流下所有子工作流的定义代码
     *
     * @param workflowDefinitionCode 父工作流定义代码
     * @return 子工作流定义代码列表
     */
    List<Long> findAllSubWorkflowDefinitionCode(long workflowDefinitionCode);

    /**
     * 获取工作流的租户
     *
     * 根据租户代码和用户ID确定工作流执行时使用的租户
     *
     * @param tenantCode 租户代码
     * @param userId 用户ID
     * @return 最终使用的租户代码
     */
    String getTenantForWorkflow(String tenantCode, int userId);

    /**
     * 查找子工作流实例
     *
     * @param parentWorkflowInstanceId 父工作流实例ID
     * @param parentTaskId 父任务ID
     * @return 子工作流实例
     */
    WorkflowInstance findSubWorkflowInstance(Integer parentWorkflowInstanceId, Integer parentTaskId);

    /**
     * 查找父工作流实例
     *
     * @param subWorkflowInstanceId 子工作流实例ID
     * @return 父工作流实例
     */
    WorkflowInstance findParentWorkflowInstance(Integer subWorkflowInstanceId);

    /**
     * 查询已发布的调度列表
     *
     * @param workflowDefinitionCode 工作流定义代码
     * @return 调度列表
     */
    List<Schedule> queryReleaseSchedulerListByWorkflowDefinitionCode(long workflowDefinitionCode);

    /**
     * 根据ID查找数据源
     *
     * @param id 数据源ID
     * @return 数据源对象
     */
    DataSource findDataSourceById(int id);

    /**
     * 列出未授权的资源
     *
     * 检查用户对指定资源的授权情况，返回未授权的资源列表
     *
     * @param userId 用户ID
     * @param needChecks 需要检查的资源数组
     * @param authorizationType 授权类型
     * @param <T> 资源类型
     * @return 未授权的资源列表
     */
    <T> List<T> listUnauthorized(int userId, T[] needChecks, AuthorizationType authorizationType);

    /**
     * 根据ID获取用户
     *
     * @param userId 用户ID
     * @return 用户对象
     */
    User getUserById(int userId);

    /**
     * 切换工作流定义版本
     *
     * @param workflowDefinition 工作流定义
     * @param workflowDefinitionLog 工作流定义日志
     * @return 更新数量
     */
    int switchVersion(WorkflowDefinition workflowDefinition, WorkflowDefinitionLog workflowDefinitionLog);

    /**
     * 切换工作流任务关系版本
     *
     * @param workflowDefinition 工作流定义
     * @return 更新数量
     */
    int switchWorkflowTaskRelationVersion(WorkflowDefinition workflowDefinition);

    /**
     * 切换任务定义版本
     *
     * @param taskCode 任务代码
     * @param taskVersion 任务版本
     * @return 更新数量
     */
    int switchTaskDefinitionVersion(long taskCode, int taskVersion);

    /**
     * 保存任务定义
     *
     * @param operator 操作用户
     * @param projectCode 项目代码
     * @param taskDefinitionLogs 任务定义日志列表
     * @param syncDefine 是否同步定义
     * @return 保存数量
     */
    int saveTaskDefine(User operator, long projectCode, List<TaskDefinitionLog> taskDefinitionLogs, Boolean syncDefine);

    /**
     * 保存工作流定义
     *
     * @param operator 操作用户
     * @param workflowDefinition 工作流定义
     * @param syncDefine 是否同步定义
     * @param isFromWorkflowDefinition 是否来自工作流定义
     * @return 保存数量
     */
    int saveWorkflowDefine(User operator, WorkflowDefinition workflowDefinition, Boolean syncDefine,
                           Boolean isFromWorkflowDefinition);

    /**
     * 保存任务关系
     *
     * @param operator 操作用户
     * @param projectCode 项目代码
     * @param workflowDefinitionCode 工作流定义代码
     * @param workflowDefinitionVersion 工作流定义版本
     * @param taskRelationList 任务关系列表
     * @param taskDefinitionLogs 任务定义日志列表
     * @param syncDefine 是否同步定义
     * @return 保存数量
     */
    int saveTaskRelation(User operator, long projectCode, long workflowDefinitionCode, int workflowDefinitionVersion,
                         List<WorkflowTaskRelationLog> taskRelationList, List<TaskDefinitionLog> taskDefinitionLogs,
                         Boolean syncDefine);

    /**
     * 判断任务是否在线
     *
     * @param taskCode 任务代码
     * @return true表示在线
     */
    boolean isTaskOnline(long taskCode);

    /**
     * 生成DAG图
     *
     * 根据工作流定义生成有向无环图
     *
     * @param workflowDefinition 工作流定义
     * @return DAG图对象
     */
    DAG<Long, TaskNode, TaskNodeRelation> genDagGraph(WorkflowDefinition workflowDefinition);

    /**
     * 生成DAG数据
     *
     * @param workflowDefinition 工作流定义
     * @return DAG数据对象
     */
    DagData genDagData(WorkflowDefinition workflowDefinition);

    /**
     * 根据代码查找任务关系
     *
     * @param workflowDefinitionCode 工作流定义代码
     * @param workflowDefinitionVersion 工作流定义版本
     * @return 任务关系列表
     */
    List<WorkflowTaskRelation> findRelationByCode(long workflowDefinitionCode, int workflowDefinitionVersion);

    /**
     * 转换任务节点
     *
     * 将任务关系和任务定义转换为任务节点
     *
     * @param taskRelationList 任务关系列表
     * @param taskDefinitionLogs 任务定义日志列表
     * @return 任务节点列表
     */
    List<TaskNode> transformTask(List<WorkflowTaskRelation> taskRelationList,
                                 List<TaskDefinitionLog> taskDefinitionLogs);

    /**
     * 根据名称查找集群配置
     *
     * @param clusterName 集群名称
     * @return 配置YAML字符串
     */
    String findConfigYamlByName(String clusterName);

    /**
     * 强制工作流实例成功
     *
     * 根据任务实例强制设置工作流实例为成功状态
     *
     * @param taskInstance 任务实例
     */
    void forceWorkflowInstanceSuccessByTaskInstanceId(TaskInstance taskInstance);

}
