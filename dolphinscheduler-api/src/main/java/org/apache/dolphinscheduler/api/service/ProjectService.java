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

import org.apache.dolphinscheduler.api.exceptions.ServiceException;
import org.apache.dolphinscheduler.api.utils.Result;
import org.apache.dolphinscheduler.dao.entity.Project;
import org.apache.dolphinscheduler.dao.entity.User;

import java.util.List;
import java.util.Map;

/**
 * 项目服务接口
 *
 * <p>该接口提供项目管理的核心功能。项目是DolphinScheduler中
 * 组织工作流和资源的基本单元，所有工作流定义都归属于某个项目。</p>
 *
 * <p>主要功能：</p>
 * <ul>
 *   <li>创建和管理项目</li>
 *   <li>查询项目信息</li>
 *   <li>项目授权管理</li>
 *   <li>项目用户关联</li>
 *   <li>项目资源管理</li>
 * </ul>
 */
public interface ProjectService {

    /**
     * 创建新项目
     * 项目是DolphinScheduler中组织工作流和资源的基本单元
     *
     * <p>业务逻辑：</p>
     * <ul>
     *   <li>验证项目名称唯一性</li>
     *   <li>检查用户创建权限</li>
     *   <li>创建项目并设置创建者为项目管理员</li>
     *   <li>初始化项目基本配置</li>
     * </ul>
     *
     * @param loginUser 登录用户，需要有项目创建权限
     * @param name 项目名称，不能为空且全局唯一
     * @param desc 项目描述，可选
     * @return 创建结果，成功返回项目信息，失败返回错误信息
     * @throws ServiceException 当项目名称重复或权限不足时抛出
     */
    Result createProject(User loginUser, String name, String desc);

    /**
     * 根据项目编码查询项目详情
     * 通过项目的唯一编码获取项目完整信息
     *
     * @param loginUser 登录用户，需要有项目查看权限
     * @param projectCode 项目编码，系统生成的唯一标识
     * @return 项目详细信息，包含创建者、创建时间等
     * @throws ServiceException 当项目不存在或没有权限时抛出
     */
    Result queryByCode(User loginUser, long projectCode);

    /**
     * 根据项目名称查询项目详情
     * 通过项目名称精确查找项目信息
     *
     * @param loginUser 登录用户，需要有项目查看权限
     * @param projectName 项目名称，精确匹配
     * @return 项目详细信息，如果不存在返回空结果
     */
    Map<String, Object> queryByName(User loginUser, String projectName);

    /**
     * 检查项目和授权
     *
     * @param loginUser 登录用户
     * @param project project
     * @param projectCode project code
     * @param perm String
     * @return true if the login user have permission to see the project
     */
    @Deprecated
    Map<String, Object> checkProjectAndAuth(User loginUser, Project project, long projectCode, String perm);

    void checkProjectAndAuthThrowException(User loginUser, Project project, String permission) throws ServiceException;

    void checkProjectAndAuthThrowException(User loginUser, Long projectCode, String permission) throws ServiceException;

    boolean hasProjectAndPerm(User loginUser, Project project, Map<String, Object> result, String perm);

    /**
     * has project and permission
     *
     * @param loginUser  login user
     * @param project    project
     * @param result     result
     * @param permission String
     * @return true if the login user have permission to the project
     */
    @Deprecated
    boolean hasProjectAndPerm(User loginUser, Project project, Result result, String permission);

    @Deprecated
    boolean hasProjectAndWritePerm(User loginUser, Project project, Result result);

    @Deprecated
    boolean hasProjectAndWritePerm(User loginUser, Project project, Map<String, Object> result);

    void checkHasProjectWritePermissionThrowException(User loginUser, long projectCode);

    void checkHasProjectWritePermissionThrowException(User loginUser, Project project);

    /**
     * 分页查询项目列表
     * 管理员可以查看所有项目，普通用户只能查看有权限的项目
     *
     * @param loginUser 登录用户，用于权限过滤
     * @param pageSize 每页大小，建议10-100
     * @param pageNo 页码，从1开始
     * @param searchVal 搜索关键词，可为空，支持项目名称模糊匹配
     * @return 用户有权限查看的项目列表，包含总数和分页信息
     */
    Result queryProjectListPaging(User loginUser, Integer pageSize, Integer pageNo, String searchVal);

    /**
     * 分页查询用户授权级别的项目列表
     * 返回指定用户的项目列表，包含用户的授权级别信息
     *
     * @param userId 目标用户ID，查询该用户的项目授权情况
     * @param loginUser 登录用户，需要有用户管理权限
     * @param pageSize 每页大小
     * @param pageNo 页码
     * @param searchVal 搜索关键词，支持项目名称模糊匹配
     * @return 包含授权级别的项目列表，管理员可查看所有项目
     */
    Result queryProjectWithAuthorizedLevelListPaging(Integer userId, User loginUser, Integer pageSize, Integer pageNo,
                                                     String searchVal);

    /**
     * 根据编码删除项目
     *
     * <p>业务逻辑：</p>
     * <ul>
     *   <li>验证项目是否存在</li>
     *   <li>检查项目下是否有工作流定义</li>
     *   <li>检查项目下是否有运行中的实例</li>
     *   <li>检查用户删除权限</li>
     *   <li>级联删除项目及相关资源</li>
     * </ul>
     *
     * <p>级联影响：删除项目会同时删除项目下所有的工作流定义、任务定义、资源文件等</p>
     *
     * @param loginUser 登录用户，需要有项目删除权限或为项目创建者
     * @param projectCode 项目编码，必须存在且没有运行中的实例
     * @return 删除结果，成功或失败信息
     * @throws ServiceException 当项目不存在、有运行中实例或权限不足时抛出
     */
    Result deleteProject(User loginUser, Long projectCode);

    /**
     * 更新项目信息
     *
     * <p>业务逻辑：</p>
     * <ul>
     *   <li>验证项目是否存在</li>
     *   <li>检查用户编辑权限</li>
     *   <li>验证新名称唯一性（如果名称变化）</li>
     *   <li>更新项目基本信息</li>
     * </ul>
     *
     * @param loginUser 登录用户，需要有项目编辑权限或为项目创建者
     * @param projectCode 项目编码，必须存在
     * @param projectName 新的项目名称，不能为空且不能与其他项目重复
     * @param desc 新的项目描述
     * @return 更新结果，成功或失败信息
     * @throws ServiceException 当项目不存在、名称冲突或权限不足时抛出
     */
    Result update(User loginUser, Long projectCode, String projectName, String desc);

    /**
     * 查询用户未授权的项目列表
     * 返回系统中该用户没有访问权限的项目，用于授权管理
     *
     * @param loginUser 登录用户，需要有用户管理权限
     * @param userId 目标用户ID
     * @return 该用户未授权的项目列表，用于授权选择
     */
    Result queryUnauthorizedProject(User loginUser, Integer userId);

    /**
     * 查询用户已授权的项目列表
     * 返回该用户具有访问权限的项目，不包括用户自己创建的项目
     *
     * @param loginUser 登录用户，需要有用户管理权限
     * @param userId 目标用户ID
     * @return 该用户已授权的项目列表，不包括用户创建的项目
     */
    Result queryAuthorizedProject(User loginUser, Integer userId);

    /**
     * 查询用户所有授权级别的项目列表
     * 返回用户可访问的所有项目及其授权级别信息
     *
     * @param loginUser 登录用户，用于权限过滤
     * @param userId 目标用户ID，查询该用户的项目授权情况
     * @return 项目列表，包含授权级别信息（读取、写入、管理等）
     */
    Result queryProjectWithAuthorizedLevel(User loginUser, Integer userId);

    /**
     * 查询项目的授权用户列表
     * 返回对指定项目有访问权限的所有用户
     *
     * @param loginUser 登录用户，需要有项目管理权限
     * @param projectCode 项目编码，查询该项目的授权情况
     * @return 对该项目有权限的用户列表，包含授权级别信息
     */
    Result queryAuthorizedUser(User loginUser, Long projectCode);

    /**
     * 查询用户创建的项目列表
     * 返回用户作为创建者的所有项目
     *
     * @param loginUser 登录用户，查询该用户创建的项目
     * @return 用户创建的项目列表，不包括其他用户授权的项目
     */
    Map<String, Object> queryProjectCreatedByUser(User loginUser);

    /**
     * 查询所有包含工作流定义的项目列表
     * 返回系统中所有至少包含一个工作流定义的项目
     *
     * @param loginUser 登录用户，用于权限过滤
     * @return 包含工作流定义的项目列表，用于依赖工作流选择等场景
     */
    Result queryAllProjectList(User loginUser);

    /**
     * 查询用户可访问的所有项目列表
     * 返回用户创建的和被授权的所有项目
     *
     * @param loginUser 登录用户，查询该用户可访问的所有项目
     * @return 用户可访问的所有项目列表，包括自己创建和其他用户授权的项目
     */
    Result queryProjectCreatedAndAuthorizedByUser(User loginUser);

    /**
     * check project and authorization
     *
     * @param result result
     * @param loginUser login user
     * @param project project
     * @param projectCode project code
     * @param perm String
     * @return true if the login user have permission to see the project
     */
    void checkProjectAndAuth(Result result, User loginUser, Project project, long projectCode, String perm);

    /**
     * 查询依赖节点可用的所有项目列表
     * 依赖节点的项目列表不应受权限限制，返回系统中所有项目
     *
     * @return 所有项目列表，不进行权限过滤，用于依赖工作流选择
     */
    Result queryAllProjectListForDependent();

    List<Long> getAuthorizedProjectCodes(User loginUser);
}
