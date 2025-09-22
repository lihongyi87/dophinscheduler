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

package org.apache.dolphinscheduler.api.service.impl;

import static org.apache.dolphinscheduler.api.constants.ApiFuncIdentificationConstant.PROJECT;
import static org.apache.dolphinscheduler.api.constants.ApiFuncIdentificationConstant.PROJECT_CREATE;
import static org.apache.dolphinscheduler.api.constants.ApiFuncIdentificationConstant.PROJECT_DELETE;

import org.apache.dolphinscheduler.api.enums.Status;
import org.apache.dolphinscheduler.api.exceptions.ServiceException;
import org.apache.dolphinscheduler.api.service.ProjectService;
import org.apache.dolphinscheduler.api.service.TaskGroupService;
import org.apache.dolphinscheduler.api.utils.PageInfo;
import org.apache.dolphinscheduler.api.utils.Result;
import org.apache.dolphinscheduler.common.constants.Constants;
import org.apache.dolphinscheduler.common.enums.AuthorizationType;
import org.apache.dolphinscheduler.common.enums.UserType;
import org.apache.dolphinscheduler.common.utils.CodeGenerateUtils;
import org.apache.dolphinscheduler.dao.entity.Project;
import org.apache.dolphinscheduler.dao.entity.ProjectUser;
import org.apache.dolphinscheduler.dao.entity.ProjectWorkflowDefinitionCount;
import org.apache.dolphinscheduler.dao.entity.User;
import org.apache.dolphinscheduler.dao.entity.WorkflowDefinition;
import org.apache.dolphinscheduler.dao.mapper.ProjectMapper;
import org.apache.dolphinscheduler.dao.mapper.ProjectUserMapper;
import org.apache.dolphinscheduler.dao.mapper.UserMapper;
import org.apache.dolphinscheduler.dao.mapper.WorkflowDefinitionMapper;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;

/**
 * 项目服务实现类
 *
 * <p>该类实现了项目管理的全部功能。项目是DolphinScheduler中
 * 的主要组织单元，所有工作流、资源、数据源等都归属于某个项目。
 * 项目提供了资源隔离和权限控制的基础。</p>
 *
 * <p>主要功能：</p>
 * <ul>
 *   <li>项目生命周期管理 - 创建、更新、删除项目</li>
 *   <li>项目查询 - 按名称、编码、分页查询</li>
 *   <li>用户授权 - 管理项目成员和权限</li>
 *   <li>资源统计 - 统计项目下的工作流数量</li>
 *   <li>依赖检查 - 检查项目是否可以安全删除</li>
 *   <li>权限验证 - 验证用户对项目的访问权限</li>
 * </ul>
 *
 * <p>该类支持多租户模式，不同用户只能看到自己有权限的项目。</p>
 */
@Service
@Slf4j
public class ProjectServiceImpl extends BaseServiceImpl implements ProjectService {

    @Lazy
    @Autowired
    private TaskGroupService taskGroupService;

    @Autowired
    private ProjectMapper projectMapper;

    @Autowired
    private ProjectUserMapper projectUserMapper;

    @Autowired
    private WorkflowDefinitionMapper workflowDefinitionMapper;

    @Autowired
    private UserMapper userMapper;

    /**
     * 创建项目
     * 创建新的项目，项目是DolphinScheduler中的主要组织单元
     *
     * @param loginUser 登录用户
     * @param name      项目名称
     * @param desc      项目描述
     * @return 创建结果
     */
    @Override
    @Transactional
    public Result createProject(User loginUser, String name, String desc) {
        // 初始化结果对象，用于返回操作结果
        Result result = new Result();

        // 检查项目描述的长度是否符合要求（不超过255个字符）
        // 防止数据库字段溢出和保持数据质量
        checkDesc(result, desc);
        if (result.getCode() != Status.SUCCESS.getCode()) {
            // 描述验证失败，直接返回错误结果
            return result;
        }

        // 检查用户是否有创建项目的权限
        // 基于权限管理系统进行精细化的权限控制
        if (!canOperatorPermissions(loginUser, null, AuthorizationType.PROJECTS, PROJECT_CREATE)) {
            log.warn("用户 {} 没有创建项目的权限", loginUser.getUserName());
            putMsg(result, Status.USER_NO_OPERATION_PERM);
            return result;
        }

        // 检查项目名称是否已存在，确保项目名称的唯一性
        Project project = projectMapper.queryByName(name);
        if (project != null) {
            log.warn("项目名称 {} 已存在，无法创建重名项目", name);
            putMsg(result, Status.PROJECT_ALREADY_EXISTS, name);
            return result;
        }

        // 获取当前时间作为创建和更新时间
        Date now = new Date();

        // 使用建造者模式构建新的项目对象
        project = Project
                .builder()
                .name(name)                         // 设置项目名称
                .code(CodeGenerateUtils.genCode())  // 生成唯一的项目编码，用于内部标识
                .description(desc)                  // 设置项目描述
                .userId(loginUser.getId())          // 设置创建者ID，用于权限管理
                .userName(loginUser.getUserName())  // 设置创建者名称，方便显示
                .createTime(now)                    // 设置创建时间
                .updateTime(now)                    // 设置更新时间
                .build();

        // 将项目记录插入到数据库中
        int insertResult = projectMapper.insert(project);
        if (insertResult > 0) {
            // 项目创建成功，记录日志并返回成功结果
            log.info("项目创建成功，项目ID: {}, 项目名称: {}, 创建者: {}",
                    project.getId(), project.getName(), loginUser.getUserName());
            result.setData(project);
            putMsg(result, Status.SUCCESS);
        } else {
            // 项目创建失败，记录错误日志并返回失败结果
            log.error("项目创建失败，项目名: {}, 创建者: {}", name, loginUser.getUserName());
            putMsg(result, Status.CREATE_PROJECT_ERROR);
        }
        return result;
    }

    /**
     * 检查项目描述的有效性
     * 验证描述字段的长度是否符合数据库约束
     *
     * @param result 结果对象，用于返回检查结果
     * @param desc   项目描述
     */
    public static void checkDesc(Result result, String desc) {
        // 检查描述字段是否为空以及长度是否超过限制
        // 使用codePointCount而不是length()来正确处理Unicode字符（包括中文）
        if (!StringUtils.isEmpty(desc) && desc.codePointCount(0, desc.length()) > 255) {
            // 描述超过255个字符限制，设置错误状态
            result.setCode(Status.DESCRIPTION_TOO_LONG_ERROR.getCode());
            result.setMsg(Status.DESCRIPTION_TOO_LONG_ERROR.getMsg());
        } else {
            // 描述长度合理，设置成功状态
            result.setCode(Status.SUCCESS.getCode());
        }
    }

    /**
     * 根据项目编码查询项目详情
     * 通过唯一的项目编码获取项目的完整信息
     *
     * @param loginUser   登录用户
     * @param projectCode 项目编码
     * @return 项目详细信息
     */
    @Override
    public Result queryByCode(User loginUser, long projectCode) {
        // 初始化结果对象
        Result result = new Result();

        // 根据项目编码查询项目信息
        // 项目编码是系统生成的唯一标识符，不会重复
        Project project = projectMapper.queryByCode(projectCode);

        // 检查项目是否存在以及用户是否有查看权限
        // 这是一个综合性的权限检查，同时检查存在性和权限
        boolean hasProjectAndPerm = hasProjectAndPerm(loginUser, project, result, PROJECT);
        if (!hasProjectAndPerm) {
            // 无权限或项目不存在，直接返回错误结果
            // 结果对象中已经设置了具体的错误信息
            return result;
        }

        // 设置查询成功的结果
        if (project != null) {
            // 将项目对象设置为返回数据
            result.setData(project);
            // 设置成功状态和消息
            putMsg(result, Status.SUCCESS);
        }
        return result;
    }

    /**
     * 根据项目名称查询项目
     * 通过项目名称进行模糊或精确查询
     *
     * @param loginUser   登录用户
     * @param projectName 项目名称
     * @return 查询结果映射
     */
    @Override
    public Map<String, Object> queryByName(User loginUser, String projectName) {
        // 初始化结果映射，使用Map结构方便扩展
        Map<String, Object> result = new HashMap<>();

        // 根据项目名称查询项目
        // 项目名称在系统中应该是唯一的，但与编码不同，名称是用户可读的
        Project project = projectMapper.queryByName(projectName);

        // 检查项目存在性和用户权限
        // 这里传入的是Map类型的result，与Result类型的重载方法
        boolean hasProjectAndPerm = hasProjectAndPerm(loginUser, project, result, PROJECT);
        if (!hasProjectAndPerm) {
            // 项目不存在或无权限访问，返回包含错误信息的结果
            return result;
        }

        // 设置查询成功的结果
        if (project != null) {
            // 使用系统常量作key，保持与其他接口的一致性
            result.put(Constants.DATA_LIST, project);
            // 设置成功状态
            putMsg(result, Status.SUCCESS);
        }
        return result;
    }

    /**
     * 检查项目和授权
     * 综合检查项目存在性和用户权限，是权限管理的核心方法
     *
     * @param loginUser   登录用户
     * @param project     项目对象
     * @param projectCode 项目编码
     * @param permission  权限类型（如查看、编辑、删除等）
     * @return 检查结果映射
     */
    @Override
    public Map<String, Object> checkProjectAndAuth(User loginUser, Project project, long projectCode,
                                                   String permission) {
        // 初始化结果映射
        Map<String, Object> result = new HashMap<>();

        // 首先检查项目是否存在
        if (project == null) {
            // 项目不存在，记录错误日志并返回错误状态
            log.error("项目不存在，项目编码: {}", projectCode);
            putMsg(result, Status.PROJECT_NOT_EXIST);
        }
        // 项目存在，检查用户是否有指定权限
        else if (!canOperatorPermissions(loginUser, new Object[]{project.getId()}, AuthorizationType.PROJECTS,
                permission)) {
            // 用户没有指定权限，记录警告日志
            // 这里包含了用户名和项目编码，方便安全审计
            log.error("用户没有 {} 权限操作项目，用户名: {}, 项目编码: {}",
                    permission, loginUser.getUserName(), projectCode);
            putMsg(result, Status.USER_NO_OPERATION_PROJECT_PERM, loginUser.getUserName(), projectCode);
        }
        // 所有检查都通过，返回成功状态
        else {
            putMsg(result, Status.SUCCESS);
        }
        return result;
    }

    /**
     * 检查项目和授权（异常版本）
     * 如果检查不通过将抛出异常，适用于需要立即停止执行的场景
     *
     * @param loginUser  登录用户
     * @param project    项目对象
     * @param permission 权限类型
     * @throws ServiceException 当项目不存在或用户无权限时抛出
     */
    public void checkProjectAndAuthThrowException(@NonNull User loginUser, @Nullable Project project,
                                                  String permission) {
        // 检查项目是否存在
        if (project == null) {
            // 项目不存在，直接抛出服务异常
            throw new ServiceException(Status.PROJECT_NOT_EXIST);
        }

        // 检查用户权限，无权限时抛出异常
        // 这里传入项目ID数组和权限类型进行精细化权限检查
        if (!canOperatorPermissions(loginUser, new Object[]{project.getId()}, AuthorizationType.PROJECTS, permission)) {
            // 抛出具体的权限不足异常，包含用户名和项目编码
            throw new ServiceException(Status.USER_NO_OPERATION_PROJECT_PERM, loginUser.getUserName(),
                    project.getCode());
        }
    }

    @Override
    public void checkProjectAndAuthThrowException(User loginUser, Long projectCode, String permission) {
        if (projectCode == null) {
            throw new ServiceException(Status.PROJECT_NOT_EXIST);
        }
        Project project = projectMapper.queryByCode(projectCode);
        checkProjectAndAuthThrowException(loginUser, project, permission);
    }

    @Override
    public boolean hasProjectAndPerm(User loginUser, Project project, Map<String, Object> result, String permission) {
        boolean checkResult = false;
        if (project == null) {
            log.error("Project does not exist.");
            putMsg(result, Status.PROJECT_NOT_FOUND, "");
        } else if (!canOperatorPermissions(loginUser, new Object[]{project.getId()}, AuthorizationType.PROJECTS,
                permission)) {
            log.error("User does not have {} permission to operate project, userName:{}, projectCode:{}.",
                    permission, loginUser.getUserName(), project.getCode());
            putMsg(result, Status.USER_NO_OPERATION_PROJECT_PERM, loginUser.getUserName(), project.getCode());
        } else {
            checkResult = true;
        }
        return checkResult;
    }

    @Override
    public boolean hasProjectAndWritePerm(User loginUser, Project project, Result result) {
        boolean checkResult = false;
        if (project == null) {
            log.error("Project does not exist.");
            putMsg(result, Status.PROJECT_NOT_FOUND, "");
        } else {
            // case 1: user is admin
            if (loginUser.getUserType() == UserType.ADMIN_USER) {
                return true;
            }
            // case 2: user is project owner
            if (project.getUserId().equals(loginUser.getId())) {
                return true;
            }
            // case 3: check user permission level
            ProjectUser projectUser = projectUserMapper.queryProjectRelation(project.getId(), loginUser.getId());
            if (projectUser == null || projectUser.getPerm() != Constants.DEFAULT_ADMIN_PERMISSION) {
                putMsg(result, Status.USER_NO_WRITE_PROJECT_PERM, loginUser.getUserName(), project.getCode());
                checkResult = false;
            } else {
                checkResult = true;
            }
        }
        return checkResult;
    }

    @Override
    public boolean hasProjectAndWritePerm(User loginUser, Project project, Map<String, Object> result) {
        boolean checkResult = false;
        if (project == null) {
            log.error("Project does not exist.");
            putMsg(result, Status.PROJECT_NOT_FOUND, "");
        } else {
            // case 1: user is admin
            if (loginUser.getUserType() == UserType.ADMIN_USER) {
                return true;
            }
            // case 2: user is project owner
            if (project.getUserId().equals(loginUser.getId())) {
                return true;
            }
            // case 3: check user permission level
            ProjectUser projectUser = projectUserMapper.queryProjectRelation(project.getId(), loginUser.getId());
            if (projectUser == null || projectUser.getPerm() != Constants.DEFAULT_ADMIN_PERMISSION) {
                putMsg(result, Status.USER_NO_WRITE_PROJECT_PERM, loginUser.getUserName(), project.getCode());
                checkResult = false;
            } else {
                checkResult = true;
            }
        }
        return checkResult;
    }

    @Override
    public void checkHasProjectWritePermissionThrowException(User loginUser, long projectCode) {
        Project project = projectMapper.queryByCode(projectCode);
        checkHasProjectWritePermissionThrowException(loginUser, project);
    }

    @Override
    public void checkHasProjectWritePermissionThrowException(User loginUser, Project project) {
        if (project == null) {
            throw new ServiceException(Status.PROJECT_NOT_FOUND, null);
        }
        // case 1: user is admin
        if (loginUser.getUserType() == UserType.ADMIN_USER) {
            return;
        }
        // case 2: user is project owner
        if (project.getUserId().equals(loginUser.getId())) {
            return;
        }
        // case 3: check user permission level
        ProjectUser projectUser = projectUserMapper.queryProjectRelation(project.getId(), loginUser.getId());
        if (projectUser == null || projectUser.getPerm() != Constants.DEFAULT_ADMIN_PERMISSION) {
            throw new ServiceException(Status.USER_NO_WRITE_PROJECT_PERM, loginUser.getUserName(), project.getCode());
        }
    }

    @Override
    public boolean hasProjectAndPerm(User loginUser, Project project, Result result, String permission) {
        boolean checkResult = false;
        if (project == null) {
            log.error("Project does not exist.");
            putMsg(result, Status.PROJECT_NOT_FOUND, "");
        } else if (!canOperatorPermissions(loginUser, new Object[]{project.getId()}, AuthorizationType.PROJECTS,
                permission)) {
            log.error("User does not have {} permission to operate project, userName:{}, projectCode:{}.",
                    permission, loginUser.getUserName(), project.getCode());
            putMsg(result, Status.USER_NO_OPERATION_PROJECT_PERM, loginUser.getUserName(), project.getName());
        } else {
            checkResult = true;
        }
        return checkResult;
    }

    /**
     * 管理员可以查看所有项目，普通用户只能查看有权限的项目
     * 分页查询项目列表，支持按名称搜索和权限过滤
     *
     * @param loginUser 登录用户
     * @param searchVal 搜索值（项目名称）
     * @param pageSize  每页显示数量
     * @param pageNo    页码（从1开始）
     * @return 用户有权限查看的项目列表
     */
    @Override
    public Result queryProjectListPaging(User loginUser, Integer pageSize, Integer pageNo, String searchVal) {
        // 初始化结果对象
        Result result = new Result();
        // 创建分页信息对象
        PageInfo<Project> pageInfo = new PageInfo<>(pageNo, pageSize);
        // 创建分页查询对象
        Page<Project> page = new Page<>(pageNo, pageSize);

        // 获取用户有权限的项目ID集合
        // 这里会根据用户类型和权限配置返回不同的项目集合
        Set<Integer> projectIds = resourcePermissionCheckService
                .userOwnedResourceIdsAcquisition(AuthorizationType.PROJECTS, loginUser.getId(), log);

        // 如果用户没有任何项目权限，返回空结果
        if (projectIds.isEmpty()) {
            result.setData(pageInfo);
            putMsg(result, Status.SUCCESS);
            return result;
        }

        // 执行分页查询，传入权限过滤后的项目ID列表和搜索条件
        IPage<Project> projectIPage =
                projectMapper.queryProjectListPaging(page, new ArrayList<>(projectIds), searchVal);

        // 获取分页查询结果
        List<Project> projectList = projectIPage.getRecords();

        // 如果不是管理员用户，需要设置默认权限
        if (loginUser.getUserType() != UserType.ADMIN_USER) {
            for (Project project : projectList) {
                // 普通用户设置为默认管理员权限，表示可以操作该项目
                project.setPerm(Constants.DEFAULT_ADMIN_PERMISSION);
            }
        }

        // 如果查询结果为空，直接返回空的分页结果
        if (CollectionUtils.isEmpty(projectList)) {
            result.setData(pageInfo);
            putMsg(result, Status.SUCCESS);
            return result;
        }

        // 批量查询项目创建者信息
        // 从项目列表中提取所有唯一的用户ID
        List<User> userList = userMapper.selectByIds(projectList.stream()
                .map(Project::getUserId).distinct().collect(Collectors.toList()));
        // 将用户列表转换为ID到名称的映射
        Map<Integer, String> userMap = userList.stream().collect(Collectors.toMap(User::getId, User::getUserName));

        // 批量查询项目下的工作流定义数量
        List<Long> projectCodes = projectList.stream().map(Project::getCode).distinct().collect(Collectors.toList());
        Map<Long, Integer> projectWorkflowDefinitionCountMap = workflowDefinitionMapper
                .queryProjectWorkflowDefinitionCountByProjectCodes(projectCodes)
                .stream()
                .collect(Collectors.toMap(ProjectWorkflowDefinitionCount::getProjectCode,
                        ProjectWorkflowDefinitionCount::getCount));

        // 为每个项目设置创建者名称和工作流数量
        for (Project project : projectList) {
            // 设置创建者名称，用于前端显示
            project.setUserName(userMap.get(project.getUserId()));
            // 设置项目下的工作流定义数量，默认为0
            project.setDefCount(projectWorkflowDefinitionCountMap.getOrDefault(project.getCode(), 0));
        }

        // 设置分页信息
        pageInfo.setTotal((int) projectIPage.getTotal());
        pageInfo.setTotalList(projectList);
        result.setData(pageInfo);
        putMsg(result, Status.SUCCESS);
        return result;
    }

    /**
     * admin can view all projects
     *
     * @param userId    user id
     * @param loginUser login user
     * @param searchVal search value
     * @param pageSize  page size
     * @param pageNo    page number
     * @return project list which with the login user's authorized level
     */
    @Override
    public Result queryProjectWithAuthorizedLevelListPaging(Integer userId, User loginUser, Integer pageSize,
                                                            Integer pageNo, String searchVal) {
        Result result = new Result();
        PageInfo<Project> pageInfo = new PageInfo<>(pageNo, pageSize);
        Page<Project> page = new Page<>(pageNo, pageSize);
        Set<Integer> allProjectIds = resourcePermissionCheckService
                .userOwnedResourceIdsAcquisition(AuthorizationType.PROJECTS, loginUser.getId(), log);
        Set<Integer> userProjectIds = resourcePermissionCheckService
                .userOwnedResourceIdsAcquisition(AuthorizationType.PROJECTS, userId, log);
        if (allProjectIds.isEmpty()) {
            result.setData(pageInfo);
            putMsg(result, Status.SUCCESS);
            return result;
        }
        IPage<Project> projectIPage =
                projectMapper.queryProjectListPaging(page, new ArrayList<>(allProjectIds), searchVal);

        List<Project> projectList = projectIPage.getRecords();

        for (Project project : projectList) {
            if (userProjectIds.contains(project.getId())) {
                ProjectUser projectUser = projectUserMapper.queryProjectRelation(project.getId(), userId);
                if (projectUser == null) {
                    // in this case, the user is the project owner, maybe it's better to set it to ALL_PERMISSION.
                    project.setPerm(Constants.DEFAULT_ADMIN_PERMISSION);
                } else {
                    project.setPerm(projectUser.getPerm());
                }
            } else {
                project.setPerm(0);
            }
        }

        pageInfo.setTotal((int) projectIPage.getTotal());
        pageInfo.setTotalList(projectList);
        result.setData(pageInfo);
        putMsg(result, Status.SUCCESS);
        return result;
    }

    /**
     * 根据项目编码删除项目
     * 删除项目时需要检查依赖关系，确保数据一致性
     *
     * @param loginUser   登录用户
     * @param projectCode 项目编码
     * @return 删除结果
     */
    @Override
    public Result deleteProject(User loginUser, Long projectCode) {
        // 初始化结果对象
        Result result = new Result();
        // 根据项目编码查询项目信息
        Project project = projectMapper.queryByCode(projectCode);

        // 检查项目存在性和用户的写权限
        // 删除操作需要高级权限，只有项目创建者或管理员才能删除
        boolean hasProjectAndWritePerm = hasProjectAndWritePerm(loginUser, project, result);
        if (!hasProjectAndWritePerm) {
            return result;
        }

        // 再次检查项目和授权，确保删除权限
        checkProjectAndAuth(result, loginUser, project, project == null ? 0L : project.getCode(), PROJECT_DELETE);
        if (result.getCode() != Status.SUCCESS.getCode()) {
            return result;
        }

        // 断言项目不为空，因为上面已经检查过
        assert project != null;

        // 检查项目下是否还有工作流定义
        // 在删除项目之前必须先删除所有的工作流定义，防止数据孤立
        List<WorkflowDefinition> workflowDefinitionList =
                workflowDefinitionMapper.queryAllDefinitionList(project.getCode());

        if (!workflowDefinitionList.isEmpty()) {
            // 如果还有工作流定义，不允许删除项目
            log.warn("请先删除项目中的工作流定义！项目编码: {}", projectCode);
            putMsg(result, Status.DELETE_PROJECT_ERROR_DEFINES_NOT_NULL);
            return result;
        }

        // 删除项目下的任务组
        // 任务组是项目的一部分，需要级联删除
        taskGroupService.deleteTaskGroupByProjectCode(project.getCode());

        // 执行项目删除操作
        int delete = projectMapper.deleteById(project.getId());
        if (delete > 0) {
            // 删除成功，记录日志并返回成功结果
            log.info("项目删除成功，项目ID: {}, 项目名称: {}, 操作者: {}",
                    project.getId(), project.getName(), loginUser.getUserName());
            result.setData(Boolean.TRUE);
            putMsg(result, Status.SUCCESS);
        } else {
            // 删除失败，记录错误日志
            log.error("项目删除失败，项目编码: {}, 项目名称: {}, 操作者: {}",
                    projectCode, project.getName(), loginUser.getUserName());
            putMsg(result, Status.DELETE_PROJECT_ERROR);
        }
        return result;
    }

    /**
     * get check result
     *
     * @param loginUser login user
     * @param project   project
     * @return check result
     */
    private Map<String, Object> getCheckResult(User loginUser, Project project, String perm) {
        Map<String, Object> checkResult =
                checkProjectAndAuth(loginUser, project, project == null ? 0L : project.getCode(), perm);
        Status status = (Status) checkResult.get(Constants.STATUS);
        if (status != Status.SUCCESS) {
            return checkResult;
        }
        return null;
    }

    /**
     * 更新项目信息
     * 允许修改项目名称和描述，但需要验证名称唯一性
     *
     * @param loginUser   登录用户
     * @param projectCode 项目编码
     * @param projectName 新的项目名称
     * @param desc        新的项目描述
     * @return 更新结果
     */
    @Override
    public Result update(User loginUser, Long projectCode, String projectName, String desc) {
        // 初始化结果对象
        Result result = new Result();

        // 验证项目描述的有效性
        checkDesc(result, desc);
        if (result.getCode() != Status.SUCCESS.getCode()) {
            return result;
        }

        // 根据项目编码查询现有项目
        Project project = projectMapper.queryByCode(projectCode);
        // 检查用户是否有修改该项目的权限
        boolean hasProjectAndWritePerm = hasProjectAndWritePerm(loginUser, project, result);
        if (!hasProjectAndWritePerm) {
            return result;
        }

        // 检查新的项目名称是否已被其他项目使用
        Project tempProject = projectMapper.queryByName(projectName);
        if (tempProject != null && tempProject.getCode() != project.getCode()) {
            // 存在同名的其他项目，不允许更新
            log.warn("项目名称 {} 已被其他项目使用，无法更新", projectName);
            putMsg(result, Status.PROJECT_ALREADY_EXISTS, projectName);
            return result;
        }

        // 重新查询用户信息确保数据最新
        User user = userMapper.selectById(loginUser.getId());
        if (user == null) {
            log.error("用户 {} 不存在", loginUser.getId());
            putMsg(result, Status.USER_NOT_EXIST, loginUser.getId());
            return result;
        }

        // 更新项目属性
        project.setName(projectName);        // 设置新的项目名称
        project.setDescription(desc);        // 设置新的项目描述
        project.setUpdateTime(new Date());   // 更新修改时间
        project.setUserId(user.getId());     // 设置修改者ID

        // 执行数据库更新操作
        int update = projectMapper.updateById(project);
        if (update > 0) {
            // 更新成功，记录日志并返回结果
            log.info("项目更新成功，项目ID: {}, 新名称: {}, 操作者: {}",
                    project.getId(), projectName, loginUser.getUserName());
            result.setData(project);
            putMsg(result, Status.SUCCESS);
        } else {
            // 更新失败，记录错误日志
            log.error("项目更新失败，项目编码: {}, 项目名称: {}, 操作者: {}",
                    project.getCode(), project.getName(), loginUser.getUserName());
            putMsg(result, Status.UPDATE_PROJECT_ERROR);
        }
        return result;
    }

    /**
     * query all project with authorized level
     *
     * @param loginUser login user
     * @return project list
     */
    @Override
    public Result queryProjectWithAuthorizedLevel(User loginUser, Integer userId) {
        Result result = new Result();

        Set<Integer> projectIds = resourcePermissionCheckService
                .userOwnedResourceIdsAcquisition(AuthorizationType.PROJECTS, loginUser.getId(), log);
        List<Project> projectList = projectMapper.listAuthorizedProjects(
                loginUser.getUserType().equals(UserType.ADMIN_USER) ? 0 : loginUser.getId(),
                new ArrayList<>(projectIds));

        List<Project> unauthorizedProjectsList = new ArrayList<>();
        List<Project> authedProjectList = new ArrayList<>();
        Set<Project> projectSet;
        if (projectList != null && !projectList.isEmpty()) {
            projectSet = new HashSet<>(projectList);
            authedProjectList = projectMapper.queryAuthedProjectListByUserId(userId);
            unauthorizedProjectsList = getUnauthorizedProjects(projectSet, authedProjectList);
        }

        for (int i = 0; i < authedProjectList.size(); i++) {
            authedProjectList.get(i).setPerm(7);
        }

        for (int i = 0; i < unauthorizedProjectsList.size(); i++) {
            unauthorizedProjectsList.get(i).setPerm(0);
        }

        List<Project> joined = new ArrayList<>();
        joined.addAll(authedProjectList);
        joined.addAll(unauthorizedProjectsList);

        result.setData(joined);
        putMsg(result, Status.SUCCESS);
        return result;
    }

    /**
     * query unauthorized project
     *
     * @param loginUser login user
     * @param userId    user id
     * @return the projects which user have not permission to see
     */
    @Override
    public Result queryUnauthorizedProject(User loginUser, Integer userId) {
        Result result = new Result();

        Set<Integer> projectIds = resourcePermissionCheckService
                .userOwnedResourceIdsAcquisition(AuthorizationType.PROJECTS, loginUser.getId(), log);
        if (projectIds.isEmpty()) {
            result.setData(Collections.emptyList());
            putMsg(result, Status.SUCCESS);
            return result;
        }
        List<Project> projectList = projectMapper.listAuthorizedProjects(
                loginUser.getUserType().equals(UserType.ADMIN_USER) ? 0 : loginUser.getId(),
                new ArrayList<>(projectIds));

        List<Project> resultList = new ArrayList<>();
        Set<Project> projectSet;
        if (projectList != null && !projectList.isEmpty()) {
            projectSet = new HashSet<>(projectList);

            List<Project> authedProjectList = projectMapper.queryAuthedProjectListByUserId(userId);

            resultList = getUnauthorizedProjects(projectSet, authedProjectList);
        }
        result.setData(resultList);
        putMsg(result, Status.SUCCESS);
        return result;
    }

    /**
     * get unauthorized project
     *
     * @param projectSet        project set
     * @param authedProjectList authed project list
     * @return project list that unauthorized
     */
    private List<Project> getUnauthorizedProjects(Set<Project> projectSet, List<Project> authedProjectList) {
        List<Project> resultList;
        Set<Project> authedProjectSet;
        if (authedProjectList != null && !authedProjectList.isEmpty()) {
            authedProjectSet = new HashSet<>(authedProjectList);
            projectSet.removeAll(authedProjectSet);
        }
        resultList = new ArrayList<>(projectSet);
        return resultList;
    }

    /**
     * query authorized project
     *
     * @param loginUser login user
     * @param userId    user id
     * @return projects which the user have permission to see, Except for items created by this user
     */
    @Override
    public Result queryAuthorizedProject(User loginUser, Integer userId) {
        Result result = new Result();

        List<Project> projects = projectMapper.queryAuthedProjectListByUserId(userId);
        result.setData(projects);
        putMsg(result, Status.SUCCESS);

        return result;
    }

    /**
     * query authorized user
     *
     * @param loginUser   login user
     * @param projectCode project code
     * @return users        who have permission for the specified project
     */
    @Override
    public Result queryAuthorizedUser(User loginUser, Long projectCode) {
        Result result = new Result();

        // 1. check read permission
        Project project = this.projectMapper.queryByCode(projectCode);
        boolean hasProjectAndPerm = this.hasProjectAndPerm(loginUser, project, result, PROJECT);
        if (!hasProjectAndPerm) {
            return result;
        }

        // 2. query authorized user list
        List<User> users = this.userMapper.queryAuthedUserListByProjectId(project.getId());
        result.setData(users);
        this.putMsg(result, Status.SUCCESS);
        return result;
    }

    /**
     * query authorized project
     *
     * @param loginUser login user
     * @return projects which the user have permission to see, Except for items created by this user
     */
    @Override
    public Map<String, Object> queryProjectCreatedByUser(User loginUser) {
        Map<String, Object> result = new HashMap<>();

        List<Project> projects = projectMapper.queryProjectCreatedByUser(loginUser.getId());
        result.put(Constants.DATA_LIST, projects);
        putMsg(result, Status.SUCCESS);

        return result;
    }

    /**
     * query authorized and user create project list by user
     *
     * @param loginUser login user
     * @return project list
     */
    @Override
    public Result queryProjectCreatedAndAuthorizedByUser(User loginUser) {
        Result result = new Result();

        Set<Integer> projectIds = resourcePermissionCheckService
                .userOwnedResourceIdsAcquisition(AuthorizationType.PROJECTS, loginUser.getId(), log);
        if (projectIds.isEmpty()) {
            result.setData(Collections.emptyList());
            putMsg(result, Status.SUCCESS);
            return result;
        }
        List<Project> projects = projectMapper.selectBatchIds(projectIds);

        result.setData(projects);
        putMsg(result, Status.SUCCESS);

        return result;
    }

    /**
     * check whether have read permission
     *
     * @param user    user
     * @param project project
     * @return true if the user have permission to see the project, otherwise return false
     */
    private boolean checkReadPermission(User user, Project project) {
        int permissionId = queryPermission(user, project);
        return (permissionId & Constants.READ_PERMISSION) != 0;
    }

    /**
     * query permission id
     *
     * @param user    user
     * @param project project
     * @return permission
     */
    private int queryPermission(User user, Project project) {
        if (user.getUserType() == UserType.ADMIN_USER) {
            return Constants.READ_PERMISSION;
        }

        if (Objects.equals(project.getUserId(), user.getId())) {
            return Constants.ALL_PERMISSIONS;
        }

        ProjectUser projectUser = projectUserMapper.queryProjectRelation(project.getId(), user.getId());

        if (projectUser == null) {
            return 0;
        }

        return projectUser.getPerm();

    }

    /**
     * query all project list
     *
     * @param user
     * @return project list
     */
    @Override
    public Result queryAllProjectList(User user) {
        Result result = new Result();
        List<Project> projects =
                projectMapper.queryAllProject(user.getUserType() == UserType.ADMIN_USER ? 0 : user.getId());

        result.setData(projects);
        putMsg(result, Status.SUCCESS);
        return result;
    }

    /**
     * check project and authorization
     *
     * @param result      result
     * @param loginUser   login user
     * @param project     project
     * @param projectCode project code
     * @return true if the login user have permission to see the project
     */
    @Override
    public void checkProjectAndAuth(Result result, User loginUser, Project project, long projectCode,
                                    String permission) {
        if (project == null) {
            log.error("Project does not exist, project code:{}.", projectCode);
            putMsg(result, Status.PROJECT_NOT_EXIST);
        } else if (!canOperatorPermissions(loginUser, new Object[]{project.getId()}, AuthorizationType.PROJECTS,
                permission)) {
            // check read permission
            putMsg(result, Status.USER_NO_OPERATION_PROJECT_PERM, loginUser.getUserName(), projectCode);
        } else {
            putMsg(result, Status.SUCCESS);
        }
    }

    /**
     * query all project for dependent node
     *
     * @return project list
     */
    @Override
    public Result queryAllProjectListForDependent() {
        Result result = new Result<>();
        List<Project> projects =
                projectMapper.queryAllProjectForDependent();
        result.setData(projects);
        putMsg(result, Status.SUCCESS);
        return result;
    }

    @Override
    public List<Long> getAuthorizedProjectCodes(User loginUser) {
        if (loginUser == null) {
            throw new IllegalArgumentException("loginUser can not be null");
        }
        Set<Integer> projectIds = resourcePermissionCheckService
                .userOwnedResourceIdsAcquisition(AuthorizationType.PROJECTS, loginUser.getId(), log);
        if (CollectionUtils.isEmpty(projectIds)) {
            return Collections.emptyList();
        }
        return projectMapper.selectBatchIds(projectIds)
                .stream()
                .map(Project::getCode)
                .collect(Collectors.toList());
    }
}
