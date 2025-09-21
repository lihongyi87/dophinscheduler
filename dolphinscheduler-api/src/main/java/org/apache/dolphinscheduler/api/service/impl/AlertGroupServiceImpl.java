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

import static org.apache.dolphinscheduler.api.constants.ApiFuncIdentificationConstant.ALERT_GROUP_CREATE;
import static org.apache.dolphinscheduler.api.constants.ApiFuncIdentificationConstant.ALERT_GROUP_DELETE;
import static org.apache.dolphinscheduler.api.constants.ApiFuncIdentificationConstant.ALERT_GROUP_UPDATE;
import static org.apache.dolphinscheduler.api.constants.ApiFuncIdentificationConstant.ALERT_GROUP_VIEW;

import org.apache.dolphinscheduler.api.enums.Status;
import org.apache.dolphinscheduler.api.exceptions.ServiceException;
import org.apache.dolphinscheduler.api.service.AlertGroupService;
import org.apache.dolphinscheduler.api.utils.PageInfo;
import org.apache.dolphinscheduler.common.enums.AuthorizationType;
import org.apache.dolphinscheduler.common.enums.UserType;
import org.apache.dolphinscheduler.dao.entity.AlertGroup;
import org.apache.dolphinscheduler.dao.entity.User;
import org.apache.dolphinscheduler.dao.mapper.AlertGroupMapper;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;

/**
 * 告警组服务实现类
 *
 * <p>该类实现了告警组的完整管理功能，包括告警组的创建、更新、删除、
 * 查询等操作。告警组用于将告警插件实例进行分组管理，便于不同的任务
 * 可以发送告警到不同的接收组。</p>
 *
 * <p>主要功能：</p>
 * <ul>
 *   <li>创建告警组 - 支持设置告警组名称、描述和关联的告警实例</li>
 *   <li>更新告警组 - 修改告警组信息和关联关系</li>
 *   <li>删除告警组 - 软删除告警组及其关联关系</li>
 *   <li>分页查询 - 支持按名称搜索和分页显示</li>
 *   <li>权限控制 - 基于用户角色进行访问控制</li>
 * </ul>
 */
@Service
@Slf4j
public class AlertGroupServiceImpl extends BaseServiceImpl implements AlertGroupService {

    @Autowired
    private AlertGroupMapper alertGroupMapper;

    /**
     * 查询所有告警组列表
     *
     * @param loginUser 登录用户
     * @return 告警组列表
     */
    @Override
    public List<AlertGroup> queryAllAlertGroup(User loginUser) {
        // 检查登录用户类型，管理员可以查看所有告警组
        if (loginUser.getUserType().equals(UserType.ADMIN_USER)) {
            // 管理员权限：直接查询所有告警组列表
            return alertGroupMapper.queryAllGroupList();
        }

        // 普通用户权限控制：获取用户拥有权限的告警组ID集合
        Set<Integer> ids = resourcePermissionCheckService.userOwnedResourceIdsAcquisition(AuthorizationType.ALERT_GROUP,
                loginUser.getId(), log);

        // 判断用户是否有任何告警组权限
        if (ids.isEmpty()) {
            // 无权限时返回空列表，避免泄露系统信息
            return Collections.emptyList();
        }

        // 根据用户有权限的ID列表批量查询告警组
        return alertGroupMapper.selectBatchIds(ids);
    }

    /**
     * 根据ID查询告警组
     *
     * @param loginUser 登录用户
     * @param id        告警组ID
     * @return 告警组对象
     */
    @Override
    public AlertGroup queryAlertGroupById(User loginUser, Integer id) {
        // 执行用户权限检查：验证用户是否有查看指定告警组的权限
        if (!canOperatorPermissions(loginUser, new Object[]{id}, AuthorizationType.ALERT_GROUP, ALERT_GROUP_VIEW)) {
            // 权限检查失败，抛出无操作权限异常
            throw new ServiceException(Status.USER_NO_OPERATION_PERM);
        }

        // 根据ID查询告警组记录
        AlertGroup alertGroup = alertGroupMapper.selectById(id);
        // 验证告警组是否存在
        if (alertGroup == null) {
            // 告警组不存在，抛出资源不存在异常
            throw new ServiceException(Status.ALERT_GROUP_NOT_EXIST, id);
        }

        // 返回查询到的告警组对象
        return alertGroup;
    }

    /**
     * 分页查询告警组列表
     *
     * @param loginUser 登录用户
     * @param searchVal 搜索关键词
     * @param pageNo    页码
     * @param pageSize  每页大小
     * @return 告警组分页结果
     */
    @Override
    public PageInfo<AlertGroup> listPaging(User loginUser, String searchVal, Integer pageNo, Integer pageSize) {
        // 构建MyBatis-Plus分页对象，设置页码和每页大小
        Page<AlertGroup> page = new Page<>(pageNo, pageSize);

        // 判断用户类型，管理员拥有查看所有告警组的权限
        if (loginUser.getUserType().equals(UserType.ADMIN_USER)) {
            // 管理员查询：使用全量分页查询，支持按名称搜索
            IPage<AlertGroup> alertGroupIPage = alertGroupMapper.queryAlertGroupPage(page, searchVal);
            // 将MyBatis-Plus分页结果转换为系统统一的分页信息格式
            return PageInfo.of(alertGroupIPage);
        }

        // 普通用户权限限制：获取用户拥有权限的告警组ID集合
        Set<Integer> ids = resourcePermissionCheckService.userOwnedResourceIdsAcquisition(AuthorizationType.ALERT_GROUP,
                loginUser.getId(), log);

        // 检查用户是否有任何告警组权限
        if (ids.isEmpty()) {
            // 无权限时返回空的分页对象，保持分页结构完整性
            return PageInfo.of(pageNo, pageSize);
        }

        // 有权限用户查询：根据权限ID列表进行受限分页查询
        IPage<AlertGroup> alertGroupIPage =
                alertGroupMapper.queryAlertGroupPageByIds(page, new ArrayList<>(ids), searchVal);
        // 转换并返回分页结果
        return PageInfo.of(alertGroupIPage);
    }

    /**
     * create alert group
     *
     * @param loginUser login user
     * @param groupName group name
     * @param desc description
     * @param alertInstanceIds alertInstanceIds
     * @return create result code
     */
    @Override
    @Transactional
    public AlertGroup createAlertGroup(User loginUser, String groupName, String desc, String alertInstanceIds) {
        // 初始化结果Map（兼容旧版本代码结构）
        Map<String, Object> result = new HashMap<>();
        // 权限验证：检查用户是否有创建告警组的权限
        if (!canOperatorPermissions(loginUser, null, AuthorizationType.ALERT_GROUP, ALERT_GROUP_CREATE)) {
            // 权限不足，抛出无操作权限异常
            throw new ServiceException(Status.USER_NO_OPERATION_PERM);
        }
        // 参数校验：检查描述信息长度是否超过系统限制
        if (checkDescriptionLength(desc)) {
            // 描述过长，记录警告日志并抛出异常
            log.warn("Parameter description is too long.");
            throw new ServiceException(Status.DESCRIPTION_TOO_LONG_ERROR);
        }
        // 构建告警组实体对象
        AlertGroup alertGroup = new AlertGroup();
        // 获取当前时间，用于创建时间和更新时间
        Date now = new Date();

        // 设置告警组基本属性
        alertGroup.setGroupName(groupName);                    // 设置告警组名称
        alertGroup.setAlertInstanceIds(alertInstanceIds);      // 设置关联的告警实例ID列表
        alertGroup.setDescription(desc);                       // 设置描述信息
        alertGroup.setCreateTime(now);                        // 设置创建时间
        alertGroup.setUpdateTime(now);                        // 设置更新时间
        alertGroup.setCreateUserId(loginUser.getId());        // 设置创建用户ID

        // 执行数据库插入操作，使用事务保证数据一致性
        try {
            // 调用MyBatis-Plus的insert方法插入告警组记录
            int insert = alertGroupMapper.insert(alertGroup);
            // 检查插入是否成功（影响行数大于0）
            if (insert > 0) {
                // 插入成功，记录操作日志
                log.info("Create alert group complete, groupName:{}", alertGroup.getGroupName());
                // 返回创建的告警组对象
                return alertGroup;
            }
            // 插入失败（影响行数为0），记录错误日志
            log.error("Create alert group error, groupName:{}", alertGroup.getGroupName());
            // 抛出创建失败异常
            throw new ServiceException(Status.CREATE_ALERT_GROUP_ERROR);
        } catch (DuplicateKeyException ex) {
            // 捕获数据库唯一键冲突异常（告警组名称重复）
            log.error("Create alert group error, groupName:{}", alertGroup.getGroupName(), ex);
            // 抛出告警组已存在异常
            throw new ServiceException(Status.ALERT_GROUP_EXIST);
        }
    }

    /**
     * updateWorkflowInstance alert group
     *
     * @param loginUser login user
     * @param id alert group id
     * @param groupName group name
     * @param desc description
     * @param alertInstanceIds alertInstanceIds
     * @return update result code
     */
    @Override
    public AlertGroup updateAlertGroupById(User loginUser, int id, String groupName, String desc,
                                           String alertInstanceIds) {
        // 权限验证：检查用户是否有更新指定告警组的权限
        if (!canOperatorPermissions(loginUser, new Object[]{id}, AuthorizationType.ALERT_GROUP, ALERT_GROUP_UPDATE)) {
            // 权限检查失败，抛出无操作权限异常
            throw new ServiceException(Status.USER_NO_OPERATION_PERM);
        }
        // 参数校验：验证描述信息长度是否符合系统要求
        if (checkDescriptionLength(desc)) {
            // 描述过长，抛出参数错误异常
            throw new ServiceException(Status.DESCRIPTION_TOO_LONG_ERROR);
        }
        // 查询现有告警组记录，确保记录存在
        AlertGroup alertGroup = alertGroupMapper.selectById(id);

        // 验证告警组是否存在
        if (alertGroup == null) {
            // 告警组不存在，抛出资源不存在异常
            throw new ServiceException(Status.ALERT_GROUP_NOT_EXIST);
        }

        // 获取当前时间，用于更新时间戳
        Date now = new Date();

        // 更新告警组属性：仅在提供新值时更新组名称
        if (!StringUtils.isEmpty(groupName)) {
            // 组名不为空时才更新，避免意外清空
            alertGroup.setGroupName(groupName);
        }
        // 更新其他属性（即使为空也允许更新）
        alertGroup.setDescription(desc);                       // 更新描述信息
        alertGroup.setUpdateTime(now);                        // 更新修改时间
        alertGroup.setCreateUserId(loginUser.getId());        // 更新操作用户ID
        alertGroup.setAlertInstanceIds(alertInstanceIds);     // 更新关联的告警实例ID列表

        // 执行数据库更新操作
        try {
            // 调用MyBatis-Plus的updateById方法更新记录
            alertGroupMapper.updateById(alertGroup);
            // 更新成功，记录操作日志
            log.info("Update alert group complete, groupName:{}", alertGroup.getGroupName());
            // 返回更新后的告警组对象
            return alertGroup;
        } catch (DuplicateKeyException ex) {
            // 捕获唯一键冲突异常（通常是组名重复）
            log.error("Update alert group error, groupName:{}", alertGroup.getGroupName(), ex);
            // 抛出告警组已存在异常
            throw new ServiceException(Status.ALERT_GROUP_EXIST);
        }
    }

    /**
     * delete alert group by id
     *
     * @param loginUser login user
     * @param id        alert group id
     * @return delete result code
     */
    @Override
    public void deleteAlertGroupById(User loginUser, int id) {

        // 权限验证：检查用户是否有删除指定告警组的权限
        if (!canOperatorPermissions(loginUser, new Object[]{id}, AuthorizationType.ALERT_GROUP, ALERT_GROUP_DELETE)) {
            // 权限不足，抛出无操作权限异常
            throw new ServiceException(Status.USER_NO_OPERATION_PERM);
        }

        // 系统保护：禁止删除默认告警组（ID=1），因为系统服务模块需要使用它
        if (id == 1) {
            // 尝试删除默认告警组，记录警告日志
            log.warn("Not allow to delete the default alarm group.");
            // 抛出不允许删除默认告警组异常
            throw new ServiceException(Status.NOT_ALLOW_TO_DELETE_DEFAULT_ALARM_GROUP);
        }

        // 存在性检查：验证告警组是否存在
        AlertGroup alertGroup = alertGroupMapper.selectById(id);
        if (alertGroup == null) {
            // 告警组不存在，抛出资源不存在异常
            throw new ServiceException(Status.ALERT_GROUP_NOT_EXIST);
        }

        // 执行删除操作：物理删除告警组记录
        alertGroupMapper.deleteById(id);
        // 记录删除成功日志，便于运维追踪
        log.info("Delete alert group complete, groupId:{}", id);
    }

    /**
     * verify group name exists
     *
     * @param groupName group name
     * @return check result code
     */
    @Override
    public boolean existGroupName(String groupName) {
        // 调用Mapper方法检查告警组名称是否已存在
        // 使用Boolean.TRUE进行严格布尔值比较，避免null值导致的异常
        return alertGroupMapper.existGroupName(groupName) == Boolean.TRUE;
    }
}
