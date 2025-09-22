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

import org.apache.dolphinscheduler.api.utils.PageInfo;
import org.apache.dolphinscheduler.dao.entity.AlertGroup;
import org.apache.dolphinscheduler.dao.entity.User;

import java.util.List;

/**
 * 告警组服务接口
 * 提供告警组的增删改查等管理功能
 */
public interface AlertGroupService {

    /**
     * 查询所有告警组列表
     *
     * @param loginUser 登录用户
     * @return 告警组列表
     */
    List<AlertGroup> queryAllAlertGroup(User loginUser);

    /**
     * 根据ID查询告警组
     *
     * @param loginUser 登录用户
     * @param id 告警组ID
     * @return 告警组对象
     */
    AlertGroup queryAlertGroupById(User loginUser, Integer id);

    /**
     * 分页查询告警组列表
     * 支持按告警组名称模糊搜索
     *
     * @param loginUser 登录用户，需要有告警组查看权限
     * @param searchVal 搜索关键词，可为空，支持告警组名称模糊匹配
     * @param pageNo 页码，从1开始
     * @param pageSize 每页大小，建议10-100
     * @return 告警组分页数据，包含总数和当前页数据
     */
    PageInfo<AlertGroup> listPaging(User loginUser, String searchVal, Integer pageNo, Integer pageSize);

    /**
     * 创建告警组
     *
     * <p>业务逻辑：</p>
     * <ul>
     *   <li>验证告警组名称唯一性</li>
     *   <li>验证告警实例ID有效性</li>
     *   <li>创建告警组并关联告警实例</li>
     * </ul>
     *
     * @param loginUser 登录用户，需要有告警组创建权限
     * @param groupName 告警组名称，不能为空且全局唯一
     * @param desc 告警组描述，可选
     * @param alertInstanceIds 告警实例ID列表，逗号分隔，关联具体的告警插件实例
     * @return 创建成功的告警组对象
     * @throws ServiceException 当告警组名称重复或告警实例ID无效时抛出
     */
    AlertGroup createAlertGroup(User loginUser, String groupName, String desc, String alertInstanceIds);

    /**
     * 根据ID更新告警组信息
     *
     * <p>业务逻辑：</p>
     * <ul>
     *   <li>验证告警组是否存在</li>
     *   <li>检查用户权限</li>
     *   <li>验证新名称唯一性（如果名称发生变化）</li>
     *   <li>更新告警组基本信息和关联的告警实例</li>
     * </ul>
     *
     * @param loginUser 登录用户，需要有告警组编辑权限
     * @param id 告警组ID，必须存在
     * @param groupName 新的告警组名称，不能为空且不能与其他告警组重复
     * @param desc 新的告警组描述
     * @param alertInstanceIds 新的告警实例ID列表，逗号分隔
     * @return 更新后的告警组对象
     * @throws ServiceException 当告警组不存在、权限不足或名称冲突时抛出
     */
    AlertGroup updateAlertGroupById(User loginUser, int id, String groupName, String desc, String alertInstanceIds);

    /**
     * 根据ID删除告警组
     *
     * <p>业务逻辑：</p>
     * <ul>
     *   <li>验证告警组是否存在</li>
     *   <li>检查是否被工作流定义引用</li>
     *   <li>检查用户删除权限</li>
     *   <li>执行物理删除</li>
     * </ul>
     *
     * <p>级联影响：删除前会检查是否有工作流定义在使用此告警组</p>
     *
     * @param loginUser 登录用户，需要有告警组删除权限
     * @param id 告警组ID，必须存在且未被使用
     * @throws ServiceException 当告警组不存在、被引用或权限不足时抛出
     */
    void deleteAlertGroupById(User loginUser, int id);

    /**
     * 验证告警组名称是否已存在
     * 用于创建和更新时的名称唯一性校验
     *
     * @param groupName 待验证的告警组名称，不能为空
     * @return true表示名称已存在，false表示名称可用
     */
    boolean existGroupName(String groupName);
}
