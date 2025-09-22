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

import org.apache.dolphinscheduler.api.utils.Result;
import org.apache.dolphinscheduler.dao.entity.Environment;
import org.apache.dolphinscheduler.dao.entity.User;

import java.util.Map;

/**
 * 环境服务接口
 * 管理任务执行环境，包括环境变量、Worker组等配置
 */
public interface EnvironmentService {

    /**
     * 创建任务执行环境
     *
     * <p>业务逻辑：</p>
     * <ul>
     *   <li>验证环境名称唯一性</li>
     *   <li>验证Worker组配置有效性</li>
     *   <li>解析环境变量配置</li>
     *   <li>保存环境信息</li>
     * </ul>
     *
     * @param loginUser 登录用户，需要有环境管理权限
     * @param name 环境名称，不能为空且全局唯一
     * @param config 环境变量配置，包含各种环境变量和参数
     * @param desc 环境描述，可选
     * @param workerGroups 可用的Worker组列表，逗号分隔，指定该环境可以在哪些Worker组上执行
     * @return 新建环境的唯一编码
     * @throws ServiceException 当环境名称重复或Worker组不存在时抛出
     */
    Long createEnvironment(User loginUser, String name, String config, String desc, String workerGroups);

    /**
     * 根据名称查询环境详情
     * 返回环境的配置信息和Worker组关联
     *
     * @param name 环境名称，精确匹配
     * @return 环境详情信息，包含配置和Worker组，如果不存在返回空结果
     */
    Map<String, Object> queryEnvironmentByName(String name);

    /**
     * 根据编码查询环境详情
     * 通过环境的唯一编码获取环境信息
     *
     * @param code 环境编码，系统生成的唯一标识
     * @return 环境详情信息，如果不存在返回空结果
     */
    Map<String, Object> queryEnvironmentByCode(Long code);

    /**
     * 根据编码删除环境
     *
     * <p>业务逻辑：</p>
     * <ul>
     *   <li>验证环境是否存在</li>
     *   <li>检查环境是否被工作流定义使用</li>
     *   <li>检查用户删除权限</li>
     *   <li>执行物理删除</li>
     * </ul>
     *
     * <p>级联影响：删除前会检查是否有工作流定义在使用此环境</p>
     *
     * @param loginUser 登录用户，需要有环境删除权限
     * @param code 环境编码，必须存在且未被使用
     * @return 删除结果信息
     * @throws ServiceException 当环境不存在、被引用或权限不足时抛出
     */
    Map<String, Object> deleteEnvironmentByCode(User loginUser, Long code);

    /**
     * 根据编码更新环境信息
     *
     * <p>业务逻辑：</p>
     * <ul>
     *   <li>验证环境是否存在</li>
     *   <li>检查用户权限</li>
     *   <li>验证新名称唯一性（如果名称变化）</li>
     *   <li>验证Worker组配置有效性</li>
     *   <li>更新环境信息</li>
     * </ul>
     *
     * @param loginUser 登录用户，需要有环境编辑权限
     * @param code 环境编码，必须存在
     * @param name 新的环境名称，不能为空且不能与其他环境重复
     * @param config 新的环境变量配置
     * @param desc 新的环境描述
     * @param workerGroups 新的Worker组列表，逗号分隔
     * @return 更新后的环境对象
     * @throws ServiceException 当环境不存在、权限不足或名称冲突时抛出
     */
    Environment updateEnvironmentByCode(User loginUser, Long code, String name, String config, String desc,
                                        String workerGroups);

    /**
     * 分页查询环境列表
     * 支持按环境名称模糊搜索，只返回用户有权限的环境
     *
     * @param loginUser 登录用户，用于权限过滤
     * @param pageNo 页码，从1开始
     * @param pageSize 每页大小，建议10-100
     * @param searchVal 搜索关键词，可为空，支持环境名称模糊匹配
     * @return 环境分页数据，包含总数和当前页数据
     */
    Result queryEnvironmentListPaging(User loginUser, Integer pageNo, Integer pageSize, String searchVal);

    /**
     * 查询所有环境列表
     * 返回用户有权限的所有环境，用于下拉选择等场景
     *
     * @param loginUser 登录用户，用于权限过滤
     * @return 所有可用环境的基本信息列表，不分页
     */
    Map<String, Object> queryAllEnvironmentList(User loginUser);

    /**
     * 验证环境名称是否可用
     * 用于创建和更新时的名称唯一性校验
     *
     * @param environmentName 待验证的环境名称，不能为空
     * @return 验证结果信息，如果名称可用返回成功结果
     * @throws ServiceException 当环境名称已存在时抛出异常
     */
    Map<String, Object> verifyEnvironment(String environmentName);

}
