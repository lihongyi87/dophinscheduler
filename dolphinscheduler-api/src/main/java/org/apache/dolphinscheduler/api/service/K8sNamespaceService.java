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
import org.apache.dolphinscheduler.dao.entity.K8sNamespace;
import org.apache.dolphinscheduler.dao.entity.User;

import java.util.List;
import java.util.Map;

/**
 * Kubernetes命名空间服务接口
 *
 * <p>该接口提供Kubernetes命名空间的管理功能。命名空间是K8s中用于
 * 资源隔离的逻辑单元，不同的任务可以运行在不同的命名空间中。</p>
 *
 * <p>主要功能：</p>
 * <ul>
 *   <li>注册K8s命名空间到系统</li>
 *   <li>查询命名空间列表</li>
 *   <li>验证命名空间可用性</li>
 *   <li>管理命名空间权限</li>
 * </ul>
 */
public interface K8sNamespaceService {

    /**
     * 分页查询K8s命名空间列表
     * 支持按命名空间名称模糊搜索，只返回用户有权限的命名空间
     *
     * @param loginUser 登录用户，用于权限过滤
     * @param searchVal 搜索关键词，可为空，支持命名空间名称模糊匹配
     * @param pageNo 页码，从1开始
     * @param pageSize 每页大小，建议10-100
     * @return K8s命名空间分页数据，包含总数和当前页数据
     */
    Result queryListPaging(User loginUser, String searchVal, Integer pageNo, Integer pageSize);

    /**
     * 将K8s命名空间注册到DolphinScheduler系统
     * 将在K8s中已存在的命名空间注册到系统中，以便任务可以使用
     *
     * <p>业务逻辑：</p>
     * <ul>
     *   <li>验证集群是否存在和可用</li>
     *   <li>验证命名空间在K8s中是否存在</li>
     *   <li>检查命名空间未被重复注册</li>
     *   <li>保存命名空间信息到数据库</li>
     * </ul>
     *
     * <p>注意：必须先在K8s集群中手动创建命名空间</p>
     *
     * @param loginUser 登录用户，需要有K8s管理权限
     * @param namespace 命名空间名称，必须在K8s中已存在
     * @param clusterCode 集群编码，不能为空，指定命名空间所属的K8s集群
     * @return 注册结果信息，包含成功或失败信息
     * @throws ServiceException 当集群不存在或命名空间无效时抛出
     */
    Map<String, Object> registerK8sNamespace(User loginUser, String namespace, Long clusterCode);

    /**
     * 验证K8s命名空间和集群的有效性
     * 检查指定的集群和命名空间是否存在且可用
     *
     * <p>验证项目：</p>
     * <ul>
     *   <li>集群是否存在且可访问</li>
     *   <li>命名空间在K8s中是否存在</li>
     *   <li>集群配置是否正确</li>
     *   <li>是否已被系统注册</li>
     * </ul>
     *
     * @param namespace 命名空间名称，不能为空
     * @param clusterCode 集群编码，不能为空
     * @return 验证结果，包含验证是否成功和相关信息
     */
    Result<Object> verifyNamespaceK8s(String namespace, Long clusterCode);

    /**
     * 根据ID删除K8s命名空间注册信息
     * 从系统中移除命名空间的注册信息，但不会删除K8s中的实际命名空间
     *
     * <p>业务逻辑：</p>
     * <ul>
     *   <li>验证命名空间是否存在</li>
     *   <li>检查命名空间是否被任务定义使用</li>
     *   <li>检查用户删除权限</li>
     *   <li>从数据库中删除注册信息</li>
     * </ul>
     *
     * <p>级联影响：删除前会检查是否有任务定义在使用此命名空间</p>
     *
     * @param loginUser 登录用户，需要有K8s管理权限
     * @param id 命名空间ID，必须存在且未被使用
     * @return 删除结果信息
     * @throws ServiceException 当命名空间不存在、被引用或权限不足时抛出
     */
    Map<String, Object> deleteNamespaceById(User loginUser, int id);

    /**
     * 查询用户未授权的K8s命名空间列表
     * 返回系统中该用户没有访问权限的命名空间，用于授权管理
     *
     * @param loginUser 登录用户，需要有用户管理权限
     * @param userId 目标用户ID
     * @return 该用户未授权的命名空间列表，用于授权选择
     */
    Map<String, Object> queryUnauthorizedNamespace(User loginUser, Integer userId);

    /**
     * 查询用户已授权的K8s命名空间列表
     * 返回该用户具有访问权限的命名空间，不包括用户自己创建的命名空间
     *
     * @param loginUser 登录用户，需要有用户管理权限
     * @param userId 目标用户ID
     * @return 该用户已授权的命名空间列表
     */
    Map<String, Object> queryAuthorizedNamespace(User loginUser, Integer userId);

    /**
     * 查询用户可使用的K8s命名空间列表
     * 返回用户有权限使用的所有命名空间，用于任务定义中的命名空间选择
     *
     * @param loginUser 登录用户，用于权限过滤
     * @return 用户可使用的命名空间列表，包括集群信息
     */
    List<K8sNamespace> queryNamespaceAvailable(User loginUser);
}
