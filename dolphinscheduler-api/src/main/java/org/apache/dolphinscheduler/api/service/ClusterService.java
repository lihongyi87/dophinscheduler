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

import org.apache.dolphinscheduler.api.dto.ClusterDto;
import org.apache.dolphinscheduler.api.utils.PageInfo;
import org.apache.dolphinscheduler.dao.entity.Cluster;
import org.apache.dolphinscheduler.dao.entity.User;

import java.util.List;

/**
 * 集群服务接口
 * 管理Kubernetes等集群配置和操作
 */
public interface ClusterService {

    /**
     * 创建集群配置
     *
     * <p>业务逻辑：</p>
     * <ul>
     *   <li>验证集群名称唯一性</li>
     *   <li>解析和验证K8s集群配置</li>
     *   <li>测试集群连接可用性</li>
     *   <li>保存集群配置信息</li>
     * </ul>
     *
     * @param loginUser 登录用户，需要有集群管理权限
     * @param name 集群名称，不能为空且全局唯一
     * @param config K8s集群配置，包含kubeconfig或集群连接信息
     * @param desc 集群描述，可选
     * @return 新建集群的唯一编码
     * @throws ServiceException 当集群名称重复或配置无效时抛出
     */
    Long createCluster(User loginUser, String name, String config, String desc);

    /**
     * 根据名称查询集群详情
     * 返回集群的基本信息和配置详情
     *
     * @param name 集群名称，精确匹配
     * @return 集群传输对象，包含集群的详细配置信息，如果不存在返回null
     */
    ClusterDto queryClusterByName(String name);

    /**
     * 根据编码查询集群详情
     * 通过集群的唯一编码获取集群信息
     *
     * @param code 集群编码，系统生成的唯一标识
     * @return 集群传输对象，包含集群的详细配置信息，如果不存在返回null
     */
    ClusterDto queryClusterByCode(Long code);

    /**
     * 根据编码删除集群
     *
     * <p>业务逻辑：</p>
     * <ul>
     *   <li>验证集群是否存在</li>
     *   <li>检查集群是否被环境或命名空间引用</li>
     *   <li>检查用户删除权限</li>
     *   <li>执行物理删除</li>
     * </ul>
     *
     * <p>级联影响：删除前会检查是否有环境和K8s命名空间在使用此集群</p>
     *
     * @param loginUser 登录用户，需要有集群删除权限
     * @param code 集群编码，必须存在且未被引用
     * @throws ServiceException 当集群不存在、被引用或权限不足时抛出
     */
    void deleteClusterByCode(User loginUser, Long code);

    /**
     * 根据编码更新集群信息
     *
     * <p>业务逻辑：</p>
     * <ul>
     *   <li>验证集群是否存在</li>
     *   <li>检查用户权限</li>
     *   <li>验证新名称唯一性（如果名称发生变化）</li>
     *   <li>验证新配置的有效性</li>
     *   <li>更新集群信息</li>
     * </ul>
     *
     * @param loginUser 登录用户，需要有集群编辑权限
     * @param code 集群编码，必须存在
     * @param name 新的集群名称，不能为空且不能与其他集群重复
     * @param config 新的K8s集群配置
     * @param desc 新的集群描述
     * @return 更新后的集群对象
     * @throws ServiceException 当集群不存在、权限不足或名称冲突时抛出
     */
    Cluster updateClusterByCode(User loginUser, Long code, String name, String config, String desc);

    /**
     * 分页查询集群列表
     * 支持按集群名称模糊搜索
     *
     * @param pageNo 页码，从1开始
     * @param pageSize 每页大小，建议10-100
     * @param searchVal 搜索关键词，可为空，支持集群名称模糊匹配
     * @return 集群分页数据，包含总数和当前页数据
     */
    PageInfo<ClusterDto> queryClusterListPaging(Integer pageNo, Integer pageSize, String searchVal);

    /**
     * 查询所有集群列表
     * 用于下拉选择等场景，不分页返回所有可用集群
     *
     * @return 所有集群的基本信息列表，按创建时间排序
     */
    List<ClusterDto> queryAllClusterList();

    /**
     * 验证集群名称是否可用
     * 用于创建和更新时的名称唯一性校验
     *
     * @param clusterName 待验证的集群名称，不能为空
     * @throws ServiceException 当集群名称已存在时抛出异常
     */
    void verifyCluster(String clusterName);

}
