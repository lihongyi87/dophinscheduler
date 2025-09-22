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

import org.apache.dolphinscheduler.api.dto.ClusterDto;
import org.apache.dolphinscheduler.api.enums.Status;
import org.apache.dolphinscheduler.api.exceptions.ServiceException;
import org.apache.dolphinscheduler.api.k8s.K8sManager;
import org.apache.dolphinscheduler.api.service.ClusterService;
import org.apache.dolphinscheduler.api.utils.PageInfo;
import org.apache.dolphinscheduler.common.constants.Constants;
import org.apache.dolphinscheduler.common.utils.CodeGenerateUtils;
import org.apache.dolphinscheduler.common.utils.DateUtils;
import org.apache.dolphinscheduler.dao.entity.Cluster;
import org.apache.dolphinscheduler.dao.entity.K8sNamespace;
import org.apache.dolphinscheduler.dao.entity.User;
import org.apache.dolphinscheduler.dao.mapper.ClusterMapper;
import org.apache.dolphinscheduler.dao.mapper.K8sNamespaceMapper;
import org.apache.dolphinscheduler.service.utils.ClusterConfUtils;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;

/**
 * 集群服务实现类
 *
 * <p>该类实现了Kubernetes等集群的完整管理功能。支持多种集群类型的
 * 配置管理，用于任务的分布式执行和资源调度。</p>
 *
 * <p>主要功能：</p>
 * <ul>
 *   <li>创建K8s集群 - 配置kubeconfig和集群参数</li>
 *   <li>更新集群配置 - 修改集群连接信息和参数</li>
 *   <li>删除集群 - 安全删除集群及其关联的命名空间</li>
 *   <li>查询集群 - 按名称、编码查询和分页查询</li>
 *   <li>验证集群 - 测试集群连接的可用性</li>
 *   <li>命名空间管理 - 维护K8s命名空间关联</li>
 * </ul>
 *
 * <p>该类通过K8sManager与底层Kubernetes API交互，实现集群资源的统一管理。</p>
 */
@Service
@Slf4j
public class ClusterServiceImpl extends BaseServiceImpl implements ClusterService {

    @Autowired
    private ClusterMapper clusterMapper;

    @Autowired
    private K8sManager k8sManager;

    @Autowired
    private K8sNamespaceMapper k8sNamespaceMapper;

    /**
     * create cluster
     *
     * @param loginUser login user
     * @param name      cluster name
     * @param config    cluster config
     * @param desc      cluster desc
     */
    @Transactional
    @Override
    public Long createCluster(User loginUser, String name, String config, String desc) {
        // 权限验证：检查当前用户是否为管理员，只有管理员可以创建集群
        if (isNotAdmin(loginUser)) {
            // 非管理员用户，抛出无操作权限异常
            throw new ServiceException(Status.USER_NO_OPERATION_PERM);
        }

        // 参数校验：验证集群名称和配置参数的合法性
        checkParams(name, config);

        // 名称唯一性检查：查询数据库中是否已存在同名集群
        Cluster clusterExistByName = clusterMapper.queryByClusterName(name);
        if (clusterExistByName != null) {
            // 集群名称已存在，抛出名称冲突异常
            throw new ServiceException(Status.CLUSTER_NAME_EXISTS, name);
        }

        // 构建集群实体对象
        Cluster cluster = new Cluster();
        // 设置集群基本信息
        cluster.setName(name);                                  // 设置集群名称
        cluster.setConfig(config);                             // 设置集群配置信息（如kubeconfig等）
        cluster.setDescription(desc);                          // 设置集群描述
        cluster.setOperator(loginUser.getId());               // 设置操作人ID
        cluster.setCreateTime(new Date());                    // 设置创建时间
        cluster.setUpdateTime(new Date());                    // 设置更新时间
        cluster.setCode(CodeGenerateUtils.genCode());         // 生成唯一的集群编码

        // 执行数据库插入操作，使用事务保证数据一致性
        if (clusterMapper.insert(cluster) > 0) {
            // 插入成功，返回生成的集群编码
            return cluster.getCode();
        }
        // 插入失败，抛出创建集群错误异常
        throw new ServiceException(Status.CREATE_CLUSTER_ERROR);
    }

    /**
     * query cluster paging
     *
     * @param pageNo    page number
     * @param searchVal search value
     * @param pageSize  page size
     * @return cluster list page
     */
    @Override
    public PageInfo<ClusterDto> queryClusterListPaging(Integer pageNo, Integer pageSize, String searchVal) {

        // 构建MyBatis-Plus分页对象，设置页码和每页大小
        Page<Cluster> page = new Page<>(pageNo, pageSize);

        // 执行分页查询：调用Mapper进行数据库分页查询，支持按名称搜索
        IPage<Cluster> clusterIPage = clusterMapper.queryClusterListPaging(page, searchVal);

        // 创建返回的分页信息对象
        PageInfo<ClusterDto> pageInfo = new PageInfo<>(pageNo, pageSize);
        // 设置查询到的总记录数
        pageInfo.setTotal((int) clusterIPage.getTotal());

        // 检查查询结果是否为空
        if (CollectionUtils.isEmpty(clusterIPage.getRecords())) {
            // 无数据时直接返回空的分页对象
            return pageInfo;
        }
        // 数据转换：将Cluster实体对象转换为ClusterDto传输对象
        List<ClusterDto> dtoList = clusterIPage.getRecords().stream().map(cluster -> {
            // 创建DTO对象
            ClusterDto dto = new ClusterDto();
            // 使用BeanUtils进行属性复制，减少手动赋值代码
            BeanUtils.copyProperties(cluster, dto);
            return dto;
        }).collect(Collectors.toList());
        // 设置转换后的数据列表
        pageInfo.setTotalList(dtoList);
        // 返回完整的分页结果
        return pageInfo;
    }

    /**
     * query all cluster
     *
     * @return all cluster list
     */
    @Override
    public List<ClusterDto> queryAllClusterList() {
        // 查询所有集群记录：从数据库获取完整的集群列表
        List<Cluster> clusterList = clusterMapper.queryAllClusterList();
        // 检查查询结果是否为空
        if (CollectionUtils.isEmpty(clusterList)) {
            // 无数据时返回空集合，避免返回null引起的空指针异常
            return Collections.emptyList();
        }

        // 批量数据转换：使用流式处理将实体对象转换为DTO对象
        return clusterList.stream()
                .map(cluster -> {
                    // 创建DTO传输对象
                    ClusterDto dto = new ClusterDto();
                    // TODO: 优化建议 - 考虑使用MapStruct等更高效的对象映射工具替代BeanUtils
                    BeanUtils.copyProperties(cluster, dto);
                    return dto;
                }).collect(Collectors.toList());
    }

    /**
     * query cluster
     *
     * @param code cluster code
     */
    @Override
    public ClusterDto queryClusterByCode(Long code) {

        // 根据集群编码查询集群信息：通过唯一编码从数据库获取集群记录
        Cluster cluster = clusterMapper.queryByClusterCode(code);

        // 验证查询结果：检查集群是否存在
        if (cluster == null) {
            // 集群不存在，抛出查询错误异常
            throw new ServiceException(Status.QUERY_CLUSTER_BY_CODE_ERROR, code);
        }
        // 创建DTO传输对象
        ClusterDto dto = new ClusterDto();
        // 执行对象属性复制，将实体属性映射到DTO
        BeanUtils.copyProperties(cluster, dto);
        // 返回查询到的集群信息
        return dto;
    }

    /**
     * query cluster
     *
     * @param name cluster name
     */
    @Override
    public ClusterDto queryClusterByName(String name) {

        // 根据集群名称查询集群信息：通过集群名称从数据库获取集群记录
        Cluster cluster = clusterMapper.queryByClusterName(name);
        // 验证查询结果：检查指定名称的集群是否存在
        if (cluster == null) {
            // 集群不存在，抛出按名称查询错误异常
            throw new ServiceException(Status.QUERY_CLUSTER_BY_NAME_ERROR, name);
        }
        // 创建DTO传输对象用于数据传递
        ClusterDto dto = new ClusterDto();
        // 复制实体属性到DTO对象
        BeanUtils.copyProperties(cluster, dto);
        // 返回查询到的集群信息
        return dto;
    }

    /**
     * delete cluster
     *
     * @param loginUser login user
     * @param code      cluster code
     */
    @Override
    public void deleteClusterByCode(User loginUser, Long code) {
        // 权限验证：检查当前用户是否为管理员
        if (isNotAdmin(loginUser)) {
            // 非管理员用户，拒绝删除操作
            throw new ServiceException(Status.USER_NO_OPERATION_PERM);
        }

        // 关联关系检查：统计该集群下是否还有关联的K8s命名空间
        Long relatedNamespaceNumber = k8sNamespaceMapper
                .selectCount(new QueryWrapper<K8sNamespace>().lambda().eq(K8sNamespace::getClusterCode, code));

        // 数据一致性保护：如果存在关联的命名空间，禁止删除集群
        if (relatedNamespaceNumber > 0) {
            // 存在关联命名空间，抛出删除失败异常
            throw new ServiceException(Status.DELETE_CLUSTER_RELATED_NAMESPACE_EXISTS);
        }

        // 执行删除操作：物理删除集群记录
        int delete = clusterMapper.deleteByCode(code);
        // 检查删除结果
        if (delete > 0) {
            // 删除成功，正常返回
            return;
        }
        // 删除失败（可能集群不存在），抛出删除错误异常
        throw new ServiceException(Status.DELETE_CLUSTER_ERROR);
    }

    /**
     * update cluster
     *
     * @param loginUser login user
     * @param code      cluster code
     * @param name      cluster name
     * @param config    cluster config
     * @param desc      cluster desc
     */
    @Override
    public Cluster updateClusterByCode(User loginUser,
                                       Long code,
                                       String name,
                                       String config,
                                       String desc) {
        // 权限验证：只有管理员可以更新集群配置
        if (isNotAdmin(loginUser)) {
            // 非管理员用户，抛出权限不足异常
            throw new ServiceException(Status.USER_NO_OPERATION_PERM);
        }

        // 参数长度校验：检查描述信息是否超过系统限制
        if (checkDescriptionLength(desc)) {
            // 描述过长，抛出参数错误异常
            throw new ServiceException(Status.DESCRIPTION_TOO_LONG_ERROR);
        }

        // 基础参数校验：验证集群名称和配置的合法性
        checkParams(name, config);

        // 名称唯一性检查：确保新名称不与其他集群冲突
        Cluster clusterExistByName = clusterMapper.queryByClusterName(name);
        if (clusterExistByName != null && !clusterExistByName.getCode().equals(code)) {
            // 名称已被其他集群使用，抛出名称冲突异常
            throw new ServiceException(Status.CLUSTER_NAME_EXISTS, name);
        }

        // 集群存在性验证：检查要更新的集群是否存在
        Cluster clusterExist = clusterMapper.queryByClusterCode(code);
        if (clusterExist == null) {
            // 集群不存在，抛出集群不存在异常
            throw new ServiceException(Status.CLUSTER_NOT_EXISTS, name);
        }

        // K8s客户端更新：如果配置发生变化且非本地测试集群，需要更新K8s客户端连接
        if (!Constants.K8S_LOCAL_TEST_CLUSTER_CODE.equals(clusterExist.getCode())
                && !config.equals(ClusterConfUtils.getK8sConfig(clusterExist.getConfig()))) {
            try {
                // 尝试更新K8s客户端连接，验证新配置的有效性
                k8sManager.getAndUpdateK8sClient(code, true);
            } catch (Exception e) {
                // K8s客户端操作失败，抛出K8s操作错误异常
                throw new ServiceException(Status.K8S_CLIENT_OPS_ERROR, name);
            }
        }

        // 更新集群实体对象属性
        // 注意：这里不需要更新关联关系，只更新集群本身的属性
        clusterExist.setConfig(config);                        // 更新集群配置
        clusterExist.setName(name);                           // 更新集群名称
        clusterExist.setDescription(desc);                    // 更新集群描述
        clusterExist.setUpdateTime(DateUtils.getCurrentDate()); // 更新修改时间
        // 执行数据库更新操作
        clusterMapper.updateById(clusterExist);
        // 返回更新后的集群对象
        return clusterExist;
    }

    /**
     * verify cluster name
     *
     * @param clusterName cluster name
     * @return true if the cluster name not exists, otherwise return false
     */
    @Override
    public void verifyCluster(String clusterName) {

        // 参数空值检查：验证集群名称是否为空
        if (StringUtils.isEmpty(clusterName)) {
            // 集群名称为空，抛出参数为空异常
            throw new ServiceException(Status.CLUSTER_NAME_IS_NULL);
        }

        // 名称唯一性验证：检查集群名称是否已被使用
        Cluster cluster = clusterMapper.queryByClusterName(clusterName);
        if (cluster != null) {
            // 集群名称已存在，抛出名称冲突异常
            throw new ServiceException(Status.CLUSTER_NAME_EXISTS);
        }
    }

    protected void checkParams(String name, String config) {
        // 集群名称参数校验：检查名称是否为空或空字符串
        if (StringUtils.isEmpty(name)) {
            // 集群名称为空，抛出参数错误异常
            throw new ServiceException(Status.CLUSTER_NAME_IS_NULL);
        }
        // 集群配置参数校验：检查配置信息是否为空
        if (StringUtils.isEmpty(config)) {
            // 集群配置为空，抛出配置为空异常
            throw new ServiceException(Status.CLUSTER_CONFIG_IS_NULL);
        }
    }

}
