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

import static org.apache.dolphinscheduler.api.constants.ApiFuncIdentificationConstant.ENVIRONMENT_CREATE;
import static org.apache.dolphinscheduler.api.constants.ApiFuncIdentificationConstant.ENVIRONMENT_DELETE;
import static org.apache.dolphinscheduler.api.constants.ApiFuncIdentificationConstant.ENVIRONMENT_UPDATE;

import org.apache.dolphinscheduler.api.dto.EnvironmentDto;
import org.apache.dolphinscheduler.api.enums.Status;
import org.apache.dolphinscheduler.api.exceptions.ServiceException;
import org.apache.dolphinscheduler.api.service.EnvironmentService;
import org.apache.dolphinscheduler.api.utils.PageInfo;
import org.apache.dolphinscheduler.api.utils.Result;
import org.apache.dolphinscheduler.common.constants.Constants;
import org.apache.dolphinscheduler.common.enums.AuthorizationType;
import org.apache.dolphinscheduler.common.enums.UserType;
import org.apache.dolphinscheduler.common.utils.CodeGenerateUtils;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.dao.entity.Environment;
import org.apache.dolphinscheduler.dao.entity.EnvironmentWorkerGroupRelation;
import org.apache.dolphinscheduler.dao.entity.TaskDefinition;
import org.apache.dolphinscheduler.dao.entity.User;
import org.apache.dolphinscheduler.dao.mapper.EnvironmentMapper;
import org.apache.dolphinscheduler.dao.mapper.EnvironmentWorkerGroupRelationMapper;
import org.apache.dolphinscheduler.dao.mapper.TaskDefinitionMapper;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.SetUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.fasterxml.jackson.core.type.TypeReference;

/**
 * 环境服务实现类
 *
 * <p>该类实现了任务执行环境的完整管理功能。环境用于定义任务运行时的
 * 配置信息，包括环境变量、Worker组等。不同的任务可以选择在不同的
 * 环境中执行，实现环境隔离和配置管理。</p>
 *
 * <p>主要功能：</p>
 * <ul>
 *   <li>创建环境 - 设置环境名称、配置和关联Worker组</li>
 *   <li>更新环境 - 修改环境配置和Worker组关联</li>
 *   <li>删除环境 - 安全删除环境及其关联关系</li>
 *   <li>查询环境 - 按名称、编码查询和分页查询</li>
 *   <li>权限控制 - 基于用户角色进行访问控制</li>
 *   <li>Worker组管理 - 维护环境与Worker组的关联关系</li>
 * </ul>
 */
@Service
@Slf4j
public class EnvironmentServiceImpl extends BaseServiceImpl implements EnvironmentService {

    @Autowired
    private EnvironmentMapper environmentMapper;

    @Autowired
    private EnvironmentWorkerGroupRelationMapper relationMapper;

    @Autowired
    private TaskDefinitionMapper taskDefinitionMapper;

    /**
     * 创建环境
     *
     * @param loginUser    登录用户
     * @param name         环境名称
     * @param config       环境配置
     * @param desc         环境描述
     * @param workerGroups Worker组列表
     * @return 环境编码
     */
    @Override
    @Transactional
    public Long createEnvironment(User loginUser,
                                  String name,
                                  String config,
                                  String desc,
                                  String workerGroups) {
        // 权限验证：检查用户是否有创建环境的权限
        if (!canOperatorPermissions(loginUser, null, AuthorizationType.ENVIRONMENT, ENVIRONMENT_CREATE)) {
            // 权限不足，抛出无操作权限异常
            throw new ServiceException(Status.USER_NO_OPERATION_PERM);
        }

        // 参数长度校验：检查描述信息是否超过系统限制
        if (checkDescriptionLength(desc)) {
            // 描述过长，抛出参数错误异常
            throw new ServiceException(Status.DESCRIPTION_TOO_LONG_ERROR);
        }

        // 基础参数校验：验证环境名称、配置和Worker组参数的合法性
        checkParams(name, config, workerGroups);

        // 名称唯一性检查：确保环境名称不与现有环境冲突
        Environment environment = environmentMapper.queryByEnvironmentName(name);
        if (environment != null) {
            // 环境名称已存在，抛出名称冲突异常
            throw new ServiceException(Status.ENVIRONMENT_NAME_EXISTS, name);
        }

        // 构建环境实体对象
        Environment env = new Environment();
        // 设置环境基本属性
        env.setName(name);                                  // 设置环境名称
        env.setConfig(config);                             // 设置环境配置信息（如环境变量等）
        env.setDescription(desc);                          // 设置环境描述
        env.setOperator(loginUser.getId());               // 设置操作人ID
        env.setCreateTime(new Date());                    // 设置创建时间
        env.setUpdateTime(new Date());                    // 设置更新时间
        env.setCode(CodeGenerateUtils.genCode());         // 生成唯一的环境编码

        // 数据库事务操作：插入环境主记录
        if (environmentMapper.insert(env) > 0) {
            // 关联关系处理：处理环境与Worker组的关联关系
            if (!StringUtils.isEmpty(workerGroups)) {
                // JSON解析：将Worker组字符串解析为列表对象
                List<String> workerGroupList = JSONUtils.parseObject(workerGroups, new TypeReference<List<String>>() {
                });

                // 检查Worker组列表是否非空
                if (CollectionUtils.isNotEmpty(workerGroupList)) {
                    // 批量创建关联关系：遍历每个Worker组，创建与环境的关联关系
                    workerGroupList.stream().forEach(workerGroup -> {
                        // 过滤空值Worker组
                        if (!StringUtils.isEmpty(workerGroup)) {
                            // 构建环境-Worker组关联实体对象
                            EnvironmentWorkerGroupRelation relation = new EnvironmentWorkerGroupRelation();
                            relation.setEnvironmentCode(env.getCode());     // 设置环境编码
                            relation.setWorkerGroup(workerGroup);           // 设置Worker组名称
                            relation.setOperator(loginUser.getId());       // 设置操作人ID
                            relation.setCreateTime(new Date());            // 设置创建时间
                            relation.setUpdateTime(new Date());            // 设置更新时间

                            // 数据库插入：保存关联关系记录
                            relationMapper.insert(relation);
                            // 记录操作日志，便于运维监控和问题追踪
                            log.info(
                                    "环境-Worker组关联创建完成，环境名: {}, Worker组: {}",
                                    env.getName(), relation.getWorkerGroup());
                        }
                    });
                }
            }
            // 返回生成的环境编码
            return env.getCode();
        }

        // 插入失败处理：抛出创建环境错误异常
        throw new ServiceException(Status.CREATE_ENVIRONMENT_ERROR);
    }

    /**
     * 分页查询环境列表
     *
     * @param loginUser 登录用户
     * @param pageNo    页码
     * @param pageSize  每页大小
     * @param searchVal 搜索关键词
     * @return 环境分页结果
     */
    @Override
    public Result queryEnvironmentListPaging(User loginUser, Integer pageNo, Integer pageSize, String searchVal) {
        // 初始化返回结果对象
        Result<Object> result = new Result();

        // 构建MyBatis-Plus分页对象，设置页码和每页大小
        Page<Environment> page = new Page<>(pageNo, pageSize);
        // 初始化分页信息对象
        PageInfo<EnvironmentDto> pageInfo = new PageInfo<>(pageNo, pageSize);
        // 声明分页查询结果变量
        IPage<Environment> environmentIPage;

        // 根据用户类型分别处理查询逻辑
        if (loginUser.getUserType().equals(UserType.ADMIN_USER)) {
            // 管理员权限：可以查询所有环境的分页数据
            environmentIPage = environmentMapper.queryEnvironmentListPaging(page, searchVal);
        } else {
            // 普通用户权限控制：获取用户有权限的环境ID集合
            Set<Integer> ids = resourcePermissionCheckService
                    .userOwnedResourceIdsAcquisition(AuthorizationType.ENVIRONMENT, loginUser.getId(), log);
            // 检查用户是否有任何环境权限
            if (ids.isEmpty()) {
                // 无权限时返回空的分页结果
                result.setData(pageInfo);
                putMsg(result, Status.SUCCESS);
                return result;
            }
            // 根据权限ID列表进行受限分页查询
            environmentIPage = environmentMapper.queryEnvironmentListPagingByIds(page, new ArrayList<>(ids), searchVal);
        }

        // 设置分页总记录数
        pageInfo.setTotal((int) environmentIPage.getTotal());

        // 处理查询结果数据
        if (CollectionUtils.isNotEmpty(environmentIPage.getRecords())) {
            // 构建环境与Worker组的映射关系：查询所有关联关系并按环境编码分组
            Map<Long, List<String>> relationMap = relationMapper.selectList(null).stream()
                    .collect(Collectors.groupingBy(EnvironmentWorkerGroupRelation::getEnvironmentCode,
                            Collectors.mapping(EnvironmentWorkerGroupRelation::getWorkerGroup, Collectors.toList())));

            // 数据转换：将Environment实体转换为EnvironmentDto，并添加Worker组信息
            List<EnvironmentDto> dtoList = environmentIPage.getRecords().stream().map(environment -> {
                // 创建DTO对象
                EnvironmentDto dto = new EnvironmentDto();
                // 复制基础属性
                BeanUtils.copyProperties(environment, dto);
                // 获取对应的Worker组列表，如果没有关联则使用空列表
                List<String> workerGroups = relationMap.getOrDefault(environment.getCode(), new ArrayList<String>());
                dto.setWorkerGroups(workerGroups);
                return dto;
            }).collect(Collectors.toList());

            // 设置转换后的数据列表
            pageInfo.setTotalList(dtoList);
        } else {
            // 无数据时设置空列表
            pageInfo.setTotalList(new ArrayList<>());
        }

        // 设置返回结果数据和状态
        result.setData(pageInfo);
        putMsg(result, Status.SUCCESS);
        return result;
    }

    /**
     * query all environment
     *
     * @param loginUser
     * @return all environment list
     */
    @Override
    public Map<String, Object> queryAllEnvironmentList(User loginUser) {
        // 初始化返回结果集合
        Map<String, Object> result = new HashMap<>();
        // 获取用户有权限的环境ID集合
        Set<Integer> ids = resourcePermissionCheckService.userOwnedResourceIdsAcquisition(AuthorizationType.ENVIRONMENT,
                loginUser.getId(), log);
        // 检查用户是否有任何环境权限
        if (ids.isEmpty()) {
            // 无权限时返回空列表
            result.put(Constants.DATA_LIST, Collections.emptyList());
            putMsg(result, Status.SUCCESS);
            return result;
        }
        // 根据权限ID列表批量查询环境信息
        List<Environment> environmentList = environmentMapper.selectBatchIds(ids);
        // 处理查询结果
        if (CollectionUtils.isNotEmpty(environmentList)) {
            // 构建环境与Worker组的映射关系：查询所有关联关系并按环境编码分组
            Map<Long, List<String>> relationMap = relationMapper.selectList(null).stream()
                    .collect(Collectors.groupingBy(EnvironmentWorkerGroupRelation::getEnvironmentCode,
                            Collectors.mapping(EnvironmentWorkerGroupRelation::getWorkerGroup, Collectors.toList())));

            // 数据转换：将Environment实体转换为EnvironmentDto，并添加Worker组信息
            List<EnvironmentDto> dtoList = environmentList.stream().map(environment -> {
                // 创建DTO对象
                EnvironmentDto dto = new EnvironmentDto();
                // 复制基础属性
                BeanUtils.copyProperties(environment, dto);
                // 获取对应的Worker组列表
                List<String> workerGroups = relationMap.getOrDefault(environment.getCode(), new ArrayList<String>());
                dto.setWorkerGroups(workerGroups);
                return dto;
            }).collect(Collectors.toList());
            // 设置返回数据列表
            result.put(Constants.DATA_LIST, dtoList);
        } else {
            // 无数据时设置空列表
            result.put(Constants.DATA_LIST, new ArrayList<>());
        }

        // 设置返回状态为成功
        putMsg(result, Status.SUCCESS);
        return result;
    }

    /**
     * query environment
     *
     * @param code environment code
     */
    @Override
    public Map<String, Object> queryEnvironmentByCode(Long code) {
        Map<String, Object> result = new HashMap<>();

        Environment env = environmentMapper.queryByEnvironmentCode(code);

        if (env == null) {
            putMsg(result, Status.QUERY_ENVIRONMENT_BY_CODE_ERROR, code);
        } else {
            List<String> workerGroups = relationMapper.queryByEnvironmentCode(env.getCode()).stream()
                    .map(item -> item.getWorkerGroup())
                    .collect(Collectors.toList());

            EnvironmentDto dto = new EnvironmentDto();
            BeanUtils.copyProperties(env, dto);
            dto.setWorkerGroups(workerGroups);
            result.put(Constants.DATA_LIST, dto);
            putMsg(result, Status.SUCCESS);
        }
        return result;
    }

    /**
     * query environment
     *
     * @param name environment name
     */
    @Override
    public Map<String, Object> queryEnvironmentByName(String name) {
        Map<String, Object> result = new HashMap<>();

        Environment env = environmentMapper.queryByEnvironmentName(name);
        if (env == null) {
            putMsg(result, Status.QUERY_ENVIRONMENT_BY_NAME_ERROR, name);
        } else {
            List<String> workerGroups = relationMapper.queryByEnvironmentCode(env.getCode()).stream()
                    .map(item -> item.getWorkerGroup())
                    .collect(Collectors.toList());

            EnvironmentDto dto = new EnvironmentDto();
            BeanUtils.copyProperties(env, dto);
            dto.setWorkerGroups(workerGroups);
            result.put(Constants.DATA_LIST, dto);
            putMsg(result, Status.SUCCESS);
        }
        return result;
    }

    /**
     * delete environment
     *
     * @param loginUser login user
     * @param code environment code
     */
    @Transactional
    @Override
    public Map<String, Object> deleteEnvironmentByCode(User loginUser, Long code) {
        Map<String, Object> result = new HashMap<>();
        if (!canOperatorPermissions(loginUser, null, AuthorizationType.ENVIRONMENT, ENVIRONMENT_DELETE)) {
            putMsg(result, Status.USER_NO_OPERATION_PERM);
            return result;
        }

        Long relatedTaskNumber = taskDefinitionMapper
                .selectCount(new QueryWrapper<TaskDefinition>().lambda().eq(TaskDefinition::getEnvironmentCode, code));

        if (relatedTaskNumber > 0) {
            log.warn("Delete environment failed because {} tasks is using it, environmentCode:{}.",
                    relatedTaskNumber, code);
            putMsg(result, Status.DELETE_ENVIRONMENT_RELATED_TASK_EXISTS);
            return result;
        }

        int delete = environmentMapper.deleteByCode(code);
        if (delete > 0) {
            relationMapper.delete(new QueryWrapper<EnvironmentWorkerGroupRelation>()
                    .lambda()
                    .eq(EnvironmentWorkerGroupRelation::getEnvironmentCode, code));
            log.info("Environment and relations delete complete, environmentCode:{}.", code);
            putMsg(result, Status.SUCCESS);
        } else {
            log.error("Environment delete error, environmentCode:{}.", code);
            putMsg(result, Status.DELETE_ENVIRONMENT_ERROR);
        }
        return result;
    }

    /**
     * update environment
     *
     * @param loginUser login user
     * @param code environment code
     * @param name environment name
     * @param config environment config
     * @param desc environment desc
     * @param workerGroups worker groups
     */
    @Transactional
    @Override
    public Environment updateEnvironmentByCode(User loginUser, Long code, String name, String config,
                                               String desc, String workerGroups) {
        // 权限验证：检查用户是否有更新环境的权限
        if (!canOperatorPermissions(loginUser, null, AuthorizationType.ENVIRONMENT, ENVIRONMENT_UPDATE)) {
            // 权限不足，抛出无操作权限异常
            throw new ServiceException(Status.USER_NO_OPERATION_PERM);
        }

        // 基础参数校验：验证名称、配置和Worker组参数的合法性
        checkParams(name, config, workerGroups);
        // 描述长度校验：检查描述信息是否超过系统限制
        if (checkDescriptionLength(desc)) {
            // 描述过长，抛出参数错误异常
            throw new ServiceException(Status.DESCRIPTION_TOO_LONG_ERROR);
        }

        // 名称唯一性检查：检查新名称是否与其他环境冲突（排除当前环境）
        Environment environment = environmentMapper.queryByEnvironmentName(name);
        if (environment != null && !environment.getCode().equals(code)) {
            // 名称已被其他环境使用，抛出名称冲突异常
            throw new ServiceException(Status.ENVIRONMENT_NAME_EXISTS, name);
        }

        // Worker组集合处理：解析新的Worker组列表
        Set<String> workerGroupSet;
        if (!StringUtils.isEmpty(workerGroups)) {
            // 解析JSON字符串为Set集合
            workerGroupSet = JSONUtils.parseObject(workerGroups, new TypeReference<Set<String>>() {
            });
        } else {
            // 空参数时创建空集合
            workerGroupSet = new TreeSet<>();
        }

        // 获取现有Worker组集合：查询当前环境已关联的Worker组
        Set<String> existWorkerGroupSet = relationMapper
                .queryByEnvironmentCode(code)
                .stream()
                .map(EnvironmentWorkerGroupRelation::getWorkerGroup)
                .collect(Collectors.toSet());

        // 计算集合差异：找出需要删除和新增的Worker组
        Set<String> deleteWorkerGroupSet = SetUtils.difference(existWorkerGroupSet, workerGroupSet).toSet();
        Set<String> addWorkerGroupSet = SetUtils.difference(workerGroupSet, existWorkerGroupSet).toSet();

        // 业务规则验证：检查要删除的Worker组是否还被任务使用，防止数据不一致
        checkUsedEnvironmentWorkerGroupRelation(deleteWorkerGroupSet, name, code);

        // 构建更新对象
        Environment env = new Environment();
        env.setCode(code);                           // 设置环境编码
        env.setName(name);                          // 设置环境名称
        env.setConfig(config);                      // 设置环境配置
        env.setDescription(desc);                   // 设置环境描述
        env.setOperator(loginUser.getId());        // 设置操作人ID
        env.setUpdateTime(new Date());             // 设置更新时间

        // 执行环境主记录更新
        int update =
                environmentMapper.update(env, new UpdateWrapper<Environment>().lambda().eq(Environment::getCode, code));
        if (update <= 0) {
            // 更新失败，抛出更新错误异帰
            throw new ServiceException(Status.UPDATE_ENVIRONMENT_ERROR, name);
        }

        // 删除操作：移除不再需要的Worker组关联关系
        deleteWorkerGroupSet.forEach(key -> {
            if (StringUtils.isNotEmpty(key)) {
                // 根据环境编码和Worker组名称删除关联关系
                relationMapper.delete(new QueryWrapper<EnvironmentWorkerGroupRelation>()
                        .lambda()
                        .eq(EnvironmentWorkerGroupRelation::getEnvironmentCode, code)
                        .eq(EnvironmentWorkerGroupRelation::getWorkerGroup, key));
            }
        });

        // 新增操作：添加新的Worker组关联关系
        addWorkerGroupSet.forEach(key -> {
            if (StringUtils.isNotEmpty(key)) {
                // 创建新的关联关系对象
                EnvironmentWorkerGroupRelation relation = new EnvironmentWorkerGroupRelation();
                relation.setEnvironmentCode(code);       // 设置环境编码
                relation.setWorkerGroup(key);            // 设置Worker组名称
                relation.setUpdateTime(new Date());      // 设置更新时间
                relation.setCreateTime(new Date());      // 设置创建时间
                relation.setOperator(loginUser.getId()); // 设置操作人ID
                // 插入新的关联关系记录
                relationMapper.insert(relation);
            }
        });
        // 返回更新后的环境对象
        return env;
    }

    /**
     * verify environment name
     *
     * @param environmentName environment name
     * @return true if the environment name not exists, otherwise return false
     */
    @Override
    public Map<String, Object> verifyEnvironment(String environmentName) {
        Map<String, Object> result = new HashMap<>();

        if (StringUtils.isEmpty(environmentName)) {
            log.warn("parameter environment name is empty.");
            putMsg(result, Status.ENVIRONMENT_NAME_IS_NULL);
            return result;
        }

        Environment environment = environmentMapper.queryByEnvironmentName(environmentName);
        if (environment != null) {
            log.warn("Environment with the same name already exist, name:{}.", environment.getName());
            putMsg(result, Status.ENVIRONMENT_NAME_EXISTS, environmentName);
            return result;
        }

        result.put(Constants.STATUS, Status.SUCCESS);
        return result;
    }

    private void checkUsedEnvironmentWorkerGroupRelation(Set<String> deleteKeySet,
                                                         String environmentName, Long environmentCode) {
        // 遍历所有要删除的Worker组，检查是否还被任务使用
        for (String workerGroup : deleteKeySet) {
            // 查询使用指定环境和Worker组的任务定义列表
            List<TaskDefinition> taskDefinitionList = taskDefinitionMapper
                    .selectList(new QueryWrapper<TaskDefinition>().lambda()
                            .eq(TaskDefinition::getEnvironmentCode, environmentCode)
                            .eq(TaskDefinition::getWorkerGroup, workerGroup));

            // 检查是否存在使用该Worker组的任务
            if (Objects.nonNull(taskDefinitionList) && taskDefinitionList.size() != 0) {
                // 收集使用该Worker组的任务名称集合，用于错误信息显示
                Set<String> collect =
                        taskDefinitionList.stream().map(TaskDefinition::getName).collect(Collectors.toSet());
                // 抛出业务规则异常：不能删除还被任务使用的Worker组关联
                throw new ServiceException(Status.UPDATE_ENVIRONMENT_WORKER_GROUP_RELATION_ERROR, workerGroup,
                        environmentName, collect);
            }
        }
    }

    protected void checkParams(String name, String config, String workerGroups) {
        // 环境名称参数校验：检查名称是否为空或空字符串
        if (StringUtils.isEmpty(name)) {
            // 环境名称为空，抛出参数错误异常
            throw new ServiceException(Status.ENVIRONMENT_NAME_IS_NULL);
        }
        // 环境配置参数校验：检查配置信息是否为空
        if (StringUtils.isEmpty(config)) {
            // 环境配置为空，抛出配置为空异常
            throw new ServiceException(Status.ENVIRONMENT_CONFIG_IS_NULL);
        }
        // Worker组参数校验：如果提供了Worker组参数，验证JSON格式是否正确
        if (StringUtils.isNotEmpty(workerGroups)) {
            try {
                // 尝试解析Worker组JSON字符串，验证格式正确性
                JSONUtils.parseObject(workerGroups, new TypeReference<List<String>>() {
                });
            } catch (IllegalArgumentException e) {
                // JSON解析失败，抛出Worker组参数无效异常
                throw new ServiceException(Status.ENVIRONMENT_WORKER_GROUPS_IS_INVALID);
            }
        }
    }

}
