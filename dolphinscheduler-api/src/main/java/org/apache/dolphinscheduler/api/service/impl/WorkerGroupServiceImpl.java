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

import static org.apache.dolphinscheduler.api.constants.ApiFuncIdentificationConstant.WORKER_GROUP_CREATE;
import static org.apache.dolphinscheduler.api.constants.ApiFuncIdentificationConstant.WORKER_GROUP_DELETE;

import org.apache.dolphinscheduler.api.enums.Status;
import org.apache.dolphinscheduler.api.exceptions.ServiceException;
import org.apache.dolphinscheduler.api.service.WorkerGroupService;
import org.apache.dolphinscheduler.api.utils.PageInfo;
import org.apache.dolphinscheduler.api.utils.Result;
import org.apache.dolphinscheduler.common.constants.Constants;
import org.apache.dolphinscheduler.common.enums.AuthorizationType;
import org.apache.dolphinscheduler.common.enums.UserType;
import org.apache.dolphinscheduler.common.enums.WorkerGroupSource;
import org.apache.dolphinscheduler.common.enums.WorkflowExecutionStatus;
import org.apache.dolphinscheduler.common.model.Server;
import org.apache.dolphinscheduler.common.model.WorkerHeartBeat;
import org.apache.dolphinscheduler.common.utils.DateUtils;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.dao.entity.EnvironmentWorkerGroupRelation;
import org.apache.dolphinscheduler.dao.entity.Schedule;
import org.apache.dolphinscheduler.dao.entity.TaskDefinition;
import org.apache.dolphinscheduler.dao.entity.User;
import org.apache.dolphinscheduler.dao.entity.WorkerGroup;
import org.apache.dolphinscheduler.dao.entity.WorkerGroupPageDetail;
import org.apache.dolphinscheduler.dao.entity.WorkflowInstance;
import org.apache.dolphinscheduler.dao.mapper.EnvironmentWorkerGroupRelationMapper;
import org.apache.dolphinscheduler.dao.mapper.ScheduleMapper;
import org.apache.dolphinscheduler.dao.mapper.TaskDefinitionMapper;
import org.apache.dolphinscheduler.dao.mapper.WorkflowDefinitionMapper;
import org.apache.dolphinscheduler.dao.mapper.WorkflowInstanceMapper;
import org.apache.dolphinscheduler.dao.repository.WorkerGroupDao;
import org.apache.dolphinscheduler.extract.base.client.Clients;
import org.apache.dolphinscheduler.extract.master.IMasterContainerService;
import org.apache.dolphinscheduler.registry.api.RegistryClient;
import org.apache.dolphinscheduler.registry.api.enums.RegistryNodeType;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.google.common.base.Strings;

/**
 * Worker组服务实现类
 *
 * <p>该类实现了Worker组的管理功能。Worker组用于将Worker节点进行
 * 逻辑分组，不同的任务可以指定在特定的Worker组上执行。</p>
 *
 * <p>主要功能：</p>
 * <ul>
 *   <li>创建和管理Worker组</li>
 *   <li>查询Worker组状态和节点信息</li>
 *   <li>分配Worker节点到组</li>
 *   <li>监控Worker节点健康状态</li>
 *   <li>管理环境与Worker组关联</li>
 * </ul>
 */
@Service
@Slf4j
public class WorkerGroupServiceImpl extends BaseServiceImpl implements WorkerGroupService {

    @Autowired
    private WorkerGroupDao workerGroupDao;

    @Autowired
    private WorkflowInstanceMapper workflowInstanceMapper;

    @Autowired
    private RegistryClient registryClient;

    @Autowired
    private EnvironmentWorkerGroupRelationMapper environmentWorkerGroupRelationMapper;

    @Autowired
    private ScheduleMapper scheduleMapper;

    @Autowired
    private TaskDefinitionMapper taskDefinitionMapper;

    @Autowired
    private WorkflowDefinitionMapper workflowDefinitionMapper;

    /**
     * 创建或更新Worker组
     *
     * @param loginUser   登录用户
     * @param id          Worker组ID，0表示新建
     * @param name        Worker组名称
     * @param addrList    Worker地址列表
     * @param description 描述
     * @return Worker组对象
     */
    @Override
    public WorkerGroup saveWorkerGroup(User loginUser,
                                       int id,
                                       String name,
                                       String addrList,
                                       String description) {
        Map<String, Object> result = new HashMap<>();

        // 检查用户是否有创建或更新Worker组的权限
        if (!canOperatorPermissions(loginUser, null, AuthorizationType.WORKER_GROUP, WORKER_GROUP_CREATE)) {
            throw new ServiceException(Status.USER_NO_OPERATION_PERM);
        }

        // 验证Worker组名称不为空
        if (StringUtils.isEmpty(name)) {
            throw new ServiceException(Status.NAME_NULL);
        }

        // 检查Worker地址列表的合法性
        checkWorkerGroupAddrList(addrList);

        final Date now = new Date();
        final WorkerGroup workerGroup;

        try {
            if (id == 0) {
                // 创建新的Worker组
                workerGroup = new WorkerGroup();
                workerGroup.setCreateTime(now);
                workerGroup.setName(name);
                workerGroup.setAddrList(addrList);
                workerGroup.setUpdateTime(now);
                workerGroup.setDescription(description);
                // 插入数据库
                workerGroupDao.insert(workerGroup);
            } else {
                // 更新现有Worker组
                workerGroup = workerGroupDao.queryById(id);
                if (workerGroup == null) {
                    throw new ServiceException(Status.WORKER_GROUP_NOT_EXIST, id);
                }

                // 如果名称发生变化，检查是否有依赖
                if (!workerGroup.getName().equals(name)) {
                    checkWorkerGroupDependencies(workerGroup, result);
                }

                // 更新Worker组信息
                workerGroup.setName(name);
                workerGroup.setAddrList(addrList);
                workerGroup.setUpdateTime(now);
                workerGroup.setDescription(description);
                workerGroupDao.updateById(workerGroup);
                log.info("更新Worker组成功: {}", workerGroup);
            }

            // 广播通知Master节点Worker组发生变化
            boardCastToMasterThatWorkerGroupChanged();
            return workerGroup;
        } catch (DuplicateKeyException duplicateKeyException) {
            // 处理Worker组名称重复的情况
            throw new ServiceException(Status.NAME_EXIST, name);
        }
    }

    /**
     * 检查Worker组是否有依赖的任务、调度或环境
     *
     * @param workerGroup Worker组对象
     * @param result      结果集合
     * @return boolean
     */
    private boolean checkWorkerGroupDependencies(WorkerGroup workerGroup, Map<String, Object> result) {
        // check if the worker group has any dependent tasks
        List<TaskDefinition> taskDefinitions = taskDefinitionMapper.selectList(
                new QueryWrapper<TaskDefinition>().lambda().eq(TaskDefinition::getWorkerGroup, workerGroup.getName()));

        if (CollectionUtils.isNotEmpty(taskDefinitions)) {
            List<String> taskNames = taskDefinitions.stream().limit(3).map(taskDefinition -> taskDefinition.getName())
                    .collect(Collectors.toList());

            putMsg(result, Status.WORKER_GROUP_DEPENDENT_TASK_EXISTS, taskDefinitions.size(),
                    JSONUtils.toJsonString(taskNames));
            return true;
        }

        // check if the worker group has any dependent schedulers
        List<Schedule> schedules = scheduleMapper
                .selectList(new QueryWrapper<Schedule>().lambda().eq(Schedule::getWorkerGroup, workerGroup.getName()));

        if (CollectionUtils.isNotEmpty(schedules)) {
            List<String> workflowDefinitionNames = schedules.stream().limit(3)
                    .map(schedule -> workflowDefinitionMapper.queryByCode(schedule.getWorkflowDefinitionCode())
                            .getName())
                    .collect(Collectors.toList());

            putMsg(result, Status.WORKER_GROUP_DEPENDENT_SCHEDULER_EXISTS, schedules.size(),
                    JSONUtils.toJsonString(workflowDefinitionNames));
            return true;
        }

        // check if the worker group has any dependent environments
        List<EnvironmentWorkerGroupRelation> environmentWorkerGroupRelations =
                environmentWorkerGroupRelationMapper.selectList(new QueryWrapper<EnvironmentWorkerGroupRelation>()
                        .lambda().eq(EnvironmentWorkerGroupRelation::getWorkerGroup, workerGroup.getName()));

        if (CollectionUtils.isNotEmpty(environmentWorkerGroupRelations)) {
            putMsg(result, Status.WORKER_GROUP_DEPENDENT_ENVIRONMENT_EXISTS, environmentWorkerGroupRelations.size());
            return true;
        }

        return false;
    }

    private void checkWorkerGroupAddrList(String workerGroupAddress) {
        if (Strings.isNullOrEmpty(workerGroupAddress)) {
            return;
        }
        Map<String, String> serverMaps = registryClient.getServerMaps(RegistryNodeType.WORKER);
        for (String addr : workerGroupAddress.split(Constants.COMMA)) {
            if (!serverMaps.containsKey(addr)) {
                throw new ServiceException(Status.WORKER_ADDRESS_INVALID);
            }
        }
    }

    /**
     * query worker group paging
     *
     * @param loginUser login user
     * @param pageNo    page number
     * @param searchVal search value
     * @param pageSize  page size
     * @return worker group list page
     */
    @Override
    public Result queryAllGroupPaging(User loginUser, Integer pageNo, Integer pageSize, String searchVal) {
        // list from index
        int fromIndex = (pageNo - 1) * pageSize;
        // list to index
        int toIndex = (pageNo - 1) * pageSize + pageSize;

        Result result = new Result();
        List<WorkerGroupPageDetail> workerGroupPageDetails;
        if (loginUser.getUserType().equals(UserType.ADMIN_USER)) {
            workerGroupPageDetails = getUiWorkerGroupPageDetails(null);
        } else {
            Set<Integer> ids = resourcePermissionCheckService
                    .userOwnedResourceIdsAcquisition(AuthorizationType.WORKER_GROUP, loginUser.getId(), log);
            workerGroupPageDetails =
                    getUiWorkerGroupPageDetails(ids.isEmpty() ? Collections.emptyList() : new ArrayList<>(ids));
        }
        List<WorkerGroupPageDetail> resultDataList = new ArrayList<>();
        int total = 0;

        if (CollectionUtils.isNotEmpty(workerGroupPageDetails)) {
            List<WorkerGroupPageDetail> searchValDataList = new ArrayList<>();

            if (!StringUtils.isEmpty(searchVal)) {
                for (WorkerGroupPageDetail workerGroup : workerGroupPageDetails) {
                    if (workerGroup.getName().contains(searchVal)) {
                        searchValDataList.add(workerGroup);
                    }
                }
            } else {
                searchValDataList = workerGroupPageDetails;
            }
            total = searchValDataList.size();
            if (fromIndex < searchValDataList.size()) {
                if (toIndex > searchValDataList.size()) {
                    toIndex = searchValDataList.size();
                }
                resultDataList = searchValDataList.subList(fromIndex, toIndex);
            }
        }
        List<WorkerGroupPageDetail> configWorkerGroupPageDetails = getConfigWorkerGroupPageDetail();
        configWorkerGroupPageDetails.addAll(resultDataList);

        PageInfo<WorkerGroupPageDetail> pageInfo = new PageInfo<>(pageNo, pageSize);
        pageInfo.setTotal(total);
        pageInfo.setTotalList(configWorkerGroupPageDetails);

        result.setData(pageInfo);
        putMsg(result, Status.SUCCESS);
        return result;
    }

    /**
     * query all worker group
     *
     * @param loginUser
     * @return all worker group list
     */
    @Override
    public Map<String, Object> queryAllGroup(User loginUser) {
        Map<String, Object> result = new HashMap<>();
        List<WorkerGroupPageDetail> workerGroups;
        if (loginUser.getUserType().equals(UserType.ADMIN_USER)) {
            workerGroups = getUiWorkerGroupPageDetails(null);
        } else {
            Set<Integer> ids = resourcePermissionCheckService
                    .userOwnedResourceIdsAcquisition(AuthorizationType.WORKER_GROUP, loginUser.getId(), log);
            workerGroups = getUiWorkerGroupPageDetails(ids.isEmpty() ? Collections.emptyList() : new ArrayList<>(ids));
        }
        List<String> configWorkerGroupNames = getConfigWorkerGroupPageDetail().stream()
                .map(WorkerGroupPageDetail::getName)
                .collect(Collectors.toList());
        List<String> availableWorkerGroupList = workerGroups.stream()
                .map(WorkerGroup::getName)
                .collect(Collectors.toList());
        availableWorkerGroupList.addAll(configWorkerGroupNames);
        result.put(Constants.DATA_LIST, availableWorkerGroupList.stream().distinct().collect(Collectors.toList()));
        putMsg(result, Status.SUCCESS);
        return result;
    }

    private List<WorkerGroupPageDetail> getUiWorkerGroupPageDetails(List<Integer> ids) {
        List<WorkerGroup> workerGroups;
        if (ids != null) {
            workerGroups = ids.isEmpty() ? new ArrayList<>() : workerGroupDao.queryByIds(ids);
        } else {
            workerGroups = workerGroupDao.queryAllWorkerGroup();
        }
        return workerGroups.stream()
                .map(workerGroup -> {
                    WorkerGroupPageDetail workerGroupPageDetail = new WorkerGroupPageDetail(workerGroup);
                    workerGroupPageDetail.setSource(WorkerGroupSource.UI);
                    workerGroupPageDetail.setSystemDefault(false);
                    return workerGroupPageDetail;
                }).collect(Collectors.toList());
    }

    /**
     * delete worker group by id
     *
     * @param id worker group id
     * @return delete result code
     */
    @Override
    @Transactional
    public Map<String, Object> deleteWorkerGroupById(User loginUser, Integer id) {
        Map<String, Object> result = new HashMap<>();
        if (!canOperatorPermissions(loginUser, null, AuthorizationType.WORKER_GROUP, WORKER_GROUP_DELETE)) {
            putMsg(result, Status.USER_NO_OPERATION_PERM);
            return result;
        }
        WorkerGroup workerGroup = workerGroupDao.queryById(id);
        if (workerGroup == null) {
            log.error("Worker group does not exist, workerGroupId:{}.", id);
            putMsg(result, Status.DELETE_WORKER_GROUP_NOT_EXIST);
            return result;
        }
        List<WorkflowInstance> workflowInstances = workflowInstanceMapper.queryByWorkerGroupNameAndStatus(
                workerGroup.getName(),
                WorkflowExecutionStatus.getNotTerminalStatus());
        if (CollectionUtils.isNotEmpty(workflowInstances)) {
            List<Integer> workflowInstanceIds =
                    workflowInstances.stream().map(WorkflowInstance::getId).collect(Collectors.toList());
            log.warn(
                    "Delete worker group failed because there are {} workflowInstances are using it, workflowInstanceIds:{}.",
                    workflowInstances.size(), workflowInstanceIds);
            putMsg(result, Status.DELETE_WORKER_GROUP_BY_ID_FAIL, workflowInstances.size());
            return result;
        }

        if (checkWorkerGroupDependencies(workerGroup, result)) {
            return result;
        }

        workerGroupDao.deleteById(id);
        boardCastToMasterThatWorkerGroupChanged();

        log.info("Delete worker group complete, workerGroupName:{}.", workerGroup.getName());
        putMsg(result, Status.SUCCESS);
        return result;
    }

    /**
     * query all worker address list
     *
     * @return all worker address list
     */
    @Override
    public Map<String, Object> getWorkerAddressList() {
        Map<String, Object> result = new HashMap<>();
        Set<String> serverNodeList = registryClient.getServerNodeSet(RegistryNodeType.WORKER);
        result.put(Constants.DATA_LIST, serverNodeList);
        putMsg(result, Status.SUCCESS);
        return result;
    }

    @Override
    public Map<Long, String> queryWorkerGroupByWorkflowDefinitionCodes(List<Long> workflowDefinitionCodeList) {
        List<Schedule> workflowDefinitionScheduleList =
                scheduleMapper.querySchedulesByWorkflowDefinitionCodes(workflowDefinitionCodeList);
        return workflowDefinitionScheduleList.stream().collect(Collectors.toMap(Schedule::getWorkflowDefinitionCode,
                Schedule::getWorkerGroup));
    }

    private void boardCastToMasterThatWorkerGroupChanged() {
        final List<Server> masters = registryClient.getServerList(RegistryNodeType.MASTER);
        if (CollectionUtils.isEmpty(masters)) {
            return;
        }
        for (Server master : masters) {
            try {
                Clients.withService(IMasterContainerService.class)
                        .withHost(master.getHost() + Constants.COLON + master.getPort())
                        .refreshWorkerGroup();
            } catch (Exception e) {
                log.error("Broadcast to master: {} that worker group changed failed", master, e);
            }
        }
    }

    @Override
    public List<WorkerGroupPageDetail> getConfigWorkerGroupPageDetail() {
        List<WorkerGroupPageDetail> workerGroupPageDetails = new ArrayList<>();
        registryClient.getServerList(RegistryNodeType.WORKER).forEach(server -> {
            WorkerGroupPageDetail workerGroupPageDetail = new WorkerGroupPageDetail();
            WorkerHeartBeat workerHeartBeat = JSONUtils.parseObject(server.getHeartBeatInfo(), WorkerHeartBeat.class);
            workerGroupPageDetail.setName(workerHeartBeat.getWorkerGroup());
            workerGroupPageDetail.setAddrList(workerHeartBeat.getHost() + Constants.COLON + workerHeartBeat.getPort());
            workerGroupPageDetail.setSource(WorkerGroupSource.CONFIG);
            workerGroupPageDetail.setCreateTime(DateUtils.timeStampToDate(workerHeartBeat.getStartupTime()));
            workerGroupPageDetail.setUpdateTime(DateUtils.timeStampToDate(workerHeartBeat.getReportTime()));
            workerGroupPageDetail.setSystemDefault(true);
            workerGroupPageDetails.add(workerGroupPageDetail);
        });
        return workerGroupPageDetails;
    }

}
