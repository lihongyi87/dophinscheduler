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

import static org.apache.dolphinscheduler.api.constants.ApiFuncIdentificationConstant.TENANT_DELETE;
import static org.apache.dolphinscheduler.api.constants.ApiFuncIdentificationConstant.YARN_QUEUE_CREATE;
import static org.apache.dolphinscheduler.api.constants.ApiFuncIdentificationConstant.YARN_QUEUE_UPDATE;

import org.apache.dolphinscheduler.api.enums.Status;
import org.apache.dolphinscheduler.api.exceptions.ServiceException;
import org.apache.dolphinscheduler.api.service.QueueService;
import org.apache.dolphinscheduler.api.utils.PageInfo;
import org.apache.dolphinscheduler.common.constants.Constants;
import org.apache.dolphinscheduler.common.enums.AuthorizationType;
import org.apache.dolphinscheduler.common.enums.UserType;
import org.apache.dolphinscheduler.dao.entity.Queue;
import org.apache.dolphinscheduler.dao.entity.Tenant;
import org.apache.dolphinscheduler.dao.entity.User;
import org.apache.dolphinscheduler.dao.mapper.QueueMapper;
import org.apache.dolphinscheduler.dao.mapper.TenantMapper;
import org.apache.dolphinscheduler.dao.mapper.UserMapper;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;

/**
 * 队列服务实现类
 *
 * <p>该类实现了队列的管理功能。队列是YARN资源调度的基本单位，
 * 不同的队列可以配置不同的资源配额和调度策略。</p>
 *
 * <p>主要功能：</p>
 * <ul>
 *   <li>创建和管理队列</li>
 *   <li>队列与租户关联</li>
 *   <li>队列与用户关联</li>
 *   <li>队列资源配额管理</li>
 *   <li>队列使用状态检查</li>
 * </ul>
 */
@Service
@Slf4j
public class QueueServiceImpl extends BaseServiceImpl implements QueueService {

    @Autowired
    private QueueMapper queueMapper;

    @Autowired
    private UserMapper userMapper;

    @Autowired
    private TenantMapper tenantMapper;

    /**
     * 检查新建队列对象的有效性
     *
     * @param queue 要创建的队列对象
     */
    private void validQueue(Queue queue) throws ServiceException {
        // 检查队列值是否为空
        if (StringUtils.isEmpty(queue.getQueue())) {
            throw new ServiceException(Status.REQUEST_PARAMS_NOT_VALID_ERROR, Constants.QUEUE);
        }
        // 检查队列名称是否为空
        else if (StringUtils.isEmpty(queue.getQueueName())) {
            throw new ServiceException(Status.REQUEST_PARAMS_NOT_VALID_ERROR, Constants.QUEUE_NAME);
        }
        // 检查队列值是否已存在
        else if (checkQueueExist(queue.getQueue())) {
            throw new ServiceException(Status.QUEUE_VALUE_EXIST, queue.getQueue());
        }
        // 检查队列名称是否已存在
        else if (checkQueueNameExist(queue.getQueueName())) {
            throw new ServiceException(Status.QUEUE_NAME_EXIST, queue.getQueueName());
        }
    }

    /**
     * Check queue update object valid or not
     *
     * @param existsQueue The exists queue object
     * @param updateQueue The queue object want to update
     */
    private void updateQueueValid(Queue existsQueue, Queue updateQueue) throws ServiceException {
        // Check the exists queue and the necessary of update operation, in not exist checker have to use updateQueue to
        // avoid NPE
        if (Objects.isNull(existsQueue)) {
            throw new ServiceException(Status.QUEUE_NOT_EXIST, updateQueue.getQueue());
        } else if (Objects.equals(existsQueue, updateQueue)) {
            throw new ServiceException(Status.NEED_NOT_UPDATE_QUEUE);
        }
        // Check the update queue parameters
        else if (StringUtils.isEmpty(updateQueue.getQueue())) {
            throw new ServiceException(Status.REQUEST_PARAMS_NOT_VALID_ERROR, Constants.QUEUE);
        } else if (StringUtils.isEmpty(updateQueue.getQueueName())) {
            throw new ServiceException(Status.REQUEST_PARAMS_NOT_VALID_ERROR, Constants.QUEUE_NAME);
        } else if (!Objects.equals(updateQueue.getQueue(), existsQueue.getQueue())
                && checkQueueExist(updateQueue.getQueue())) {
            throw new ServiceException(Status.QUEUE_VALUE_EXIST, updateQueue.getQueue());
        } else if (!Objects.equals(updateQueue.getQueueName(), existsQueue.getQueueName())
                && checkQueueNameExist(updateQueue.getQueueName())) {
            throw new ServiceException(Status.QUEUE_NAME_EXIST, updateQueue.getQueueName());
        }
    }

    /**
     * 查询队列列表
     *
     * @param loginUser 登录用户
     * @return 队列列表
     */
    @Override
    public List<Queue> queryList(User loginUser) {
        // 获取用户有权限访问的队列ID集合
        Set<Integer> ids = resourcePermissionCheckService.userOwnedResourceIdsAcquisition(AuthorizationType.QUEUE,
                loginUser.getId(), log);
        // 如果是普通用户，需要添加默认队列ID
        if (loginUser.getUserType().equals(UserType.GENERAL_USER)) {
            ids = ids.isEmpty() ? new HashSet<>() : ids;
            ids.add(Constants.DEFAULT_QUEUE_ID);
        }
        // 根据ID集合查询队列
        return queueMapper.selectBatchIds(ids);
    }

    /**
     * query queue list paging
     *
     * @param loginUser login user
     * @param pageNo    page number
     * @param searchVal search value
     * @param pageSize  page size
     * @return queue list
     */
    @Override
    public PageInfo<Queue> queryList(User loginUser, String searchVal, Integer pageNo, Integer pageSize) {
        PageInfo<Queue> pageInfo = new PageInfo<>(pageNo, pageSize);
        Set<Integer> ids = resourcePermissionCheckService.userOwnedResourceIdsAcquisition(AuthorizationType.QUEUE,
                loginUser.getId(), log);
        if (ids.isEmpty()) {
            return pageInfo;
        }
        Page<Queue> page = new Page<>(pageNo, pageSize);
        IPage<Queue> queueList = queueMapper.queryQueuePaging(page, new ArrayList<>(ids), searchVal);
        Integer count = (int) queueList.getTotal();
        pageInfo.setTotal(count);
        pageInfo.setTotalList(queueList.getRecords());
        return pageInfo;
    }

    /**
     * 创建队列
     *
     * @param loginUser 登录用户
     * @param queue     队列值
     * @param queueName 队列名称
     * @return 创建的队列对象
     */
    @Override
    public Queue createQueue(User loginUser, String queue, String queueName) {
        // 检查用户是否有创建队列的权限
        if (!canOperatorPermissions(loginUser, null, AuthorizationType.QUEUE, YARN_QUEUE_CREATE)) {
            throw new ServiceException(Status.USER_NO_OPERATION_PERM);
        }

        // 创建队列对象
        Queue queueObj = new Queue(queueName, queue);
        // 验证队列有效性
        validQueue(queueObj);
        // 插入队列到数据库
        queueMapper.insert(queueObj);

        return queueObj;
    }

    /**
     * 更新队列
     *
     * @param loginUser 登录用户
     * @param queue     队列值
     * @param id        队列ID
     * @param queueName 队列名称
     * @return 更新后的队列对象
     */
    @Override
    public Queue updateQueue(User loginUser, int id, String queue, String queueName) {
        // 检查用户是否有更新队列的权限
        if (!canOperatorPermissions(loginUser, new Object[]{id}, AuthorizationType.QUEUE, YARN_QUEUE_UPDATE)) {
            throw new ServiceException(Status.USER_NO_OPERATION_PERM);
        }

        // 创建更新的队列对象
        Queue updateQueue = new Queue(id, queueName, queue);
        updateQueue.setCreateTime(null);
        // 查询现有队列
        Queue existsQueue = queueMapper.selectById(id);
        // 验证更新的有效性
        updateQueueValid(existsQueue, updateQueue);

        // 检查旧队列是否被任何用户使用
        if (checkIfQueueIsInUsing(existsQueue.getQueueName(), updateQueue.getQueueName())) {
            // 更新与旧队列关联的用户
            Integer relatedUserNums =
                    userMapper.updateUserQueue(existsQueue.getQueueName(), updateQueue.getQueueName());
            log.info("Old queue have related {} users, exec update user success.", relatedUserNums);
        }

        // 更新队列信息
        queueMapper.updateById(updateQueue);
        return updateQueue;
    }

    /**
     * 删除队列
     *
     * @param loginUser 登录用户
     * @param id        队列ID
     * @throws Exception 异常
     */
    @Override
    public void deleteQueueById(User loginUser, int id) throws Exception {

        // 检查用户是否有删除权限
        if (!canOperatorPermissions(loginUser, null, AuthorizationType.TENANT, TENANT_DELETE)) {
            throw new ServiceException(Status.USER_NO_OPERATION_PERM);
        }

        // 查询队列是否存在
        Queue queue = queueMapper.selectById(id);
        if (Objects.isNull(queue)) {
            log.error("Queue does not exist");
            throw new ServiceException(Status.QUEUE_NOT_EXIST);
        }

        // 检查是否有租户使用该队列
        List<Tenant> tenantList = tenantMapper.queryTenantListByQueueId(queue.getId());
        if (CollectionUtils.isNotEmpty(tenantList)) {
            log.warn("Delete queue failed, because there are {} tenants using it.", tenantList.size());
            throw new ServiceException(Status.DELETE_TENANT_BY_ID_FAIL_TENANTS, tenantList.size());
        }

        // 检查是否有用户使用该队列
        List<User> userList = userMapper.queryUserListByQueue(queue.getQueueName());
        if (CollectionUtils.isNotEmpty(userList)) {
            log.warn("Delete queue failed, because there are {} users using it.", userList.size());
            throw new ServiceException(Status.DELETE_QUEUE_BY_ID_FAIL_USERS, userList.size());
        }

        // 执行删除操作
        int delete = queueMapper.deleteById(id);
        if (delete <= 0) {
            throw new ServiceException(Status.DELETE_QUEUE_BY_ID_ERROR);
        }

    }

    /**
     * verify queue and queueName
     *
     * @param queue queue
     * @param queueName queue name
     * @return true if the queue name not exists, otherwise return false
     */
    @Override
    public void verifyQueue(String queue, String queueName) {
        Queue queueValidator = new Queue(queueName, queue);
        validQueue(queueValidator);
    }

    /**
     * check queue exist
     * if exists return true，not exists return false
     * check queue exist
     *
     * @param queue queue
     * @return true if the queue not exists, otherwise return false
     */
    private boolean checkQueueExist(String queue) {
        return queueMapper.existQueue(queue, null) == Boolean.TRUE;
    }

    /**
     * check queue name exist
     * if exists return true，not exists return false
     *
     * @param queueName queue name
     * @return true if the queue name not exists, otherwise return false
     */
    private boolean checkQueueNameExist(String queueName) {
        return queueMapper.existQueue(null, queueName) == Boolean.TRUE;
    }

    /**
     * check old queue name using by any user
     * if need to update user
     *
     * @param oldQueue old queue name
     * @param newQueue new queue name
     * @return true if need to update user
     */
    private boolean checkIfQueueIsInUsing(String oldQueue, String newQueue) {
        return !oldQueue.equals(newQueue) && userMapper.existUser(oldQueue) == Boolean.TRUE;
    }

    /**
     * Make sure queue with given name exists, and create the queue if not exists
     *
     * ONLY for python gateway server, and should not use this in web ui function
     *
     * @param queue queue value
     * @param queueName queue name
     * @return Queue object
     */
    @Override
    public Queue createQueueIfNotExists(String queue, String queueName) {
        Queue existsQueue = queueMapper.queryQueueName(queue, queueName);
        if (!Objects.isNull(existsQueue)) {
            log.info("Queue exists, so return it, queueName:{}.", queueName);
            return existsQueue;
        }
        Queue queueObj = new Queue(queueName, queue);
        validQueue(queueObj);
        queueMapper.insert(queueObj);
        log.info("Queue create complete, queueName:{}.", queueObj.getQueueName());
        return queueObj;
    }

}
