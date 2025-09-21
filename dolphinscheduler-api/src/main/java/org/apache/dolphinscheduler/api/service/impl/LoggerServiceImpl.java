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

import static org.apache.dolphinscheduler.api.constants.ApiFuncIdentificationConstant.DOWNLOAD_LOG;
import static org.apache.dolphinscheduler.api.constants.ApiFuncIdentificationConstant.VIEW_LOG;

import org.apache.dolphinscheduler.api.enums.Status;
import org.apache.dolphinscheduler.api.exceptions.ServiceException;
import org.apache.dolphinscheduler.api.executor.logging.LogClientDelegate;
import org.apache.dolphinscheduler.api.service.LoggerService;
import org.apache.dolphinscheduler.api.service.ProjectService;
import org.apache.dolphinscheduler.api.utils.Result;
import org.apache.dolphinscheduler.common.constants.Constants;
import org.apache.dolphinscheduler.dao.entity.Project;
import org.apache.dolphinscheduler.dao.entity.ResponseTaskLog;
import org.apache.dolphinscheduler.dao.entity.TaskDefinition;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.dao.entity.User;
import org.apache.dolphinscheduler.dao.mapper.ProjectMapper;
import org.apache.dolphinscheduler.dao.mapper.TaskDefinitionMapper;
import org.apache.dolphinscheduler.dao.repository.TaskInstanceDao;

import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.google.common.primitives.Bytes;

/**
 * 日志服务实现类
 *
 * <p>该类实现了任务执行日志的查询和下载功能。通过远程调用
 * Worker节点的日志服务，获取任务执行过程中产生的日志内容。</p>
 *
 * <p>主要功能：</p>
 * <ul>
 *   <li>查询任务执行日志</li>
 *   <li>下载完整日志文件</li>
 *   <li>分页查看日志内容</li>
 *   <li>获取日志文件信息</li>
 * </ul>
 */
@Service
@Slf4j
public class LoggerServiceImpl extends BaseServiceImpl implements LoggerService {

    /** 日志头部格式 - 显示日志路径和主机信息 */
    private static final String LOG_HEAD_FORMAT = "[LOG-PATH]: %s, [HOST]: %s%s";

    @Autowired
    private TaskInstanceDao taskInstanceDao;

    @Autowired
    private ProjectMapper projectMapper;

    @Autowired
    private ProjectService projectService;

    @Autowired
    private TaskDefinitionMapper taskDefinitionMapper;

    @Autowired
    private LogClientDelegate logClientDelegate;

    /**
     * 查看任务日志
     *
     * @param loginUser   登录用户
     * @param taskInstId  任务实例ID
     * @param skipLineNum 跳过的行数（用于分页）
     * @param limit       限制返回的行数
     * @return 日志内容响应对象
     */
    @Override
    @SuppressWarnings("unchecked")
    public Result<ResponseTaskLog> queryLog(User loginUser, int taskInstId, int skipLineNum, int limit) {
        // 查询任务实例，获取任务的基本信息
        TaskInstance taskInstance = taskInstanceDao.queryById(taskInstId);

        // 检查任务实例是否存在，防止非法访问
        if (taskInstance == null) {
            log.error("任务实例不存在，任务实例ID: {}", taskInstId);
            return Result.error(Status.TASK_INSTANCE_NOT_FOUND);
        }

        // 检查任务实例的主机信息是否存在
        // 只有已经分配到Worker节点的任务才有主机信息和日志文件
        if (StringUtils.isBlank(taskInstance.getHost())) {
            log.error("任务实例的主机为空，任务实例ID: {}", taskInstId);
            return Result.error(Status.TASK_INSTANCE_HOST_IS_NULL);
        }

        // 检查用户是否有查看日志的权限
        // 基于项目级别的权限控制，确保用户只能查看有权限的项目日志
        projectService.checkProjectAndAuthThrowException(loginUser, taskInstance.getProjectCode(), VIEW_LOG);

        // 创建成功状态的结果对象
        Result<ResponseTaskLog> result = new Result<>(Status.SUCCESS.getCode(), Status.SUCCESS.getMsg());

        // 调用私有方法查询日志内容，支持分页查看
        String log = queryLog(taskInstance, skipLineNum, limit);

        // 计算返回的日志行数
        // 使用回车换行符分割日志内容，统计行数
        int lineNum = log.split("\\r\\n").length;
        // 封装日志响应对象，包含行数和日志内容
        result.setData(new ResponseTaskLog(lineNum, log));
        return result;
    }

    /**
     * 获取日志字节数组（用于下载日志文件）
     *
     * @param loginUser  登录用户
     * @param taskInstId 任务实例ID
     * @return 日志字节数组
     */
    @Override
    public byte[] getLogBytes(User loginUser, int taskInstId) {
        // 查询任务实例信息
        TaskInstance taskInstance = taskInstanceDao.queryById(taskInstId);
        // 检查任务实例和主机信息的有效性
        if (taskInstance == null || StringUtils.isBlank(taskInstance.getHost())) {
            throw new ServiceException("task instance is null or host is null");
        }
        // 根据任务实例ID查询所属项目信息
        Project project = projectMapper.queryProjectByTaskInstanceId(taskInstId);
        // 检查用户是否有下载日志的权限
        projectService.checkProjectAndAuthThrowException(loginUser, project, DOWNLOAD_LOG);
        // 调用私有方法获取日志字节数组
        return getLogBytes(taskInstance);
    }

    /**
     * 查询日志（指定项目编码版本）
     *
     * @param loginUser   登录用户
     * @param projectCode 项目编码
     * @param taskInstId  任务实例ID
     * @param skipLineNum 跳过的行数
     * @param limit       限制返回的行数
     * @return 日志字符串数据
     */
    @Override
    @SuppressWarnings("unchecked")
    public String queryLog(User loginUser, long projectCode, int taskInstId, int skipLineNum, int limit) {
        // 检查用户对项目的访问权限
        // 首先验证用户是否有权访问指定项目的日志
        projectService.checkProjectAndAuthThrowException(loginUser, projectCode, VIEW_LOG);

        // 检查任务实例是否可以找到
        TaskInstance task = taskInstanceDao.queryById(taskInstId);
        // 验证任务实例存在性和主机信息有效性
        if (task == null || StringUtils.isBlank(task.getHost())) {
            throw new ServiceException(Status.TASK_INSTANCE_NOT_FOUND);
        }

        // 查询任务定义信息进行项目匹配验证
        TaskDefinition taskDefinition = taskDefinitionMapper.queryByCode(task.getTaskCode());
        // 检查任务是否属于指定的项目，防止跨项目访问
        if (taskDefinition != null && projectCode != taskDefinition.getProjectCode()) {
            throw new ServiceException(Status.TASK_INSTANCE_NOT_FOUND, taskInstId);
        }
        // 调用私有方法查询日志内容
        return queryLog(task, skipLineNum, limit);
    }

    /**
     * 获取日志字节数组（指定项目编码版本）
     *
     * @param loginUser   登录用户
     * @param projectCode 项目编码
     * @param taskInstId  任务实例ID
     * @return 日志字节数组
     */
    @Override
    public byte[] getLogBytes(User loginUser, long projectCode, int taskInstId) {
        // 检查用户对项目的访问权限
        // 验证用户是否有下载指定项目日志的权限
        projectService.checkProjectAndAuthThrowException(loginUser, projectCode, DOWNLOAD_LOG);

        // 检查任务实例是否可以找到
        TaskInstance task = taskInstanceDao.queryById(taskInstId);
        // 验证任务实例和主机信息的有效性
        if (task == null || StringUtils.isBlank(task.getHost())) {
            throw new ServiceException("task instance is null or host is null");
        }

        // 查询任务定义信息进行项目匹配验证
        TaskDefinition taskDefinition = taskDefinitionMapper.queryByCode(task.getTaskCode());
        // 验证任务是否属于指定项目，确保数据安全
        if (taskDefinition != null && projectCode != taskDefinition.getProjectCode()) {
            throw new ServiceException("task instance does not exist in project");
        }
        // 调用私有方法获取日志字节数组
        return getLogBytes(task);
    }

    /**
     * 查询任务日志的核心方法
     *
     * @param taskInstance 任务实例
     * @param skipLineNum  跳过的行数（用于分页）
     * @param limit        限制返回的行数
     * @return 日志字符串数据
     */
    private String queryLog(TaskInstance taskInstance, int skipLineNum, int limit) {
        // 获取任务实例的日志文件路径
        final String logPath = taskInstance.getLogPath();
        // 记录日志查询请求的详细信息，用于调试和监控
        log.info("查询任务实例日志，任务实例ID: {}, 任务名称: {}, 主机: {}, 日志路径: {}",
                taskInstance.getId(), taskInstance.getName(), taskInstance.getHost(), logPath);

        // 检查日志路径是否为空，空路径表示任务尚未被分配或者配置错误
        if (StringUtils.isBlank(logPath)) {
            throw new ServiceException(Status.QUERY_TASK_INSTANCE_LOG_ERROR,
                    "TaskInstanceLogPath is empty, maybe the taskInstance doesn't be dispatched");
        }

        // 使用StringBuilder构建日志内容，提高字符串拼接性能
        StringBuilder sb = new StringBuilder();

        // 如果是从第一行开始查询，需要添加日志头部信息
        // 头部信息包含日志文件路径和主机信息，方便用户了解日志来源
        if (skipLineNum == 0) {
            String head = String.format(LOG_HEAD_FORMAT,
                    logPath,                           // 日志文件路径
                    taskInstance.getHost(),            // 执行任务的主机
                    Constants.SYSTEM_LINE_SEPARATOR);  // 系统换行符
            sb.append(head);
        }

        try {
            // 通过日志客户端代理获取部分日志内容
            // 这里会远程调用Worker节点的日志服务获取实际的日志数据
            String logContent = logClientDelegate.getPartLogString(taskInstance, skipLineNum, limit);
            // 如果获取到日志内容，将其追加到结果中
            if (logContent != null) {
                sb.append(logContent);
            }
            // 返回完整的日志字符串
            return sb.toString();
        } catch (Throwable ex) {
            // 捕获所有异常并封装为服务异常，提供统一的错误处理
            throw new ServiceException(Status.QUERY_TASK_INSTANCE_LOG_ERROR, ex.getMessage(), ex);
        }
    }

    /**
     * 获取任务日志字节数组的核心方法
     *
     * @param taskInstance 任务实例
     * @return 日志字节数组
     */
    private byte[] getLogBytes(TaskInstance taskInstance) {
        // 获取任务执行的主机信息
        String host = taskInstance.getHost();
        // 获取日志文件路径
        String logPath = taskInstance.getLogPath();

        // 创建日志头部信息的字节数组
        // 包含日志路径和主机信息，使用UTF-8编码确保中文兼容性
        byte[] head = String.format(LOG_HEAD_FORMAT,
                logPath,                           // 日志文件的完整路径
                host,                              // 执行任务的Worker节点主机
                Constants.SYSTEM_LINE_SEPARATOR)   // 系统换行符
                .getBytes(StandardCharsets.UTF_8);

        // 声明日志内容字节数组
        byte[] logBytes;

        try {
            // 通过日志客户端代理获取完整的日志文件字节数组
            // 这里会远程调用Worker节点下载完整的日志文件
            logBytes = logClientDelegate.getWholeLogBytes(taskInstance);
            // 将头部信息和日志内容合并为一个完整的字节数组
            // 使用Guava的Bytes.concat方法高效地合并字节数组
            return Bytes.concat(head, logBytes);
        } catch (Exception ex) {
            // 记录下载失败的错误日志，包含任务名称和异常信息
            log.error("下载任务实例: {} 的日志出错", taskInstance.getName(), ex);
            // 抛出服务异常，提供统一的错误处理
            throw new ServiceException(Status.DOWNLOAD_TASK_INSTANCE_LOG_FILE_ERROR);
        }
    }
}
