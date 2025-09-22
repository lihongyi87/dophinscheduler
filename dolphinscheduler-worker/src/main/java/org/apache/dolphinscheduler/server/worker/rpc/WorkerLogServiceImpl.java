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

package org.apache.dolphinscheduler.server.worker.rpc;

import org.apache.dolphinscheduler.extract.common.ILogService;
import org.apache.dolphinscheduler.extract.common.service.impl.LogServiceImpl;
import org.apache.dolphinscheduler.extract.common.transportor.TaskInstanceLogFileDownloadRequest;
import org.apache.dolphinscheduler.extract.common.transportor.TaskInstanceLogFileDownloadResponse;
import org.apache.dolphinscheduler.extract.common.transportor.TaskInstanceLogPageQueryRequest;
import org.apache.dolphinscheduler.extract.common.transportor.TaskInstanceLogPageQueryResponse;

import lombok.extern.slf4j.Slf4j;

import org.springframework.stereotype.Service;

/**
 * Worker日志服务实现类
 *
 * <p>该类是Worker节点中负责日志管理相关操作的RPC服务实现。
 * 继承自通用的LogServiceImpl，为Worker节点提供日志服务功能。</p>
 *
 * <p>主要功能：</p>
 * <ul>
 *   <li>读取任务执行日志</li>
 *   <li>获取日志文件信息</li>
 *   <li>查询日志内容</li>
 *   <li>删除过期日志</li>
 * </ul>
 *
 * <p>该服务支持远程调用，允许Master节点或Web UI通过RPC方式
 * 访问Worker节点上的任务执行日志。</p>
 *
 * @see LogServiceImpl
 * @see ILogService
 */
@Slf4j
@Service
public class WorkerLogServiceImpl extends LogServiceImpl implements ILogService {

    /**
     * 获取任务实例的完整日志文件字节数组
     *
     * <p>该方法用于下载指定任务实例的完整日志文件内容。
     * 通过RPC调用，允许Master节点或Web UI获取Worker节点上的任务执行日志。</p>
     *
     * <p>处理流程：</p>
     * <ol>
     *   <li>接收日志文件下载请求</li>
     *   <li>从请求中提取任务实例日志文件的绝对路径</li>
     *   <li>调用LogUtils从本地文件系统读取日志文件内容</li>
     *   <li>将文件内容转换为字节数组</li>
     *   <li>封装到响应对象中返回</li>
     *   <li>如果发生异常，设置错误状态和错误信息</li>
     * </ol>
     *
     * <p>注意事项：</p>
     * <ul>
     *   <li>只能访问Worker节点本地的日志文件</li>
     *   <li>需要确保日志文件路径的安全性</li>
     *   <li>大文件读取可能消耗较多内存</li>
     * </ul>
     *
     * @param taskInstanceLogFileDownloadRequest 日志文件下载请求，包含日志文件路径
     * @return TaskInstanceLogFileDownloadResponse 日志文件下载响应，包含文件字节数组
     */
    @Override
    public TaskInstanceLogFileDownloadResponse getTaskInstanceWholeLogFileBytes(TaskInstanceLogFileDownloadRequest taskInstanceLogFileDownloadRequest) {
        // 记录接收到的日志文件下载请求
        log.info("Receive TaskInstanceLogFileDownloadRequest: {}", taskInstanceLogFileDownloadRequest);
        // 调用父类方法执行实际的日志文件读取操作
        return super.getTaskInstanceWholeLogFileBytes(taskInstanceLogFileDownloadRequest);
    }

    /**
     * 分页查询任务实例日志内容
     *
     * <p>该方法用于分页获取任务实例的日志内容，支持指定起始行数和读取行数。
     * 主要用于Web UI中的日志查看功能，避免一次性加载大文件。</p>
     *
     * <p>处理流程：</p>
     * <ol>
     *   <li>接收日志分页查询请求</li>
     *   <li>从请求中提取日志文件路径、跳过行数和读取限制</li>
     *   <li>调用LogUtils读取指定范围的日志内容</li>
     *   <li>对日志内容进行格式化处理</li>
     *   <li>封装到响应对象中返回</li>
     *   <li>如果发生异常，设置错误状态和错误信息</li>
     * </ol>
     *
     * <p>分页参数：</p>
     * <ul>
     *   <li>skipLineNum: 跳过的行数，用于分页定位</li>
     *   <li>limit: 读取的最大行数，控制返回数据量</li>
     * </ul>
     *
     * @param taskInstanceLogPageQueryRequest 日志分页查询请求，包含路径和分页参数
     * @return TaskInstanceLogPageQueryResponse 日志分页查询响应，包含日志内容
     */
    @Override
    public TaskInstanceLogPageQueryResponse pageQueryTaskInstanceLog(TaskInstanceLogPageQueryRequest taskInstanceLogPageQueryRequest) {
        // 记录接收到的日志分页查询请求
        log.info("Receive TaskInstanceLogPageQueryRequest: {}", taskInstanceLogPageQueryRequest);
        // 调用父类方法执行实际的日志分页查询操作
        return super.pageQueryTaskInstanceLog(taskInstanceLogPageQueryRequest);
    }

    /**
     * 删除任务实例日志文件
     *
     * <p>该方法用于删除指定的任务实例日志文件，通常在日志清理或任务完成后调用。
     * 有助于节省磁盘空间和管理日志文件的生命周期。</p>
     *
     * <p>处理流程：</p>
     * <ol>
     *   <li>接收日志文件删除请求</li>
     *   <li>验证日志文件路径的有效性和安全性</li>
     *   <li>调用FileUtils删除指定的日志文件</li>
     *   <li>记录删除操作的结果</li>
     * </ol>
     *
     * <p>安全考虑：</p>
     * <ul>
     *   <li>只能删除Worker节点本地的日志文件</li>
     *   <li>需要验证文件路径的合法性</li>
     *   <li>避免删除系统关键文件</li>
     * </ul>
     *
     * @param taskInstanceLogAbsolutePath 要删除的任务实例日志文件的绝对路径
     */
    @Override
    public void removeTaskInstanceLog(String taskInstanceLogAbsolutePath) {
        // 记录接收到的日志文件删除请求
        log.info("Receive removeTaskInstanceLog request, path: {}", taskInstanceLogAbsolutePath);
        // 调用父类方法执行实际的日志文件删除操作
        super.removeTaskInstanceLog(taskInstanceLogAbsolutePath);
        // 记录日志文件删除完成
        log.info("TaskInstanceLog removed successfully, path: {}", taskInstanceLogAbsolutePath);
    }

}
