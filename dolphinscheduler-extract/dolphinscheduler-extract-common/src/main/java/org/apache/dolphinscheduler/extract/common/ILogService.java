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

package org.apache.dolphinscheduler.extract.common;

import org.apache.dolphinscheduler.extract.base.RpcMethod;
import org.apache.dolphinscheduler.extract.base.RpcService;
import org.apache.dolphinscheduler.extract.common.transportor.TaskInstanceLogFileDownloadRequest;
import org.apache.dolphinscheduler.extract.common.transportor.TaskInstanceLogFileDownloadResponse;
import org.apache.dolphinscheduler.extract.common.transportor.TaskInstanceLogPageQueryRequest;
import org.apache.dolphinscheduler.extract.common.transportor.TaskInstanceLogPageQueryResponse;

/**
 * 日志服务接口
 *
 * <p>该接口定义了任务实例日志的管理和查询方法。</p>
 *
 * <p>主要功能：</p>
 * <ul>
 *   <li>下载任务实例的完整日志文件</li>
 *   <li>分页查询任务实例日志</li>
 *   <li>删除任务实例日志</li>
 *   <li>支持远程日志访问</li>
 * </ul>
 */
@RpcService
public interface ILogService {

    /**
     * 获取任务实例完整日志文件字节
     * 下载指定任务实例的全部日志内容
     *
     * @param taskInstanceLogFileDownloadRequest 日志文件下载请求，包含任务实例信息
     * @return 日志文件下载响应，包含日志内容的字节数组
     */
    @RpcMethod
    TaskInstanceLogFileDownloadResponse getTaskInstanceWholeLogFileBytes(TaskInstanceLogFileDownloadRequest taskInstanceLogFileDownloadRequest);

    /**
     * 分页查询任务实例日志
     * 按页获取任务实例的日志内容，支持大文件日志的分段加载
     *
     * @param taskInstanceLogPageQueryRequest 分页查询请求，包含页码和每页行数
     * @return 分页查询响应，包含当前页的日志内容
     */
    @RpcMethod
    TaskInstanceLogPageQueryResponse pageQueryTaskInstanceLog(TaskInstanceLogPageQueryRequest taskInstanceLogPageQueryRequest);

    /**
     * 删除任务实例日志
     * 从文件系统中删除指定的任务日志文件
     *
     * @param taskInstanceLogAbsolutePath 任务实例日志的绝对路径
     */
    @RpcMethod
    void removeTaskInstanceLog(String taskInstanceLogAbsolutePath);

}
