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
 * 日志服务RPC接口
 *
 * 这是DolphinScheduler分布式日志管理的核心RPC服务接口。
 * 它提供了任务实例日志的远程访问、查询和管理功能，支持分布式环境下的日志操作。
 * 类比：一个分布式图书馆的日志查阅服务，支持远程查找、阅读和管理各种日志记录。
 *
 * 主要功能：
 * 1. 日志文件下载：获取任务实例的完整日志文件内容
 * 2. 日志分页查询：支持大日志文件的分页浏览
 * 3. 日志清理：删除不再需要的日志文件
 *
 * 使用场景：
 * - Master节点查询Worker节点上的任务执行日志
 * - Web界面展示任务执行的详细日志信息
 * - 运维人员远程查看和下载日志文件
 * - 系统自动清理过期的日志文件
 *
 * 设计特点：
 * - 跨节点访问：支持从任意节点访问其他节点的日志
 * - 大文件支持：通过分页查询处理大型日志文件
 * - 安全可靠：通过RPC框架确保日志访问的安全性
 * - 资源管理：提供日志清理功能避免磁盘空间耗尽
 *
 * 实现要求：
 * - 实现类需要添加@RpcService注解进行服务注册
 * - 方法需要添加@RpcMethod注解支持远程调用
 * - 需要处理文件不存在、权限不足等异常情况
 * - 支持并发访问和大文件处理
 */
@RpcService
public interface ILogService {

    /**
     * 获取任务实例完整日志文件内容
     *
     * 下载指定任务实例的完整日志文件，以字节数组形式返回文件内容。
     * 适用于小到中型日志文件的完整下载场景。
     *
     * 功能特点：
     * - 一次性获取完整日志内容
     * - 支持二进制安全传输
     * - 自动处理文件编码和格式
     * - 包含文件元信息（大小、修改时间等）
     *
     * 使用场景：
     * - 用户需要下载完整的任务执行日志
     * - 系统需要将日志文件传输到其他节点
     * - 日志分析工具需要完整的日志数据
     * - 调试时需要查看完整的执行过程
     *
     * 注意事项：
     * - 大文件可能导致内存溢出，建议先检查文件大小
     * - 网络传输时间与文件大小成正比
     * - 并发下载可能影响系统性能
     * - 需要确保目标文件存在且可读
     *
     * @param taskInstanceLogFileDownloadRequest 日志文件下载请求，包含文件路径和相关参数
     * @return 日志文件下载响应，包含文件内容和元信息
     */
    @RpcMethod
    TaskInstanceLogFileDownloadResponse getTaskInstanceWholeLogFileBytes(TaskInstanceLogFileDownloadRequest taskInstanceLogFileDownloadRequest);

    /**
     * 分页查询任务实例日志
     *
     * 对大型日志文件进行分页查询，返回指定页面的日志内容。
     * 这是处理大型日志文件的推荐方式，避免一次性加载整个文件。
     *
     * 功能特点：
     * - 支持按行数或字节数分页
     * - 可指定查询的起始位置和数量
     * - 返回当前页内容和分页元信息
     * - 支持正向和反向查询（从文件头或文件尾开始）
     *
     * 使用场景：
     * - Web界面分页展示任务日志
     * - 大日志文件的分段传输
     * - 日志搜索和过滤功能
     * - 实时日志监控和追踪
     *
     * 分页策略：
     * - 行分页：按日志行数进行分页，适合查看结构化日志
     * - 字节分页：按字节数分页，适合处理非结构化日志
     * - 时间分页：按时间范围分页，适合长时间运行的任务
     *
     * 性能优化：
     * - 缓存常用页面内容
     * - 支持异步加载和预加载
     * - 合理设置页面大小避免性能问题
     *
     * @param taskInstanceLogPageQueryRequest 日志分页查询请求，包含分页参数和查询条件
     * @return 日志分页查询响应，包含当前页内容和分页信息
     */
    @RpcMethod
    TaskInstanceLogPageQueryResponse pageQueryTaskInstanceLog(TaskInstanceLogPageQueryRequest taskInstanceLogPageQueryRequest);

    /**
     * 删除任务实例日志文件
     *
     * 删除指定路径的任务实例日志文件，用于日志清理和磁盘空间管理。
     * 这是一个危险操作，需要谨慎使用，建议有适当的权限控制。
     *
     * 功能特点：
     * - 物理删除指定的日志文件
     * - 支持绝对路径删除
     * - 同步操作，删除完成后立即返回
     * - 不可恢复操作，需要确认后执行
     *
     * 使用场景：
     * - 系统自动清理过期日志文件
     * - 用户手动删除不需要的日志
     * - 磁盘空间不足时的紧急清理
     * - 数据隐私要求的日志销毁
     *
     * 安全考虑：
     * - 验证路径合法性，防止路径遍历攻击
     * - 检查文件权限，确保有删除权限
     * - 记录删除操作日志，便于审计
     * - 考虑添加删除确认机制
     *
     * 清理策略：
     * - 按时间策略：删除超过指定时间的日志
     * - 按大小策略：当磁盘使用率过高时删除旧日志
     * - 按任务状态：删除已完成任务的日志
     * - 手动清理：用户指定删除特定日志
     *
     * 异常处理：
     * - 文件不存在：静默处理或返回警告
     * - 权限不足：抛出权限异常
     * - 文件被占用：等待或强制删除
     * - 磁盘错误：记录错误并重试
     *
     * @param taskInstanceLogAbsolutePath 要删除的日志文件绝对路径
     */
    @RpcMethod
    void removeTaskInstanceLog(String taskInstanceLogAbsolutePath);

}
