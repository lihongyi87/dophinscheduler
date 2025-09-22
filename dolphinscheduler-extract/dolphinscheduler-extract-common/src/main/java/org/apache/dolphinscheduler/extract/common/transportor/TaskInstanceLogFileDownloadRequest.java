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

package org.apache.dolphinscheduler.extract.common.transportor;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 任务实例日志文件下载请求
 *
 * 用于封装任务实例日志文件下载的请求参数，支持通过任务实例ID或文件绝对路径下载日志。
 * 这是日志服务RPC调用中的请求数据传输对象。
 * 类比：一个图书馆的借书申请单，包含要借阅的图书编号和具体位置信息。
 *
 * 主要用途：
 * 1. 标识要下载的任务实例日志
 * 2. 提供日志文件的定位信息
 * 3. 作为RPC调用的参数传递
 * 4. 支持日志访问的权限验证
 *
 * 使用场景：
 * - 用户通过Web界面下载任务执行日志
 * - 系统自动备份或传输日志文件
 * - 运维人员远程获取任务执行详情
 * - 日志分析工具获取原始数据
 *
 * 设计特点：
 * - 双重定位：既支持任务ID查找，也支持直接路径访问
 * - 简单高效：只包含必要的定位信息
 * - 序列化友好：支持RPC框架的序列化传输
 * - 类型安全：使用强类型避免参数错误
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TaskInstanceLogFileDownloadRequest {

    /**
     * 任务实例ID
     *
     * 用于唯一标识一个任务实例的数字ID。
     * 这是数据库中task_instance表的主键，用于关联具体的任务执行记录。
     *
     * 作用：
     * - 唯一标识任务实例
     * - 关联任务执行信息
     * - 用于权限验证和访问控制
     * - 支持日志文件路径的动态生成
     *
     * 使用方式：
     * - 通过任务ID查找对应的日志文件
     * - 验证用户是否有权限访问该任务的日志
     * - 生成标准化的日志文件路径
     * - 记录访问日志和审计信息
     *
     * 注意事项：
     * - 必须是有效的任务实例ID
     * - 对应的任务实例必须存在
     * - 需要检查任务的执行状态
     * - 考虑任务的权限和可见性
     */
    private long taskInstanceId;

    /**
     * 任务实例日志文件绝对路径
     *
     * 指定要下载的日志文件在服务器文件系统中的完整路径。
     * 这是一个可选的参数，当提供时可以直接访问指定路径的文件。
     *
     * 路径格式：
     * - 必须是绝对路径（以/或盘符开头）
     * - 通常指向日志存储目录下的具体文件
     * - 文件名通常包含任务ID和时间戳信息
     * - 支持标准的文件系统路径格式
     *
     * 安全考虑：
     * - 需要验证路径的合法性，防止路径遍历攻击
     * - 确保路径在允许的日志目录范围内
     * - 检查文件是否存在且有读取权限
     * - 记录文件访问操作用于安全审计
     *
     * 使用场景：
     * - 系统已知确切的日志文件路径
     * - 绕过路径查找逻辑直接访问文件
     * - 访问特殊位置的日志文件
     * - 提高访问效率避免路径计算
     *
     * 示例路径：
     * - Linux: /opt/dolphinscheduler/logs/task/20231201/12345.log
     * - Windows: D:\dolphinscheduler\logs\task\20231201\12345.log
     *
     * 与taskInstanceId的关系：
     * - 两个参数可以同时提供，用于双重验证
     * - 当只提供路径时，可能需要从路径中解析任务ID
     * - 优先使用明确的路径，避免路径计算的开销
     * - 路径和ID不匹配时应该报错或警告
     */
    private String taskInstanceLogAbsolutePath;

}
