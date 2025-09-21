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

package org.apache.dolphinscheduler.server.worker.executor;

import org.apache.dolphinscheduler.task.executor.TaskExecutorRepository;

import org.springframework.stereotype.Component;

/**
 * 物理任务执行器仓库
 *
 * 专门用于存储和管理Worker节点上所有正在执行的物理任务执行器实例。
 * 类比：仓库管理员，负责记录和管理所有在库物品的位置和状态。
 *
 * 主要功能：
 * 1. 注册新的物理任务执行器实例
 * 2. 根据任务ID查找对应的执行器实例
 * 3. 移除已完成或取消的任务执行器
 * 4. 提供所有活跃任务执行器的查询接口
 * 5. 支持任务执行器的状态统计和监控
 *
 * 存储特点：
 * - 使用任务实例ID作为唯一标识符
 * - 提供线程安全的并发访问
 * - 支持快速查找和删除操作
 * - 内存存储，重启后清空
 *
 * 生命周期管理：
 * - 任务开始时注册执行器
 * - 任务执行期间可查询状态
 * - 任务完成后自动清理
 * - 支持强制清理异常任务
 *
 * 使用场景：
 * - 任务生命周期事件处理
 * - 任务状态查询和监控
 * - 任务取消和清理操作
 * - Worker节点健康检查
 *
 * 架构角色：
 * - 继承父类TaskExecutorRepository的通用仓库功能
 * - 专门适配物理任务执行器的存储需求
 * - 作为任务管理的核心数据结构
 */
@Component
public class PhysicalTaskExecutorRepository extends TaskExecutorRepository {

}
