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

package org.apache.dolphinscheduler.server.master.engine.executor.plugin;

import org.apache.dolphinscheduler.plugin.task.api.parameters.AbstractParameters;
import org.apache.dolphinscheduler.server.master.exception.LogicTaskInitializeException;
import org.apache.dolphinscheduler.task.executor.ITaskExecutor;

/**
 * 逻辑任务插件工厂接口
 * 负责创建特定类型的逻辑任务实例
 *
 * 该工厂模式的设计目的：
 * 1. 解耦任务创建逻辑和任务执行逻辑
 * 2. 支持运行时动态注册不同类型的逻辑任务
 * 3. 为每种任务类型提供统一的创建入口
 * 4. 便于任务类型的扩展和插件化管理
 *
 * 工厂实现类通常会：
 * - 验证任务参数的有效性
 * - 初始化任务执行所需的资源和依赖
 * - 返回配置好的任务实例
 *
 * @param <T> 逻辑任务类型，必须实现ILogicTask接口
 *
 * @author DolphinScheduler Team
 */
public interface ILogicTaskPluginFactory<T extends ILogicTask<? extends AbstractParameters>> {

    /**
     * 创建逻辑任务实例
     * 根据提供的任务执行器信息创建对应的逻辑任务实例
     *
     * 创建过程包括：
     * 1. 从taskExecutor中提取任务定义信息
     * 2. 解析和验证任务参数
     * 3. 初始化任务执行环境
     * 4. 返回准备就绪的任务实例
     *
     * @param taskExecutor 任务执行器，包含任务定义和运行时上下文信息
     * @return 创建好的逻辑任务实例
     * @throws LogicTaskInitializeException 当任务初始化失败时抛出异常
     */
    T createLogicTask(final ITaskExecutor taskExecutor) throws LogicTaskInitializeException;

    /**
     * 获取支持的任务类型
     * 返回该工厂能够创建的任务类型标识符
     *
     * 任务类型标识符用于：
     * - 任务类型路由和分发
     * - 工厂注册和查找
     * - 任务定义验证
     *
     * @return 任务类型字符串，如"CONDITIONS"、"DEPENDENT"、"SUB_PROCESS"等
     */
    String getTaskType();

}
