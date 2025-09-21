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

/**
 * 任务参数反序列化器接口
 * 负责将任务定义中的JSON格式参数字符串反序列化为具体的参数对象
 *
 * 设计目的：
 * 1. 提供统一的参数反序列化规范
 * 2. 支持不同任务类型的自定义参数结构
 * 3. 将JSON解析逻辑与任务执行逻辑解耦
 * 4. 便于参数验证和类型安全
 *
 * 使用场景：
 * - 任务创建时解析用户配置的参数
 * - 任务重启时恢复参数状态
 * - 任务参数校验和预处理
 *
 * 实现类通常会：
 * - 使用JSON库（如Jackson、Gson）进行反序列化
 * - 进行参数有效性验证
 * - 设置参数默认值
 * - 处理参数格式兼容性
 *
 * @param <T> 反序列化后的参数对象类型，通常继承自AbstractParameters
 *
 * @author DolphinScheduler Team
 */
public interface ITaskParameterDeserializer<T> {

    /**
     * 将JSON格式的任务参数字符串反序列化为参数对象
     *
     * 该方法的职责：
     * 1. 解析JSON字符串为Java对象
     * 2. 验证参数的合法性和完整性
     * 3. 处理参数的默认值和兼容性
     * 4. 返回类型安全的参数对象
     *
     * @param taskParamsJson JSON格式的任务参数字符串，来自任务定义
     * @return 反序列化后的参数对象实例
     * @throws IllegalArgumentException 当参数格式不正确或验证失败时抛出
     * @throws NullPointerException 当输入参数为null时抛出
     */
    T deserialize(String taskParamsJson);

}
