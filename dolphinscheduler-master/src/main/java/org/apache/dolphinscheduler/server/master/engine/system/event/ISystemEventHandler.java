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

package org.apache.dolphinscheduler.server.master.engine.system.event;

/**
 * 系统事件处理器接口
 *
 * 定义了处理系统事件的标准接口，所有具体的系统事件处理器都必须实现此接口。
 * 这是系统事件处理机制中的核心抽象，采用策略模式设计，确保不同类型的
 * 系统事件能够被分发到对应的处理器进行处理。
 *
 * 类比：企业中各个部门的专业处理人员，每个人员都有自己擅长处理的业务类型，
 *      当收到对应类型的工作任务时，就会按照标准流程进行处理。
 *
 * 设计模式：
 * - 策略模式：不同的处理器实现不同的处理策略
 * - 泛型设计：确保类型安全，避免类型转换错误
 * - 接口隔离：只定义必要的方法，保持接口简洁
 *
 * 实现规范：
 * 1. 每个处理器只处理一种特定类型的系统事件
 * 2. 处理器必须是无状态的，确保线程安全
 * 3. 处理逻辑应该是幂等的，支持重复执行
 * 4. 异常处理应该优雅，不能影响其他事件的处理
 *
 * 现有实现：
 * - {@link GlobalMasterFailoverEventHandler} - 全局Master故障转移处理器
 * - {@link MasterFailoverEventHandler} - 单个Master故障转移处理器
 * - {@link WorkerFailoverEventHandler} - Worker故障转移处理器
 *
 * 扩展说明：
 * 如果需要增加新的系统事件类型，只需要：
 * 1. 在SystemEventType枚举中添加新的事件类型
 * 2. 创建对应的事件类继承AbstractSystemEvent
 * 3. 实现ISystemEventHandler接口处理新事件
 * 4. 将处理器注册为Spring组件
 *
 * @param <T> 处理器能够处理的系统事件类型，必须继承自AbstractSystemEvent
 */
public interface ISystemEventHandler<T extends AbstractSystemEvent> {

    /**
     * 处理系统事件
     *
     * 这是事件处理的核心方法，实现具体的业务逻辑来处理传入的系统事件。
     * 方法应该是无状态和幂等的，确保多次调用不会产生副作用。
     *
     * 实现要求：
     * 1. 处理逻辑必须是幂等的，重复执行不会造成问题
     * 2. 异常情况要妥善处理，记录必要的日志信息
     * 3. 不能长时间阻塞，影响其他事件的处理
     * 4. 如果处理失败，应该抛出异常以便重试机制生效
     *
     * 类比：收到工作任务后，按照标准操作流程完成具体的业务处理，
     *      如果处理过程中遇到问题，要么妥善解决，要么上报给上级处理。
     *
     * @param systemEvent 要处理的系统事件实例，不会为null
     * @throws Exception 如果处理过程中发生错误，可以抛出异常触发重试机制
     */
    void handle(final T systemEvent);

    /**
     * 匹配处理器能够处理的事件类型
     *
     * 返回当前处理器能够处理的系统事件类型，用于SystemEventBusFireWorker
     * 进行事件分发时的匹配判断。每个处理器只应该处理一种类型的事件。
     *
     * 这是策略模式中策略选择的关键方法，确保事件能够被正确分发到
     * 对应的处理器。处理器注册后，系统会自动根据这个方法的返回值
     * 进行事件类型匹配。
     *
     * 类比：部门招聘时说明自己擅长处理的业务类型，这样HR就知道
     *      什么样的工作任务应该分配给哪个部门。
     *
     * 实现注意：
     * - 返回值必须与泛型参数T对应的事件类型一致
     * - 每个处理器只能返回一种事件类型
     * - 不应该返回null值
     *
     * @return 当前处理器能够处理的系统事件类型
     */
    SystemEventType matchState();
}
