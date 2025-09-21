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
import org.apache.dolphinscheduler.server.master.exception.LogicTaskFactoryNotFoundException;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import lombok.extern.slf4j.Slf4j;

import org.springframework.stereotype.Component;

/**
 * 逻辑任务插件工厂构建器
 * 负责管理和查找各种类型的逻辑任务插件工厂
 *
 * 该类的设计目的：
 * 1. 统一管理所有逻辑任务类型的工厂实例
 * 2. 提供根据任务类型快速查找对应工厂的能力
 * 3. 支持插件化的任务类型扩展机制
 * 4. 确保工厂实例的线程安全访问
 *
 * 工作原理：
 * - 通过Spring依赖注入收集所有逻辑任务工厂实现
 * - 将工厂按任务类型存储在线程安全的Map中
 * - 运行时根据任务类型快速定位对应工厂
 *
 * @author DolphinScheduler Team
 */
@Slf4j
@Component
public class LogicTaskPluginFactoryBuilder {

    /**
     * 逻辑任务工厂映射表
     * Key: 任务类型标识符（如"CONDITIONS"、"DEPENDENT"等）
     * Value: 对应的工厂实例
     * 使用ConcurrentHashMap保证线程安全
     */
    private final Map<String, ILogicTaskPluginFactory<? extends ILogicTask<? extends AbstractParameters>>> logicTaskPluginFactoryMap =
            new ConcurrentHashMap<>();

    /**
     * 构造函数 - 初始化工厂构建器
     * 通过Spring依赖注入获取所有逻辑任务工厂实现，并建立任务类型到工厂的映射关系
     *
     * @param logicTaskPluginFactories 所有逻辑任务工厂实现列表，由Spring自动注入
     */
    public LogicTaskPluginFactoryBuilder(List<ILogicTaskPluginFactory<? extends ILogicTask<? extends AbstractParameters>>> logicTaskPluginFactories) {
        // 遍历所有工厂实现，建立任务类型到工厂的映射
        logicTaskPluginFactories.forEach(
                logicTaskPluginFactory -> {
                    String taskType = logicTaskPluginFactory.getTaskType();
                    logicTaskPluginFactoryMap.put(taskType, logicTaskPluginFactory);
                    log.debug("Registered logic task factory for type: {}", taskType);
                });
        log.info("Logic task plugin factory builder initialized with {} factories", logicTaskPluginFactories.size());
    }

    /**
     * 根据任务类型创建对应的逻辑任务工厂
     * 这是工厂构建器的核心方法，用于获取特定任务类型的工厂实例
     *
     * @param taskType 任务类型标识符（如"CONDITIONS"、"DEPENDENT"、"SUB_PROCESS"、"SWITCH"等）
     * @return 对应的逻辑任务工厂实例
     * @throws LogicTaskFactoryNotFoundException 当找不到对应任务类型的工厂时抛出异常
     */
    public ILogicTaskPluginFactory<? extends ILogicTask<? extends AbstractParameters>> createILogicTaskPluginFactory(String taskType) throws LogicTaskFactoryNotFoundException {
        // 从映射表中查找对应的工厂实例
        ILogicTaskPluginFactory<? extends ILogicTask<? extends AbstractParameters>> logicTaskPluginFactory =
                logicTaskPluginFactoryMap.get(taskType);

        // 如果未找到对应的工厂，抛出异常
        if (logicTaskPluginFactory == null) {
            log.error("Cannot find logic task factory for task type: {}, available types: {}",
                    taskType, logicTaskPluginFactoryMap.keySet());
            throw new LogicTaskFactoryNotFoundException("Cannot find the logic task factory: " + taskType);
        }

        log.debug("Found logic task factory for type: {}", taskType);
        return logicTaskPluginFactory;
    }

}
