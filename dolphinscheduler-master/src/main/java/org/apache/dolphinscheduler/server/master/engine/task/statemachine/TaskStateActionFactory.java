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

package org.apache.dolphinscheduler.server.master.engine.task.statemachine;

import org.apache.dolphinscheduler.plugin.task.api.enums.TaskExecutionStatus;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

/**
 * 任务状态动作工厂
 * 
 * 这是任务状态机的核心组件，负责管理和分发不同任务状态对应的处理动作。
 * 类比：就像一个智能的工序处理调度中心，根据工序状态自动分配对应的处理程序。
 * 
 * 核心职责：
 * 1. 动作注册：收集和注册所有可能的任务状态处理动作
 * 2. 状态映射：建立任务状态与处理动作之间的映射关系
 * 3. 动作分发：根据任务状态快速定位并返回对应的处理动作
 * 4. 完整性验证：确保每个任务状态都有对应的处理动作
 * 5. 异常处理：处理未知状态或缺失动作的异常情况
 * 
 * 设计模式应用：
 * - 工厂模式：根据状态类型创建对应的处理动作
 * - 策略模式：不同状态对应不同的处理策略
 * - 注册模式：通过Spring自动注册所有状态动作实现
 * - 单例模式：作为Spring组件保证全局唯一性
 * 
 * 初始化过程：
 * 1. Spring注入：通过构造函数注入所有ITaskStateAction实现类
 * 2. 映射构建：遍历所有动作，根据matchState()方法建立状态映射
 * 3. 完整性检查：验证每个TaskExecutionStatus都有对应的动作
 * 4. 异常预检：提前发现缺失的状态处理动作，避免运行时错误
 * 
 * 状态动作类型：
 * - 提交动作：处理任务提交状态的逻辑
 * - 运行动作：处理任务运行状态的逻辑
 * - 成功动作：处理任务成功完成的逻辑
 * - 失败动作：处理任务执行失败的逻辑
 * - 暂停动作：处理任务暂停的逻辑
 * - 终止动作：处理任务被终止的逻辑
 * - 重试动作：处理任务重试的逻辑
 * - 故障转移动作：处理任务故障转移的逻辑
 * 
 * 类比理解：
 * 就像医院的分诊台系统：
 * - 不同病情对应不同科室（状态对应动作）
 * - 分诊护士根据症状分配科室（工厂根据状态分发动作）
 * - 确保每种病情都有对应科室（完整性验证）
 * - 处理特殊情况和转诊（异常处理）
 */
@Component
public class TaskStateActionFactory {

    /**
     * 任务状态与动作的映射表
     * 
     * 存储每个任务执行状态对应的处理动作，实现O(1)时间复杂度的快速查找。
     * Key是TaskExecutionStatus枚举值，Value是对应的状态处理动作实现。
     * 类比：医院分诊台的科室对照表，快速匹配病情和科室。
     */
    private final Map<TaskExecutionStatus, ITaskStateAction> taskStateActionMap = new HashMap<>();

    /**
     * 构造函数 - 初始化状态动作映射
     * 
     * 通过Spring的依赖注入机制获取所有ITaskStateAction的实现类，
     * 然后建立状态与动作的映射关系，并进行完整性验证。
     * 
     * 初始化流程：
     * 1. 动作收集：Spring自动注入所有状态动作实现类
     * 2. 映射构建：遍历每个动作，调用matchState()获取对应状态
     * 3. 映射存储：将状态-动作对存储到HashMap中
     * 4. 完整性验证：检查每个TaskExecutionStatus枚举值都有对应动作
     * 
     * 设计优势：
     * - 自动发现：利用Spring的IoC容器自动发现所有动作实现
     * - 类型安全：编译时确保状态和动作的类型匹配
     * - 快速查找：HashMap提供O(1)的查找性能
     * - 完整性保证：启动时验证确保没有遗漏的状态处理
     * 
     * @param taskStateActions Spring注入的所有任务状态动作实现列表
     * @throws IllegalArgumentException 如果某个状态没有对应的动作实现
     * 
     * 类比：就像医院开业前的科室配置过程：
     * - 统计所有医生专业（收集状态动作）
     * - 建立病情-科室对照表（构建映射关系）
     * - 检查科室覆盖完整性（完整性验证）
     */
    public TaskStateActionFactory(List<ITaskStateAction> taskStateActions) {
        taskStateActions.forEach(
                taskStateAction -> taskStateActionMap.put(taskStateAction.matchState(), taskStateAction));
        Arrays.stream(TaskExecutionStatus.values()).forEach(this::getTaskStateAction);
    }

    /**
     * 获取指定状态的处理动作
     * 
     * 根据任务执行状态快速查找并返回对应的状态处理动作。
     * 这是状态机处理任务状态变化时的核心调用方法。
     * 
     * 查找逻辑：
     * 1. 状态查找：在映射表中查找指定状态对应的动作
     * 2. 存在性验证：检查动作是否存在，不存在则抛出异常
     * 3. 动作返回：返回找到的状态处理动作实例
     * 
     * 异常处理：
     * - 如果指定状态没有对应的动作，抛出IllegalArgumentException
     * - 这种情况通常表示系统配置不完整或新增状态未实现动作
     * 
     * 性能特点：
     * - O(1)查找：HashMap提供常数时间复杂度的查找性能
     * - 无锁访问：只读操作，支持高并发访问
     * - 内存友好：所有动作实例在启动时创建并复用
     * 
     * @param taskExecutionStatus 任务执行状态
     * @return 对应的状态处理动作实例
     * @throws IllegalArgumentException 当指定状态没有对应的处理动作时
     * 
     * 类比：就像患者到分诊台报告病情，分诊护士根据症状表快速查找
     * 对应的科室，如果找不到合适的科室就需要特殊处理。
     */
    public ITaskStateAction getTaskStateAction(final TaskExecutionStatus taskExecutionStatus) {
        final ITaskStateAction taskStateAction = taskStateActionMap.get(taskExecutionStatus);
        if (taskStateAction == null) {
            throw new IllegalArgumentException("Cannot find TaskStateAction for state: " + taskExecutionStatus);
        }
        return taskStateAction;
    }

}
