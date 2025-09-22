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

package org.apache.dolphinscheduler.server.master.engine.task.runnable;

import org.apache.dolphinscheduler.dao.entity.TaskDefinition;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.dao.entity.WorkflowInstance;
import org.apache.dolphinscheduler.dao.mapper.EnvironmentMapper;
import org.apache.dolphinscheduler.dao.utils.EnvironmentUtils;
import org.apache.dolphinscheduler.dao.utils.WorkerGroupUtils;

import org.springframework.beans.factory.annotation.Autowired;

/**
 * 抽象任务实例工厂
 *
 * 提供任务实例创建的通用逻辑和基础功能，是所有具体任务实例工厂的基类。
 * 实现了公共的任务实例操作方法，减少具体工厂类的重复代码。
 *
 * 设计模式：
 * - 模板方法模式：定义创建任务实例的通用流程，具体实现由子类完成
 * - 继承模式：提供公共功能的基础实现，子类继承并扩展
 * - 泛型设计：支持不同类型的Builder，保证类型安全
 *
 * 核心功能：
 * 1. 任务实例克隆：提供完整的任务实例深度复制功能
 * 2. 元数据注入：从任务定义和工作流实例注入元数据
 * 3. 环境配置：处理环境相关的配置和映射
 * 4. 通用工具：提供子类共用的工具方法
 *
 * 子类职责：
 * - 实现具体的builder()方法，返回对应的建造者
 * - 实现具体的createTaskInstance()方法，处理特定场景的创建逻辑
 * - 根据需要重写或扩展基类提供的通用方法
 *
 * 使用场景：
 * - 首次运行：创建全新任务实例
 * - 重试执行：基于失败任务创建重试实例
 * - 故障转移：创建故障转移任务实例
 * - 恢复执行：创建各种恢复场景的任务实例
 *
 * 类比理解：
 * 就像一个标准化的产品生产基地，提供通用的生产设施和流程：
 * - 标准厂房：抽象工厂提供的基础设施
 * - 通用工具：克隆、注入等通用方法
 * - 专业生产线：具体的子类工厂
 * - 质量标准：统一的产品（任务实例）规格
 *
 * @param <BUILDER> 建造者类型，必须实现ITaskInstanceBuilder接口
 */
public abstract class AbstractTaskInstanceFactory<BUILDER extends ITaskInstanceFactory.ITaskInstanceBuilder>
        implements
            ITaskInstanceFactory<BUILDER> {

    /**
     * 环境映射器
     *
     * 用于处理任务执行环境的配置和映射。提供环境相关的数据访问和转换功能，
     * 确保任务实例能够获取正确的环境配置信息。
     */
    @Autowired
    protected EnvironmentMapper environmentMapper;

    /**
     * 克隆任务实例
     *
     * 创建原任务实例的完整副本，保留所有属性和配置信息。这是重试、故障转移等
     * 场景中创建新任务实例的基础方法。
     *
     * 克隆策略：
     * - 深度复制：复制所有字段值，避免引用共享
     * - 完整性：保留原实例的所有配置和状态信息
     * - 独立性：克隆的实例与原实例完全独立
     * - 一致性：确保克隆后的实例数据一致性
     *
     * 复制的信息包括：
     * - 基本标识：ID、名称、类型、代码等
     * - 层次关系：工作流实例、项目等层次信息
     * - 执行信息：时间、状态、主机、路径等
     * - 配置信息：参数、重试、优先级等
     * - 资源信息：Worker组、环境、资源配额等
     * - 扩展信息：标志、链接、执行器等
     *
     * 注意事项：
     * - 克隆后的实例通常需要重新设置某些字段（如ID、状态）
     * - 时间字段可能需要根据具体场景进行调整
     * - 某些字段可能需要在子类中进行特殊处理
     *
     * 类比：就像复印一份完整的档案，保留所有原始信息，
     * 然后可以在副本上进行必要的修改。
     *
     * @param originTaskInstance 原始任务实例
     * @return 克隆的任务实例
     */
    protected TaskInstance cloneTaskInstance(TaskInstance originTaskInstance) {
        final TaskInstance result = new TaskInstance();
        result.setId(originTaskInstance.getId());
        result.setName(originTaskInstance.getName());
        result.setTaskType(originTaskInstance.getTaskType());
        result.setWorkflowInstanceId(originTaskInstance.getWorkflowInstanceId());
        result.setWorkflowInstanceName(originTaskInstance.getWorkflowInstanceName());
        result.setProjectCode(originTaskInstance.getProjectCode());
        result.setTaskCode(originTaskInstance.getTaskCode());
        result.setTaskDefinitionVersion(originTaskInstance.getTaskDefinitionVersion());
        result.setState(originTaskInstance.getState());
        result.setFirstSubmitTime(originTaskInstance.getFirstSubmitTime());
        result.setSubmitTime(originTaskInstance.getSubmitTime());
        result.setStartTime(originTaskInstance.getStartTime());
        result.setEndTime(originTaskInstance.getEndTime());
        result.setHost(originTaskInstance.getHost());
        result.setExecutePath(originTaskInstance.getExecutePath());
        result.setLogPath(originTaskInstance.getLogPath());
        result.setRetryTimes(originTaskInstance.getRetryTimes());
        result.setAlertFlag(originTaskInstance.getAlertFlag());
        result.setPid(originTaskInstance.getPid());
        result.setAppLink(originTaskInstance.getAppLink());
        result.setFlag(originTaskInstance.getFlag());
        result.setMaxRetryTimes(originTaskInstance.getMaxRetryTimes());
        result.setRetryInterval(originTaskInstance.getRetryInterval());
        result.setTaskInstancePriority(originTaskInstance.getTaskInstancePriority());
        result.setWorkerGroup(originTaskInstance.getWorkerGroup());
        result.setEnvironmentCode(originTaskInstance.getEnvironmentCode());
        result.setExecutorId(originTaskInstance.getExecutorId());
        result.setVarPool(originTaskInstance.getVarPool());
        result.setExecutorName(originTaskInstance.getExecutorName());
        result.setDelayTime(originTaskInstance.getDelayTime());
        result.setTaskParams(originTaskInstance.getTaskParams());
        result.setDryRun(originTaskInstance.getDryRun());
        result.setTaskGroupId(originTaskInstance.getTaskGroupId());
        result.setCpuQuota(originTaskInstance.getCpuQuota());
        result.setMemoryMax(originTaskInstance.getMemoryMax());
        result.setTaskExecuteType(originTaskInstance.getTaskExecuteType());
        return result;
    }

    /**
     * 从任务定义注入元数据
     *
     * 将任务定义中的静态配置信息注入到任务实例中，确保任务实例继承任务定义的基础配置。
     * 这是任务实例初始化过程中的关键步骤，建立了任务定义与实例之间的数据关联。
     *
     * 注入策略：
     * - 基础信息：名称、类型、代码、版本等标识信息
     * - 重试配置：最大重试次数、重试间隔等容错设置
     * - 优先级配置：任务实例优先级设置
     * - 资源配置：Worker组、环境、资源配额等执行环境
     * - 调度配置：延迟时间、任务组等调度参数
     * - 参数配置：任务参数、执行类型等功能参数
     *
     * 优先级处理：
     * - 对于Worker组和环境配置，使用"实例优先，定义补充"的策略
     * - 如果任务实例已有配置则保留，否则使用任务定义的配置
     * - 通过工具类确保配置的有效性和一致性
     *
     * 配置映射：
     * - 任务名称 ← 任务定义名称
     * - 任务类型 ← 任务定义类型
     * - 任务代码 ← 任务定义代码
     * - 版本信息 ← 任务定义版本
     * - 重试设置 ← 任务定义失败重试配置
     * - 优先级 ← 任务定义任务优先级
     * - 资源配置 ← 任务定义资源配置
     * - 执行参数 ← 任务定义参数配置
     *
     * 使用场景：
     * - 创建新任务实例时的基础配置设置
     * - 重试任务实例时的配置继承
     * - 故障转移时的配置复制
     * - 确保任务实例与定义的一致性
     *
     * 类比理解：
     * 就像从产品规格书复制配置到生产订单，确保每个产品实例都按照
     * 标准规格进行生产，同时允许订单特殊要求优先。
     *
     * @param taskInstance 目标任务实例，将被注入元数据
     * @param taskDefinition 源任务定义，提供元数据配置
     */
    protected void injectMetadataFromTaskDefinition(TaskInstance taskInstance, TaskDefinition taskDefinition) {
        taskInstance.setName(taskDefinition.getName());
        taskInstance.setTaskType(taskDefinition.getTaskType());
        taskInstance.setTaskCode(taskDefinition.getCode());
        taskInstance.setTaskDefinitionVersion(taskDefinition.getVersion());
        taskInstance.setMaxRetryTimes(taskDefinition.getFailRetryTimes());
        taskInstance.setRetryInterval(taskDefinition.getFailRetryInterval());
        taskInstance.setTaskInstancePriority(taskDefinition.getTaskPriority());
        taskInstance.setWorkerGroup(
                WorkerGroupUtils.getWorkerGroupOrDefault(
                        taskInstance.getWorkerGroup(), taskDefinition.getWorkerGroup()));
        taskInstance.setEnvironmentCode(
                EnvironmentUtils.getEnvironmentCodeOrDefault(
                        taskInstance.getEnvironmentCode(), taskDefinition.getEnvironmentCode()));
        taskInstance.setDelayTime(taskDefinition.getDelayTime());
        taskInstance.setTaskParams(taskDefinition.getTaskParams());
        taskInstance.setTaskGroupId(taskDefinition.getTaskGroupId());
        taskInstance.setCpuQuota(taskDefinition.getCpuQuota());
        taskInstance.setMemoryMax(taskDefinition.getMemoryMax());
        taskInstance.setTaskExecuteType(taskDefinition.getTaskExecuteType());
    }

    /**
     * 从工作流实例注入元数据
     *
     * 将工作流实例的运行时信息注入到任务实例中，建立任务与工作流执行上下文的关联。
     * 这确保了任务实例能够继承工作流级别的配置和状态信息。
     *
     * 注入策略：
     * - 层次关系：工作流实例ID、名称等层次标识
     * - 项目信息：项目代码等归属信息
     * - 执行环境：Worker组、环境代码等运行环境
     * - 执行者信息：执行器ID、执行器名称等责任信息
     * - 运行参数：变量池、干运行模式等运行时配置
     *
     * 优先级策略：
     * - Worker组和环境配置采用"任务优先，工作流补充"原则
     * - 如果任务实例已配置则保留，否则继承工作流实例配置
     * - 通过工具类确保配置的合法性和默认值处理
     *
     * 继承关系：
     * - 工作流实例ID ← 确定任务的归属工作流
     * - 工作流实例名称 ← 用于日志和追踪
     * - 项目代码 ← 确定任务的项目归属
     * - Worker组配置 ← 继承工作流的执行资源配置
     * - 环境配置 ← 继承工作流的环境设置
     * - 执行者信息 ← 继承工作流的责任人信息
     * - 变量池 ← 继承工作流级别的变量和参数
     * - 运行模式 ← 继承干运行等执行模式
     *
     * 运行时上下文：
     * - 变量池提供工作流级别的参数传递
     * - 执行器信息确保责任追踪
     * - 干运行模式支持测试和验证
     * - 环境配置确保运行环境一致性
     *
     * 使用场景：
     * - 任务实例创建时的上下文继承
     * - 确保任务与工作流的一致性
     * - 运行时参数和配置的传递
     * - 资源和环境配置的继承
     *
     * 类比理解：
     * 就像员工继承部门的基础配置和资源，包括办公环境、
     * 预算分配、管理者信息等，确保工作在统一的上下文中进行。
     *
     * @param taskInstance 目标任务实例，将被注入工作流上下文
     * @param workflowInstance 源工作流实例，提供运行时上下文
     */
    protected void injectMetadataFromWorkflowInstance(TaskInstance taskInstance, WorkflowInstance workflowInstance) {
        taskInstance.setWorkflowInstanceId(workflowInstance.getId());
        taskInstance.setWorkflowInstanceName(workflowInstance.getName());
        taskInstance.setProjectCode(workflowInstance.getProjectCode());
        taskInstance.setWorkerGroup(
                WorkerGroupUtils.getWorkerGroupOrDefault(
                        taskInstance.getWorkerGroup(), workflowInstance.getWorkerGroup()));
        taskInstance.setEnvironmentCode(
                EnvironmentUtils.getEnvironmentCodeOrDefault(
                        taskInstance.getEnvironmentCode(), workflowInstance.getEnvironmentCode()));
        taskInstance.setExecutorId(workflowInstance.getExecutorId());
        taskInstance.setVarPool(workflowInstance.getVarPool());
        taskInstance.setExecutorName(workflowInstance.getExecutorName());
        taskInstance.setDryRun(workflowInstance.getDryRun());
    }

}
