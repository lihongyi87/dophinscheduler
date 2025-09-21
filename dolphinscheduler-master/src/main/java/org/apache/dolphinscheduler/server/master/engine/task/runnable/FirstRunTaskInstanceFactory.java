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

import org.apache.dolphinscheduler.common.enums.Flag;
import org.apache.dolphinscheduler.dao.entity.TaskDefinition;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.dao.entity.WorkflowInstance;
import org.apache.dolphinscheduler.dao.repository.TaskInstanceDao;
import org.apache.dolphinscheduler.plugin.task.api.enums.TaskExecutionStatus;
import org.apache.dolphinscheduler.server.master.engine.task.runnable.FirstRunTaskInstanceFactory.FirstRunTaskInstanceBuilder;

import java.util.Date;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;

/**
 * 首次运行任务实例工厂
 *
 * 专门用于创建全新任务实例的工厂类，负责处理任务的首次执行场景。
 * 继承抽象任务实例工厂，实现了首次运行任务的特定创建逻辑。
 *
 * 核心职责：
 * - 创建首次执行的任务实例
 * - 设置任务实例的初始状态和属性
 * - 注入任务定义和工作流实例的元数据
 * - 持久化新创建的任务实例
 *
 * 设计特点：
 * - 工厂模式：封装首次运行任务实例的创建逻辑
 * - 建造者模式：通过Builder简化复杂对象的构造
 * - 模板方法：复用父类的通用元数据注入方法
 * - 依赖注入：使用Spring管理DAO层依赖
 *
 * 创建流程：
 * 1. 验证必需参数（任务定义、工作流实例）
 * 2. 创建新的任务实例对象
 * 3. 从任务定义注入静态元数据
 * 4. 从工作流实例注入运行时上下文
 * 5. 设置首次运行的特定属性
 * 6. 持久化到数据库并返回
 *
 * 首次运行特征：
 * - 状态设置为SUBMITTED_SUCCESS（提交成功）
 * - 重试次数初始化为0
 * - 设置首次提交时间和提交时间为当前时间
 * - 运行时字段（开始时间、结束时间、主机等）初始化为null
 * - 启用任务标志，禁用告警标志
 *
 * 使用场景：
 * - 工作流首次启动时创建任务实例
 * - 新任务节点的初始化
 * - 定时任务的首次调度
 * - 手动触发任务的创建
 *
 * 类比理解：
 * 就像工厂生产线上的"新产品首次生产"工艺，负责：
 * - 按照产品规格（任务定义）设置基础参数
 * - 根据生产计划（工作流实例）配置运行环境
 * - 初始化产品状态为"已投产"
 * - 记录生产开始信息到生产记录
 */
@Component
public class FirstRunTaskInstanceFactory extends AbstractTaskInstanceFactory<FirstRunTaskInstanceBuilder> {

    /**
     * 任务实例数据访问对象
     * 用于持久化新创建的任务实例到数据库
     */
    @Autowired
    private TaskInstanceDao taskInstanceDao;

    /**
     * 创建首次运行任务实例建造者
     * @return 配置好的建造者实例
     */
    @Override
    public FirstRunTaskInstanceBuilder builder() {
        return new FirstRunTaskInstanceBuilder(this);
    }

    /**
     * 创建首次运行任务实例
     *
     * 根据建造者提供的参数创建全新的任务实例，设置首次运行的特定属性，
     * 并持久化到数据库中。这是首次运行场景的核心创建逻辑。
     *
     * 创建策略：
     * - 参数验证：确保任务定义和工作流实例不为空
     * - 元数据注入：继承任务定义和工作流实例的配置
     * - 状态初始化：设置为提交成功状态，准备执行
     * - 时间设置：记录首次提交和提交时间
     * - 运行时清零：将运行时字段初始化为默认值
     * - 数据持久化：保存到数据库并生成ID
     *
     * 首次运行属性设置：
     * - 执行状态：SUBMITTED_SUCCESS（已成功提交）
     * - 首次提交时间：当前时间
     * - 提交时间：当前时间
     * - 开始时间：null（待执行时设置）
     * - 结束时间：null（执行完成时设置）
     * - 执行主机：null（分配到Worker时设置）
     * - 执行路径：null（Worker执行时设置）
     * - 日志路径：null（Worker执行时设置）
     * - 重试次数：0（首次执行）
     * - 告警标志：NO（默认不告警）
     * - 任务标志：YES（启用任务）
     *
     * @param builder 包含创建参数的建造者对象
     * @return 创建并持久化的任务实例
     * @throws NullPointerException 当必需参数为null时抛出
     */
    @Override
    public TaskInstance createTaskInstance(FirstRunTaskInstanceBuilder builder) {
        final TaskDefinition taskDefinition = Preconditions.checkNotNull(builder.taskDefinition);
        final WorkflowInstance workflowInstance = Preconditions.checkNotNull(builder.workflowInstance);

        TaskInstance taskInstance = new TaskInstance();
        injectMetadataFromTaskDefinition(taskInstance, taskDefinition);
        injectMetadataFromWorkflowInstance(taskInstance, workflowInstance);

        taskInstance.setState(TaskExecutionStatus.SUBMITTED_SUCCESS);
        taskInstance.setFirstSubmitTime(new Date());
        taskInstance.setSubmitTime(new Date());
        taskInstance.setStartTime(null);
        taskInstance.setEndTime(null);
        taskInstance.setHost(null);
        taskInstance.setExecutePath(null);
        taskInstance.setLogPath(null);
        taskInstance.setRetryTimes(0);
        taskInstance.setAlertFlag(Flag.NO);
        taskInstance.setFlag(Flag.YES);
        taskInstanceDao.insert(taskInstance);
        return taskInstance;
    }

    /**
     * 首次运行任务实例建造者
     *
     * 专门用于构建首次运行任务实例的建造者类，实现了ITaskInstanceBuilder接口。
     * 提供流式API来设置任务创建所需的参数，简化任务实例的构造过程。
     *
     * 建造者特点：
     * - 流式接口：支持方法链式调用
     * - 参数收集：收集任务创建所需的各种参数
     * - 延迟构建：参数设置完成后统一构建对象
     * - 类型安全：编译时确保参数类型正确
     *
     * 必需参数：
     * - 工作流实例：提供工作流级别的运行时上下文
     * - 任务定义：提供任务级别的静态配置信息
     *
     * 使用模式：
     * ```java
     * TaskInstance taskInstance = factory.builder()
     *     .withWorkflowInstance(workflowInstance)
     *     .withTaskDefinition(taskDefinition)
     *     .build();
     * ```
     */
    public static class FirstRunTaskInstanceBuilder implements ITaskInstanceFactory.ITaskInstanceBuilder {

        /**
         * 首次运行任务实例工厂引用
         * 用于调用工厂的创建方法
         */
        private final FirstRunTaskInstanceFactory firstRunTaskInstanceFactory;

        /**
         * 工作流实例
         * 提供任务实例所需的工作流级别上下文信息
         */
        private WorkflowInstance workflowInstance;

        /**
         * 任务定义
         * 提供任务实例所需的任务级别配置信息
         */
        private TaskDefinition taskDefinition;

        /**
         * 构造首次运行任务实例建造者
         * @param firstRunTaskInstanceFactory 工厂实例引用
         */
        public FirstRunTaskInstanceBuilder(FirstRunTaskInstanceFactory firstRunTaskInstanceFactory) {
            this.firstRunTaskInstanceFactory = firstRunTaskInstanceFactory;
        }

        /**
         * 设置工作流实例
         * @param workflowInstance 工作流实例对象
         * @return 建造者自身，支持链式调用
         */
        public FirstRunTaskInstanceBuilder withWorkflowInstance(WorkflowInstance workflowInstance) {
            this.workflowInstance = workflowInstance;
            return this;
        }

        /**
         * 设置任务定义
         * @param taskDefinition 任务定义对象
         * @return 建造者自身，支持链式调用
         */
        public FirstRunTaskInstanceBuilder withTaskDefinition(TaskDefinition taskDefinition) {
            this.taskDefinition = taskDefinition;
            return this;
        }

        /**
         * 构建首次运行任务实例
         * 调用工厂的创建方法，生成配置完整的任务实例
         * @return 创建的任务实例
         */
        @Override
        public TaskInstance build() {
            return firstRunTaskInstanceFactory.createTaskInstance(this);
        }
    }
}
