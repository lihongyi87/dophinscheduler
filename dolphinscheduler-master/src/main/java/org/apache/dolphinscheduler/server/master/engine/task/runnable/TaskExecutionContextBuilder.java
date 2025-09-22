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

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.dolphinscheduler.common.enums.TimeoutFlag;
import org.apache.dolphinscheduler.common.utils.DateUtils;
import org.apache.dolphinscheduler.dao.entity.TaskDefinition;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.dao.entity.WorkflowInstance;
import org.apache.dolphinscheduler.plugin.task.api.K8sTaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.enums.TaskTimeoutStrategy;
import org.apache.dolphinscheduler.plugin.task.api.model.Property;
import org.apache.dolphinscheduler.plugin.task.api.parameters.resource.ResourceParametersHelper;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;

/**
 * 任务执行上下文建造者
 *
 * 专门用于构建任务执行上下文(TaskExecutionContext)的建造者模式实现。
 * 通过分步骤设置不同来源的信息，最终构建出完整的任务执行上下文。
 *
 * 设计目的：
 * - 信息整合：将来自任务实例、任务定义、工作流实例等多个数据源的信息整合
 * - 分离关注点：将复杂的上下文构建过程分解为多个独立的构建步骤
 * - 链式调用：提供流畅的API，支持方法链式调用
 * - 参数验证：在构建过程中验证关键参数的完整性
 *
 * 核心职责：
 * 1. 任务实例信息：设置任务运行时的基本信息和状态
 * 2. 任务定义信息：设置任务的静态配置和参数
 * 3. 工作流实例信息：设置工作流级别的上下文信息
 * 4. 资源参数：设置任务执行所需的资源配置
 * 5. 容器化信息：设置K8s等容器化执行环境信息
 * 6. 运行时参数：设置任务执行的动态参数
 * 7. 环境配置：设置执行环境的相关配置
 *
 * 构建流程：
 * 1. 创建建造者实例
 * 2. 逐步设置各类信息
 * 3. 验证必需参数
 * 4. 创建最终的执行上下文
 *
 * 使用模式：
 * TaskExecutionContextBuilder.get()
 *     .buildTaskInstanceRelatedInfo(taskInstance)
 *     .buildTaskDefinitionRelatedInfo(taskDefinition)
 *     .buildProcessInstanceRelatedInfo(workflowInstance)
 *     .buildResourceParameters(resourceHelper)
 *     .buildPrepareParams(paramMap)
 *     .buildWorkflowInstanceHost(masterHost)
 *     .buildEnvironmentConfig(envConfig)
 *     .create()
 *
 * 类比理解：
 * 就像为演员准备表演所需的完整道具和背景资料：
 * - 角色信息：演员的基本信息和当前状态
 * - 剧本信息：角色的台词、动作、性格设定
 * - 剧目信息：整部戏的背景、主题、其他角色关系
 * - 道具资源：表演需要的各种道具和设备
 * - 舞台环境：灯光、音响、布景等环境配置
 * - 临时参数：临时调整的台词或动作
 * - 最终准备：确保所有信息完整，演员可以开始表演
 */
@Slf4j
public class TaskExecutionContextBuilder {

    /**
     * 获取建造者实例
     *
     * 提供静态工厂方法创建新的建造者实例。使用静态方法而非直接构造函数
     * 可以提供更好的API语义，明确表达获取建造者的意图。
     *
     * 优势：
     * - 语义清晰：方法名表达获取建造者的意图
     * - 扩展性：未来可以支持建造者的缓存或池化
     * - 一致性：与其他建造者类保持一致的创建方式
     *
     * 类比：从工具箱中取出一个新的配置表格。
     *
     * @return 新的任务执行上下文建造者实例
     */
    public static TaskExecutionContextBuilder get() {
        return new TaskExecutionContextBuilder();
    }

    /**
     * 正在构建的任务执行上下文实例
     *
     * 建造者内部维护的目标对象，所有的构建操作都在这个对象上进行。
     * 使用final确保实例不可变更，保证构建过程的线程安全。
     */
    private final TaskExecutionContext taskExecutionContext;

    /**
     * 私有构造函数
     *
     * 防止外部直接实例化，强制使用静态工厂方法。
     * 在构造时创建空的任务执行上下文，准备接受后续的信息填充。
     *
     * 初始化策略：
     * - 创建空的TaskExecutionContext实例
     * - 所有属性使用默认值，等待后续设置
     * - 确保对象处于可用状态
     */
    public TaskExecutionContextBuilder() {
        this.taskExecutionContext = new TaskExecutionContext();
    }

    /**
     * 构建任务实例相关信息
     *
     * 从任务实例中提取运行时信息，设置到执行上下文中。任务实例包含了
     * 任务执行过程中产生的动态信息，如执行时间、状态、日志路径等。
     *
     * 设置的信息类别：
     * - 标识信息：任务实例ID、任务名称、任务类型
     * - 时间信息：首次提交时间、开始时间（转换为时间戳）
     * - 执行信息：Worker组、执行主机、日志路径
     * - 资源信息：CPU配额、内存限制
     * - 状态信息：干跑模式、应用链接
     *
     * 数据转换：
     * - 时间对象转换为时间戳，便于网络传输和存储
     * - 保持原始数据的完整性和准确性
     *
     * 类比：填写演员的基本信息，包括姓名、当前状态、分配的化妆间等。
     *
     * @param taskInstance 任务实例，包含运行时动态信息
     * @return 建造者自身，支持链式调用
     */
    public TaskExecutionContextBuilder buildTaskInstanceRelatedInfo(final TaskInstance taskInstance) {
        taskExecutionContext.setTaskInstanceId(taskInstance.getId());
        taskExecutionContext.setTaskName(taskInstance.getName());
        taskExecutionContext.setFirstSubmitTime(DateUtils.dateToTimeStamp(taskInstance.getFirstSubmitTime()));
        taskExecutionContext.setStartTime(DateUtils.dateToTimeStamp(taskInstance.getStartTime()));
        taskExecutionContext.setTaskType(taskInstance.getTaskType());
        taskExecutionContext.setLogPath(taskInstance.getLogPath());
        taskExecutionContext.setWorkerGroup(taskInstance.getWorkerGroup());
        taskExecutionContext.setHost(taskInstance.getHost());
        taskExecutionContext.setDryRun(taskInstance.getDryRun());
        taskExecutionContext.setCpuQuota(taskInstance.getCpuQuota());
        taskExecutionContext.setMemoryMax(taskInstance.getMemoryMax());
        taskExecutionContext.setAppIds(taskInstance.getAppLink());
        return this;
    }

    /**
     * 构建任务定义相关信息
     *
     * 从任务定义中提取静态配置信息，设置到执行上下文中。任务定义包含了
     * 任务的元数据和配置信息，如超时策略、任务参数等。
     *
     * 超时处理逻辑：
     * 1. 默认设置：初始超时时间设为最大值，避免不必要的超时
     * 2. 超时开关：检查任务定义中的超时标志是否开启
     * 3. 策略设置：根据超时通知策略设置对应的处理方式
     * 4. 时间转换：将分钟转换为秒，并防止整数溢出
     *
     * 支持的超时策略：
     * - FAILED：超时后直接失败
     * - WARNFAILED：先警告后失败
     * - 其他策略：仅记录但不强制终止
     *
     * 参数设置：
     * - 从任务定义中获取完整的任务参数
     * - 参数包含任务执行所需的各种配置
     *
     * 注意：代码中有TODO注释，表明超时处理逻辑未来可能会重构，
     * 超时策略可能会移到Master端统一处理。
     *
     * 类比：设置演员的剧本信息，包括台词、动作要求、表演时长限制等。
     *
     * @param taskDefinition 任务定义，包含静态配置信息
     * @return 建造者自身，支持链式调用
     */
    public TaskExecutionContextBuilder buildTaskDefinitionRelatedInfo(final TaskDefinition taskDefinition) {
        // TODO: 移除此处的超时设置，超时策略应该在Master端处理
        taskExecutionContext.setTaskTimeout(Integer.MAX_VALUE);
        if (taskDefinition.getTimeoutFlag() == TimeoutFlag.OPEN) {
            taskExecutionContext.setTaskTimeoutStrategy(taskDefinition.getTimeoutNotifyStrategy());
            if (taskDefinition.getTimeoutNotifyStrategy() == TaskTimeoutStrategy.FAILED
                    || taskDefinition.getTimeoutNotifyStrategy() == TaskTimeoutStrategy.WARNFAILED) {
                taskExecutionContext.setTaskTimeout(
                        (int) Math.min(TimeUnit.MINUTES.toSeconds(taskDefinition.getTimeout()), Integer.MAX_VALUE));
            }
        }
        taskExecutionContext.setTaskParams(taskDefinition.getTaskParams());
        return this;
    }

    /**
     * 构建工作流实例相关信息
     *
     * 从工作流实例中提取工作流级别的上下文信息，设置到执行上下文中。
     * 工作流实例提供了任务执行所需的全局信息和环境配置。
     *
     * 设置的信息类别：
     * - 工作流标识：工作流实例ID、定义代码、版本号
     * - 时间信息：调度时间（转换为时间戳）
     * - 参数信息：全局参数，供所有任务使用
     * - 执行者信息：执行者ID和名称
     * - 租户信息：租户代码，用于资源隔离
     *
     * 全局参数的作用：
     * - 参数共享：在工作流的所有任务间共享参数
     * - 动态配置：支持运行时动态设置参数值
     * - 参数传递：支持任务间的参数传递和依赖
     *
     * 版本管理：
     * - 记录工作流定义的具体版本
     * - 支持工作流的版本化管理和回滚
     * - 确保执行时使用正确的工作流版本
     *
     * 类比：设置整部戏的背景信息，包括剧目名称、导演、演出时间、
     * 全体演员共享的背景设定等。
     *
     * @param workflowInstance 工作流实例，包含工作流级别的上下文信息
     * @return 建造者自身，支持链式调用
     */
    public TaskExecutionContextBuilder buildProcessInstanceRelatedInfo(final WorkflowInstance workflowInstance) {
        taskExecutionContext.setWorkflowInstanceId(workflowInstance.getId());
        taskExecutionContext.setScheduleTime(DateUtils.dateToTimeStamp(workflowInstance.getScheduleTime()));
        taskExecutionContext.setGlobalParams(workflowInstance.getGlobalParams());
        taskExecutionContext.setExecutorId(workflowInstance.getExecutorId());
        taskExecutionContext.setTenantCode(workflowInstance.getTenantCode());
        taskExecutionContext.setWorkflowDefinitionCode(workflowInstance.getWorkflowDefinitionCode());
        taskExecutionContext.setWorkflowDefinitionVersion(workflowInstance.getWorkflowDefinitionVersion());
        return this;
    }

    /**
     * 构建资源参数信息
     *
     * 设置任务执行所需的资源参数助手。资源参数助手封装了任务执行时
     * 需要的各种资源配置和参数信息。
     *
     * 资源参数的内容：
     * - 文件资源：任务执行需要的文件、脚本、数据等
     * - 配置参数：数据库连接、API密钥等配置信息
     * - 环境变量：任务运行时需要的环境变量
     * - 依赖资源：第三方库、工具等依赖资源
     *
     * 参数助手的优势：
     * - 封装复杂性：将复杂的资源管理逻辑封装在助手中
     * - 统一接口：提供统一的资源访问接口
     * - 延迟加载：支持资源的延迟加载和按需获取
     * - 缓存机制：避免重复加载相同的资源
     *
     * 类比：为演员准备完整的道具箱，里面包含表演需要的所有道具、
     * 服装、化妆品等资源。
     *
     * @param parametersHelper 资源参数助手，封装任务执行所需的资源信息
     * @return 建造者自身，支持链式调用
     */
    public TaskExecutionContextBuilder buildResourceParameters(final ResourceParametersHelper parametersHelper) {
        taskExecutionContext.setResourceParametersHelper(parametersHelper);
        return this;
    }

    /**
     * 构建Kubernetes任务相关信息
     *
     * 设置Kubernetes容器化执行环境的专用上下文信息。当任务需要在
     * Kubernetes集群中执行时，需要额外的容器化相关配置。
     *
     * K8s上下文的内容：
     * - 集群配置：K8s集群的连接和认证信息
     * - 命名空间：任务执行的命名空间
     * - 资源限制：Pod的CPU、内存等资源限制
     * - 镜像配置：容器镜像和版本信息
     * - 卷挂载：数据卷和配置文件的挂载信息
     * - 网络配置：Service、Ingress等网络配置
     *
     * 容器化执行的优势：
     * - 环境隔离：每个任务在独立的容器中执行
     * - 资源管理：精确控制任务的资源使用
     * - 扩展性：支持自动扩缩容和负载均衡
     * - 可移植性：容器化的任务可以在不同环境中运行
     *
     * 类比：为演员准备专门的表演舞台，包括灯光、音响、布景等
     * 完整的舞台环境配置。
     *
     * @param k8sTaskExecutionContext K8s任务执行上下文，包含容器化执行的配置信息
     * @return 建造者自身，支持链式调用
     */
    public TaskExecutionContextBuilder buildK8sTaskRelatedInfo(final K8sTaskExecutionContext k8sTaskExecutionContext) {
        taskExecutionContext.setK8sTaskExecutionContext(k8sTaskExecutionContext);
        return this;
    }

    /**
     * 构建运行时准备参数
     *
     * 设置任务执行时的完整参数映射。这些参数来源于多个不同的层面，
     * 经过合并和处理后形成最终的运行时参数集合。
     *
     * 参数来源层次（按优先级从低到高）：
     * 1. 系统内置参数：系统提供的标准参数和函数
     * 2. 任务本地参数：任务定义中配置的本地参数
     * 3. 工作流全局参数：工作流级别的共享参数
     * 4. 启动命令参数：执行时通过命令行传入的参数
     * 5. 前置任务参数：从前置任务的变量池中获取的参数
     *
     * 参数合并规则：
     * - 高优先级参数覆盖低优先级参数
     * - 参数值支持引用和表达式计算
     * - 支持参数的动态解析和替换
     * - 保持参数类型的正确性
     *
     * 参数的用途：
     * - 任务配置：动态配置任务的执行参数
     * - 数据传递：在任务间传递数据和状态
     * - 条件判断：支持基于参数的条件逻辑
     * - 模板渲染：支持参数化的脚本和配置模板
     *
     * 类比：为演员准备完整的台词本，包含所有需要的台词、动作说明、
     * 临时调整的内容等。
     *
     * @param propertyMap 运行时参数映射，包含合并后的完整参数信息
     * @return 建造者自身，支持链式调用
     */
    public TaskExecutionContextBuilder buildPrepareParams(final Map<String, Property> propertyMap) {
        taskExecutionContext.setPrepareParamsMap(propertyMap);
        return this;
    }

    /**
     * 构建工作流实例主机信息
     *
     * 设置工作流实例所在的Master主机地址。这个信息对于任务执行过程中
     * 与Master节点的通信至关重要。
     *
     * 主机信息的用途：
     * - 状态回报：任务执行器向Master报告任务状态
     * - 心跳检测：维持任务执行器与Master的心跳连接
     * - 数据传输：传输任务结果和日志信息
     * - 故障恢复：Master故障时的重连和恢复
     *
     * 地址格式：
     * - 通常包含IP地址和端口号
     * - 支持域名解析和负载均衡
     * - 可能包含协议信息（HTTP/HTTPS）
     *
     * 高可用考虑：
     * - 支持多Master节点的故障转移
     * - 自动重连机制和重试策略
     * - 网络分区时的降级处理
     *
     * 类比：告知演员导演的联系方式，演出过程中有问题时可以随时沟通。
     *
     * @param masterHost Master主机地址，用于任务执行器与Master通信
     * @return 建造者自身，支持链式调用
     */
    public TaskExecutionContextBuilder buildWorkflowInstanceHost(final String masterHost) {
        taskExecutionContext.setWorkflowInstanceHost(masterHost);
        return this;
    }

    /**
     * 构建环境配置信息
     *
     * 设置任务执行环境的配置信息。环境配置包含了任务运行时需要的
     * 各种环境参数和设置。
     *
     * 环境配置的内容：
     * - 系统环境变量：任务执行时需要的环境变量
     * - 路径配置：可执行文件、库文件的路径设置
     * - 软件版本：依赖软件的版本要求
     * - 网络配置：代理、DNS等网络相关配置
     * - 安全配置：证书、密钥等安全相关设置
     *
     * 配置的格式：
     * - 可能是JSON、YAML或其他结构化格式
     * - 支持模板化和参数化配置
     * - 支持配置的继承和覆盖
     *
     * 环境隔离：
     * - 不同任务可以使用不同的环境配置
     * - 支持环境的版本化管理
     * - 避免不同任务间的环境冲突
     *
     * 类比：为演员设置表演环境，包括舞台灯光、音响效果、
     * 背景音乐等环境要素。
     *
     * @param environmentConfig 环境配置字符串，包含任务执行环境的各种设置
     * @return 建造者自身，支持链式调用
     */
    public TaskExecutionContextBuilder buildEnvironmentConfig(final String environmentConfig) {
        taskExecutionContext.setEnvironmentConfig(environmentConfig);
        return this;
    }

    /**
     * 创建任务执行上下文
     *
     * 完成所有信息设置后，创建最终的任务执行上下文实例。这是建造者模式的
     * 最终步骤，返回构建完成的目标对象。
     *
     * 创建前验证：
     * - 检查关键字段的完整性
     * - 工作流实例主机地址是必需的，用于任务与Master通信
     * - 其他关键信息的完整性验证
     *
     * 验证失败处理：
     * - 抛出运行时异常，明确指出缺失的必需信息
     * - 提供有意义的错误消息，便于问题定位
     * - 确保不会返回不完整或无效的上下文
     *
     * 返回的上下文特点：
     * - 包含完整的任务执行信息
     * - 可以直接用于任务分发和执行
     * - 信息的一致性和完整性得到保证
     *
     * 使用注意事项：
     * - 创建后的上下文通常是不可变的
     * - 建造者在create()后通常不再使用
     * - 上下文包含敏感信息，需要注意安全传输
     *
     * 类比：最终检查演员的所有准备工作是否完成，确认可以开始表演后，
     * 正式开始演出。
     *
     * @return 构建完成的任务执行上下文
     * @throws NullPointerException 当工作流实例主机地址为空时
     */
    public TaskExecutionContext create() {
        checkNotNull(taskExecutionContext.getWorkflowInstanceHost(), "The workflow instance host cannot be empty");
        return taskExecutionContext;
    }

}
