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

package org.apache.dolphinscheduler.server.master.runner;

import static org.apache.dolphinscheduler.plugin.task.api.TaskConstants.CLUSTER;
import static org.apache.dolphinscheduler.plugin.task.api.TaskConstants.NAMESPACE_NAME;

import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.dao.entity.DataSource;
import org.apache.dolphinscheduler.dao.entity.Environment;
import org.apache.dolphinscheduler.dao.entity.Project;
import org.apache.dolphinscheduler.dao.entity.TaskDefinition;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.dao.entity.WorkflowDefinition;
import org.apache.dolphinscheduler.dao.entity.WorkflowInstance;
import org.apache.dolphinscheduler.dao.repository.IEnvironmentDao;
import org.apache.dolphinscheduler.dao.utils.EnvironmentUtils;
import org.apache.dolphinscheduler.plugin.task.api.K8sTaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.TaskPluginManager;
import org.apache.dolphinscheduler.plugin.task.api.model.Property;
import org.apache.dolphinscheduler.plugin.task.api.parameters.AbstractParameters;
import org.apache.dolphinscheduler.plugin.task.api.parameters.K8sTaskParameters;
import org.apache.dolphinscheduler.plugin.task.api.parameters.resource.AbstractResourceParameters;
import org.apache.dolphinscheduler.plugin.task.api.parameters.resource.DataSourceParameters;
import org.apache.dolphinscheduler.plugin.task.api.parameters.resource.ResourceParametersHelper;
import org.apache.dolphinscheduler.plugin.task.api.utils.MapUtils;
import org.apache.dolphinscheduler.plugin.task.api.utils.VarPoolUtils;
import org.apache.dolphinscheduler.server.master.config.MasterConfig;
import org.apache.dolphinscheduler.server.master.engine.graph.IWorkflowExecutionGraph;
import org.apache.dolphinscheduler.server.master.engine.task.runnable.ITaskExecutionRunnable;
import org.apache.dolphinscheduler.server.master.engine.task.runnable.TaskExecutionContextBuilder;
import org.apache.dolphinscheduler.server.master.engine.task.runnable.TaskExecutionContextCreateRequest;
import org.apache.dolphinscheduler.service.expand.CuringParamsService;
import org.apache.dolphinscheduler.service.process.ProcessService;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * 任务执行上下文工厂类
 * <p>
 * 负责创建和构建任务执行所需的完整上下文信息。该工厂类是任务执行准备阶段的核心组件，
 * 它将分散的任务相关信息（如任务实例、工作流实例、环境配置、资源参数等）整合成一个
 * 完整的任务执行上下文对象，为任务的实际执行提供所需的所有信息和资源。
 * </p>
 *
 * <h3>主要功能：</h3>
 * <ul>
 *   <li>任务执行上下文创建：根据任务和工作流信息创建执行上下文</li>
 *   <li>环境配置装配：从数据库加载和装配任务执行环境配置</li>
 *   <li>资源参数处理：解析和装配任务所需的各类资源参数</li>
 *   <li>K8s任务支持：为Kubernetes任务提供专门的执行上下文</li>
 *   <li>变量池管理：处理任务间的变量传递和共享</li>
 *   <li>参数解析：解析和准备任务执行所需的各种参数</li>
 * </ul>
 *
 * @author DolphinScheduler
 * @since 1.0.0
 */
@Slf4j
@Component
public class TaskExecutionContextFactory {

    /**
     * 流程服务，用于访问工作流和任务相关的数据
     */
    @Autowired
    private ProcessService processService;

    /**
     * 参数解析服务，负责处理任务参数的解析和准备
     */
    @Autowired
    private CuringParamsService curingParamsService;

    /**
     * Master节点配置信息
     */
    @Autowired
    private MasterConfig masterConfig;

    /**
     * 环境配置数据访问对象
     */
    @Autowired
    private IEnvironmentDao environmentDao;

    /**
     * 创建任务执行上下文
     * <p>
     * 这是工厂类的核心方法，负责根据提供的请求信息创建完整的任务执行上下文。
     * 该方法会整合任务实例、工作流实例、环境配置、资源参数等所有必要信息，
     * 并构建出任务执行器所需的完整上下文对象。
     * </p>
     *
     * <h4>处理流程：</h4>
     * <ol>
     *   <li>从请求中提取基础信息（任务实例、工作流实例等）</li>
     *   <li>生成任务实例的变量池，合并前置任务的输出变量</li>
     *   <li>使用TaskExecutionContextBuilder构建执行上下文</li>
     *   <li>装配环境配置、资源参数、K8s配置等扩展信息</li>
     * </ol>
     *
     * @param request 任务执行上下文创建请求，包含创建上下文所需的所有信息
     * @return 完整的任务执行上下文对象
     */
    public TaskExecutionContext createTaskExecutionContext(TaskExecutionContextCreateRequest request) {
        // ========== 第一步：提取核心对象 ==========
        // 从请求中提取创建上下文所需的核心对象
        final TaskInstance taskInstance = request.getTaskInstance();
        final WorkflowInstance workflowInstance = request.getWorkflowInstance();
        final WorkflowDefinition workflowDefinition = request.getWorkflowDefinition();
        final Project project = request.getProject();

        // ========== 第二步：生成并设置变量池 ==========
        // 生成任务实例的变量池，这个变量池包含：
        // 1. 工作流级别的全局变量
        // 2. 前置任务输出的变量（用于任务间数据传递）
        // 3. 任务自定义的局部变量
        final List<Property> varPools =
                generateTaskInstanceVarPool(request.getTaskDefinition(), request.getWorkflowExecutionGraph());
        // 将变量池序列化为JSON字符串并保存到任务实例中
        taskInstance.setVarPool(VarPoolUtils.serializeVarPool(varPools));

        // ========== 第三步：使用建造者模式构建上下文 ==========
        // 使用TaskExecutionContextBuilder按步骤装配各个组件
        return TaskExecutionContextBuilder.get()
                // 设置工作流实例所在的Master节点地址
                // 这个地址用于Worker节点回调结果时的目标地址
                .buildWorkflowInstanceHost(masterConfig.getMasterAddress())
                // 装配任务实例相关信息（ID、名称、状态、重试次数等）
                // 包含任务的基本执行信息和状态管理相关数据
                .buildTaskInstanceRelatedInfo(taskInstance)
                // 从数据库加载并装配环境配置（如队列、Worker组等）
                // 环境配置决定了任务在哪个环境中执行，包含环境变量和配置信息
                .buildEnvironmentConfig(getEnvironmentConfigFromDB(taskInstance).orElse(null))
                // 装配任务定义信息（任务类型、参数、超时配置等）
                // 任务定义包含了任务的静态配置信息，如类型、参数模板等
                .buildTaskDefinitionRelatedInfo(request.getTaskDefinition())
                // 装配工作流实例信息（工作流ID、名称、调度时间等）
                // 工作流实例信息提供任务执行的上下文环境和调度信息
                .buildProcessInstanceRelatedInfo(request.getWorkflowInstance())
                // 装配资源参数（数据源连接信息、文件资源路径等）
                // 解析任务参数中的资源引用，并加载具体的资源配置信息
                .buildResourceParameters(getResourceParameters(taskInstance))
                // 装配预处理参数（变量替换、表达式计算后的最终参数）
                // 将任务参数中的变量占位符替换为实际值，进行表达式计算
                // todo: use TaskRuntimeParameters to replace Map<String, Property> in TaskExecutionContext
                .buildPrepareParams(getPrepareParams(taskInstance, workflowInstance, workflowDefinition, project))
                // 装配K8s任务相关信息（集群配置、命名空间等）
                // 仅对K8S和KUBEFLOW类型的任务有效，提供Kubernetes执行环境配置
                .buildK8sTaskRelatedInfo(getK8sTaskExecutionContext(taskInstance))
                // 创建最终的任务执行上下文对象
                .create();
    }

    /**
     * 获取并装配任务的资源参数
     * <p>
     * 解析任务参数中的资源配置信息，并根据资源类型进行相应的装配处理。
     * 目前主要支持数据源类型的资源参数装配。
     * </p>
     *
     * <h4>处理流程：</h4>
     * <ol>
     *   <li>根据任务类型获取对应的任务通道插件</li>
     *   <li>解析任务参数字符串，提取资源配置信息</li>
     *   <li>遍历资源类型，对每种资源进行具体的装配处理</li>
     *   <li>返回装配完成的资源参数助手对象</li>
     * </ol>
     *
     * @param taskInstance 任务实例，包含任务参数信息
     * @return 装配完成的资源参数助手对象
     */
    private ResourceParametersHelper getResourceParameters(final TaskInstance taskInstance) {
        // ========== 第一步：解析任务参数获取资源配置 ==========
        // 根据任务类型获取对应的任务通道，解析任务参数中的资源引用
        final ResourceParametersHelper resourceParameters = TaskPluginManager.getTaskChannel(taskInstance.getTaskType())
                .parseParameters(taskInstance.getTaskParams())  // 解析JSON格式的任务参数
                .getResources();  // 提取其中的资源配置信息

        // ========== 第二步：装配具体的资源参数 ==========
        if (resourceParameters != null) {
            // todo: add DataSourceParametersAssembler to assemble DataSourceParameters
            // 遍历所有资源类型，对每种资源类型进行专门的装配处理
            resourceParameters.getResourceMap().forEach((type, map) -> {
                switch (type) {
                    case DATASOURCE:
                        // 装配数据源参数：将数据源ID转换为完整的连接信息
                        assembleDataSourceParameters(map);
                        break;
                    default:
                        // 其他资源类型暂不需要特殊处理
                        break;
                }
            });
        }
        return resourceParameters;
    }

    /**
     * 装配数据源参数
     * <p>
     * 根据数据源ID从数据库中查询数据源信息，并将其转换为任务执行所需的
     * 数据源参数对象。这个过程确保任务能够获取到正确的数据库连接信息。
     * </p>
     *
     * <h4>装配流程：</h4>
     * <ol>
     *   <li>检查数据源映射表是否为空</li>
     *   <li>遍历每个数据源ID，从数据库查询数据源详细信息</li>
     *   <li>创建数据源参数对象，设置类型和连接参数</li>
     *   <li>将装配好的参数对象替换原有的参数引用</li>
     * </ol>
     *
     * @param map 数据源ID到资源参数的映射表
     */
    private void assembleDataSourceParameters(Map<Integer, AbstractResourceParameters> map) {
        // ========== 参数校验 ==========
        // 如果数据源映射表为空，则无需处理
        if (MapUtils.isEmpty(map)) {
            return;
        }

        // ========== 遍历装配每个数据源 ==========
        map.forEach((code, parameters) -> {
            // 根据数据源编码从数据库查询数据源的详细配置信息
            DataSource datasource = processService.findDataSourceById(code);

            // 如果数据源不存在，跳过该数据源的处理
            // 这种情况可能是数据源被删除或配置错误导致的
            if (Objects.isNull(datasource)) {
                return;
            }

            // ========== 创建并设置数据源参数 ==========
            // 创建新的数据源参数对象，包含完整的连接信息
            DataSourceParameters dataSourceParameters = new DataSourceParameters();
            // 设置数据源类型（如MySQL、PostgreSQL、Oracle等）
            dataSourceParameters.setType(datasource.getType());
            // 设置数据源连接参数（包含URL、用户名、密码等连接信息）
            dataSourceParameters.setConnectionParams(datasource.getConnectionParams());

            // 将装配完成的数据源参数对象替换原有的参数引用
            // 这样任务执行时就能获取到完整的数据库连接信息
            map.put(code, dataSourceParameters);
        });
    }

    /**
     * 获取Kubernetes任务执行上下文
     * <p>
     * 为K8S和KUBEFLOW类型的任务创建专门的Kubernetes执行上下文。
     * 该方法会解析任务参数中的namespace配置，并获取对应的集群配置信息。
     * </p>
     *
     * <h4>处理步骤：</h4>
     * <ol>
     *   <li>检查任务类型是否为K8S相关类型</li>
     *   <li>解析任务参数，提取namespace配置信息</li>
     *   <li>根据集群名称获取Kubernetes集群配置</li>
     *   <li>创建K8s任务执行上下文对象</li>
     * </ol>
     *
     * @param taskInstance 任务实例
     * @return K8s任务执行上下文，如果不是K8s任务则返回null
     */
    private K8sTaskExecutionContext getK8sTaskExecutionContext(final TaskInstance taskInstance) {
        K8sTaskExecutionContext k8sTaskExecutionContext = null;
        String namespace = "";

        // ========== 第一步：检查任务类型并解析参数 ==========
        switch (taskInstance.getTaskType()) {
            case "K8S":
            case "KUBEFLOW":
                // 解析K8s任务的特定参数，提取namespace配置
                K8sTaskParameters k8sTaskParameters =
                        JSONUtils.parseObject(taskInstance.getTaskParams(), K8sTaskParameters.class);
                // 获取namespace字符串，包含集群名和命名空间名的JSON配置
                namespace = k8sTaskParameters.getNamespace();
                break;
            default:
                // 非K8s类型任务，直接返回null
                break;
        }

        // ========== 第二步：解析namespace配置并创建执行上下文 ==========
        if (StringUtils.isNotEmpty(namespace)) {
            // 将namespace字符串解析为Map，提取集群名称
            // namespace格式示例：{"cluster":"k8s-cluster","namespace":"default"}
            String clusterName = JSONUtils.toMap(namespace).get(CLUSTER);

            // 根据集群名称从数据库查询Kubernetes集群的配置文件（kubeconfig）
            String configYaml = processService.findConfigYamlByName(clusterName);

            // ========== 第三步：创建K8s执行上下文 ==========
            if (configYaml != null) {
                // 创建K8s任务执行上下文，包含：
                // 1. configYaml: Kubernetes集群的连接配置（kubeconfig内容）
                // 2. namespaceName: 任务执行的目标命名空间
                k8sTaskExecutionContext =
                        new K8sTaskExecutionContext(configYaml, JSONUtils.toMap(namespace).get(NAMESPACE_NAME));
            }
        }
        return k8sTaskExecutionContext;
    }

    /**
     * 获取任务执行的准备参数
     * <p>
     * 解析任务参数并进行参数预处理，包括变量替换、表达式计算等操作。
     * 这个过程确保任务执行时能够获取到正确解析后的参数值。
     * </p>
     *
     * <h4>参数预处理流程：</h4>
     * <ol>
     *   <li>根据任务类型解析任务参数为具体的参数对象</li>
     *   <li>调用参数解析服务进行变量替换和表达式计算</li>
     *   <li>将预处理后的参数转换为属性映射表</li>
     * </ol>
     *
     * <h4>处理内容包括：</h4>
     * <ul>
     *   <li>全局变量替换：${variable_name}格式的变量占位符</li>
     *   <li>系统内置变量：如${system.biz.date}、${system.biz.curdate}等</li>
     *   <li>任务实例变量：从变量池中获取的前置任务输出变量</li>
     *   <li>表达式计算：如日期计算、数学运算等</li>
     * </ul>
     *
     * @param taskInstance 任务实例
     * @param workflowInstance 工作流实例
     * @param workflowDefinition 工作流定义
     * @param project 项目信息
     * @return 处理后的参数映射表
     */
    private Map<String, Property> getPrepareParams(final TaskInstance taskInstance,
                                                   final WorkflowInstance workflowInstance,
                                                   final WorkflowDefinition workflowDefinition,
                                                   final Project project) {
        // ========== 第一步：解析任务参数为具体的参数对象 ==========
        // 根据任务类型（如SHELL、SQL、PYTHON等）获取对应的参数解析器
        // 将JSON格式的任务参数字符串解析为具体的参数对象
        final AbstractParameters baseParam = TaskPluginManager.parseTaskParameters(
                taskInstance.getTaskType(),
                taskInstance.getTaskParams());

        // ========== 第二步：进行参数预处理 ==========
        // 调用参数解析服务，进行以下处理：
        // 1. 变量替换：将${variable_name}格式的占位符替换为实际值
        // 2. 表达式计算：处理日期表达式、数学表达式等
        // 3. 系统变量注入：注入系统内置的变量值
        // 4. 上下文变量获取：从工作流和任务上下文中获取变量值
        return curingParamsService.paramParsingPreparation(
                taskInstance,      // 任务实例，提供任务级别的变量和配置
                baseParam,         // 解析后的基础参数对象
                workflowInstance,  // 工作流实例，提供工作流级别的变量和调度信息
                project.getName(), // 项目名称，用于项目级别的变量替换
                workflowDefinition.getName()); // 工作流定义名称，用于工作流级别的变量替换
    }

    /**
     * 从数据库获取环境配置信息
     * <p>
     * 根据任务实例中的环境编码，从数据库查询对应的环境配置信息。
     * 环境配置包含了任务执行时所需的环境变量、路径配置等信息。
     * </p>
     *
     * @param taskInstance 任务实例，包含环境编码信息
     * @return 环境配置字符串的Optional包装，如果没有配置则为空
     * @throws IllegalArgumentException 当指定的环境编码不存在时抛出
     */
    private Optional<String> getEnvironmentConfigFromDB(final TaskInstance taskInstance) {
        if (EnvironmentUtils.isEnvironmentCodeEmpty(taskInstance.getEnvironmentCode())) {
            return Optional.empty();
        }
        final Optional<Environment> environmentOptional =
                environmentDao.queryByEnvironmentCode(taskInstance.getEnvironmentCode());
        if (!environmentOptional.isPresent()) {
            throw new IllegalArgumentException("Cannot find the environment: " + taskInstance.getEnvironmentCode());
        }
        return Optional.ofNullable(environmentOptional.get().getConfig());
    }

    /**
     * 生成任务实例的变量池
     * <p>
     * 通过合并前置任务的输出变量来构建当前任务的变量池。这个机制实现了
     * 任务间的数据传递，使得下游任务能够使用上游任务产生的变量值。
     * </p>
     *
     * <h4>处理逻辑：</h4>
     * <ul>
     *   <li>获取当前任务的所有前置任务</li>
     *   <li>提取前置任务中已初始化任务实例的变量池</li>
     *   <li>将所有前置任务的变量池合并成一个统一的变量列表</li>
     * </ul>
     *
     * <h4>变量池机制说明：</h4>
     * <ul>
     *   <li>变量池是任务间数据传递的核心机制</li>
     *   <li>上游任务通过setValue函数向变量池输出变量</li>
     *   <li>下游任务可以通过${variable_name}引用这些变量</li>
     *   <li>变量池支持变量覆盖，后执行的任务变量会覆盖先执行的同名变量</li>
     * </ul>
     *
     * @param taskDefinition 任务定义，用于在执行图中查找前置任务
     * @param workflowExecutionGraph 工作流执行图，包含任务间的依赖关系
     * @return 合并后的变量属性列表
     */
    private List<Property> generateTaskInstanceVarPool(TaskDefinition taskDefinition,
                                                       IWorkflowExecutionGraph workflowExecutionGraph) {
        // ========== 第一步：获取前置任务列表 ==========
        // 从工作流执行图中查找当前任务的所有前置任务（依赖的上游任务）
        List<ITaskExecutionRunnable> predecessors = workflowExecutionGraph.getPredecessors(taskDefinition.getName());

        // 如果没有前置任务，返回空的变量池
        // 这通常发生在工作流的起始任务中
        if (CollectionUtils.isEmpty(predecessors)) {
            return Collections.emptyList();
        }

        // ========== 第二步：提取前置任务的变量池 ==========
        // 遍历所有前置任务，提取已经初始化的任务实例的变量池
        List<String> varPoolsFromPredecessors = predecessors
                .stream()
                // 过滤出已经初始化的任务实例（即已经创建了TaskInstance对象的任务）
                // 只有已初始化的任务才可能有变量池数据
                .filter(ITaskExecutionRunnable::isTaskInstanceInitialized)
                // 获取任务实例对象
                .map(ITaskExecutionRunnable::getTaskInstance)
                // 提取任务实例的变量池（JSON字符串格式）
                // 变量池包含了该任务执行过程中通过setValue函数设置的所有变量
                .map(TaskInstance::getVarPool)
                .collect(Collectors.toList());

        // ========== 第三步：合并所有前置任务的变量池 ==========
        // 将多个JSON格式的变量池字符串合并为一个统一的变量属性列表
        // 合并过程中会处理变量名冲突，后面的变量会覆盖前面的同名变量
        return VarPoolUtils.mergeVarPoolJsonString(varPoolsFromPredecessors);
    }

}
