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

package org.apache.dolphinscheduler.server.master.engine.graph;

import org.apache.dolphinscheduler.dao.entity.TaskDefinition;

import java.util.List;
import java.util.Set;

/**
 * 工作流图接口
 *
 * 定义工作流有向无环图(DAG)的基本操作接口。
 * 工作流图是DolphinScheduler中表示任务依赖关系的核心数据结构。
 *
 * 类比：就像一个项目甘特图的查询接口，提供查看项目任务间依赖关系的各种方法。
 *
 * 核心职责：
 * 1. 图节点管理：提供获取图中所有任务节点的功能
 * 2. 依赖关系查询：支持查询任务的前驱和后继关系
 * 3. 起始节点识别：找出可以立即开始执行的任务
 * 4. 任务定义查询：支持按名称和编码快速查找任务定义
 * 5. 图结构遍历：为图遍历算法提供基础访问接口
 *
 * 接口设计原则：
 * - 只读性：接口只提供查询操作，不提供修改操作，保证图结构的稳定性
 * - 完整性：提供图操作所需的所有基础查询方法
 * - 高效性：支持多种查询方式，适应不同的使用场景
 * - 一致性：方法命名和参数设计遵循统一的规范
 *
 * 图的特性：
 * - 有向性：任务间的依赖关系是有方向的
 * - 无环性：不允许存在循环依赖，确保工作流能够正常执行
 * - 完整性：图包含所有需要执行的任务及其依赖关系
 * - 连通性：支持多个起始点和结束点的复杂工作流结构
 *
 * 使用场景：
 * - 工作流执行调度：确定任务的执行顺序
 * - 依赖关系分析：分析任务间的依赖关系
 * - 并行度计算：确定可以同时执行的任务数量
 * - 图可视化：为前端提供图结构数据
 * - 状态传播：根据依赖关系传播任务状态变化
 *
 * 类比理解：
 * 就像建筑施工的工序图：
 * - 起始节点 = 可以立即开始的工序（场地准备、材料采购等）
 * - 前驱关系 = 工序的前置条件（装修前必须完成水电工程）
 * - 后继关系 = 工序完成后可以开始的下一步工序
 * - 任务定义 = 每个工序的详细作业指导书
 */
public interface IWorkflowGraph {

    /**
     * 获取工作流图的起始节点
     *
     * 返回没有前驱任务的所有任务名称列表。
     * 起始节点是工作流执行时首先被调度的任务，它们不依赖于任何其他任务。
     *
     * 实现要求：
     * - 必须返回所有入度为0的节点
     * - 返回的列表不应为空（除非是空图）
     * - 节点名称必须在图中存在
     *
     * @return 起始节点的任务名称列表
     *
     * 类比：找出项目中可以立即开始的工作，比如需求调研、环境准备等
     * 不需要等待其他工作完成就能开始的任务。
     */
    List<String> getStartNodes();

    /**
     * 获取指定任务的前驱任务集合
     *
     * 返回指定任务的所有前驱任务名称，即该任务依赖的其他任务。
     * 这些前驱任务必须全部成功完成后，指定任务才能开始执行。
     *
     * 依赖关系说明：
     * - 前驱任务的执行状态决定当前任务是否可以启动
     * - 如果任何一个前驱任务失败，当前任务通常不能执行
     * - 空集合表示该任务是起始节点，可以立即执行
     *
     * @param taskName 任务名称，必须在图中存在
     * @return 前驱任务名称的集合，使用Set保证唯一性
     *
     * 类比：查询某项工作需要等待哪些前置工作完成，
     * 比如系统测试需要等待开发完成和环境部署完成。
     */
    Set<String> getPredecessors(String taskName);

    /**
     * 获取指定任务的后继任务集合
     *
     * 返回依赖指定任务的所有后继任务名称。
     * 当指定任务完成后，这些后继任务的一个依赖条件就得到满足。
     *
     * 注意事项：
     * - 此方法不会返回被禁用的任务
     * - 后继任务可能还有其他前驱任务需要完成
     * - 空集合表示该任务是结束节点，没有后续任务
     *
     * @param taskName 任务名称，必须在图中存在
     * @return 后继任务名称的集合，不包括被禁用的任务
     *
     * 类比：查询某项工作完成后可以启动哪些后续工作，
     * 比如开发完成后可以开始代码审查和测试准备。
     */
    Set<String> getSuccessors(String taskName);

    /**
     * 根据任务编码获取任务定义
     *
     * 通过任务的唯一编码快速查找对应的任务定义对象。
     * 任务编码是系统分配的唯一标识符，主要用于内部引用。
     *
     * 查询特性：
     * - 基于任务编码的O(1)时间复杂度查询
     * - 任务编码是Long类型的唯一标识
     * - 如果编码不存在应该抛出异常或返回null
     *
     * @param taskCode 任务编码，系统生成的唯一标识
     * @return 对应的任务定义对象，包含任务的所有元数据
     *
     * 类比：根据员工工号查找员工档案，工号是系统分配的唯一标识。
     */
    TaskDefinition getTaskNodeByCode(Long taskCode);

    /**
     * 根据任务名称获取任务定义
     *
     * 通过用户可读的任务名称查找对应的任务定义对象。
     * 任务名称通常用于用户界面显示和日志记录。
     *
     * 查询特性：
     * - 基于任务名称的快速查询
     * - 任务名称是String类型的用户友好标识
     * - 名称在同一工作流中必须唯一
     *
     * @param taskName 任务名称，用户定义的可读标识
     * @return 对应的任务定义对象，包含任务的所有配置信息
     *
     * 类比：根据员工姓名查找员工档案，姓名是用户友好的标识。
     */
    TaskDefinition getTaskNodeByName(String taskName);

    /**
     * 获取图中所有任务节点
     *
     * 返回工作流图中包含的所有任务定义对象的列表。
     * 这是图的完整节点集合，用于图的遍历、统计和分析。
     *
     * 返回特性：
     * - 包含图中的所有任务节点
     * - 不保证返回顺序，可能是任意顺序
     * - 返回的列表应该是不可变的或安全副本
     *
     * @return 所有任务定义的列表，包含完整的任务元数据
     *
     * 类比：获取整个项目的任务清单，用于项目管理和进度跟踪。
     */
    List<TaskDefinition> getAllTaskNodes();

}
