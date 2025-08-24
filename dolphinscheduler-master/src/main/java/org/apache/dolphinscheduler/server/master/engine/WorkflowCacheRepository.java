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

package org.apache.dolphinscheduler.server.master.engine;

import org.apache.dolphinscheduler.server.master.engine.workflow.runnable.IWorkflowExecutionRunnable;
import org.apache.dolphinscheduler.server.master.metrics.WorkflowInstanceMetrics;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.PostConstruct;

import lombok.NonNull;

import org.springframework.stereotype.Component;

import com.google.common.collect.ImmutableList;

/**
 * 工作流缓存仓库
 * 
 * 这是Master节点中用于缓存正在执行工作流实例的核心仓库组件。
 * 类比：一个高效的项目档案管理系统，实时记录所有正在执行的项目状态。
 * 
 * 主要职责：
 * 1. 内存缓存：在内存中缓存正在执行的工作流实例，提供快速访问能力
 * 2. 实例管理：提供工作流实例的增删改查操作接口
 * 3. 监控集成：集成监控指标，实时报告运行中的工作流实例数量
 * 4. 并发安全：使用线程安全的数据结构确保多线程环境下的数据一致性
 * 
 * 设计特点：
 * - 高性能：基于ConcurrentHashMap实现，支持高并发读写操作
 * - 线程安全：所有操作都是线程安全的，适合多线程环境
 * - 监控友好：自动注册监控指标，便于运维监控
 * - 内存高效：只缓存正在执行的实例，避免内存溢出
 * 
 * 使用场景：
 * - 工作流启动时将实例加入缓存
 * - 执行过程中快速查询工作流状态
 * - 工作流完成或失败时从缓存移除
 * - 系统监控时统计运行中的工作流数量
 * 
 * 类比理解：
 * 就像一个项目管理办公室的"进行中项目"白板：
 * - 新项目开始时添加到白板上
 * - 项目执行过程中可以快速查看状态
 * - 项目完成时从白板上擦除
 * - 管理层可以一目了然地看到有多少个项目在进行中
 */
@Component
public class WorkflowCacheRepository implements IWorkflowRepository {

    /**
     * 工作流实例缓存映射表
     * 
     * 使用ConcurrentHashMap存储正在执行的工作流实例。
     * Key: 工作流实例ID（Integer）
     * Value: 工作流执行实例（IWorkflowExecutionRunnable）
     * 
     * 选择ConcurrentHashMap的原因：
     * - 线程安全：支持多线程并发读写
     * - 高性能：读操作无锁，写操作局部锁
     * - 弱一致性：满足工作流管理的一致性需求
     * 
     * 类比：一个智能的项目档案柜，每个抽屉存放一个项目的文件夹。
     */
    private final Map<Integer, IWorkflowExecutionRunnable> workflowExecutionRunnableMap = new ConcurrentHashMap<>();

    /**
     * 初始化监控指标
     * 
     * 在Bean初始化完成后自动注册工作流实例数量的监控指标。
     * 这个指标会实时反映当前Master节点正在处理的工作流实例数量。
     * 
     * 监控意义：
     * - 性能监控：了解系统负载情况
     * - 容量规划：为集群扩容提供数据支持
     * - 问题诊断：异常情况下快速定位问题
     * 
     * 类比：在项目管理办公室安装一个数字显示器，实时显示正在进行的项目数量。
     */
    @PostConstruct
    public void registerMetrics() {
        WorkflowInstanceMetrics.registerWorkflowInstanceRunningGauge(workflowExecutionRunnableMap::size);
    }

    /**
     * 根据工作流实例ID获取工作流执行实例
     * 
     * 这是最常用的查询方法，根据工作流实例ID快速定位到对应的执行实例。
     * 时间复杂度：O(1)，哈希表查找操作。
     * 
     * @param workflowInstanceId 工作流实例ID，全局唯一标识符
     * @return 工作流执行实例，如果不存在则返回null
     * 
     * 类比：根据项目编号快速找到对应的项目文件夹。
     */
    @Override
    public IWorkflowExecutionRunnable get(final int workflowInstanceId) {
        return workflowExecutionRunnableMap.get(workflowInstanceId);
    }

    /**
     * 检查是否包含指定的工作流实例
     * 
     * 判断缓存中是否存在指定ID的工作流实例。
     * 这个方法用于快速检查工作流是否正在执行中，避免重复启动。
     * 
     * @param workflowInstanceId 工作流实例ID
     * @return true表示存在，false表示不存在
     * 
     * 类比：检查项目白板上是否已经有某个项目的记录。
     */
    @Override
    public boolean contains(final int workflowInstanceId) {
        return workflowExecutionRunnableMap.containsKey(workflowInstanceId);
    }

    /**
     * 从缓存中移除工作流实例
     * 
     * 当工作流执行完成（成功或失败）时，从缓存中移除该实例以释放内存。
     * 这是工作流生命周期的最后一步，确保内存不会无限增长。
     * 
     * @param workflowInstanceId 要移除的工作流实例ID
     * 
     * 类比：项目完成后从白板上擦除该项目的记录。
     */
    @Override
    public void remove(final int workflowInstanceId) {
        workflowExecutionRunnableMap.remove(workflowInstanceId);
    }

    /**
     * 将工作流实例添加到缓存中
     * 
     * 当新的工作流开始执行时，将其加入缓存进行管理。
     * 使用@NonNull注解确保传入的参数不为空，避免空指针异常。
     * 
     * @param workflowExecutionRunnable 工作流执行实例，必须非空
     * 
     * 类比：新项目开始时将项目信息写到白板上。
     */
    @Override
    public void put(@NonNull final IWorkflowExecutionRunnable workflowExecutionRunnable) {
        final Integer workflowInstanceId = workflowExecutionRunnable.getId();
        workflowExecutionRunnableMap.put(workflowInstanceId, workflowExecutionRunnable);
    }

    /**
     * 获取所有正在执行的工作流实例
     * 
     * 返回当前缓存中所有工作流实例的不可变集合。
     * 使用ImmutableList确保返回的集合不能被修改，保护内部数据结构。
     * 
     * 使用场景：
     * - 系统监控：查看所有运行中的工作流
     * - 批量操作：对所有工作流执行某些操作
     * - 故障恢复：Master节点重启时重新加载工作流状态
     * 
     * @return 所有工作流实例的不可变集合
     * 
     * 类比：拍摄项目白板的照片，获得当前所有项目的快照。
     */
    @Override
    public Collection<IWorkflowExecutionRunnable> getAll() {
        return ImmutableList.copyOf(workflowExecutionRunnableMap.values());
    }

}