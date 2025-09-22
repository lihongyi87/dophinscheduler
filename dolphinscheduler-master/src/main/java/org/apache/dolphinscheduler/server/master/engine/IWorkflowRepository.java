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

import java.util.Collection;

import lombok.NonNull;

/**
 * 工作流仓库接口
 * 用于管理和存储工作流执行运行时对象的仓库，提供增删改查功能
 */
public interface IWorkflowRepository {

    /**
     * 根据工作流实例ID获取工作流执行运行时对象
     *
     * @param workflowInstanceId 工作流实例ID
     * @return 工作流执行运行时对象，如果不存在则返回null
     */
    IWorkflowExecutionRunnable get(int workflowInstanceId);

    /**
     * 获取所有工作流执行运行时对象
     *
     * @return 包含所有工作流执行运行时对象的集合
     */
    Collection<IWorkflowExecutionRunnable> getAll();

    /**
     * 存储工作流执行运行时对象到仓库
     *
     * @param workflowExecuteThread 工作流执行运行时对象，不能为null
     */
    void put(@NonNull IWorkflowExecutionRunnable workflowExecuteThread);

    /**
     * 检查仓库中是否包含指定工作流实例ID的对象
     *
     * @param workflowInstanceId 工作流实例ID
     * @return 如果包含则返回true，否则返回false
     */
    boolean contains(int workflowInstanceId);

    /**
     * 从仓库中移除指定工作流实例ID的对象
     *
     * @param workflowInstanceId 工作流实例ID
     */
    void remove(int workflowInstanceId);

}
