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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * 任务实例工厂容器
 *
 * 集中管理所有类型的任务实例工厂，提供统一的工厂访问接口。
 * 使用Spring的依赖注入机制管理各种工厂实例，简化工厂的获取和使用。
 *
 * 设计目的：
 * - 工厂聚合：将不同类型的任务实例工厂集中管理
 * - 统一接口：提供一致的工厂获取方式
 * - 依赖管理：通过Spring容器管理工厂的生命周期
 * - 代码简化：避免在使用方直接注入多个工厂
 *
 * 包含的工厂类型：
 * 1. 首次运行工厂：创建全新的任务实例
 * 2. 重试工厂：创建重试执行的任务实例
 * 3. 失败恢复工厂：创建失败恢复的任务实例
 * 4. 暂停恢复工厂：创建暂停恢复的任务实例
 * 5. 故障转移工厂：创建故障转移的任务实例
 *
 * 使用场景：
 * - 任务执行器根据不同场景选择合适的工厂
 * - 统一的工厂管理和配置
 * - 便于工厂的扩展和维护
 *
 * 类比理解：
 * 就像一个完整的生产车间调度中心，包含不同类型的生产线：
 * - 新品生产线：首次运行工厂
 * - 返工生产线：重试工厂
 * - 修复生产线：恢复工厂
 * - 转移生产线：故障转移工厂
 */
@Component
public class TaskInstanceFactories {

    /**
     * 首次运行任务实例工厂
     * 专门用于创建全新的任务实例，适用于任务的首次执行
     */
    @Autowired
    private FirstRunTaskInstanceFactory firstRunTaskInstanceFactory;

    /**
     * 重试任务实例工厂
     * 专门用于创建重试任务实例，适用于任务执行失败后的重试
     */
    @Autowired
    private RetryTaskInstanceFactory retryTaskInstanceFactory;

    /**
     * 失败恢复任务实例工厂
     * 专门用于创建失败恢复任务实例，适用于失败任务的恢复执行
     */
    @Autowired
    private FailedRecoverTaskInstanceFactory failedRecoverTaskInstanceFactory;

    /**
     * 暂停恢复任务实例工厂
     * 专门用于创建暂停恢复任务实例，适用于暂停任务的恢复执行
     */
    @Autowired
    private PauseRecoverTaskInstanceFactory pauseRecoverTaskInstanceFactory;

    /**
     * 故障转移任务实例工厂
     * 专门用于创建故障转移任务实例，适用于节点故障时的任务转移
     */
    @Autowired
    private FailoverTaskInstanceFactory failoverTaskInstanceFactory;

    /**
     * 获取首次运行任务实例工厂
     * @return 首次运行任务实例工厂
     */
    public FirstRunTaskInstanceFactory firstRunTaskInstanceFactory() {
        return firstRunTaskInstanceFactory;
    }

    /**
     * 获取重试任务实例工厂
     * @return 重试任务实例工厂
     */
    public RetryTaskInstanceFactory retryTaskInstanceFactory() {
        return retryTaskInstanceFactory;
    }

    /**
     * 获取失败恢复任务实例工厂
     * @return 失败恢复任务实例工厂
     */
    public FailedRecoverTaskInstanceFactory failedRecoverTaskInstanceFactory() {
        return failedRecoverTaskInstanceFactory;
    }

    /**
     * 获取故障转移任务实例工厂
     * @return 故障转移任务实例工厂
     */
    public FailoverTaskInstanceFactory failoverTaskInstanceFactory() {
        return failoverTaskInstanceFactory;
    }

    /**
     * 获取暂停恢复任务实例工厂
     * @return 暂停恢复任务实例工厂
     */
    public PauseRecoverTaskInstanceFactory pauseRecoverTaskInstanceFactory() {
        return pauseRecoverTaskInstanceFactory;
    }
}
