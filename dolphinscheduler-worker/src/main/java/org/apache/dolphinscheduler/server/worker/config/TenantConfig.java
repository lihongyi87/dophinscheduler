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

package org.apache.dolphinscheduler.server.worker.config;

import lombok.Data;

/**
 * 租户配置类
 *
 * 用于配置Worker节点的租户相关设置，包括自动创建租户和默认租户的启用状态。
 * 租户是DolphinScheduler中的一个重要概念，用于实现多用户的资源隔离和权限管理。
 *
 * 租户的作用：
 * - 在Linux系统中，租户对应系统用户，任务以租户身份执行
 * - 提供资源隔离，不同租户的任务使用不同的用户权限
 * - 实现文件系统和进程的隔离，保证安全性
 *
 * 配置说明：
 * - autoCreateTenantEnabled: 控制是否自动为新用户创建系统租户
 * - defaultTenantEnabled: 控制是否使用默认租户执行任务
 *
 * 使用场景：
 * - 多用户环境下的权限管理
 * - 不同业务部门的资源隔离
 * - 开发、测试、生产环境的权限控制
 */
@Data
public class TenantConfig {

    /**
     * 是否启用自动创建租户功能
     * 当设置为true时，系统会自动为新用户创建对应的租户
     * 默认值：true
     */
    private boolean autoCreateTenantEnabled = true;
    
    /**
     * 是否启用默认租户功能
     * 当设置为true时，系统会使用默认租户来执行任务
     * 默认值：false
     */
    private boolean defaultTenantEnabled = false;
}
