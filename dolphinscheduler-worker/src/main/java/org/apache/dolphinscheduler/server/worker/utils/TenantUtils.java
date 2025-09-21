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

package org.apache.dolphinscheduler.server.worker.utils;

import org.apache.dolphinscheduler.common.constants.TenantConstants;
import org.apache.dolphinscheduler.common.utils.OSUtils;
import org.apache.dolphinscheduler.plugin.task.api.TaskException;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.server.worker.config.TenantConfig;
import org.apache.dolphinscheduler.server.worker.config.WorkerConfig;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

/**
 * 租户工具类
 *
 * 此工具类负责管理DolphinScheduler中的租户机制，提供租户相关的验证、创建和安全隔离功能。
 * 租户机制是DolphinScheduler多租户架构的核心组件，确保不同租户之间的任务执行环境隔离。
 *
 * 租户安全隔离机制：
 * 1. 进程级隔离：每个租户使用独立的系统用户执行任务，防止跨租户数据访问
 * 2. 文件系统隔离：不同租户的任务在不同的用户权限下运行，确保文件访问安全
 * 3. 资源隔离：通过操作系统的用户权限控制，限制租户对系统资源的访问
 * 4. 环境隔离：每个租户可以有独立的环境变量和配置
 *
 * 主要功能：
 * 1. 判断租户功能是否启用（基于sudo权限检查）
 * 2. 获取或创建实际的租户用户
 * 3. 处理默认租户的特殊逻辑
 * 4. 自动创建租户用户（如果配置允许）
 * 5. 验证租户用户是否存在
 * 6. 提供租户相关的判断方法
 *
 * 租户类型说明：
 * - 默认租户：使用系统默认用户，适用于测试环境或单租户场景
 * - 普通租户：使用独立的系统用户，提供完整的隔离保护
 * - 引导租户：系统启动用户，具有特殊权限
 */
@Slf4j
@UtilityClass
public class TenantUtils {

    /**
     * 判断租户功能是否启用
     *
     * 当前实现通过检查sudo权限来判断租户功能是否启用。
     * 这是因为租户隔离需要以不同的系统用户身份执行任务，
     * 而切换用户需要sudo权限。
     *
     * 注意：未来应该将租户功能和sudo功能分开配置，
     * 这样可以更灵活地控制租户机制的启用。
     *
     * @return true如果租户功能启用，false否则
     */
    public static boolean isTenantEnable() {
        // todo: add tenantEnable in workerConfig, the tenantEnable shouldn't judged by sudoEnable, these should be two
        // config
        // 通过检查sudo功能是否启用来判断租户功能是否可用
        // 这是一个临时的实现方式，未来应该独立配置租户功能开关
        // 租户功能需要sudo权限来切换不同的系统用户执行任务
        return OSUtils.isSudoEnable();
    }

    /**
     * 获取或创建实际的租户用户
     *
     * 此方法是租户管理的核心功能，负责根据配置和任务上下文确定实际执行任务的租户用户。
     * 它实现了复杂的租户决策逻辑，确保任务在正确的用户环境下执行。
     *
     * 执行逻辑：
     * 1. 如果租户功能未启用，使用引导用户执行所有任务
     * 2. 如果是默认租户且默认租户功能启用，使用引导用户
     * 3. 如果是普通租户，检查租户是否存在，必要时自动创建
     * 4. 验证最终确定的租户用户确实存在于系统中
     *
     * 安全考虑：
     * - 只有在明确配置允许的情况下才会自动创建租户用户
     * - 严格验证租户用户的存在性，防止安全漏洞
     * - 对于不存在的租户用户会抛出异常，避免任务执行失败
     *
     * @param workerConfig Worker配置，包含租户相关的配置选项
     * @param taskExecutionContext 任务执行上下文，包含租户代码信息
     * @return 实际用于执行任务的租户用户名
     * @throws TaskException 如果租户配置不正确或租户用户不存在
     */
    public static String getOrCreateActualTenant(WorkerConfig workerConfig, TaskExecutionContext taskExecutionContext) {
        // 获取Worker的租户配置信息
        TenantConfig tenantConfig = workerConfig.getTenantConfig();

        // 如果租户功能未启用，使用引导用户
        // 在租户功能禁用的情况下，所有任务都将以系统启动用户的身份运行
        if (!isTenantEnable()) {
            log.info("Tenant is not enabled, will use the bootstrap: {} user as tenant", getBootstrapTenant());
            return getBootstrapTenant();
        }

        // 从任务执行上下文中获取租户代码
        String tenantCode = taskExecutionContext.getTenantCode();

        // 处理默认租户的逻辑
        // 默认租户是系统预定义的特殊租户，用于测试或简化部署场景
        if (isDefaultTenant(tenantCode)) {
            // 检查是否允许使用默认租户
            if (tenantConfig.isDefaultTenantEnabled()) {
                log.info("Current tenant is default tenant, will use bootstrap user: {} to execute the task",
                        getBootstrapTenant());
                // 默认租户使用引导用户执行任务，避免创建额外的系统用户
                return getBootstrapTenant();
            } else {
                // 如果默认租户功能被禁用，抛出异常提示用户启用配置
                throw new TaskException(
                        "The tenantCode is " + tenantCode + ", please enable TenantConfig#isDefaultTenantEnabled");
            }
        }

        // 如果配置允许，自动创建不存在的租户用户
        // 这是一个便利功能，可以自动在系统中创建对应的用户账号
        if (tenantConfig.isAutoCreateTenantEnabled()) {
            // 检查用户是否存在，如果不存在则创建
            // 这里会调用系统命令创建用户，需要适当的权限
            OSUtils.createUserIfAbsent(tenantCode);
        }

        // 验证租户用户是否存在
        // 确保任务执行前租户用户确实存在于系统中
        if (!tenantExists(tenantCode)) {
            // 如果租户用户不存在且未启用自动创建，抛出异常
            throw new TaskException(String.format("TenantCode: %s doesn't exist", tenantCode));
        }
        // 返回确认存在的租户代码，供任务执行时使用
        return tenantCode;
    }

    /**
     * 判断指定的租户代码是否为默认租户
     *
     * 默认租户是系统预定义的特殊租户，通常用于测试环境或
     * 不需要严格租户隔离的场景。
     *
     * @param tenantCode 要检查的租户代码
     * @return true如果是默认租户，false否则
     */
    public static boolean isDefaultTenant(String tenantCode) {
        // 通过比较租户代码与预定义的默认租户常量来判断
        // 默认租户是系统内置的特殊租户，通常用于测试环境
        return TenantConstants.DEFAULT_TENANT_CODE.equals(tenantCode);
    }

    /**
     * 获取引导租户（系统启动用户）
     *
     * 引导租户是系统启动时使用的用户，通常具有较高的权限。
     * 在租户功能未启用或使用默认租户时，会使用此用户执行任务。
     *
     * @return 引导租户的用户名
     */
    public static String getBootstrapTenant() {
        // 返回系统启动时的用户名，作为引导租户
        // 引导租户通常是启动DolphinScheduler服务的系统用户
        return TenantConstants.BOOTSTRAP_SYSTEM_USER;
    }

    /**
     * 判断指定的租户代码是否为引导租户
     *
     * @param tenantCode 要检查的租户代码
     * @return true如果是引导租户，false否则
     */
    public static boolean isBootstrapTenant(String tenantCode) {
        // 通过比较租户代码与引导系统用户常量来判断
        // 引导租户是系统启动用户，具有特殊的权限和地位
        return TenantConstants.BOOTSTRAP_SYSTEM_USER.equals(tenantCode);
    }

    /**
     * 检查指定的租户用户是否在系统中存在
     *
     * 此方法通过查询系统用户列表来验证租户用户的存在性，
     * 这是确保任务能够正确执行的重要安全检查。
     *
     * @param tenantCode 要检查的租户代码（用户名）
     * @return true如果租户用户存在，false否则
     */
    public static boolean tenantExists(String tenantCode) {
        // 获取系统中所有用户的列表
        // 通过检查用户列表中是否包含指定的租户代码来验证租户是否存在
        // 这里假设租户代码与系统用户名一致
        return OSUtils.getUserList().contains(tenantCode);
    }

}
