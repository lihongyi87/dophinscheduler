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

import org.apache.dolphinscheduler.common.utils.NetUtils;
import org.apache.dolphinscheduler.registry.api.enums.RegistryNodeType;

import org.apache.commons.lang3.StringUtils;

import java.time.Duration;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.validation.Errors;
import org.springframework.validation.Validator;
import org.springframework.validation.annotation.Validated;

/**
 * DolphinScheduler Worker节点配置类
 * 
 * 这个类存储了Worker节点运行所需的所有配置参数。
 * 可以把它想象为一个“工人身份证”，记录了这个Worker的各种信息。
 * 
 * 主要包括：
 * - 基本信息：监听端口、心跳间隔等
 * - 负载信息：权重值、负载保护配置等
 * - 分组信息：所属的Worker组
 * - 任务配置：物理任务执行参数
 * - 租户配置：任务执行时的用户权限配置
 * 
 * @ConfigurationProperties 注解表示这个类会从 application.yaml 中的 worker 配置项加载值
 */
@Data
@Validated
@Configuration
@ConfigurationProperties(prefix = "worker")
@Slf4j
public class WorkerConfig implements Validator {

    /**
     * Worker RPC服务器监听端口
     * 
     * 这个端口用于接收以下请求：
     * - Master节点的任务分发请求
     * - API服务的日志查看请求
     * - 其他管理操作请求
     * 
     * 默认值：1234
     */
    private int listenPort = 1234;
    
    /**
     * Worker心跳任务执行间隔
     * 
     * Worker节点会定期向注册中心发送心跳信号，包含：
     * - 节点健康状态
     * - 当前资源使用情况（CPU、内存、磁盘）
     * - 正在执行的任务数量
     * 
     * 默认值：10秒
     */
    private Duration maxHeartbeatInterval = Duration.ofSeconds(10);
    
    /**
     * Worker节点权重
     * 
     * 用于任务分发时的负载均衡计算。
     * 权重越高的Worker会被分配更多的任务。
     * 
     * 使用场景：
     * - 高配置服务器可以设置更高的权重
     * - 低配置或测试服务器可以设置较低的权重
     * 
     * 默认值：100
     */
    private int hostWeight = 100;
    
    /**
     * Worker服务器负载保护配置
     * 
     * 负载保护机制用于防止Worker节点过载：
     * - 当CPU使用率过高时，不接收新任务
     * - 当内存使用率过高时，拒绝执行内存密集型任务
     * - 当磁盘空间不足时，停止接收需要大量磁盘I/O的任务
     */
    private WorkerServerLoadProtectionConfig serverLoadProtection = new WorkerServerLoadProtectionConfig();
    
    /**
     * Worker组名
     *
     * Worker组用于将Worker节点按照功能或环境分类。
     * 原理：
     * - 不同的任务可以分发给不同的Worker组
     * - 可以实现资源隔离和环境隔离
     *
     * 例如：
     * - "default": 通用Worker组
     * - "big-data": 大数据处理Worker组
     * - "gpu": GPU计算Worker组
     *
     * 如果不设置，默认为 "default"
     */
    private String group;

    /**
     * Worker地址 - 由 workerIp:listenPort 自动计算生成
     * 
     * 这个字段不需要在配置文件中手动设置，
     * 系统会自动根据本机IP和监听端口生成。
     * 
     * 格式示例："192.168.1.100:1234"
     */
    private String workerAddress;
    
    /**
     * Worker注册路径 - 在注册中心中的路径
     * 
     * 这个字段也不需要手动设置，系统会根据注册中心类型和节点信息生成。
     * 
     * 格式示例："/dolphinscheduler/workers/default/192.168.1.100:1234"
     */
    private String workerRegistryPath;

    /**
     * 租户配置 - 用于任务执行时的用户权限管理
     * 
     * 在Linux系统中，任务需要以特定用户身份执行，
     * 这个配置控制如何处理用户权限问题。
     */
    private TenantConfig tenantConfig = new TenantConfig();

    /**
     * 物理任务配置 - 用于配置Worker端任务执行参数
     * 
     * 包含：
     * - 任务执行线程数量
     * - 任务超时设置
     * - 任务执行环境配置等
     */
    private PhysicalTaskConfig physicalTaskConfig = new PhysicalTaskConfig();

    /**
     * 检查是否支持指定的配置类类型
     *
     * @param clazz 要检查的类类型
     * @return 如果是WorkerConfig类类型或其子类，返回true
     */
    @Override
    public boolean supports(Class<?> clazz) {
        // 使用反射检查传入的类是否为WorkerConfig类或其子类
        // isAssignableFrom方法判断当前类是否为指定类的父类或相同类
        return WorkerConfig.class.isAssignableFrom(clazz);
    }

    /**
     * 验证Worker配置的有效性并进行初始化
     *
     * 这个方法会：
     * 1. 验证配置参数的合法性
     * 2. 自动生成一些派生配置（如Worker地址、注册路径等）
     * 3. 设置默认值
     * 4. 打印最终的配置信息
     *
     * @param target 要验证的配置对象
     * @param errors 验证错误收集器
     */
    @Override
    public void validate(Object target, Errors errors) {
        // 将传入的通用对象转换为WorkerConfig类型，以便访问具体的配置属性
        WorkerConfig workerConfig = (WorkerConfig) target;

        // 验证心跳间隔必须大于0秒，确保Worker能正常向注册中心发送心跳
        // 如果心跳间隔无效，将错误信息添加到errors对象中
        if (workerConfig.getMaxHeartbeatInterval().getSeconds() <= 0) {
            errors.rejectValue("max-heartbeat-interval", null, "should be a valid duration");
        }

        // 如果用户没有手动配置Worker地址，系统自动生成
        // NetUtils.getAddr会获取本机IP地址并拼接上监听端口
        if (StringUtils.isEmpty(workerConfig.getWorkerAddress())) {
            workerConfig.setWorkerAddress(NetUtils.getAddr(workerConfig.getListenPort()));
        }

        // 根据注册中心的路径规范和Worker地址，自动生成注册路径
        // 格式：/dolphinscheduler/workers/{workerAddress}
        // 这个路径用于在Zookeeper等注册中心中标识该Worker节点
        workerConfig.setWorkerRegistryPath(
                RegistryNodeType.WORKER.getRegistryPath() + "/" + workerConfig.getWorkerAddress());

        // 如果用户没有配置Worker组名，设置为默认组"default"
        // Worker组用于任务分发时的节点分组管理
        if (StringUtils.isEmpty(group)) {
            workerConfig.setGroup("default");
        }

        // 调用打印方法，将最终的配置信息输出到日志中，便于运维人员检查
        printConfig();
    }

    /**
     * 打印Worker配置信息到日志
     *
     * 这个方法会将所有重要的配置参数格式化输出到日志中，
     * 方便运维人员检查配置是否正确。
     */
    private void printConfig() {
        // 构建格式化的配置信息字符串，包含所有关键配置参数
        // 使用分隔线和缩进格式，提高日志的可读性
        String config =
                // 配置信息开始标记，使用星号分隔线突出显示
                "\n****************************Worker Configuration**************************************" +
                        // Worker RPC服务监听端口，用于接收Master分发的任务
                        "\n  listen-port -> " + listenPort +
                        // 心跳发送间隔，Worker向注册中心汇报状态的频率
                        "\n  max-heartbeat-interval -> " + maxHeartbeatInterval +
                        // Worker权重值，影响任务分发时的负载均衡计算
                        "\n  host-weight -> " + hostWeight +
                        // 租户配置信息，用于任务执行时的用户权限管理
                        "\n  tenantConfig -> " + tenantConfig +
                        // 服务器负载保护配置，防止资源使用过度
                        "\n  server-load-protection -> " + serverLoadProtection +
                        // Worker完整地址（IP:端口），其他节点通过此地址访问
                        "\n  address -> " + workerAddress +
                        // 在注册中心的注册路径，用于服务发现
                        "\n  registry-path: " + workerRegistryPath +
                        // 物理任务执行相关配置，如线程池大小等
                        "\n  physical-task-config -> " + physicalTaskConfig +
                        // Worker所属分组，用于任务调度时的节点分类
                        "\n  group -> " + group +
                        // 配置信息结束标记
                        "\n****************************Worker Configuration**************************************";
        // 将完整的配置信息以INFO级别输出到日志
        log.info(config);
    }
}
