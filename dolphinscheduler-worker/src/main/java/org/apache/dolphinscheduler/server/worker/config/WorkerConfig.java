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
     * 传理：
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

    @Override
    public boolean supports(Class<?> clazz) {
        return WorkerConfig.class.isAssignableFrom(clazz);
    }

    @Override
    public void validate(Object target, Errors errors) {
        WorkerConfig workerConfig = (WorkerConfig) target;
        if (workerConfig.getMaxHeartbeatInterval().getSeconds() <= 0) {
            errors.rejectValue("max-heartbeat-interval", null, "should be a valid duration");
        }
        if (StringUtils.isEmpty(workerConfig.getWorkerAddress())) {
            workerConfig.setWorkerAddress(NetUtils.getAddr(workerConfig.getListenPort()));
        }

        workerConfig.setWorkerRegistryPath(
                RegistryNodeType.WORKER.getRegistryPath() + "/" + workerConfig.getWorkerAddress());

        if (StringUtils.isEmpty(group)) {
            workerConfig.setGroup("default");
        }

        printConfig();
    }

    private void printConfig() {
        String config =
                "\n****************************Worker Configuration**************************************" +
                        "\n  listen-port -> " + listenPort +
                        "\n  max-heartbeat-interval -> " + maxHeartbeatInterval +
                        "\n  host-weight -> " + hostWeight +
                        "\n  tenantConfig -> " + tenantConfig +
                        "\n  server-load-protection -> " + serverLoadProtection +
                        "\n  address -> " + workerAddress +
                        "\n  registry-path: " + workerRegistryPath +
                        "\n  physical-task-config -> " + physicalTaskConfig +
                        "\n  group -> " + group +
                        "\n****************************Worker Configuration**************************************";
        log.info(config);
    }
}
