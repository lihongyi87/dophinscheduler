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

package org.apache.dolphinscheduler.server.master.config;

import org.apache.dolphinscheduler.common.utils.NetUtils;
import org.apache.dolphinscheduler.registry.api.enums.RegistryNodeType;
import org.apache.dolphinscheduler.server.master.cluster.loadbalancer.WorkerLoadBalancerConfigurationProperties;

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
 * DolphinScheduler Master节点配置类
 * 
 * 这个类存储了Master节点运行所需的所有配置参数。
 * 可以把它想象为一个“设置面板”，里面有各种可以调节的参数。
 * 
 * 主要包括：
 * - 网络配置：监听端口、心跳间隔等
 * - 性能配置：线程数量、负载保护阈值等
 * - 调度配置：工作流刷新间隔、负载均衡策略等
 * 
 * @ConfigurationProperties 注解表示这个类会从 application.yaml 中的 master 配置项加载值
 */
@Data
@Validated
@Configuration
@ConfigurationProperties(prefix = "master")
@Slf4j
public class MasterConfig implements Validator {

    /**
     * Master RPC服务器监听端口
     * 
     * 这个端口用于接收以下请求：
     * - Worker节点的任务状态上报
     * - 其他Master节点的协调通信
     * - API服务的工作流操作请求
     * 
     * 默认值：5678
     */
    private int listenPort = 5678;

    /**
     * 工作流事件总线触发线程数量
     * 
     * 这些线程负责处理工作流生命周期事件，如：
     * - 工作流启动事件
     * - 任务完成事件
     * - 工作流失败事件
     * 
     * 默认值：CPU核数 * 2 + 1（优化的经验公式）
     */
    private int workflowEventBusFireThreadCount = Runtime.getRuntime().availableProcessors() * 2 + 1;

    /**
     * 逻辑任务配置 - 用于配置在Master端执行的任务
     * 
     * 逻辑任务包括：
     * - 条件判断任务：根据条件决定后续流程
     * - 子工作流任务：调用其他工作流
     * - Switch任务：根据参数选择不同的执行分支
     */
    private LogicTaskConfig logicTaskConfig = new LogicTaskConfig();

    /**
     * Master心跳任务执行间隔
     * 
     * Master节点会定期向注册中心发送心跳信号，包含：
     * - 节点健康状态
     * - 当前资源使用情况（CPU、内存等）
     * - 工作负载情况
     * 
     * 默认值：10秒
     */
    private Duration maxHeartbeatInterval = Duration.ofSeconds(10);

    /**
     * Master服务器负载保护配置
     * 
     * 负载保护机制用于防止Master节点过载：
     * - 当CPU使用率过高时，拒绝接收新的工作流
     * - 当内存使用率过高时，限制并发执行的任务数量
     * - 当磁盘空间不足时，暴停新任务的创建
     */
    private MasterServerLoadProtectionConfig serverLoadProtection = new MasterServerLoadProtectionConfig();

    /**
     * Worker组刷新间隔
     * 
     * Master会定期从注册中心拉取最新的Worker节点列表，
     * 以便进行任务分发时能获取到最新的节点信息。
     * 
     * 默认值：5分钟
     */
    private Duration workerGroupRefreshInterval = Duration.ofMinutes(5);

    private CommandFetchStrategy commandFetchStrategy = new CommandFetchStrategy();

    private WorkerLoadBalancerConfigurationProperties workerLoadBalancerConfigurationProperties =
            new WorkerLoadBalancerConfigurationProperties();

    /**
     * The IP address and listening port of the master server in the format 'ip:listenPort'.
     */
    private String masterAddress;

    /**
     * The registry path for the master server in the format '/nodes/master/ip:listenPort'.
     */
    private String masterRegistryPath;

    @Override
    public boolean supports(Class<?> clazz) {
        return MasterConfig.class.isAssignableFrom(clazz);
    }

    @Override
    public void validate(Object target, Errors errors) {
        MasterConfig masterConfig = (MasterConfig) target;
        if (masterConfig.getListenPort() <= 0) {
            errors.rejectValue("listen-port", null, "is invalidated");
        }

        if (masterConfig.getWorkflowEventBusFireThreadCount() <= 0) {
            errors.rejectValue("workflow-event-bus-fire-thread-count", null, "should be a positive value");
        }

        if (masterConfig.getMaxHeartbeatInterval().toMillis() < 0) {
            errors.rejectValue("max-heartbeat-interval", null, "should be a valid duration");
        }

        if (masterConfig.getWorkerGroupRefreshInterval().getSeconds() < 10) {
            errors.rejectValue("worker-group-refresh-interval", null, "should >= 10s");
        }
        if (StringUtils.isEmpty(masterConfig.getMasterAddress())) {
            masterConfig.setMasterAddress(NetUtils.getAddr(masterConfig.getListenPort()));
        }
        commandFetchStrategy.validate(errors);
        workerLoadBalancerConfigurationProperties.validate(errors);

        masterConfig.setMasterRegistryPath(
                RegistryNodeType.MASTER.getRegistryPath() + "/" + masterConfig.getMasterAddress());
        printConfig();
    }

    private void printConfig() {
        String config =
                "\n****************************Master Configuration**************************************" +
                        "\n  listen-port -> " + listenPort +
                        "\n  workflow-event-bus-fire-thread-count -> " + workflowEventBusFireThreadCount +
                        "\n  logic-task-config -> " + logicTaskConfig +
                        "\n  max-heartbeat-interval -> " + maxHeartbeatInterval +
                        "\n  server-load-protection -> " + serverLoadProtection +
                        "\n  master-address -> " + masterAddress +
                        "\n  master-registry-path: " + masterRegistryPath +
                        "\n  worker-group-refresh-interval: " + workerGroupRefreshInterval +
                        "\n  command-fetch-strategy: " + commandFetchStrategy +
                        "\n  worker-load-balancer-configuration-properties: "
                        + workerLoadBalancerConfigurationProperties +
                        "\n****************************Master Configuration**************************************";
        log.info(config);
    }
}
