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

    /**
     * 命令获取策略配置
     *
     * 定义Master节点如何从数据库中获取待执行的命令。
     * 包括获取频率、数量控制、负载均衡等策略。
     *
     * 默认使用基于ID槽位的获取策略，在多Master环境中能有效避免重复获取。
     */
    private CommandFetchStrategy commandFetchStrategy = new CommandFetchStrategy();

    /**
     * Worker节点负载均衡配置
     *
     * 用于配置Master向Worker节点分发任务时的负载均衡策略。
     * 支持多种算法：轮询、随机、基于负载等。
     *
     * 主要包括：
     * - 负载均衡算法选择
     * - 节点权重配置
     * - 健康检查参数
     * - 故障转移策略
     */
    private WorkerLoadBalancerConfigurationProperties workerLoadBalancerConfigurationProperties =
            new WorkerLoadBalancerConfigurationProperties();

    /**
     * Master服务器地址
     *
     * Master节点的完整网络地址，格式为 'ip:端口'。
     * 这个地址用于：
     * - 其他节点连接到当前Master
     * - 注册中心记录节点信息
     * - 集群内节点通信
     *
     * 如果未手动设置，系统会自动获取本机IP和监听端口组合。
     */
    private String masterAddress;

    /**
     * Master节点在注册中心的路径
     *
     * 在分布式注册中心（如ZooKeeper）中的完整路径，
     * 格式为 '/nodes/master/ip:端口'。
     *
     * 这个路径用于：
     * - 其他节点发现当前Master
     * - 集群选主和故障转移
     * - 监控和管理工具获取节点信息
     *
     * 系统会根据masterAddress自动生成这个路径。
     */
    private String masterRegistryPath;

    /**
     * 检查当前验证器是否支持指定的类
     *
     * 这是Spring Validator接口的必需方法，用于告诉Spring框架
     * 这个验证器可以验证哪些类型的对象。
     *
     * @param clazz 需要验证的类类型
     * @return true 如果可以验证该类型，false 否则
     */
    @Override
    public boolean supports(Class<?> clazz) {
        // 检查传入的类是否是MasterConfig类或其子类
        // isAssignableFrom方法返回true表示clazz是MasterConfig的子类或本身
        return MasterConfig.class.isAssignableFrom(clazz);
    }

    /**
     * 验证Master配置参数的有效性
     *
     * 这个方法是Spring Validator接口的核心方法，在配置加载时自动执行。
     * 它会检查所有配置项是否符合业务要求，如果有问题就报错。
     *
     * 验证内容包括：
     * - 端口号必须是正数
     * - 线程数量必须是正数
     * - 时间间隔必须有效
     * - 自动生成缺失的配置项
     *
     * @param target 要验证的配置对象
     * @param errors Spring的错误收集器，用于记录验证失败的信息
     */
    @Override
    public void validate(Object target, Errors errors) {
        // 将传入的对象转换为MasterConfig类型
        MasterConfig masterConfig = (MasterConfig) target;

        // 验证监听端口号是否有效（必须是正数）
        if (masterConfig.getListenPort() <= 0) {
            // 如果端口号不是正数，记录错误信息
            errors.rejectValue("listen-port", null, "is invalidated");
        }

        // 验证工作流事件总线线程数是否有效（必须是正数）
        if (masterConfig.getWorkflowEventBusFireThreadCount() <= 0) {
            // 如果线程数不是正数，记录错误信息
            errors.rejectValue("workflow-event-bus-fire-thread-count", null, "should be a positive value");
        }

        // 验证心跳间隔时间是否有效（不能是负数）
        if (masterConfig.getMaxHeartbeatInterval().toMillis() < 0) {
            // 如果心跳间隔是负数，记录错误信息
            errors.rejectValue("max-heartbeat-interval", null, "should be a valid duration");
        }

        // 验证Worker组刷新间隔是否合理（至少10秒）
        // 太短的刷新间隔会给注册中心造成不必要的压力
        if (masterConfig.getWorkerGroupRefreshInterval().getSeconds() < 10) {
            errors.rejectValue("worker-group-refresh-interval", null, "should >= 10s");
        }

        // 如果Master地址没有手动配置，则自动生成
        // 地址格式为：本机IP:监听端口
        if (StringUtils.isEmpty(masterConfig.getMasterAddress())) {
            masterConfig.setMasterAddress(NetUtils.getAddr(masterConfig.getListenPort()));
        }

        // 验证命令获取策略配置是否有效
        commandFetchStrategy.validate(errors);

        // 验证Worker负载均衡配置是否有效
        workerLoadBalancerConfigurationProperties.validate(errors);

        // 根据Master地址自动生成在注册中心的路径
        // 路径格式：/nodes/master/IP:端口
        masterConfig.setMasterRegistryPath(
                RegistryNodeType.MASTER.getRegistryPath() + "/" + masterConfig.getMasterAddress());

        // 打印配置信息到日志，方便调试和运维
        printConfig();
    }

    /**
     * 打印Master配置信息到日志
     *
     * 这个方法在Master启动时被调用，将所有重要的配置参数输出到日志中。
     * 主要用于：
     * - 运维人员快速查看当前配置
     * - 问题排查时确认配置是否正确
     * - 配置变更后的确认
     */
    private void printConfig() {
        // 构建包含所有配置信息的格式化字符串
        // 使用分隔线和缩进来提高可读性
        String config =
                "\n****************************Master Configuration**************************************" +
                        "\n  listen-port -> " + listenPort +                                // RPC服务监听端口
                        "\n  workflow-event-bus-fire-thread-count -> " + workflowEventBusFireThreadCount +  // 工作流事件处理线程数
                        "\n  logic-task-config -> " + logicTaskConfig +                     // 逻辑任务配置
                        "\n  max-heartbeat-interval -> " + maxHeartbeatInterval +           // 最大心跳间隔
                        "\n  server-load-protection -> " + serverLoadProtection +          // 服务器负载保护配置
                        "\n  master-address -> " + masterAddress +                         // Master服务器地址
                        "\n  master-registry-path: " + masterRegistryPath +                // 注册中心路径
                        "\n  worker-group-refresh-interval: " + workerGroupRefreshInterval + // Worker组刷新间隔
                        "\n  command-fetch-strategy: " + commandFetchStrategy +            // 命令获取策略
                        "\n  worker-load-balancer-configuration-properties: "              // Worker负载均衡配置
                        + workerLoadBalancerConfigurationProperties +
                        "\n****************************Master Configuration**************************************";

        // 使用INFO级别输出配置信息，确保在生产环境中也能看到
        log.info(config);
    }
}
