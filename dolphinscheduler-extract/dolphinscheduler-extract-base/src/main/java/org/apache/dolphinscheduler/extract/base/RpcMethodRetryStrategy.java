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

package org.apache.dolphinscheduler.extract.base;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.net.ConnectException;

/**
 * RPC方法重试策略配置注解
 *
 * 用于配置RPC方法调用失败时的重试行为，提供细粒度的容错控制机制。
 * 在分布式环境中，网络波动、服务暂时不可用等问题很常见，合理的重试策略可以显著提高系统的可靠性。
 * 类比：给快递员设定投递规则 - 如果第一次投递失败，间隔多长时间再试，最多尝试几次，什么情况下才重试。
 *
 * 重试机制的价值：
 * 1. 网络抖动容忍：短暂的网络问题可以通过重试自动恢复
 * 2. 服务恢复等待：目标服务重启时可以等待其重新可用
 * 3. 负载分散：通过重试间隔避免瞬间大量请求
 * 4. 提升用户体验：减少因临时故障导致的操作失败
 *
 * 使用场景：
 * - 关键业务接口：如工作流状态同步、任务分发等不能失败的操作
 * - 网络敏感操作：跨机房、跨网络的远程调用
 * - 外部依赖调用：调用可能不稳定的第三方服务
 * - 资源竞争场景：可能因为并发导致暂时失败的操作
 *
 * 重试策略设计原则：
 * - 快速失败 vs 持久重试：根据业务重要性选择
 * - 指数退避：避免重试风暴，逐渐增加重试间隔
 * - 异常分类：只对可恢复的异常进行重试
 * - 资源保护：设置合理的最大重试次数避免资源耗尽
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface RpcMethodRetryStrategy {

    /**
     * 最大重试次数
     *
     * 定义方法调用失败时的最大重试次数，包含首次调用。
     * 默认值为3，表示总共会尝试3次（1次初始调用 + 2次重试）。
     *
     * 配置建议：
     * - 关键业务操作：3-5次，确保高成功率
     * - 一般业务操作：1-3次，平衡成功率和性能
     * - 性能敏感操作：0-1次，快速失败
     * - 外部服务调用：根据SLA协议设定
     *
     * 注意事项：
     * - 过多重试会增加系统负载和响应时间
     * - 过少重试可能导致临时故障造成业务失败
     * - 需要考虑下游服务的承载能力
     *
     * 类比：快递投递最多尝试几次，超过次数就放弃或转人工处理
     */
    int maxRetryTimes() default 3;

    /**
     * 重试间隔时间（毫秒）
     *
     * 定义每次重试之间的等待时间，用于避免重试风暴和给目标服务恢复时间。
     * 默认值为0，表示立即重试。
     *
     * 配置策略：
     * - 立即重试（0ms）：适用于网络抖动等瞬时故障
     * - 短间隔（100-500ms）：适用于服务短暂繁忙
     * - 中间隔（1-5秒）：适用于服务重启等场景
     * - 长间隔（10秒以上）：适用于依赖服务的计划性维护
     *
     * 高级策略（需要代码实现）：
     * - 固定间隔：每次重试间隔相同
     * - 指数退避：间隔时间逐渐增长（如100ms, 200ms, 400ms）
     * - 随机抖动：在基础间隔上增加随机偏移，避免同时重试
     *
     * 类比：快递员投递失败后，等待多久再次尝试投递
     *
     * @return 重试间隔时间，单位毫秒，<=0表示不设置间隔
     */
    long retryInterval() default 0;

    /**
     * 指定哪些异常类型触发重试
     *
     * 定义了触发重试机制的异常类型列表。只有当方法抛出这些指定类型的异常时，
     * 才会根据重试策略进行重试。默认只对ConnectException（连接异常）进行重试。
     *
     * 常见的可重试异常类型：
     * - ConnectException：网络连接异常，通常由网络问题引起
     * - SocketTimeoutException：Socket超时，可能是网络延迟或服务繁忙
     * - IOException：IO异常，可能是网络中断或服务不可达
     * - RuntimeException的特定子类：业务层面的可恢复异常
     *
     * 不应重试的异常类型：
     * - IllegalArgumentException：参数错误，重试无意义
     * - SecurityException：权限问题，需要修复权限配置
     * - NullPointerException：编程错误，需要修复代码
     * - 业务逻辑异常：如数据不存在、状态不正确等
     *
     * 配置原则：
     * 1. 可恢复性：只对可能自动恢复的异常进行重试
     * 2. 幂等性：确保重试不会产生副作用
     * 3. 成本考虑：避免对昂贵操作进行无效重试
     * 4. 快速失败：对明确的错误立即失败，不浪费资源
     *
     * 示例配置：
     * - 网络调用：{ConnectException.class, SocketTimeoutException.class}
     * - 文件操作：{IOException.class}
     * - 数据库操作：{SQLException.class}（需要注意事务处理）
     * - 全部异常：{Exception.class}（谨慎使用）
     *
     * 类比：定义哪些投递失败原因需要再次尝试（如收件人不在家），
     *      哪些不需要重试（如地址错误、禁止投递）
     *
     * @return 触发重试的异常类型数组，默认为连接异常
     */
    Class<? extends Throwable>[] retryFor() default {ConnectException.class};

}
