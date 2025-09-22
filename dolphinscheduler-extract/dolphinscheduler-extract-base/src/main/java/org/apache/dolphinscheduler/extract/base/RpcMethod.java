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

/**
 * RPC方法标记注解
 *
 * 用于标记RPC服务中的远程调用方法，配置方法级别的调用策略。
 * 支持超时控制、重试策略等高级特性，确保分布式调用的可靠性。
 *
 * 核心配置：
 * - timeout: 方法调用超时时间，-1表示使用默认超时
 * - retry: 重试策略配置，包含重试次数、间隔等参数
 *
 * 使用场景：
 * - 关键业务方法需要自定义超时时间
 * - 对网络波动敏感的方法需要配置重试策略
 * - 需要细粒度控制的RPC调用
 *
 * 类比：为每个远程方法配置专属的"服务等级协议"(SLA)
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface RpcMethod {

    /**
     * 方法调用超时时间（毫秒）
     *
     * -1表示使用客户端默认超时时间
     * 0表示永不超时（不推荐）
     * >0表示自定义超时时间
     */
    long timeout() default -1;

    /**
     * 重试策略配置
     *
     * 定义方法调用失败时的重试行为，包括：
     * - 重试次数
     * - 重试间隔
     * - 重试条件
     */
    RpcMethodRetryStrategy retry() default @RpcMethodRetryStrategy;

}
