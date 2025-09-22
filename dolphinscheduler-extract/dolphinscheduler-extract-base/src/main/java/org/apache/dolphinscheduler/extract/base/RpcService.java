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
 * RPC服务标记注解
 *
 * 用于标记一个类为RPC服务提供者，被标记的类将自动注册到RPC服务器中。
 * 这是extract框架的核心注解，类比于@Service注解，但专门用于RPC服务注册。
 *
 * 使用方式：
 * - 在服务实现类上添加此注解
 * - 框架会自动扫描并注册RPC方法
 * - 支持跨节点的远程调用
 *
 * 适用场景：
 * - Master节点提供的工作流控制服务
 * - Worker节点提供的任务执行服务
 * - Alert节点提供的告警发送服务
 * - 各节点间的状态同步服务
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface RpcService {
}
