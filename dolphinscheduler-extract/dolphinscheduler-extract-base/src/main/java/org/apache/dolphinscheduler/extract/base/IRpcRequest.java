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

import java.io.Serializable;

/**
 * RPC请求接口
 *
 * 定义了RPC调用请求的标准接口，所有远程方法调用的参数都需要实现此接口。
 * 这是一个标记接口，主要用于类型约束和序列化支持。
 * 类比：一个标准的快递包裹标签，确保所有寄送的物品都符合快递公司的处理标准。
 *
 * 设计目的：
 * 1. 类型约束：确保所有RPC请求参数都实现统一的接口
 * 2. 序列化支持：继承Serializable确保可以进行网络传输
 * 3. 框架集成：便于RPC框架对请求对象进行统一处理
 * 4. 扩展预留：为未来添加通用请求处理逻辑预留接口
 *
 * 实现要求：
 * - 所有RPC方法的参数对象都必须实现此接口
 * - 实现类必须支持Java序列化机制
 * - 实现类应该是不可变对象或线程安全的
 * - 建议实现equals()和hashCode()方法用于请求去重
 *
 * 使用场景：
 * - 作为RPC方法参数的基础类型
 * - 请求对象的序列化和反序列化
 * - 请求拦截器和过滤器的类型检查
 * - 日志记录和监控统计
 *
 * 扩展说明：
 * 虽然当前接口为空，但它为未来扩展提供了基础：
 * - 可以添加请求ID、时间戳等通用属性
 * - 可以添加请求验证、安全检查等通用方法
 * - 可以支持请求链路追踪、性能监控等功能
 */
public interface IRpcRequest extends Serializable {

}
