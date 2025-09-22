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

import org.apache.dolphinscheduler.extract.base.serialize.JsonSerializer;

import lombok.Data;

/**
 * 标准RPC响应实现类
 *
 * 这是IRpcResponse接口的标准实现，用于封装RPC方法调用的返回结果。
 * 它提供了完整的响应信息，包括执行状态、错误消息、返回数据和类型信息。
 * 类比：一个标准化的快递回执单，包含投递状态、备注信息、包裹内容等完整信息。
 *
 * 核心功能：
 * 1. 状态封装：明确标识调用成功或失败状态
 * 2. 消息传递：提供详细的状态描述或错误信息
 * 3. 数据承载：携带方法返回的实际业务数据
 * 4. 类型保持：记录返回数据的类型信息用于反序列化
 *
 * 设计特点：
 * - 包含完整的响应元信息（状态、消息、数据、类型）
 * - 提供静态工厂方法简化对象创建
 * - 支持成功和失败两种响应场景
 * - 实现完整的序列化支持
 *
 * 使用模式：
 * - 成功响应：包含返回数据和类型信息
 * - 失败响应：包含错误消息和失败状态
 * - 混合响应：可以同时包含部分数据和警告信息
 */
@Data
public class StandardRpcResponse implements IRpcResponse {

    /**
     * 响应成功标志
     *
     * 标识此次RPC调用是否成功执行。
     * true表示成功：方法正常执行并返回结果
     * false表示失败：执行过程中发生异常或错误
     *
     * 成功条件：
     * - 网络通信正常
     * - 目标方法找到并执行
     * - 执行过程无异常
     * - 返回结果序列化成功
     */
    private boolean success;

    /**
     * 响应消息
     *
     * 提供关于此次调用的描述性信息。
     * 成功时：可能包含执行摘要、警告信息等
     * 失败时：包含详细的错误描述和诊断信息
     *
     * 消息用途：
     * - 用户界面显示
     * - 日志记录
     * - 调试诊断
     * - 监控告警
     */
    private String message;

    /**
     * 响应数据体
     *
     * 存储方法返回值的序列化字节数组。
     * 只有在调用成功时才会有有效数据。
     * 失败时此字段通常为null。
     *
     * 数据格式：
     * - 使用JsonSerializer序列化的Java对象
     * - 保持与原始返回值的数据完整性
     * - 支持复杂对象的完整序列化
     */
    private byte[] body;

    /**
     * 响应数据类型
     *
     * 记录body字段中数据的原始Java类型。
     * 用于客户端正确反序列化返回数据。
     *
     * 类型信息的重要性：
     * - 确保反序列化的类型正确性
     * - 支持泛型和复杂类型
     * - 避免类型转换异常
     * - 支持多态和继承关系
     */
    private Class<?> bodyType;

    /**
     * 创建成功响应的静态工厂方法
     *
     * 用于创建表示成功执行的RPC响应对象。
     * 这是创建成功响应的推荐方式，确保状态一致性。
     *
     * 创建逻辑：
     * 1. 设置成功状态为true
     * 2. 设置返回数据的字节数组
     * 3. 记录数据的类型信息
     * 4. 消息字段保持默认值（null）
     *
     * 使用场景：
     * - 方法正常执行并有返回值
     * - 方法执行成功但返回void（body为null）
     * - 部分成功的情况（可后续设置警告消息）
     *
     * @param body 序列化后的返回数据，可以为null
     * @param bodyType 返回数据的Java类型，用于反序列化
     * @return 配置好的成功响应对象
     */
    public static StandardRpcResponse success(byte[] body, Class<?> bodyType) {
        StandardRpcResponse rpcResponse = new StandardRpcResponse();
        rpcResponse.setSuccess(true);
        rpcResponse.setBody(body);
        rpcResponse.setBodyType(bodyType);
        return rpcResponse;
    }

    /**
     * 创建失败响应的静态工厂方法
     *
     * 用于创建表示执行失败的RPC响应对象。
     * 这是创建失败响应的推荐方式，确保错误信息正确传递。
     *
     * 创建逻辑：
     * 1. 设置成功状态为false
     * 2. 设置详细的错误消息
     * 3. body和bodyType保持默认值（null）
     *
     * 使用场景：
     * - 方法执行时抛出异常
     * - 参数验证失败
     * - 权限检查不通过
     * - 系统资源不足
     * - 业务逻辑验证失败
     *
     * 错误消息建议：
     * - 包含足够的诊断信息
     * - 避免泄露敏感信息
     * - 提供可操作的解决建议
     * - 包含错误发生的上下文
     *
     * @param message 详细的错误描述信息
     * @return 配置好的失败响应对象
     */
    public static StandardRpcResponse fail(String message) {
        StandardRpcResponse rpcResponse = new StandardRpcResponse();
        rpcResponse.setSuccess(false);
        rpcResponse.setMessage(message);
        return rpcResponse;
    }

    /**
     * 将响应对象序列化为字节数组
     *
     * 实现IRpcResponse接口的toBytes()方法。
     * 将整个响应对象转换为字节数组，用于网络传输。
     *
     * 序列化内容：
     * - success状态标志
     * - message错误或状态消息
     * - body返回数据字节数组
     * - bodyType返回数据类型信息
     *
     * 序列化特点：
     * - 使用JsonSerializer确保兼容性
     * - 保持数据完整性和正确性
     * - 支持完整的往返序列化
     * - 处理null值和特殊情况
     *
     * @return 响应对象的字节数组表示
     */
    @Override
    public byte[] toBytes() {
        return JsonSerializer.serialize(this);
    }
}
