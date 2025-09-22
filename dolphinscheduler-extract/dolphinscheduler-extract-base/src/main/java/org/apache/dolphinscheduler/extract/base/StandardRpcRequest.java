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

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 标准RPC请求实现类
 *
 * 这是IRpcRequest接口的标准实现，用于封装RPC方法调用的参数信息。
 * 它负责将Java对象参数序列化为字节数组，以便在网络中传输。
 * 类比：一个标准化的快递包装箱，将各种物品（参数）打包成统一格式进行运输。
 *
 * 核心功能：
 * 1. 参数序列化：将Java对象转换为可传输的字节数组
 * 2. 类型保持：记录原始参数的类型信息，用于反序列化
 * 3. 空值处理：正确处理null参数和空参数列表
 * 4. 批量转换：支持多个参数的统一处理
 *
 * 设计特点：
 * - 使用字节数组存储序列化后的参数数据
 * - 单独记录每个参数的类型信息
 * - 提供静态工厂方法简化对象创建
 * - 支持Lombok注解减少样板代码
 *
 * 序列化策略：
 * - 使用JsonSerializer进行对象序列化
 * - 每个参数独立序列化，避免相互影响
 * - 保留类型信息用于服务端反序列化
 * - 支持null值的正确处理
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class StandardRpcRequest implements IRpcRequest {

    /**
     * 序列化后的参数数据数组
     *
     * 存储每个方法参数序列化后的字节数据。
     * 数组中的每个元素对应一个方法参数的序列化结果。
     * 如果某个参数为null，对应的字节数组也为null。
     *
     * 数据结构：
     * args[0] -> 第一个参数的序列化字节数组
     * args[1] -> 第二个参数的序列化字节数组
     * ...
     * args[n] -> 第n个参数的序列化字节数组
     */
    private byte[][] args;

    /**
     * 参数类型数组
     *
     * 记录每个参数的原始Java类型，用于服务端进行反序列化。
     * 类型信息是正确反序列化的关键，确保数据类型一致性。
     *
     * 类型保存策略：
     * - 非null参数：保存其实际运行时类型（getClass()）
     * - null参数：保存null值
     * - 基本类型：保存对应的包装类型
     *
     * 用途：
     * - 服务端方法调用时的参数类型匹配
     * - 反序列化时的类型指导
     * - 方法重载的正确分发
     */
    private Class<?>[] argsTypes;

    /**
     * 静态工厂方法：从对象数组创建RPC请求
     *
     * 将Java对象数组转换为StandardRpcRequest对象，完成参数的序列化和类型记录。
     * 这是创建RPC请求对象的推荐方式，封装了复杂的序列化逻辑。
     *
     * 处理逻辑：
     * 1. 空值检查：处理null或空数组的情况
     * 2. 逐一序列化：对每个参数进行JSON序列化
     * 3. 类型记录：保存每个参数的运行时类型
     * 4. 对象构造：创建完整的请求对象
     *
     * 异常处理：
     * - 序列化失败时会抛出运行时异常
     * - null参数被正确处理，不会导致异常
     * - 不支持序列化的对象会导致失败
     *
     * 使用示例：
     * ```java
     * // 调用方法：service.process(userId, taskName, config)
     * Object[] args = {123, "myTask", configObject};
     * StandardRpcRequest request = StandardRpcRequest.of(args);
     * ```
     *
     * @param args 方法调用的参数数组，可以为null或空数组
     * @return 创建的StandardRpcRequest对象，包含序列化后的参数数据
     */
    public static StandardRpcRequest of(Object[] args) {
        // 处理空参数情况：null或空数组
        if (args == null || args.length == 0) {
            return new StandardRpcRequest(null, null);
        }

        // 初始化存储数组
        final byte[][] argsBytes = new byte[args.length][];
        final Class<?>[] argsTypes = new Class[args.length];

        // 逐个处理每个参数
        for (int i = 0; i < args.length; i++) {
            // 序列化参数：使用JsonSerializer将对象转换为字节数组
            // 如果参数为null，JsonSerializer.serialize()会返回null
            argsBytes[i] = JsonSerializer.serialize(args[i]);

            // 记录参数类型：null参数的类型也记录为null
            // 非null参数记录其运行时实际类型
            argsTypes[i] = args[i] == null ? null : args[i].getClass();
        }

        // 创建并返回标准RPC请求对象
        return new StandardRpcRequest(argsBytes, argsTypes);
    }

}
