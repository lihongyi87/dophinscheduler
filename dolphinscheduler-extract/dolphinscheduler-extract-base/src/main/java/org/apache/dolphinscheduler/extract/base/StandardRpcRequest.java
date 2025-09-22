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
 * <p>该类实现了IRpcRequest接口，提供了标准的RPC请求数据结构。</p>
 *
 * <p>主要功能：</p>
 * <ul>
 *   <li>存储方法调用参数的字节数组</li>
 *   <li>保存参数类型信息用于反序列化</li>
 *   <li>提供便捷的静态工厂方法创建请求对象</li>
 * </ul>
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class StandardRpcRequest implements IRpcRequest {

    /**
     * 参数的字节数组
     * 每个参数被序列化为独立的字节数组
     */
    private byte[][] args;

    /**
     * 参数的类型数组
     * 用于在反序列化时恢复参数的原始类型
     */
    private Class<?>[] argsTypes;

    /**
     * 创建标准RPC请求的静态工厂方法
     *
     * @param args 方法调用参数数组
     * @return 包含序列化参数的StandardRpcRequest对象
     */
    public static StandardRpcRequest of(Object[] args) {
        // 如果参数为空，返回空的请求对象
        if (args == null || args.length == 0) {
            return new StandardRpcRequest(null, null);
        }
        // 初始化参数字节数组和类型数组
        final byte[][] argsBytes = new byte[args.length][];
        final Class<?>[] argsTypes = new Class[args.length];
        // 遍历参数，进行序列化并记录类型
        for (int i = 0; i < args.length; i++) {
            // 使用JSON序列化器将参数转换为字节数组
            argsBytes[i] = JsonSerializer.serialize(args[i]);
            // 记录参数的实际类型，用于反序列化
            argsTypes[i] = args[i] == null ? null : args[i].getClass();
        }
        // 返回包含序列化参数的请求对象
        return new StandardRpcRequest(argsBytes, argsTypes);
    }

}
