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

package org.apache.dolphinscheduler.extract.base.protocal;

import org.apache.dolphinscheduler.extract.base.StandardRpcRequest;
import org.apache.dolphinscheduler.extract.base.StandardRpcResponse;
import org.apache.dolphinscheduler.extract.base.serialize.JsonSerializer;

import java.io.Serializable;

import lombok.Data;
import lombok.NonNull;

/**
 * 传输器类
 *
 * <p>该类定义了RPC通信的传输数据结构，包括消息头和消息体。</p>
 *
 * <p>主要功能：</p>
 * <ul>
 *   <li>封装RPC请求和响应数据</li>
 *   <li>定义通信协议的魔数和版本</li>
 *   <li>提供便捷的静态工厂方法</li>
 *   <li>支持序列化传输</li>
 * </ul>
 */
@Data
public class Transporter implements Serializable {

    private static final long serialVersionUID = -1L;

    /**
     * 魔数，用于识别协议
     */
    public static final byte MAGIC = (byte) 0xbabe;

    /**
     * 协议版本号
     */
    public static final byte VERSION = 0;

    /**
     * 传输器头部
     * 包含请求ID、方法信息等元数据
     */
    private TransporterHeader header;

    /**
     * 传输器消息体
     * 存储序列化后的请求或响应数据
     */
    private byte[] body;

    /**
     * 创建包含RPC响应的传输器
     *
     * @param header 传输器头部
     * @param iRpcResponse RPC响应对象
     * @return 传输器实例
     */
    public static Transporter of(@NonNull TransporterHeader header, StandardRpcResponse iRpcResponse) {
        // 序列化响应对象并创建传输器
        return of(header, JsonSerializer.serialize(iRpcResponse));
    }

    /**
     * 创建包含RPC请求的传输器
     *
     * @param header 传输器头部
     * @param iRpcRequest RPC请求对象
     * @return 传输器实例
     */
    public static Transporter of(@NonNull TransporterHeader header, StandardRpcRequest iRpcRequest) {
        // 序列化请求对象并创建传输器
        return of(header, JsonSerializer.serialize(iRpcRequest));
    }

    /**
     * 创建传输器
     *
     * @param header 传输器头部
     * @param body 消息体字节数组
     * @return 传输器实例
     */
    public static Transporter of(@NonNull TransporterHeader header, byte[] body) {
        // 创建新的传输器实例
        Transporter transporter = new Transporter();
        // 设置头部信息
        transporter.setHeader(header);
        // 设置消息体
        transporter.setBody(body);
        return transporter;
    }

}
