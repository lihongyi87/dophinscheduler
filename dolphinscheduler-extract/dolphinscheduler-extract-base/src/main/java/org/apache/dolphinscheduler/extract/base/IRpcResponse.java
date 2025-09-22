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

/**
 * RPC响应接口
 *
 * 定义了RPC调用返回结果的标准接口，用于统一处理远程方法调用的响应数据。
 * 这个接口抽象了RPC响应的核心要素：执行状态、错误信息、返回数据等。
 * 类比：一个标准化的快递签收单，包含投递状态、备注信息、包裹内容等标准字段。
 *
 * 设计原则：
 * 1. 状态明确：通过isSuccess()方法明确标识调用是否成功
 * 2. 信息完整：提供详细的错误或状态信息用于诊断
 * 3. 数据标准：统一的数据格式便于序列化和网络传输
 * 4. 扩展性：接口设计支持未来功能扩展
 *
 * 使用场景：
 * - 同步RPC调用的返回结果封装
 * - 异步RPC调用的Future结果
 * - 批量调用的响应聚合
 * - 错误处理和异常传播
 *
 * 实现要求：
 * - 实现类需要确保线程安全
 * - 序列化/反序列化的正确性
 * - 状态和数据的一致性
 */
public interface IRpcResponse {

    /**
     * 判断RPC调用是否成功
     *
     * 返回此次远程调用是否成功执行。这是判断调用结果的关键方法。
     * 成功意味着：
     * 1. 网络通信正常完成
     * 2. 远程方法正常执行
     * 3. 没有抛出业务异常
     * 4. 返回结果符合预期
     *
     * 失败情况包括：
     * - 网络连接失败
     * - 远程服务不可用
     * - 方法执行抛出异常
     * - 超时或其他系统级错误
     *
     * @return true表示调用成功，false表示调用失败
     */
    boolean isSuccess();

    /**
     * 获取响应消息
     *
     * 返回与此次调用相关的描述信息，通常包含：
     * - 成功时的执行摘要或确认信息
     * - 失败时的详细错误描述
     * - 警告或调试信息
     *
     * 消息内容用途：
     * - 用户界面的错误提示
     * - 日志记录和问题诊断
     * - 调试和运维分析
     * - 监控和告警
     *
     * @return 响应消息字符串，可能为null
     */
    String getMessage();

    /**
     * 获取响应数据体
     *
     * 返回RPC调用的实际返回数据，以字节数组形式表示。
     * 这是远程方法执行的真正结果数据。
     *
     * 数据格式说明：
     * - 通常是序列化后的Java对象
     * - 可能是JSON、Protocol Buffers等格式
     * - 需要根据具体的序列化协议进行反序列化
     *
     * 使用注意事项：
     * - 调用前应检查isSuccess()状态
     * - 失败时此字段可能为null或包含错误详情
     * - 需要配合相应的反序列化器使用
     *
     * @return 响应数据的字节数组，可能为null
     */
    byte[] getBody();

    /**
     * 将响应对象转换为字节数组
     *
     * 将整个响应对象（包括状态、消息、数据等）序列化为字节数组，
     * 用于网络传输或持久化存储。
     *
     * 序列化包含的内容：
     * - 成功/失败状态
     * - 响应消息
     * - 响应数据体
     * - 其他元数据（如时间戳、ID等）
     *
     * 应用场景：
     * - 网络传输时的数据打包
     * - 响应结果的缓存存储
     * - 批量响应的聚合处理
     * - 调试时的数据导出
     *
     * @return 完整响应对象的字节数组表示
     */
    byte[] toBytes();

}
