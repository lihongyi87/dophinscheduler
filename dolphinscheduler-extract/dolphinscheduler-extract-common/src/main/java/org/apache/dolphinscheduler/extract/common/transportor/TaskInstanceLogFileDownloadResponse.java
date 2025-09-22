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

package org.apache.dolphinscheduler.extract.common.transportor;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 任务实例日志文件下载响应
 *
 * 用于封装任务实例日志文件下载的响应结果，包含日志内容、状态码和相关消息。
 * 这是日志服务RPC调用的返回数据传输对象。
 * 类比：一个快递包裹，包含所需的物品（日志内容）、投递状态和备注信息。
 *
 * 主要功能：
 * 1. 承载日志文件的实际内容
 * 2. 标识下载操作的执行状态
 * 3. 提供详细的操作结果信息
 * 4. 支持异常情况的错误处理
 *
 * 使用场景：
 * - 返回成功下载的日志文件内容
 * - 通知客户端下载操作的执行结果
 * - 传递错误信息和诊断详情
 * - 支持大文件的安全传输
 *
 * 设计特点：
 * - 结果完整性：包含状态、数据和消息的完整响应
 * - 二进制安全：使用字节数组确保文件内容完整性
 * - 状态明确：通过枚举值清晰表示操作结果
 * - 错误友好：提供详细的错误信息便于问题诊断
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TaskInstanceLogFileDownloadResponse {

    /**
     * 日志文件字节内容
     *
     * 存储下载的日志文件的完整二进制内容。
     * 使用字节数组确保文件内容的完整性和二进制安全性。
     *
     * 内容特点：
     * - 原始文件内容：保持文件的原始格式和编码
     * - 二进制安全：支持任何类型的文件内容
     * - 完整性保证：确保传输过程中数据不丢失
     * - 内存友好：适合中小型文件的内存传输
     *
     * 数据格式：
     * - 通常是UTF-8编码的文本日志
     * - 可能包含特殊字符和换行符
     * - 保持原始的时间戳和格式
     * - 支持多种日志框架的输出格式
     *
     * 使用注意：
     * - 大文件可能导致内存压力，需要考虑文件大小限制
     * - 网络传输时间与文件大小成正比
     * - 客户端需要根据编码正确解析内容
     * - 空文件时此字段为null或空数组
     *
     * 处理建议：
     * - 检查数组长度避免空指针异常
     * - 使用合适的字符编码解析文本内容
     * - 考虑分块传输处理大文件
     * - 验证文件内容的完整性
     */
    private byte[] logBytes;

    /**
     * 响应状态码
     *
     * 使用枚举值标识日志下载操作的执行结果。
     * 默认值为SUCCESS，表示操作成功完成。
     *
     * 状态分类：
     * - SUCCESS：下载成功，日志内容有效
     * - FILE_NOT_FOUND：指定的日志文件不存在
     * - PERMISSION_DENIED：没有访问文件的权限
     * - IO_ERROR：文件读取过程中发生IO错误
     * - INVALID_REQUEST：请求参数无效或格式错误
     *
     * 使用方式：
     * - 客户端首先检查状态码确定操作结果
     * - 只有SUCCESS状态下logBytes才包含有效数据
     * - 其他状态下应查看message字段获取错误详情
     * - 可用于统计和监控下载操作的成功率
     *
     * 错误处理：
     * - 根据不同状态码采取相应的处理策略
     * - 记录失败状态用于问题诊断
     * - 向用户展示友好的错误提示
     * - 支持重试机制的决策依据
     */
    private LogResponseStatus code = LogResponseStatus.SUCCESS;

    /**
     * 响应消息
     *
     * 提供关于下载操作的详细描述信息。
     * 成功时可能包含文件信息，失败时包含错误详情。
     *
     * 消息内容：
     * - 成功场景：文件大小、最后修改时间等元信息
     * - 失败场景：具体的错误原因和建议解决方案
     * - 警告场景：文件部分可读、权限受限等情况
     * - 调试信息：有助于问题诊断的技术细节
     *
     * 消息格式：
     * - 使用用户友好的语言描述
     * - 包含足够的技术细节用于调试
     * - 避免泄露敏感的系统信息
     * - 支持国际化和本地化
     *
     * 使用场景：
     * - 用户界面的提示信息显示
     * - 日志记录和问题追踪
     * - API文档的错误说明
     * - 运维监控的告警内容
     *
     * 内容示例：
     * - 成功："文件下载成功，大小：1.2MB，最后修改：2023-12-01 10:30:00"
     * - 失败："文件不存在：/logs/task/12345.log"
     * - 错误："权限不足，无法访问指定的日志文件"
     * - 警告："文件过大（>10MB），建议使用分页查询"
     */
    private String message;

}
