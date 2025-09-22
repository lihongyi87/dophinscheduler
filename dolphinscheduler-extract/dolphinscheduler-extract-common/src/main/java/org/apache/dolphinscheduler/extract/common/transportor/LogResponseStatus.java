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

/**
 * 日志响应状态码枚举
 *
 * 定义了日志服务操作的各种状态码，用于标识日志查询、下载等操作的执行结果。
 * 这些状态码帮助客户端准确理解操作结果，并采取相应的处理策略。
 * 类比：医院检查报告的结果状态，明确告知检查是否成功、有什么问题、需要什么处理。
 *
 * 设计原则：
 * 1. 状态明确：每个状态都有清晰的含义和适用场景
 * 2. 易于扩展：可以方便地添加新的状态码
 * 3. 便于处理：客户端可以根据状态码采取不同的处理逻辑
 * 4. 兼容性好：状态码的添加不会影响现有代码
 *
 * 使用场景：
 * - 日志文件下载操作的结果标识
 * - 日志分页查询的状态反馈
 * - 日志删除操作的执行结果
 * - 错误诊断和问题排查
 *
 * 状态分类：
 * - 成功状态：操作正常完成
 * - 错误状态：操作执行失败
 * - 特定错误：针对特定问题的详细状态
 */
public enum LogResponseStatus {

    /**
     * 成功状态码
     *
     * 表示日志操作成功完成，所有请求的数据都正确返回。
     *
     * 适用场景：
     * - 日志文件成功下载，内容完整
     * - 日志分页查询成功，返回有效数据
     * - 日志删除操作成功执行
     * - 系统运行正常，无任何异常
     *
     * 客户端处理：
     * - 可以安全地使用返回的日志数据
     * - 进行后续的日志分析和处理
     * - 向用户展示操作成功的反馈
     * - 记录成功操作用于统计分析
     */
    SUCCESS,

    /**
     * 通用错误状态码
     *
     * 表示日志操作过程中发生了未分类的错误。
     * 这是一个兜底的错误状态，用于处理各种非特定的异常情况。
     *
     * 可能的错误原因：
     * - 文件系统IO异常（权限不足、磁盘故障等）
     * - 网络传输错误（连接中断、超时等）
     * - 系统资源不足（内存、CPU过载等）
     * - 服务内部逻辑错误（空指针、类型转换等）
     * - 配置错误（路径不正确、参数无效等）
     *
     * 客户端处理：
     * - 检查错误消息获取具体原因
     * - 记录错误信息用于问题排查
     * - 向用户展示友好的错误提示
     * - 考虑重试机制或降级处理
     * - 上报错误到监控系统
     *
     * 诊断建议：
     * - 查看详细的错误消息和堆栈信息
     * - 检查系统资源使用情况
     * - 验证配置参数的正确性
     * - 分析日志查找根本原因
     */
    ERROR,

    /**
     * 日志文件未找到状态码
     *
     * 表示请求的日志文件在文件系统中不存在。
     * 这是一个具体的错误状态，专门用于处理文件不存在的情况。
     *
     * 可能的原因：
     * - 指定的日志文件路径错误或不存在
     * - 日志文件已被删除或移动
     * - 任务实例ID对应的日志文件尚未生成
     * - 日志轮转或清理导致文件丢失
     * - 文件系统故障或挂载问题
     *
     * 常见场景：
     * - 查询一个尚未开始执行的任务的日志
     * - 访问已被清理的历史任务日志
     * - 日志文件路径配置错误
     * - 日志文件权限问题导致无法访问
     *
     * 客户端处理：
     * - 向用户提示文件不存在的友好信息
     * - 建议检查任务执行状态或路径配置
     * - 不进行重试，因为文件确实不存在
     * - 记录访问日志用于审计和分析
     * - 可能需要重新生成或恢复文件
     *
     * 区别于ERROR：
     * - 这是一个明确的、可预期的错误状态
     * - 不需要系统级的错误处理和告警
     * - 客户端可以提供更精确的用户提示
     * - 通常不需要技术人员介入处理
     */
    LOG_FILE_NOT_FOUND,
}
