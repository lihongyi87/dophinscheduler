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

package org.apache.dolphinscheduler.server.worker.config;

/**
 * 任务执行线程池满时的处理策略枚举
 *
 * 定义了当Worker节点的任务执行线程池达到最大容量时的处理策略。
 * 这个策略决定了Worker在高负载情况下如何处理新来的任务请求。
 *
 * 使用场景：
 * - 在Master分发任务时，Worker会根据这个策略决定是否接受新任务
 * - 有助于防止Worker节点过载，保证服务稳定性
 * - 可以配合负载均衡策略一起使用，实现更好的集群资源管理
 *
 * 配置方式：
 * - 可在Worker配置文件中设置此策略
 * - 建议根据Worker节点的硬件配置和任务特性来选择合适的策略
 */
public enum TaskExecuteThreadsFullPolicy {

    /**
     * 继续策略
     *
     * 当线程池满时，继续接受新的任务执行请求。
     *
     * 特点：
     * - 不拒绝任务，新任务会进入队列等待
     * - 可能导致任务排队时间较长
     * - 适合任务执行时间较短且可以接受一定延迟的场景
     *
     * 风险：
     * - 可能导致内存占用过高（队列堆积）
     * - 可能影响系统响应性能
     */
    CONTINUE,

    /**
     * 拒绝策略
     *
     * 当线程池满时，拒绝接受新的任务执行请求。
     *
     * 特点：
     * - 主动拒绝新任务，避免线程池过载
     * - 能有效控制Worker节点的负载
     * - 适合对资源控制要求严格的场景
     *
     * 好处：
     * - 保护Worker节点稳定性
     * - 有助于Master将任务分发给其他可用的Worker
     * - 避免因过载导致的系统崩溃
     */
    REJECT,
    ;
}
