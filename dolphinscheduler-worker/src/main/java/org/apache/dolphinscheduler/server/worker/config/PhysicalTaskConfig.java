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

import lombok.Data;

/**
 * 物理任务配置类
 *
 * 这个类配置Worker节点执行物理任务时的相关参数。
 * 物理任务是指在Worker节点上实际运行的Shell、SQL、Python等类型的任务。
 *
 * 主要配置项：
 * - 任务执行线程池大小
 * - 任务超时设置
 * - 任务执行环境参数
 */
@Data
public class PhysicalTaskConfig {

    /**
     * 任务执行线程池大小
     *
     * 这个参数决定了Worker节点能同时执行多少个物理任务。
     *
     * 计算策略：
     * - 默认为 CPU核心数 * 2 + 1
     * - 这是一个经验值，适合CPU密集型和I/O密集型任务的混合场景
     *
     * 调优建议：
     * - CPU密集型任务多：设置为CPU核心数 + 1
     * - I/O密集型任务多：可以设置更大的值（如CPU核心数 * 4）
     * - 需要根据实际任务类型和服务器性能调整
     *
     * 默认值计算逻辑：
     * 1. Runtime.getRuntime().availableProcessors() - 获取JVM可用的处理器核心数
     * 2. 乘以2 - 考虑到任务可能包含I/O等待时间，可以让更多线程并发执行
     * 3. 加1 - 额外的缓冲线程，避免所有线程都阻塞时系统无响应
     *
     * 例如：4核CPU的服务器，默认线程池大小为 4 * 2 + 1 = 9 个线程
     */
    private int taskExecutorThreadSize = Runtime.getRuntime().availableProcessors() * 2 + 1;

}
