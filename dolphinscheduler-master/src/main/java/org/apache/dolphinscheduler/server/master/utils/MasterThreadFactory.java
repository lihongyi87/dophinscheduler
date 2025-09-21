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

package org.apache.dolphinscheduler.server.master.utils;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Master线程工厂工具类
 *
 * 为DolphinScheduler Master节点提供线程池创建和管理的工具方法。
 * 主要用于创建定时任务执行器，支持Master节点的各种定时任务和周期性操作。
 * 采用单例模式设计，避免线程池资源的重复创建和浪费。
 *
 * @author DolphinScheduler
 * @since 3.2.0
 */
public class MasterThreadFactory {

    /**
     * 获取默认的定时任务线程执行器
     *
     * 创建一个单线程的定时任务执行器，用于执行定时任务和周期性任务。
     * 该执行器适用于以下场景：
     * 1. Master节点的定时检查任务
     * 2. 周期性的系统状态监控
     * 3. 定时清理和维护任务
     * 4. 其他需要定时执行的后台任务
     *
     * 注意：该方法每次调用都会创建新的线程池实例，请根据实际需要合理使用。
     *
     * @return 单线程的定时任务执行器
     */
    public static ScheduledExecutorService getDefaultSchedulerThreadExecutor() {
        return Executors.newSingleThreadScheduledExecutor();
    }

}
