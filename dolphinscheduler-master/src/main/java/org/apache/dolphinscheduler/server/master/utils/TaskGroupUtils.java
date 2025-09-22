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

import org.apache.dolphinscheduler.dao.entity.TaskInstance;

/**
 * 任务组工具类
 *
 * 提供与任务组相关的工具方法，主要用于判断和检查任务实例是否使用了任务组功能。
 * 任务组是DolphinScheduler中的一个重要特性，用于控制并发执行的任务数量，
 * 防止资源耗尽和系统过载。
 *
 * @author DolphinScheduler
 * @since 3.2.0
 */
public class TaskGroupUtils {

    /**
     * 检查任务实例是否使用了任务组
     *
     * 通过检查任务实例的任务组ID来判断是否启用了任务组功能。
     * 当任务组ID大于0时，表示该任务实例属于某个任务组，
     * 需要在任务组的并发控制下执行。
     *
     * 任务组的作用：
     * 1. 限制同一组内同时执行的任务数量
     * 2. 防止资源竞争和系统过载
     * 3. 实现任务的优先级排队和调度
     *
     * @param taskInstance 需要检查的任务实例
     * @return true表示使用了任务组，false表示未使用任务组
     */
    public static boolean isUsingTaskGroup(final TaskInstance taskInstance) {
        return taskInstance.getTaskGroupId() > 0;
    }

}
