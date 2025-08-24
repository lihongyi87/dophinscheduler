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
 * 定义了当Worker节点的任务执行线程池达到最大容量时的处理策略
 */
public enum TaskExecuteThreadsFullPolicy {
    
    /**
     * 继续策略
     * 当线程池满时，继续接受新的任务执行请求
     */
    CONTINUE,
    
    /**
     * 拒绝策略
     * 当线程池满时，拒绝接受新的任务执行请求
     */
    REJECT,
    ;
}
