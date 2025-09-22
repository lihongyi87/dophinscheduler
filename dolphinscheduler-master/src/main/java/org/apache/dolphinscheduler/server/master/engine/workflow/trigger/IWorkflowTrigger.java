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

package org.apache.dolphinscheduler.server.master.engine.workflow.trigger;

/**
 * 工作流触发器接口
 *
 * 这是所有工作流触发器的核心接口，定义了触发工作流并生成工作流实例的标准行为。
 * DolphinScheduler支持多种类型的工作流触发方式，包括手动触发、定时调度、
 * 补数据、失败重跑、暂停恢复等，每种触发方式都会实现这个接口。
 *
 * <p>触发器的作用机制：</p>
 * <ul>
 *   <li>接收触发请求，包含启动工作流所需的所有参数</li>
 *   <li>创建工作流实例，设置实例的各种属性</li>
 *   <li>生成执行命令，告诉Master如何执行这个工作流</li>
 *   <li>返回触发结果，通知调用者触发是否成功</li>
 * </ul>
 *
 * <p>泛型参数说明：</p>
 * <ul>
 *   <li>TriggerRequest：触发请求类型，包含触发所需的所有信息</li>
 *   <li>TriggerResponse：触发响应类型，包含触发结果信息</li>
 * </ul>
 *
 * 简单理解：这就是一个"启动工作流"的标准化接口，
 * 无论是用户手动点击、定时器到期，还是其他情况，
 * 最终都会通过实现这个接口的具体触发器来启动工作流。
 *
 * @param <TriggerRequest>  触发请求类型
 * @param <TriggerResponse> 触发响应类型
 */
public interface IWorkflowTrigger<TriggerRequest, TriggerResponse> {

    /**
     * 触发工作流执行
     *
     * 这是触发器的核心方法，负责根据触发请求启动工作流实例。
     * 该方法会完成以下主要工作：
     * 1. 解析触发请求，获取工作流定义和执行参数
     * 2. 创建工作流实例，设置实例状态和属性
     * 3. 生成执行命令，放入命令队列等待Master调度
     * 4. 返回触发结果，包含工作流实例ID等信息
     *
     * @param triggerRequest 触发请求，包含启动工作流所需的所有信息
     * @return 触发响应，包含触发结果和工作流实例信息
     */
    TriggerResponse triggerWorkflow(final TriggerRequest triggerRequest);

}
