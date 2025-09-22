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

package org.apache.dolphinscheduler.server.master.exception;

/**
 * 逻辑任务工厂未找到异常
 * <p>
 * 当Master节点尝试创建逻辑任务实例时，无法找到对应的任务工厂类时抛出此异常。
 * 逻辑任务工厂负责根据任务类型创建相应的逻辑任务处理器，如条件任务、依赖任务、子工作流任务等。
 * </p>
 *
 * <p>触发场景：</p>
 * <ul>
 *   <li>请求的任务类型在系统中未注册</li>
 *   <li>任务插件未正确加载或初始化</li>
 *   <li>任务类型名称拼写错误或不匹配</li>
 *   <li>插件版本不兼容导致工厂类缺失</li>
 *   <li>系统配置中缺少相应的任务类型定义</li>
 *   <li>插件jar文件损坏或缺失</li>
 * </ul>
 *
 * <p>处理方式：</p>
 * <ul>
 *   <li>记录未找到的任务类型和详细错误信息</li>
 *   <li>检查系统中已注册的任务工厂列表</li>
 *   <li>将任务状态标记为失败</li>
 *   <li>停止工作流执行并发送告警</li>
 *   <li>建议检查插件配置和任务定义</li>
 *   <li>不进行重试，需要修复配置后重新运行</li>
 * </ul>
 *
 * @author DolphinScheduler
 */
public class LogicTaskFactoryNotFoundException extends MasterException {

    /**
     * 构造逻辑任务工厂未找到异常
     *
     * @param message 异常消息，描述未找到的任务类型和相关信息
     */
    public LogicTaskFactoryNotFoundException(String message) {
        super(message);
    }

}
