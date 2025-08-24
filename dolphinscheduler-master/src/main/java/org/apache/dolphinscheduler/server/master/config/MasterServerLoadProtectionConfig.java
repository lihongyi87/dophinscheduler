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

package org.apache.dolphinscheduler.server.master.config;

import org.apache.dolphinscheduler.meter.metrics.BaseServerLoadProtectionConfig;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * Master服务器负载保护配置类
 * 
 * 这个类定义了Master服务器的负载保护特定配置参数。
 * 继承自通用的基类，在基础保护（CPU、内存、磁盘）之外，
 * 增加了Master特有的保护机制。
 * 
 * 简单理解：就像给车辆限载一样，防止超负荷运行。
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class MasterServerLoadProtectionConfig extends BaseServerLoadProtectionConfig {

    /**
     * Master最大并发工作流实例数量
     * 
     * 这个参数用于限制在同一时间内Master节点最多能处理多少个工作流实例。
     * 
     * 作用：
     * - 防止Master节点因为处理过多工作流而崩溃
     * - 保证系统的稳定性和响应性能
     * - 在集群环境中实现负载均衡
     * 
     * 参考值：
     * - 小型环境：100-500个并发工作流
     * - 中型环境：500-2000个并发工作流
     * - 大型环境：2000+个并发工作流
     * 
     * 默认值：Integer.MAX_VALUE（无限制）
     * 注意：生产环境建议根据实际情况设置合理的上限值
     */
    private int maxConcurrentWorkflowInstances = Integer.MAX_VALUE;

}
