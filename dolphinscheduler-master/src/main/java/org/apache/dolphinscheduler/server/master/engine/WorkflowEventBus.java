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

package org.apache.dolphinscheduler.server.master.engine;

import org.apache.dolphinscheduler.eventbus.AbstractDelayEventBus;

import java.util.concurrent.atomic.AtomicInteger;

import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * 工作流事件总线
 * 
 * 这个类是工作流实例的专用事件总线，用于管理单个工作流实例中的所有事件，
 * 包括工作流级别的事件和任务级别的事件。采用延迟事件总线的设计，
 * 能够有序地处理各种生命周期事件。
 * 
 * 主要功能：
 * 1. 管理工作流实例内的所有生命周期事件
 * 2. 提供事件发布和订阅机制
 * 3. 支持事件延迟处理和排队
 * 4. 统计事件处理的各项指标
 * 5. 确保事件的有序处理和可靠传递
 * 
 * 事件类型包括：
 * - 工作流事件：启动、暂停、停止、成功、失败等
 * - 任务事件：分发、运行、成功、失败、超时等
 * 
 * 简单理解：就像一个工作流内部的"消息中心"，
 * 所有相关事件都通过这里进行统一分发和处理。
 */
@Slf4j
@Getter
public class WorkflowEventBus extends AbstractDelayEventBus<AbstractLifecycleEvent> {

    /**
     * 工作流事件总线统计信息
     */
    private final WorkflowEventBusSummary workflowEventBusSummary = new WorkflowEventBusSummary();

    /**
     * 发布事件到事件总线
     * 
     * 将事件发布到总线中，事件会被异步处理。
     * 同时更新统计信息，记录事件发布的详细情况。
     * 
     * @param event 要发布的生命周期事件
     */
    public void publish(final AbstractLifecycleEvent event) {
        // 调用父类的发布方法
        super.publish(event);
        // 增加事件计数
        workflowEventBusSummary.increaseEventCount();
        log.info("Publish event: {}", event);
    }

    /**
     * 工作流事件总线统计信息
     * 
     * 这个内部类用于统计事件总线的运行状态和性能指标，
     * 帮助监控和诊断事件处理的情况。
     */
    @Data
    @NoArgsConstructor
    public static final class WorkflowEventBusSummary {

        /**
         * 总事件数量 - 发布到总线的事件总数
         */
        private AtomicInteger eventCount = new AtomicInteger();
        
        /**
         * 成功处理的事件数量
         */
        private AtomicInteger fireSuccessEventCount = new AtomicInteger();
        
        /**
         * 处理失败的事件数量
         */
        private AtomicInteger fireFailedEventCount = new AtomicInteger();

        /**
         * 增加事件总数计数
         * 
         * @return 增加后的事件总数
         */
        public Integer increaseEventCount() {
            return eventCount.incrementAndGet();
        }

        /**
         * 增加成功处理事件计数
         * 
         * @return 增加后的成功事件数
         */
        public Integer increaseFireSuccessEventCount() {
            return fireSuccessEventCount.incrementAndGet();
        }

        /**
         * 减少成功处理事件计数
         * 
         * @return 减少后的成功事件数
         */
        public Integer decreaseFireSuccessEventCount() {
            return fireSuccessEventCount.decrementAndGet();
        }

        /**
         * 增加失败处理事件计数
         * 
         * @return 增加后的失败事件数
         */
        public Integer increaseFireFailedEventCount() {
            return fireFailedEventCount.incrementAndGet();
        }

        @Override
        public String toString() {
            return "WorkflowEventBusSummary{" +
                    "eventCount=" + eventCount +
                    ", fireSuccessEventCount=" + fireSuccessEventCount +
                    ", fireFailedEventCount=" + fireFailedEventCount +
                    '}';
        }
    }
}
