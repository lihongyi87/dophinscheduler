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

import lombok.Data;

import org.springframework.validation.Errors;

/**
 * 命令获取策略配置类
 *
 * 这个类定义了Master节点从数据库中获取待执行命令的策略。
 * 合理的获取策略能够提高命令处理效率，避免数据库压力过大。
 *
 * 主要作用：
 * - 控制命令获取的频率和数量
 * - 支持多种获取策略以适应不同场景
 * - 在集群环境中实现命令的合理分配
 *
 * 使用场景：
 * - 高并发环境下的命令获取优化
 * - 多Master节点的负载均衡
 * - 数据库查询性能优化
 *
 * @author DolphinScheduler Team
 */
@Data
public class CommandFetchStrategy {

    /**
     * 命令获取策略类型
     *
     * 当前支持的策略：
     * - ID_SLOT_BASED：基于ID槽位的获取策略
     *
     * 默认使用ID_SLOT_BASED策略，这是经过验证的高效策略。
     */
    private CommandFetchStrategyType type = CommandFetchStrategyType.ID_SLOT_BASED;

    /**
     * 命令获取配置对象
     *
     * 根据不同的策略类型，使用相应的配置实现。
     * 默认使用IdSlotBasedFetchConfig配置。
     */
    private CommandFetchConfig config = new IdSlotBasedFetchConfig();

    /**
     * 验证配置参数的有效性
     *
     * 这个方法会委托给具体的配置实现来进行参数验证。
     * 在Master启动时自动调用，确保配置的有效性。
     *
     * @param errors Spring的错误收集对象，用于记录验证失败的信息
     */
    public void validate(Errors errors) {
        // 委托给具体的配置实现类进行验证
        // 不同类型的策略有不同的验证逻辑
        config.validate(errors);
    }

    /**
     * 命令获取策略类型枚举
     *
     * 定义了支持的所有命令获取策略类型。
     */
    public enum CommandFetchStrategyType {
        /**
         * 基于ID槽位的获取策略
         *
         * 这种策略通过ID范围分片来获取命令，特点：
         * - 避免不同Master节点获取重复的命令
         * - 通过ID步长实现负载均衡
         * - 批量获取提高数据库查询效率
         */
        ID_SLOT_BASED,
        ;
    }

    /**
     * 命令获取配置接口
     *
     * 所有具体的命令获取配置类都需要实现这个接口。
     */
    public interface CommandFetchConfig {

        /**
         * 验证配置参数的有效性
         *
         * 每个具体的配置实现都需要实现这个方法，
         * 用于验证其特定的配置参数是否符合要求。
         *
         * @param errors Spring的错误收集对象，用于记录验证失败的信息
         */
        void validate(Errors errors);

    }

    /**
     * 基于ID槽位的命令获取配置
     *
     * 这种配置策略通过ID范围分片来实现命令的均匀获取。
     * 适用于多Master节点环境下的负载均衡。
     *
     * 工作原理：
     * 1. 每个Master节点根据自己的ID步长获取命令
     * 2. 通过fetchSize控制每次获取的命令数量
     * 3. 避免不同节点获取重复的命令
     */
    @Data
    public static class IdSlotBasedFetchConfig implements CommandFetchConfig {

        /**
         * ID步长
         *
         * 在多Master节点环境中，每个节点使用不同的ID步长来避免获取重复的命令。
         *
         * 工作机制：
         * - Master节点1：获取ID为 1, 1+step, 1+2*step... 的命令
         * - Master节点2：获取ID为 2, 2+step, 2+2*step... 的命令
         * - 以此类推...
         *
         * 配置建议：
         * - 单Master环境：设置为1
         * - 多Master环境：设置为Master节点总数
         *
         * 默认值：1（适用于单Master环境）
         */
        private int idStep = 1;

        /**
         * 每次获取的命令数量
         *
         * 控制每次从数据库中获取多少个待执行的命令。
         *
         * 影响因素：
         * - 数量过小：频繁查询数据库，影响性能
         * - 数量过大：内存占用增加，处理延迟可能增大
         *
         * 配置建议：
         * - 低负载环境：5-10个
         * - 中等负载环境：10-20个
         * - 高负载环境：20-50个
         *
         * 默认值：10（平衡性能和资源占用的经验值）
         */
        private int fetchSize = 10;

        /**
         * 验证ID槽位基础获取配置的参数有效性
         *
         * 这个方法会检查idStep和fetchSize两个关键参数：
         * - idStep必须大于0，确保能够正确进行ID分片
         * - fetchSize必须大于0，确保每次能获取到命令
         *
         * @param errors Spring的错误收集对象，用于记录验证失败的信息
         */
        @Override
        public void validate(Errors errors) {
            // 验证ID步长是否有效（必须大于0）
            // ID步长为0或负数会导致无法正确分片，造成获取重复或无法获取命令
            if (idStep <= 0) {
                errors.rejectValue("step", null, "step must be greater than 0");
            }

            // 验证获取数量是否有效（必须大于0）
            // 获取数量为0或负数会导致无法获取任何命令，Master无法正常工作
            if (fetchSize <= 0) {
                errors.rejectValue("fetchSize", null, "fetchSize must be greater than 0");
            }
        }
    }

}
