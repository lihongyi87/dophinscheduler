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

package org.apache.dolphinscheduler.server.master.engine.task.runnable;

import org.apache.dolphinscheduler.dao.entity.TaskInstance;

/**
 * 任务实例工厂接口
 *
 * 定义了创建不同类型任务实例的统一工厂接口。使用工厂模式和建造者模式相结合，
 * 为不同场景下的任务实例创建提供标准化的抽象。
 *
 * 设计模式：
 * - 工厂模式：封装任务实例的创建逻辑，客户端无需了解具体的创建细节
 * - 建造者模式：通过Builder模式提供灵活的参数设置和对象构建
 * - 泛型设计：支持不同类型的Builder，确保类型安全和代码复用
 *
 * 核心思想：
 * 1. 统一接口：所有任务实例工厂都实现相同的接口，便于管理和扩展
 * 2. 类型安全：通过泛型确保Builder和Factory的类型匹配
 * 3. 建造者模式：复杂对象的构建过程与表示分离，支持链式调用
 * 4. 扩展性：可以轻松添加新的工厂实现，支持新的任务创建场景
 *
 * 应用场景：
 * - 首次运行：创建全新的任务实例
 * - 重试执行：基于失败任务创建重试实例
 * - 故障转移：为故障任务创建转移实例
 * - 恢复执行：为暂停或失败的任务创建恢复实例
 *
 * 工厂层次结构：
 * - 抽象工厂接口：定义通用的创建规范
 * - 具体工厂实现：针对特定场景的创建逻辑
 * - 建造者接口：定义参数设置和构建规范
 * - 具体建造者：实现特定的参数设置逻辑
 *
 * 类比理解：
 * 就像一个汽车工厂的生产线标准：
 * - 工厂接口：定义生产汽车的标准流程
 * - 具体工厂：不同车型的专门生产线（轿车、SUV、卡车）
 * - 建造者：汽车配置单，指定颜色、配置、选装件等
 * - 最终产品：根据配置单生产出来的具体汽车
 *
 * @param <BUILDER> 建造者类型，必须实现ITaskInstanceBuilder接口
 */
public interface ITaskInstanceFactory<BUILDER extends ITaskInstanceFactory.ITaskInstanceBuilder> {

    /**
     * 创建建造者实例
     *
     * 返回一个新的建造者实例，用于设置任务实例的各种参数。
     * 建造者模式的入口方法，开始任务实例的构建过程。
     *
     * 建造者的作用：
     * - 参数设置：提供链式调用方式设置任务实例的各种属性
     * - 验证逻辑：在构建过程中验证参数的合法性和完整性
     * - 默认值：为未设置的参数提供合理的默认值
     * - 灵活性：支持可选参数的设置，提高API的易用性
     *
     * 使用模式：
     * factory.builder()
     *     .withParam1(value1)
     *     .withParam2(value2)
     *     .build()
     *
     * 类比：获取一张汽车配置单，可以在上面填写各种配置选项。
     *
     * @return 建造者实例，用于设置任务实例参数
     */
    BUILDER builder();

    /**
     * 创建任务实例
     *
     * 根据建造者中设置的参数创建具体的任务实例。这是工厂的核心方法，
     * 封装了任务实例创建的所有复杂逻辑。
     *
     * 创建过程：
     * 1. 参数验证：检查建造者中的参数是否完整和合法
     * 2. 实例创建：根据参数创建新的TaskInstance对象
     * 3. 属性设置：将建造者中的参数设置到任务实例中
     * 4. 业务逻辑：执行特定场景下的创建逻辑（如重试计数、状态设置等）
     * 5. 持久化：将创建的任务实例保存到数据库
     *
     * 不同工厂的差异：
     * - FirstRunFactory：创建全新任务，初始化所有基础属性
     * - RetryFactory：基于失败任务创建重试实例，递增重试次数
     * - FailoverFactory：创建故障转移实例，保留原有配置
     * - RecoverFactory：创建恢复实例，重置执行状态
     *
     * 错误处理：
     * - 参数不足：抛出IllegalStateException异常
     * - 创建失败：回滚已执行的操作，确保数据一致性
     * - 数据库异常：记录日志并抛出适当的异常
     *
     * 类比：工厂根据配置单生产汽车，检查配置，组装零件，
     * 最后交付一辆完整的汽车。
     *
     * @param builder 包含创建参数的建造者实例
     * @return 创建完成的任务实例
     * @throws IllegalStateException 当建造者参数不完整或无效时
     */
    TaskInstance createTaskInstance(BUILDER builder);

    /**
     * 任务实例建造者接口
     *
     * 定义了任务实例建造者的基本规范。所有具体的建造者都必须实现此接口，
     * 提供统一的构建方法。
     *
     * 建造者模式的优势：
     * - 参数灵活：支持可选参数的设置，提高API易用性
     * - 链式调用：方法返回自身，支持优雅的链式调用
     * - 类型安全：编译时检查参数类型，避免运行时错误
     * - 可读性：代码表达意图清晰，易于理解和维护
     *
     * 实现要求：
     * - 每个setter方法都应该返回建造者自身（this）
     * - build方法应该验证所有必需参数的完整性
     * - 支持多次调用setter，后设置的值覆盖前面的值
     * - build方法应该是幂等的，多次调用产生相同结果
     *
     * 生命周期：
     * 1. 创建：通过factory.builder()创建建造者实例
     * 2. 配置：通过withXxx()方法设置各种参数
     * 3. 构建：调用build()方法创建最终的任务实例
     * 4. 销毁：build()后建造者通常不再使用
     *
     * 类比：汽车配置单的标准格式，定义了填写和提交的基本规范。
     */
    interface ITaskInstanceBuilder {

        /**
         * 构建任务实例
         *
         * 根据建造者中设置的参数构建最终的任务实例。这是建造者模式的
         * 核心方法，完成从参数到对象的转换。
         *
         * 构建职责：
         * - 参数验证：确保所有必需的参数都已设置
         * - 对象创建：调用工厂的createTaskInstance方法
         * - 异常处理：处理构建过程中可能出现的异常
         * - 结果返回：返回创建完成的任务实例
         *
         * 注意事项：
         * - build方法通常只能调用一次，多次调用行为未定义
         * - 如果参数不完整，应该抛出有意义的异常信息
         * - 构建失败时不应该修改建造者的状态
         *
         * 类比：提交汽车配置单，工厂根据配置单生产汽车。
         *
         * @return 构建完成的任务实例
         * @throws IllegalStateException 当必需参数缺失时
         */
        TaskInstance build();
    }

}
