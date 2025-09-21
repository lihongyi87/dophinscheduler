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

package org.apache.dolphinscheduler.server.master.engine.task.dispatcher.event;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.dolphinscheduler.eventbus.AbstractDelayEvent;

import java.util.concurrent.Delayed;

import lombok.Getter;

/**
 * 任务可调度事件
 *
 * 这是一个封装了任务数据和调度信息的事件对象，专门用于任务分发系统中的延迟调度。
 * 该事件继承了延迟事件的特性，支持按时间和优先级进行排序和调度。
 *
 * 核心特性：
 * 1. 数据封装：封装了需要分发的任务数据，保持数据的完整性和类型安全
 * 2. 延迟调度：支持指定延迟时间，实现定时分发和重试机制
 * 3. 优先级排序：支持基于任务数据的优先级比较，确保高优先级任务优先处理
 * 4. 类型安全：使用泛型确保任务数据类型的一致性和安全性
 * 5. 不可变性：任务数据一旦设置就不可变，保证了事件的稳定性
 *
 * 设计模式：
 * - 装饰器模式：为任务数据添加时间和优先级属性
 * - 模板方法：定义了事件比较的标准流程
 * - 泛型设计：支持不同类型的任务数据
 *
 * 比较策略：
 * 事件的比较分为两个层次：
 * 1. 优先级比较：首先比较任务数据的优先级（如果实现了Comparable接口）
 * 2. 时间比较：如果优先级相同，则比较延迟时间，时间早的优先
 *
 * 使用场景：
 * - 任务延迟分发：需要在特定时间后分发的任务
 * - 失败重试：任务分发失败后的延迟重试
 * - 优先级调度：高优先级任务优先分发
 * - 负载均衡：通过时间分散来平衡系统负载
 * - 定时任务：需要定时执行的任务调度
 *
 * 类比理解：
 * 就像一个带有时间标签和优先级标签的快递包裹：
 * - 包裹内容：实际的任务数据
 * - 时间标签：指定什么时候可以配送
 * - 优先级标签：决定在相同时间的包裹中哪个先配送
 * - 不可拆封：包裹一旦封装就不能修改内容
 *
 * @param <V> 任务数据类型，必须实现Comparable接口用于优先级比较
 */
@Getter
public class TaskDispatchableEvent<V extends Comparable<V>> extends AbstractDelayEvent {

    /**
     * 任务数据
     *
     * 封装在事件中的实际任务数据，通常是ITaskExecutionRunnable的实例。
     * 这个字段是final的，确保事件的不可变性和线程安全性。
     *
     * 特性：
     * - 不可变：字段声明为final，一旦设置就不能修改
     * - 非空保证：通过checkNotNull验证确保数据不为空
     * - 类型安全：泛型约束确保数据类型的一致性
     * - 优先级支持：数据类型必须实现Comparable接口
     *
     * 设计考虑：
     * - 封装性：使用protected修饰符，允许子类访问但限制外部直接访问
     * - 一致性：与Lombok的@Getter注解配合提供标准的访问方法
     * - 安全性：防止外部代码意外修改事件数据
     */
    protected final V data;

    /**
     * 构造任务可调度事件
     *
     * 创建一个带有指定延迟时间和任务数据的可调度事件。
     * 这是事件对象的唯一构造方式，确保所有事件都有完整的时间和数据信息。
     *
     * 构造过程：
     * 1. 时间设置：调用父类构造器设置事件的延迟时间
     * 2. 数据验证：使用Guava的checkNotNull确保任务数据非空
     * 3. 数据存储：将验证后的任务数据存储到不可变字段中
     *
     * 参数验证：
     * - delayTimeMills：延迟时间，由父类AbstractDelayEvent处理
     * - data：任务数据，必须非空且实现Comparable接口
     *
     * 异常处理：
     * - 如果data为null，会抛出NullPointerException并带有描述性消息
     * - 这种设计遵循了"快速失败"原则，在构造时就发现问题
     *
     * 使用示例：
     * ```java
     * // 创建立即执行的任务事件
     * TaskDispatchableEvent<ITaskExecutionRunnable> immediateEvent =
     *     new TaskDispatchableEvent<>(0, taskRunnable);
     *
     * // 创建延迟5秒执行的任务事件
     * TaskDispatchableEvent<ITaskExecutionRunnable> delayedEvent =
     *     new TaskDispatchableEvent<>(5000, taskRunnable);
     * ```
     *
     * @param delayTimeMills 延迟时间（毫秒），0表示立即执行，大于0表示延迟执行
     * @param data 任务数据，不能为null且必须实现Comparable接口
     * @throws NullPointerException 当data为null时抛出
     */
    public TaskDispatchableEvent(long delayTimeMills, V data) {
        super(delayTimeMills);
        this.data = checkNotNull(data, "data is null");
    }

    /**
     * 比较两个任务可调度事件的优先级和时间顺序
     *
     * 这是事件排序的核心方法，定义了任务分发系统中事件的优先级规则。
     * 实现了基于优先级和时间的双重排序策略，确保任务按正确的顺序被分发。
     *
     * 比较策略（按优先级降序）：
     * 1. 类型检查：首先验证比较对象是否为TaskDispatchableEvent类型
     * 2. 优先级比较：比较任务数据的优先级（基于Comparable接口）
     * 3. 时间比较：如果优先级相同，则比较延迟时间，时间早的优先
     *
     * 详细比较流程：
     * 第一层 - 类型安全：
     * - 检查other是否为TaskDispatchableEvent实例
     * - 如果不是，抛出RuntimeException，确保类型安全
     *
     * 第二层 - 优先级比较：
     * - 如果两个事件的data都不为空，进行优先级比较
     * - 使用data.compareTo()方法比较任务的业务优先级
     * - 如果比较结果不为0，直接返回结果（高优先级任务排在前面）
     *
     * 第三层 - 时间比较：
     * - 如果优先级相同或为空，使用父类的时间比较逻辑
     * - 时间早的事件排在前面，确保按时间顺序处理
     *
     * 使用场景：
     * - 优先队列排序：在DelayQueue中自动维护事件的排序
     * - 任务调度：确保高优先级任务优先执行
     * - 时间调度：在相同优先级下按时间顺序执行
     * - 重试排序：失败重试任务的时间排序
     *
     * 排序示例：
     * 假设有以下事件：
     * - Event A: priority=1, delay=1000ms
     * - Event B: priority=2, delay=500ms
     * - Event C: priority=1, delay=2000ms
     *
     * 排序结果：B -> A -> C
     * （优先级高的B最先，然后是相同优先级但时间早的A）
     *
     * 异常处理：
     * - 类型不匹配：抛出RuntimeException，提供清晰的错误信息
     * - 空数据处理：当data为null时跳过优先级比较，直接进行时间比较
     *
     * 线程安全：
     * - 方法本身是线程安全的，因为只读取不可变的字段
     * - compareTo的实现应该与equals方法保持一致性
     *
     * 性能考虑：
     * - 优先级比较在前，可以快速区分大部分事件
     * - 时间比较作为后备，保证完整的排序逻辑
     * - 避免了不必要的类型转换和复杂计算
     *
     * 注意事项：
     * - 返回值遵循Comparable接口规范：负数、零、正数分别表示小于、等于、大于
     * - 与DelayQueue的使用兼容，支持自动排序和延迟处理
     * - 错误消息中提到"TaskReadyForDispatchEvent"可能是历史遗留，实际应为TaskDispatchableEvent
     *
     * @param other 要比较的另一个延迟事件对象
     * @return 负数表示当前事件优先级更高或时间更早，0表示相等，正数表示优先级更低或时间更晚
     * @throws RuntimeException 当other不是TaskDispatchableEvent实例时抛出
     */
    @Override
    public int compareTo(Delayed other) {
        if (!(other instanceof TaskDispatchableEvent)) {
            throw new RuntimeException("The object being compared is not a TaskReadyForDispatchEvent.");
        }

        @SuppressWarnings("unchecked")
        final TaskDispatchableEvent<V> otherEvent = (TaskDispatchableEvent<V>) other;

        // 应该首先比较数据的优先级
        if (data != null && otherEvent.data != null) {
            final int compareResult = data.compareTo(otherEvent.data);
            if (compareResult != 0) {
                return compareResult;
            }
        }

        return super.compareTo(other);
    }
}
