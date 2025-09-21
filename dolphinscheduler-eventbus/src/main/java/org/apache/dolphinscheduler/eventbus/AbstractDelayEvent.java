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

package org.apache.dolphinscheduler.eventbus;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import lombok.Builder;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/**
 * 延迟事件抽象类
 *
 * <p>该类实现了延迟事件的核心功能，事件将在指定的延迟时间后才能被消费。
 * 继承此类可以创建自定义的延迟事件。
 *
 * <p>主要特性：
 * <ul>
 *   <li>支持设置事件的延迟时间（毫秒级）</li>
 *   <li>实现Delayed接口，可以被DelayQueue正确处理</li>
 *   <li>记录事件创建时间和过期时间</li>
 *   <li>支持事件间的比较和排序</li>
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>任务超时检查</li>
 *   <li>定时重试机制</li>
 *   <li>延迟执行的调度任务</li>
 * </ul>
 */
@ToString
@SuperBuilder
public abstract class AbstractDelayEvent implements IEvent, Delayed {

    /**
     * 默认延迟时间（毫秒）
     * 值为0表示事件立即可用，无需延迟
     */
    private static final long DEFAULT_DELAY_TIME = 0;

    /**
     * 延迟时间（单位：毫秒）
     * 事件需要延迟的时间长度
     */
    protected long delayTime;

    /**
     * 事件创建时间（单位：纳秒）
     * 使用System.nanoTime()记录，用于计算相对时间
     */
    @Builder.Default
    protected long createTimeInNano = System.nanoTime();

    /**
     * 事件过期时间（单位：纳秒）
     * 表示事件何时可以被消费，由创建时间+延迟时间计算得出
     * 默认值设为当前时间，以防子类未调用super()
     */
    @Builder.Default
    protected long expiredTimeInNano = System.nanoTime();

    /**
     * 默认构造函数
     * 创建一个无延迟的事件（延迟时间为0）
     */
    public AbstractDelayEvent() {
        this(DEFAULT_DELAY_TIME);
    }

    /**
     * 指定延迟时间的构造函数
     *
     * @param delayTime 延迟时间（毫秒）
     */
    public AbstractDelayEvent(final long delayTime) {
        this(delayTime, System.nanoTime());
    }

    /**
     * 完整构造函数
     *
     * @param delayTime 延迟时间（毫秒）
     * @param createTimeInNano 创建时间（纳秒）
     */
    public AbstractDelayEvent(final long delayTime, final long createTimeInNano) {
        // 设置延迟时间（毫秒）
        this.delayTime = delayTime;
        // 设置事件创建时间（纳秒）
        this.createTimeInNano = createTimeInNano;
        // 计算过期时间 = 创建时间 + 延迟时间
        // 将毫秒转换为纳秒（1毫秒 = 1,000,000纳秒）
        // 使用下划线分隔符提高大数字的可读性
        this.expiredTimeInNano = this.delayTime * 1_000_000 + this.createTimeInNano;
    }

    /**
     * 获取剩余延迟时间
     *
     * <p>计算事件距离可被消费还有多少时间。
     * 如果返回值小于等于0，表示事件已经过期，可以被立即消费。
     *
     * @param unit 时间单位
     * @return 指定单位的剩余延迟时间
     */
    @Override
    public long getDelay(TimeUnit unit) {
        // 计算从当前时间到过期时间的差值
        // 公式: 剩余延迟 = (创建时间 + 延迟时间) - 当前时间
        // 注意：delayTime需要从毫秒转换为纳秒（乘以1,000,000）
        long delay = createTimeInNano + delayTime * 1_000_000 - System.nanoTime();
        // 将纳秒转换为调用者指定的时间单位
        return unit.convert(delay, TimeUnit.NANOSECONDS);
    }

    /**
     * 比较两个延迟事件的优先级
     *
     * <p>根据过期时间进行比较，过期时间早的事件优先级更高。
     * 这保证了DelayQueue能够按照过期时间顺序排列事件。
     *
     * @param other 其他延迟事件
     * @return 负数表示当前事件优先，正数表示other优先，0表示相同
     */
    @Override
    public int compareTo(Delayed other) {
        // 将other强转为AbstractDelayEvent类型，以获取expiredTimeInNano字段
        // 比较两个事件的过期时间，时间较早的优先级更高
        return Long.compare(this.expiredTimeInNano, ((AbstractDelayEvent) other).expiredTimeInNano);
    }

}
