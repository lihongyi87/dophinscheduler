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

import java.util.Optional;
import java.util.concurrent.DelayQueue;

/**
 * 延迟事件总线抽象类
 *
 * <p>该类提供了支持延迟事件的事件总线实现。
 * 它使用DelayQueue作为底层存储，确保事件按照延迟时间顺序被消费。
 *
 * <p>主要特性：
 * <ul>
 *   <li>支持延迟事件的发布和消费</li>
 *   <li>事件按照延迟时间自动排序</li>
 *   <li>支持阻塞和非阻塞的消费模式</li>
 *   <li>线程安全，DelayQueue内部实现了同步</li>
 * </ul>
 *
 * <p>子类可以继承此类并添加特定的业务逻辑。
 *
 * @param <T> 延迟事件类型，必须继承自{@link AbstractDelayEvent}
 */
public abstract class AbstractDelayEventBus<T extends AbstractDelayEvent> implements IEventBus<T> {

    /**
     * 延迟事件队列
     * 使用JDK提供的DelayQueue实现，它是一个无界阻塞队列，
     * 内部使用PriorityQueue实现，保证事件按延迟时间排序
     */
    protected final DelayQueue<T> delayEventQueue = new DelayQueue<>();

    /**
     * 发布延迟事件
     *
     * <p>将事件添加到延迟队列中。事件会根据其延迟时间自动排序，
     * 只有在延迟时间到达后才能被消费。
     *
     * @param event 要发布的延迟事件
     */
    @Override
    public void publish(final T event) {
        // 直接将事件添加到DelayQueue中
        // DelayQueue会根据事件的getDelay()和compareTo()方法自动排序
        // add()方法是线程安全的，内部使用了ReentrantLock
        delayEventQueue.add(event);
    }

    /**
     * 非阻塞地获取并移除已过期的事件
     *
     * <p>尝试获取队列头部的事件。只有当事件的延迟时间已经过期时，
     * 才能成功获取。如果没有过期的事件，立即返回empty。
     *
     * @return 包含过期事件的Optional，如果没有过期事件则返回empty
     */
    @Override
    public Optional<T> poll() {
        // 调用DelayQueue的poll()方法，非阻塞获取已过期的事件
        // 只有当事件的getDelay()返回值 <= 0时才能获取到
        // 使用Optional包装，避免null值引起的问题
        return Optional.ofNullable(delayEventQueue.poll());
    }

    /**
     * 查看队列头部的事件但不移除
     *
     * <p>获取队列头部的事件（最先过期的事件），但不从队列中移除。
     * 注意：即使事件尚未过期，也可以查看到。
     *
     * @return 包含队列头部事件的Optional，队列为空时返回empty
     */
    @Override
    public Optional<T> peek() {
        // 查看队列头部元素，但不移除
        // 返回的是最先过期的事件（根据compareTo排序）
        // 注意：即使事件未过期，也可以peek到
        return Optional.ofNullable(delayEventQueue.peek());
    }

    /**
     * 阻塞地获取并移除已过期的事件
     *
     * <p>获取并移除队列头部的事件。如果队列为空或没有过期的事件，
     * 当前线程将被阻塞，直到有事件过期。
     *
     * <p>这是消费者线程的主要消费方式，保证了事件按顺序处理。
     *
     * @return 获取到的过期事件
     * @throws InterruptedException 如果在等待过程中线程被中断
     */
    @Override
    public T take() throws InterruptedException {
        // 阻塞获取已过期的事件
        // 如果队列为空或没有过期事件，线程会被阻塞
        // 内部使用Condition实现等待/通知机制
        // 当有新事件加入或事件过期时，阻塞的线程会被唤醒
        return delayEventQueue.take();
    }

    /**
     * 移除队列头部的事件
     *
     * <p>尝试移除队列头部的事件。与poll()不同，这个方法可能会
     * 移除尚未过期的事件（具体行为取决于实现）。
     *
     * @return 包含被移除事件的Optional，队列为空时返回empty
     */
    @Override
    public Optional<T> remove() {
        // 移除队列头部元素
        // 注意：DelayQueue的remove()可能抛出NoSuchElementException
        // 这里使用try-catch来处理异常情况
        try {
            return Optional.ofNullable(delayEventQueue.remove());
        } catch (Exception e) {
            // 队列为空时返回empty Optional
            return Optional.empty();
        }
    }

    /**
     * 检查延迟事件队列是否为空
     *
     * <p>判断队列中是否还有待处理的事件（无论是否已过期）。
     *
     * @return 队列为空时返回true，否则返回false
     */
    @Override
    public boolean isEmpty() {
        // 检查DelayQueue是否为空
        // 这个方法是线程安全的
        return delayEventQueue.isEmpty();
    }
}
