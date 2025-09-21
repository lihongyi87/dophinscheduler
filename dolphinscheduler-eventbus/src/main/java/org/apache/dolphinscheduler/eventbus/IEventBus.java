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

/**
 * 事件总线接口
 *
 * <p>事件总线用于在系统不同组件之间发布和消费事件，实现解耦和异步通信。
 * 它提供了事件的发布、轮询、获取等操作，支持阻塞和非阻塞两种消费模式。
 *
 * <p>使用场景：
 * <ul>
 *   <li>任务状态变更通知</li>
 *   <li>工作流生命周期事件传递</li>
 *   <li>系统级事件广播</li>
 *   <li>异步消息传递</li>
 * </ul>
 *
 * @param <T> 事件类型，必须实现{@link IEvent}接口
 */
public interface IEventBus<T extends IEvent> {

    /**
     * 发布事件到事件总线
     *
     * <p>将事件添加到事件总线队列中，等待消费者处理。
     * 该方法通常是非阻塞的，事件会被立即放入队列。
     *
     * @param event 要发布的事件，不能为null
     */
    void publish(T event);

    /**
     * 非阻塞地获取并移除队列头部的事件
     *
     * <p>尝试从事件总线中获取并移除第一个事件。如果事件总线为空，
     * 该方法会立即返回一个空的Optional，不会阻塞等待。
     *
     * <p>此方法适用于需要快速检查是否有事件可处理的场景。
     *
     * @return 包含事件的Optional，如果队列为空则返回empty
     * @throws InterruptedException 如果当前线程被中断
     */
    Optional<T> poll() throws InterruptedException;

    /**
     * 阻塞地获取并移除队列头部的事件
     *
     * <p>从事件总线中获取并移除第一个事件。如果事件总线为空，
     * 该方法会阻塞当前线程，直到有新的事件被发布。
     *
     * <p>此方法适用于消费者线程需要持续处理事件的场景。
     *
     * @return 获取到的事件，保证不为null
     * @throws InterruptedException 如果在等待过程中线程被中断
     */
    T take() throws InterruptedException;

    /**
     * 查看队列头部的事件但不移除
     *
     * <p>获取事件总线中的第一个事件，但不将其从队列中移除。
     * 如果队列为空，立即返回空的Optional。
     *
     * <p>此方法用于预览下一个要处理的事件，而不影响队列状态。
     *
     * @return 包含事件的Optional，如果队列为空则返回empty
     */
    Optional<T> peek();

    /**
     * 移除队列头部的事件
     *
     * <p>尝试移除事件总线中的第一个事件。如果队列为空，
     * 立即返回空的Optional，不会阻塞。
     *
     * <p>与poll方法类似，但可能在某些实现中有不同的语义。
     *
     * @return 包含被移除事件的Optional，如果队列为空则返回empty
     */
    Optional<T> remove();

    /**
     * 检查事件总线是否为空
     *
     * <p>判断当前事件总线中是否还有待处理的事件。
     *
     * @return 如果事件总线为空返回true，否则返回false
     */
    boolean isEmpty();
}
