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

/**
 * 事件接口
 *
 * <p>这是事件总线系统的基础事件接口，所有需要通过{@link IEventBus}传递的事件
 * 都必须实现这个接口。
 *
 * <p>该接口作为标记接口，不包含任何方法，具体的事件类可以根据业务需要
 * 扩展此接口并添加相应的属性和方法。
 *
 * @see IEventBus 事件总线接口，用于发布和订阅事件
 * @see AbstractDelayEvent 延迟事件的抽象实现
 */
public interface IEvent {

}
