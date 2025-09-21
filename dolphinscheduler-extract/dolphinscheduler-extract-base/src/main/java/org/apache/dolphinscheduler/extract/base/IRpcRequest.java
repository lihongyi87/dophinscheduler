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

package org.apache.dolphinscheduler.extract.base;

import java.io.Serializable;

/**
 * RPC请求接口
 *
 * <p>定义了RPC请求的基本结构，所有RPC请求都必须实现此接口。</p>
 *
 * <p>主要用途：</p>
 * <ul>
 *   <li>作为RPC框架中请求的标准接口</li>
 *   <li>继承Serializable接口支持网络传输</li>
 *   <li>为具体的RPC请求实现提供统一的父接口</li>
 * </ul>
 */
public interface IRpcRequest extends Serializable {

}
