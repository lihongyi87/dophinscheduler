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

package org.apache.dolphinscheduler.server.master.utils;

import org.springframework.dao.DataAccessResourceFailureException;

/**
 * 异常处理工具类
 *
 * 提供对各种异常类型的判断和分类方法，主要用于系统异常的统一处理。
 * 目前主要支持数据库连接异常的识别和分类。
 *
 * @author DolphinScheduler
 * @since 3.2.0
 */
public class ExceptionUtils {

    /**
     * 判断是否为数据库连接失败异常
     *
     * 检查传入的异常是否为Spring框架中的数据访问资源失败异常。
     * 这种异常通常表示数据库连接不可用、连接池耗尽或网络问题等情况。
     * 通过识别这类异常，系统可以采取相应的重试或降级策略。
     *
     * @param e 需要检查的异常对象
     * @return true表示是数据库连接失败异常，false表示不是
     */
    public static boolean isDatabaseConnectedFailedException(Throwable e) {
        return e instanceof DataAccessResourceFailureException;
    }

}
