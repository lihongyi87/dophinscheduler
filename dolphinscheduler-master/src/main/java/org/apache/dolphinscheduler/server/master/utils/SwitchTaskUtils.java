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

import org.apache.dolphinscheduler.plugin.task.api.model.Property;
import org.apache.dolphinscheduler.plugin.task.api.utils.ParameterUtils;

import org.apache.commons.collections4.MapUtils;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.script.ScriptException;

import lombok.extern.slf4j.Slf4j;

import com.google.common.collect.Maps;

import delight.nashornsandbox.NashornSandbox;
import delight.nashornsandbox.NashornSandboxes;

/**
 * Switch任务工具类
 *
 * 为DolphinScheduler的Switch任务提供条件表达式的求值和参数替换功能。
 * Switch任务允许用户根据条件表达式的结果来决定工作流的执行路径。
 * 主要功能包括：
 * 1. JavaScript表达式的安全执行
 * 2. 动态参数替换和类型转换
 * 3. 对数组原型方法的Polyfill支持
 *
 * @author DolphinScheduler
 * @since 3.2.0
 */
@Slf4j
public class SwitchTaskUtils {

    /**
     * Nashorn JavaScript沙箱执行环境
     * 用于安全地执行JavaScript表达式，防止恶意代码执行
     */
    private static final NashornSandbox sandbox;
    /**
     * 参数替换的正则表达式
     * 匹配 ${paramName} 格式的参数占位符，支持单引号和双引号包围
     */
    private static final String rgex = "['\"]*\\$\\{(.*?)\\}['\"]*";
    /**
     * Array.prototype.includes方法的Polyfill实现
     * 由于Nashorn引擎可能不支持ES6的includes方法，需要手动添加支持
     */
    public static final String NASHORN_POLYFILL_ARRAY_PROTOTYPE_INCLUDES =
            "if (!Array.prototype.includes) {" +
                    "   Object.defineProperty(Array.prototype, 'includes', {" +
                    "       value: function(valueToFind, fromIndex) {" +
                    "           if (this == null) {" +
                    "               throw new TypeError('\"this\" is null or not defined');" +
                    "           }" +
                    "           var o = Object(this);" +
                    "           var len = o.length >>> 0;" +
                    "           if (len === 0) { return false; }" +
                    "           var n = fromIndex | 0;" +
                    "           var k = Math.max(n >= 0 ? n : len - Math.abs(n), 0);" +
                    "           function sameValueZero(x, y) {" +
                    "               return x === y || (typeof x === 'number' && " +
                    "                   typeof y === 'number' && isNaN(x) && isNaN(y));" +
                    "           }" +
                    "           while (k < len) {" +
                    "               if (sameValueZero(o[k], valueToFind)) { return true; }" +
                    "               k++;" +
                    "           }" +
                    "           return false;" +
                    "       }" +
                    "   });" +
                    "}";

    static {
        // 初始化Nashorn沙箱环境
        sandbox = NashornSandboxes.create();
        try {
            // 加载Array.prototype.includes的Polyfill实现
            sandbox.eval(NASHORN_POLYFILL_ARRAY_PROTOTYPE_INCLUDES);
        } catch (ScriptException e) {
            log.error("加载Nashorn Polyfill失败", e);
        }
    }

    /**
     * 求值条件表达式
     *
     * 在安全的沙箱环境中执行JavaScript表达式，并返回布尔类型的结果。
     * 这个方法主Switch任务的条件分支选择提供支持，允许用户编写复杂的
     * 条件逻辑来控制工作流的执行路径。
     *
     * @param expression JavaScript条件表达式，应该返回布尔值
     * @return true表示条件成立，false表示条件不成立
     * @throws ScriptException 当JavaScript表达式执行失败时抛出
     */
    public static boolean evaluate(String expression) throws ScriptException {
        Object result = sandbox.eval(expression);
        return Boolean.TRUE.equals(result);
    }

    /**
     * 生成包含任务参数的内容
     *
     * 将条件表达式中的参数占位符替换为实际的参数值。
     * 该方法支持以下功能：
     * 1. 将单引号替换为双引号以保证JavaScript兼容性
     * 2. 根据参数类型进行适当的格式化（数字和布尔直接使用，字符串加引号）
     * 3. 支持全局参数和局部变量的替换
     * 4. 优先使用局部变量，如果不存在则使用全局参数
     *
     * @param condition 原始条件表达式，包含 ${paramName} 格式的参数占位符
     * @param globalParams 全局参数映射，来自工作流级别的参数定义
     * @param varParams 局部变量映射，来自任务级别的变量定义
     * @return 替换参数后的条件表达式，可以直接用于JavaScript求值
     */
    public static String generateContentWithTaskParams(String condition, Map<String, Property> globalParams,
                                                       Map<String, Property> varParams) {
        String content = condition.replaceAll("'", "\"");
        if (MapUtils.isEmpty(globalParams) && MapUtils.isEmpty(varParams)) {
            return content;
        }
        Map<String, Property> params = Maps.newHashMap();
        if (MapUtils.isNotEmpty(globalParams)) {
            params.putAll(globalParams);
        }
        if (MapUtils.isNotEmpty(varParams)) {
            params.putAll(varParams);
        }
        Pattern pattern = Pattern.compile(rgex);
        Matcher m = pattern.matcher(content);
        while (m.find()) {
            String paramName = m.group(1);
            Property property = params.get(paramName);
            if (property == null) {
                continue;
            }
            String value;
            if (ParameterUtils.isNumber(property) || ParameterUtils.isBoolean(property)) {
                value = "" + ParameterUtils.getParameterValue(property);
            } else {
                value = "\"" + ParameterUtils.getParameterValue(property) + "\"";
            }
            log.info("paramName:{}，paramValue:{}", paramName, value);
            content = content.replace("${" + paramName + "}", value);
        }

        return content;
    }

}
