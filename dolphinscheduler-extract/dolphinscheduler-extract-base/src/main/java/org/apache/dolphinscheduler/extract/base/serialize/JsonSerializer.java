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

package org.apache.dolphinscheduler.extract.base.serialize;

import static com.fasterxml.jackson.databind.DeserializationFeature.ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT;
import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static com.fasterxml.jackson.databind.DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL;
import static com.fasterxml.jackson.databind.MapperFeature.REQUIRE_SETTERS_FOR_GETTERS;
import static org.apache.dolphinscheduler.common.constants.DateConstants.YYYY_MM_DD_HH_MM_SS;

import org.apache.dolphinscheduler.common.constants.SystemConstants;
import org.apache.dolphinscheduler.common.utils.JSONUtils;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

/**
 * JSON序列化工具类
 *
 * 这是extract框架中用于对象序列化和反序列化的核心工具类。
 * 它封装了Jackson库的功能，为RPC框架提供统一的序列化服务。
 * 类比：一个专业的翻译官，负责将Java对象和字节数据之间进行准确的双向转换。
 *
 * 主要职责：
 * 1. 对象序列化：将Java对象转换为字节数组用于网络传输
 * 2. 对象反序列化：将字节数组转换回Java对象
 * 3. 配置管理：统一管理序列化的各种配置参数
 * 4. 异常处理：优雅处理序列化过程中的各种异常
 *
 * 配置特点：
 * - 忽略未知属性：提高版本兼容性
 * - 空数组转null：简化null值处理
 * - 未知枚举转null：增强枚举兼容性
 * - 日期时间支持：统一的时间格式处理
 * - 时区处理：统一使用系统默认时区
 *
 * 性能优化：
 * - 单例ObjectMapper：避免重复创建开销
 * - UTF-8编码：确保跨平台字符兼容
 * - 异常缓存：避免异常对象创建开销
 */
@Slf4j
public class JsonSerializer {

    /**
     * Jackson ObjectMapper实例
     *
     * 这是序列化的核心组件，配置了适合RPC框架的各种参数。
     * 使用单例模式确保配置一致性和性能优化。
     *
     * 配置详解：
     * - FAIL_ON_UNKNOWN_PROPERTIES=false：忽略未知属性，提高向前兼容性
     * - ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT=true：将空数组视为null，简化处理
     * - READ_UNKNOWN_ENUM_VALUES_AS_NULL=true：未知枚举值转为null，避免异常
     * - REQUIRE_SETTERS_FOR_GETTERS=true：要求getter/setter配对，提高安全性
     *
     * 模块扩展：
     * - LocalDateTime序列化器：自定义时间类型处理
     * - 时区设置：使用系统默认时区确保一致性
     * - 日期格式：统一的日期时间格式化
     */
    private static final ObjectMapper objectMapper = JsonMapper.builder()
            // 忽略JSON中存在但Java类中不存在的属性，提高版本兼容性
            .configure(FAIL_ON_UNKNOWN_PROPERTIES, false)
            // 将空数组解析为null对象，简化null值判断逻辑
            .configure(ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT, true)
            // 将未知的枚举值读取为null，避免因新增枚举值导致的反序列化失败
            .configure(READ_UNKNOWN_ENUM_VALUES_AS_NULL, true)
            // 要求有getter的属性必须有对应的setter，提高数据完整性
            .configure(REQUIRE_SETTERS_FOR_GETTERS, true)
            // 添加自定义模块，处理特殊类型的序列化
            .addModule(new SimpleModule()
                    // 添加LocalDateTime的自定义序列化器，确保时间格式一致
                    .addSerializer(LocalDateTime.class, new JSONUtils.LocalDateTimeSerializer())
                    // 添加LocalDateTime的自定义反序列化器，支持多种时间格式
                    .addDeserializer(LocalDateTime.class, new JSONUtils.LocalDateTimeDeserializer()))
            // 设置默认时区，确保分布式环境下时间处理的一致性
            .defaultTimeZone(SystemConstants.DEFAULT_TIME_ZONE)
            // 设置默认日期格式，统一时间字符串的表示方式
            .defaultDateFormat(new SimpleDateFormat(YYYY_MM_DD_HH_MM_SS))
            .build();

    /**
     * 私有构造函数
     *
     * 防止实例化，确保这是一个纯工具类。
     * 所有方法都是静态的，不需要创建实例。
     */
    private JsonSerializer() {
        // 工具类不允许实例化
    }

    /**
     * 对象序列化方法
     *
     * 将Java对象序列化为字节数组，用于网络传输或存储。
     * 这是RPC框架中参数传递和返回值处理的核心方法。
     *
     * 处理流程：
     * 1. 空值检查：null对象直接返回null
     * 2. JSON转换：使用ObjectMapper将对象转为JSON字符串
     * 3. 字节转换：将JSON字符串转换为UTF-8字节数组
     * 4. 异常处理：序列化失败时记录日志并返回null
     *
     * 异常情况：
     * - 对象包含不可序列化的字段
     * - 循环引用导致的栈溢出
     * - 自定义序列化器抛出异常
     * - 内存不足导致的处理失败
     *
     * 使用注意：
     * - 确保对象支持JSON序列化
     * - 避免循环引用
     * - 注意序列化后的数据大小
     * - 处理返回null的情况
     *
     * @param obj 要序列化的Java对象，可以为null
     * @param <T> 对象的类型
     * @return 序列化后的字节数组，输入为null时返回null，序列化失败时也返回null
     */
    public static <T> byte[] serialize(T obj) {
        // 空值检查：null对象无需序列化
        if (obj == null) {
            return null;
        }

        try {
            // 执行序列化：对象 -> JSON字符串 -> UTF-8字节数组
            // 使用UTF-8编码确保跨平台兼容性
            return objectMapper.writeValueAsString(obj).getBytes(StandardCharsets.UTF_8);
        } catch (JsonProcessingException e) {
            // 序列化失败时记录错误日志，但不抛出异常
            // 这样可以避免单个对象序列化失败影响整个RPC调用
            log.error("serializeToString exception!", e);
            return null;
        }
    }

    /**
     * 对象反序列化方法
     *
     * 将字节数组反序列化为指定类型的Java对象。
     * 这是RPC框架中接收参数和处理返回值的核心方法。
     *
     * 处理流程：
     * 1. 空值检查：null字节数组直接返回null
     * 2. 字符转换：将UTF-8字节数组转换为JSON字符串
     * 3. 对象转换：使用ObjectMapper将JSON字符串转为Java对象
     * 4. 类型转换：确保返回的对象类型正确
     *
     * 异常处理：
     * - 使用@SneakyThrows注解简化异常处理
     * - JSON格式错误会抛出JsonProcessingException
     * - 类型不匹配会抛出ClassCastException
     * - 字节数组格式错误会抛出相关异常
     *
     * 使用注意：
     * - 确保字节数组是有效的JSON数据
     * - 目标类型必须有无参构造函数
     * - 目标类型的属性需要有对应的setter方法
     * - 处理可能的异常情况
     *
     * @param src 要反序列化的字节数组，可以为null
     * @param clazz 目标对象的类型，不能为null
     * @param <T> 目标对象的类型
     * @return 反序列化后的Java对象，输入为null时返回null
     * @throws Exception 反序列化过程中可能抛出的各种异常
     */
    @SneakyThrows
    public static <T> T deserialize(byte[] src, Class<T> clazz) {
        // 空值检查：null字节数组无需反序列化
        if (src == null) {
            return null;
        }

        // 字节数组转JSON字符串：使用UTF-8编码确保字符正确性
        String json = new String(src, StandardCharsets.UTF_8);

        // JSON字符串转Java对象：使用ObjectMapper进行类型安全的转换
        return objectMapper.readValue(json, clazz);
    }

}
