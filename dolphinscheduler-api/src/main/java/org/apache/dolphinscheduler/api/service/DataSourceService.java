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

package org.apache.dolphinscheduler.api.service;

import org.apache.dolphinscheduler.api.utils.PageInfo;
import org.apache.dolphinscheduler.dao.entity.DataSource;
import org.apache.dolphinscheduler.dao.entity.User;
import org.apache.dolphinscheduler.plugin.datasource.api.datasource.BaseDataSourceParamDTO;
import org.apache.dolphinscheduler.spi.datasource.ConnectionParam;
import org.apache.dolphinscheduler.spi.enums.DbType;
import org.apache.dolphinscheduler.spi.params.base.ParamsOptions;

import java.util.List;

/**
 * 数据源服务接口
 * 管理各种类型数据库的连接配置和测试
 */
public interface DataSourceService {

    /**
     * 创建数据源连接
     *
     * <p>业务逻辑：</p>
     * <ul>
     *   <li>验证数据源名称唯一性</li>
     *   <li>验证数据源连接参数</li>
     *   <li>测试数据库连接可用性</li>
     *   <li>加密存储敏感信息（密码等）</li>
     *   <li>保存数据源配置</li>
     * </ul>
     *
     * @param loginUser 登录用户，需要有数据源创建权限
     * @param datasourceParam 数据源参数，包括数据库类型、连接信息等
     * @return 创建成功的数据源对象
     * @throws ServiceException 当数据源名称重复或连接参数无效时抛出
     */
    DataSource createDataSource(User loginUser, BaseDataSourceParamDTO datasourceParam);

    /**
     * 更新数据源配置
     *
     * <p>业务逻辑：</p>
     * <ul>
     *   <li>验证数据源是否存在</li>
     *   <li>检查用户权限</li>
     *   <li>验证新名称唯一性（如果名称变化）</li>
     *   <li>测试新配置的连接可用性</li>
     *   <li>更新数据源信息</li>
     * </ul>
     *
     * @param loginUser 登录用户，需要有数据源编辑权限
     * @param dataSourceParam 新的数据源参数，必须包含数据源ID
     * @return 更新后的数据源对象
     * @throws ServiceException 当数据源不存在、权限不足或配置无效时抛出
     */
    DataSource updateDataSource(User loginUser, BaseDataSourceParamDTO dataSourceParam);

    /**
     * 根据ID查询数据源详情
     * 返回数据源的详细配置信息，用于编辑和查看
     *
     * @param id 数据源ID，必须存在
     * @param loginUser 登录用户，需要有数据源查看权限
     * @return 数据源参数传输对象，包含连接配置等详细信息
     * @throws ServiceException 当数据源不存在或权限不足时抛出
     */
    BaseDataSourceParamDTO queryDataSource(int id, User loginUser);

    /**
     * 分页查询数据源列表
     * 支持按数据源名称模糊搜索，只返回用户有权限的数据源
     *
     * @param loginUser 登录用户，用于权限过滤
     * @param searchVal 搜索关键词，可为空，支持数据源名称模糊匹配
     * @param pageNo 页码，从1开始
     * @param pageSize 每页大小，建议10-100
     * @return 数据源分页数据，包含总数和当前页数据
     */
    PageInfo<DataSource> queryDataSourceListPaging(User loginUser, String searchVal, Integer pageNo, Integer pageSize);

    /**
     * 按类型查询数据源列表
     * 返回指定类型的所有数据源，用于下拉选择等场景
     *
     * @param loginUser 登录用户，用于权限过滤
     * @param type 数据源类型，可为空表示查询所有类型（如MySQL、PostgreSQL等）
     * @return 指定类型的数据源列表，不分页
     */
    List<DataSource> queryDataSourceList(User loginUser, Integer type);

    /**
     * 验证数据源名称是否可用
     * 用于创建和更新时的名称唯一性校验
     *
     * @param name 待验证的数据源名称，不能为空
     * @throws ServiceException 当数据源名称已存在时抛出异常
     */
    void verifyDataSourceName(String name);

    /**
     * 测试数据源连接
     * 通过给定的参数尝试连接数据库，验证配置的正确性
     *
     * @param type 数据源类型（MySQL、PostgreSQL等）
     * @param parameter 数据源连接参数，包含主机、端口、数据库名、用户名密码等
     * @throws ServiceException 当连接失败时抛出异常，包含具体的错误信息
     */
    void checkConnection(DbType type, ConnectionParam parameter);

    /**
     * 测试现有数据源连接
     * 使用数据源的已保存配置测试连接可用性
     *
     * @param id 数据源ID，必须存在
     * @throws ServiceException 当数据源不存在或连接失败时抛出异常
     */
    void connectionTest(int id);

    /**
     * 删除数据源
     *
     * <p>业务逻辑：</p>
     * <ul>
     *   <li>验证数据源是否存在</li>
     *   <li>检查数据源是否被任务定义引用</li>
     *   <li>检查用户删除权限</li>
     *   <li>执行物理删除</li>
     * </ul>
     *
     * <p>级联影响：删除前会检查是否有任务定义在使用此数据源</p>
     *
     * @param loginUser 登录用户，需要有数据源删除权限
     * @param datasourceId 数据源ID，必须存在且未被使用
     * @throws ServiceException 当数据源不存在、被引用或权限不足时抛出
     */
    void delete(User loginUser, int datasourceId);

    /**
     * 查询用户未授权的数据源列表
     * 返回系统中该用户没有访问权限的数据源，用于授权管理
     *
     * @param loginUser 登录用户，需要有用户管理权限
     * @param userId 目标用户ID
     * @return 该用户未授权的数据源列表
     */
    List<DataSource> unAuthDatasource(User loginUser, Integer userId);

    /**
     * 查询用户已授权的数据源列表
     * 返回该用户具有访问权限的数据源，不包括用户自己创建的数据源
     *
     * @param loginUser 登录用户，需要有用户管理权限
     * @param userId 目标用户ID
     * @return 该用户已授权的数据源列表
     */
    List<DataSource> authedDatasource(User loginUser, Integer userId);

    /**
     * 获取数据库中的表列表
     * 连接指定数据源，查询指定数据库下的所有表
     *
     * @param datasourceId 数据源ID，必须存在且可连接
     * @param database 数据库名称，可为空使用默认数据库
     * @return 表名选项列表，用于前端下拉选择
     * @throws ServiceException 当数据源不可用或数据库不存在时抛出
     */
    List<ParamsOptions> getTables(Integer datasourceId, String database);

    /**
     * 获取数据表的列信息
     * 查询指定表的所有列名和类型信息
     *
     * @param datasourceId 数据源ID，必须存在且可连接
     * @param database 数据库名称
     * @param tableName 表名称，必须存在
     * @return 列信息选项列表，包含列名和数据类型
     * @throws ServiceException 当数据源不可用或表不存在时抛出
     */
    List<ParamsOptions> getTableColumns(Integer datasourceId, String database, String tableName);

    /**
     * 获取数据源的数据库列表
     * 连接指定数据源，查询所有可用的数据库
     *
     * @param datasourceId 数据源ID，必须存在且可连接
     * @return 数据库名选项列表，用于前端下拉选择
     * @throws ServiceException 当数据源不可用时抛出
     */
    List<ParamsOptions> getDatabases(Integer datasourceId);
}
