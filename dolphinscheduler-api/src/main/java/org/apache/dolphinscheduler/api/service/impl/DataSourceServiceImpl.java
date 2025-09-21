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

package org.apache.dolphinscheduler.api.service.impl;

import static org.apache.dolphinscheduler.api.constants.ApiFuncIdentificationConstant.DATASOURCE_DELETE;
import static org.apache.dolphinscheduler.api.constants.ApiFuncIdentificationConstant.DATASOURCE_UPDATE;

import org.apache.dolphinscheduler.api.constants.ApiFuncIdentificationConstant;
import org.apache.dolphinscheduler.api.enums.Status;
import org.apache.dolphinscheduler.api.exceptions.ServiceException;
import org.apache.dolphinscheduler.api.service.DataSourceService;
import org.apache.dolphinscheduler.api.utils.PageInfo;
import org.apache.dolphinscheduler.common.constants.Constants;
import org.apache.dolphinscheduler.common.enums.AuthorizationType;
import org.apache.dolphinscheduler.common.enums.UserType;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.dao.entity.DataSource;
import org.apache.dolphinscheduler.dao.entity.User;
import org.apache.dolphinscheduler.dao.mapper.DataSourceMapper;
import org.apache.dolphinscheduler.dao.mapper.DataSourceUserMapper;
import org.apache.dolphinscheduler.plugin.datasource.api.datasource.BaseDataSourceParamDTO;
import org.apache.dolphinscheduler.plugin.datasource.api.datasource.DataSourceProcessor;
import org.apache.dolphinscheduler.plugin.datasource.api.utils.DataSourceUtils;
import org.apache.dolphinscheduler.spi.datasource.BaseConnectionParam;
import org.apache.dolphinscheduler.spi.datasource.ConnectionParam;
import org.apache.dolphinscheduler.spi.enums.DbType;
import org.apache.dolphinscheduler.spi.params.base.ParamsOptions;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * 数据源服务实现类
 *
 * <p>该类实现了数据源的完整管理功能，支持多种数据库类型的连接管理。
 * 提供数据源的创建、更新、删除、查询、测试连接等核心功能。</p>
 *
 * <p>主要功能：</p>
 * <ul>
 *   <li>创建数据源 - 支持MySQL、PostgreSQL、Hive、Spark等多种数据库</li>
 *   <li>更新数据源 - 修改数据源连接信息和参数</li>
 *   <li>删除数据源 - 安全删除数据源及其关联关系</li>
 *   <li>测试连接 - 验证数据源配置的正确性</li>
 *   <li>查询元数据 - 获取数据库、表、列的元数据信息</li>
 *   <li>权限管理 - 基于用户角色进行访问控制</li>
 * </ul>
 *
 * <p>该类通过插件化架构支持动态扩展新的数据源类型。</p>
 */
@Service
@Slf4j
public class DataSourceServiceImpl extends BaseServiceImpl implements DataSourceService {

    @Autowired
    private DataSourceMapper dataSourceMapper;

    @Autowired
    private DataSourceUserMapper datasourceUserMapper;

    /** 表类型常量 - TABLE */
    private static final String TABLE = "TABLE";
    /** 视图类型常量 - VIEW */
    private static final String VIEW = "VIEW";
    /** 支持的表类型数组 */
    private static final String[] TABLE_TYPES = new String[]{TABLE, VIEW};
    /** 表名列名常量 */
    private static final String TABLE_NAME = "TABLE_NAME";
    /** 列名常量 */
    private static final String COLUMN_NAME = "COLUMN_NAME";

    /**
     * 创建数据源
     *
     * @param loginUser       登录用户
     * @param datasourceParam 数据源参数
     * @return 创建的数据源对象
     */
    @Override
    public DataSource createDataSource(User loginUser, BaseDataSourceParamDTO datasourceParam) {
        // 参数校验：验证数据源参数的完整性和合法性（包括连接信息、数据库类型等）
        DataSourceUtils.checkDatasourceParam(datasourceParam);

        // 权限验证：检查用户是否有创建数据源的权限
        if (!canOperatorPermissions(loginUser, null, AuthorizationType.DATASOURCE,
                ApiFuncIdentificationConstant.DATASOURCE_CREATE_DATASOURCE)) {
            // 权限不足，抛出无操作权限异常
            throw new ServiceException(Status.USER_NO_OPERATION_PERM);
        }

        // 名称唯一性检查：确保数据源名称不与现有数据源冲突
        if (checkName(datasourceParam.getName())) {
            // 数据源名称已存在，抛出重复异常
            throw new ServiceException(Status.DATASOURCE_EXIST);
        }

        // 参数长度校验：检查描述信息是否超过系统限制
        if (checkDescriptionLength(datasourceParam.getNote())) {
            // 描述过长，抛出参数错误异常
            throw new ServiceException(Status.DESCRIPTION_TOO_LONG_ERROR);
        }

        // 连接参数构建：将DTO参数转换为数据库连接参数对象
        ConnectionParam connectionParam = DataSourceUtils.buildConnectionParams(datasourceParam);

        // 创建数据源实体对象
        DataSource dataSource = new DataSource();
        // 获取当前时间作为创建和更新时间
        Date now = new Date();

        // 设置数据源基本属性
        dataSource.setName(datasourceParam.getName().trim());  // 设置数据源名称（去除首尾空格）
        dataSource.setNote(datasourceParam.getNote());        // 设置数据源备注信息
        dataSource.setUserId(loginUser.getId());              // 设置数据源创建者ID
        dataSource.setUserName(loginUser.getUserName());      // 设置数据源创建者名称
        dataSource.setType(datasourceParam.getType());        // 设置数据源类型（MySQL、PostgreSQL等）
        dataSource.setConnectionParams(JSONUtils.toJsonString(connectionParam));  // 将连接参数序列化为JSON字符串存储
        dataSource.setCreateTime(now);                        // 设置创建时间
        dataSource.setUpdateTime(now);                        // 设置更新时间

        // 数据库事务操作：插入数据源记录
        try {
            // 执行数据库插入操作
            dataSourceMapper.insert(dataSource);
            // 返回创建成功的数据源对象
            return dataSource;
        } catch (DuplicateKeyException ex) {
            // 处理数据库唯一键冲突异常（名称重复）
            throw new ServiceException(Status.DATASOURCE_EXIST);
        }
    }

    /**
     * 更新数据源
     *
     * @param loginUser       登录用户
     * @param dataSourceParam 数据源参数
     * @return 更新后的数据源对象
     */
    @Override
    public DataSource updateDataSource(User loginUser, BaseDataSourceParamDTO dataSourceParam) {
        // 参数校验：验证更新参数的完整性和合法性
        DataSourceUtils.checkDatasourceParam(dataSourceParam);

        // 存在性检查：验证要更新的数据源是否存在
        DataSource dataSource = dataSourceMapper.selectById(dataSourceParam.getId());
        if (dataSource == null) {
            // 数据源不存在，抛出资源不存在异常
            throw new ServiceException(Status.RESOURCE_NOT_EXIST);
        }

        // 权限验证：检查用户是否有更新指定数据源的权限
        if (!canOperatorPermissions(loginUser, new Object[]{dataSource.getId()}, AuthorizationType.DATASOURCE,
                DATASOURCE_UPDATE)) {
            // 权限不足，抛出无操作权限异常
            throw new ServiceException(Status.USER_NO_OPERATION_PERM);
        }

        // 名称唯一性检查：如果名称发生变更，需要检查新名称是否与其他数据源冲突
        if (!dataSourceParam.getName().trim().equals(dataSource.getName()) && checkName(dataSourceParam.getName())) {
            // 新名称已被其他数据源使用，抛出重复异常
            throw new ServiceException(Status.DATASOURCE_EXIST);
        }

        // 参数长度验证：检查描述信息长度是否符合要求
        if (checkDescriptionLength(dataSourceParam.getNote())) {
            // 描述过长，抛出参数错误异常
            throw new ServiceException(Status.DESCRIPTION_TOO_LONG_ERROR);
        }

        // 连接参数重新构建：根据新参数生成连接配置
        ConnectionParam connectionParam = DataSourceUtils.buildConnectionParams(dataSourceParam);

        // 获取新的密码信息
        String password = connectionParam.getPassword();

        // 密码处理策略：如果新密码为空，则保持原有密码不变
        if (StringUtils.isBlank(password)) {
            // 获取原有数据源的连接参数
            String oldConnectionParams = dataSource.getConnectionParams();
            // 解析原有连接参数为JSON对象
            ObjectNode oldParams = JSONUtils.parseObject(oldConnectionParams);
            // 提取原有密码并设置到新的连接参数中
            connectionParam.setPassword(oldParams.path(Constants.PASSWORD).asText());
        }

        // 获取当前时间作为更新时间
        Date now = new Date();

        // 更新数据源对象属性
        dataSource.setName(dataSourceParam.getName().trim());  // 更新数据源名称
        dataSource.setNote(dataSourceParam.getNote());        // 更新数据源备注
        dataSource.setUserName(loginUser.getUserName());      // 更新操作用户名
        dataSource.setType(dataSource.getType());             // 保持数据源类型不变
        dataSource.setConnectionParams(JSONUtils.toJsonString(connectionParam)); // 更新连接参数
        dataSource.setUpdateTime(now);                        // 更新修改时间

        // 数据库更新操作
        try {
            // 执行数据库更新操作
            dataSourceMapper.updateById(dataSource);
            // 返回更新后的数据源对象
            return dataSource;
        } catch (DuplicateKeyException ex) {
            // 处理数据库唯一键冲突异常（名称重复）
            throw new ServiceException(Status.DATASOURCE_EXIST);
        }
    }

    private boolean checkName(String name) {
        // 名称唯一性检查：查询数据库中是否已存在同名数据源（去除首尾空格后查询）
        List<DataSource> queryDataSource = dataSourceMapper.queryDataSourceByName(name.trim());
        // 返回检查结果：如果查询结果非空且列表不为空，则表示名称已存在
        return queryDataSource != null && !queryDataSource.isEmpty();
    }

    /**
     * updateWorkflowInstance datasource
     *
     * @param id datasource id
     * @return data source detail
     */
    @Override
    public BaseDataSourceParamDTO queryDataSource(int id, User loginUser) {
        // 数据源查询：根据ID查询数据源记录
        DataSource dataSource = dataSourceMapper.selectById(id);
        // 存在性验证：检查数据源是否存在
        if (dataSource == null) {
            // 数据源不存在，记录错误日志并抛出异常
            log.error("Datasource does not exist, id:{}.", id);
            throw new ServiceException(Status.RESOURCE_NOT_EXIST);
        }

        // 权限验证：检查用户是否有查看指定数据源的权限
        if (!canOperatorPermissions(loginUser, new Object[]{dataSource.getId()}, AuthorizationType.DATASOURCE,
                ApiFuncIdentificationConstant.DATASOURCE)) {
            // 权限不足，抛出无操作权限异常
            throw new ServiceException(Status.USER_NO_OPERATION_PERM);
        }

        // 数据转换：将数据库实体转换为前端显示DTO对象
        BaseDataSourceParamDTO baseDataSourceParamDTO = DataSourceUtils.buildDatasourceParamDTO(
                dataSource.getType(), dataSource.getConnectionParams());
        // 设置DTO基本属性
        baseDataSourceParamDTO.setId(dataSource.getId());      // 设置数据源ID
        baseDataSourceParamDTO.setName(dataSource.getName());  // 设置数据源名称
        baseDataSourceParamDTO.setNote(dataSource.getNote());  // 设置数据源备注
        // 安全处理：隐藏真实密码，返回掉蒙版密码保障安全
        baseDataSourceParamDTO.setPassword(getHiddenPassword());

        // 返回数据源详细信息
        return baseDataSourceParamDTO;
    }

    /**
     * query datasource list by keyword
     *
     * @param loginUser login user
     * @param searchVal search value
     * @param pageNo page number
     * @param pageSize page size
     * @return data source list page
     */
    @Override
    public PageInfo<DataSource> queryDataSourceListPaging(User loginUser, String searchVal, Integer pageNo,
                                                          Integer pageSize) {
        // 声明分页查询结果变量
        IPage<DataSource> dataSourceList;
        // 创建MyBatis-Plus分页对象
        Page<DataSource> dataSourcePage = new Page<>(pageNo, pageSize);
        // 初始化返回的分页信息对象
        PageInfo<DataSource> pageInfo = new PageInfo<>(pageNo, pageSize);

        // 根据用户类型分别处理查询逻辑
        if (loginUser.getUserType().equals(UserType.ADMIN_USER)) {
            // 管理员权限：可以查询所有数据源（userId=0表示不限制用户）
            dataSourceList = dataSourceMapper.selectPaging(dataSourcePage, 0, searchVal);
        } else {
            // 普通用户权限限制：获取用户有权限的数据源ID集合
            Set<Integer> ids = resourcePermissionCheckService
                    .userOwnedResourceIdsAcquisition(AuthorizationType.DATASOURCE, loginUser.getId(), log);
            // 检查用户是否有任何数据源权限
            if (ids.isEmpty()) {
                // 无权限时返回空的分页对象
                return pageInfo;
            }
            // 根据权限ID列表进行受限分页查询
            dataSourceList = dataSourceMapper.selectPagingByIds(dataSourcePage, new ArrayList<>(ids), searchVal);
        }

        // 提取查询结果列表，处理空值情况
        List<DataSource> dataSources = dataSourceList != null ? dataSourceList.getRecords() : new ArrayList<>();
        // 安全处理：隐藏数据源列表中的敏感密码信息
        handlePasswd(dataSources);
        // 设置分页总记录数
        pageInfo.setTotal((int) (dataSourceList != null ? dataSourceList.getTotal() : 0L));
        // 设置分页数据列表
        pageInfo.setTotalList(dataSources);
        // 返回完整的分页结果
        return pageInfo;
    }

    /**
     * handle datasource connection password for safety
     */
    private void handlePasswd(List<DataSource> dataSourceList) {
        // 批量处理数据源列表中的密码信息，保障数据安全
        for (DataSource dataSource : dataSourceList) {
            // 获取数据源的连接参数JSON字符串
            String connectionParams = dataSource.getConnectionParams();
            // 解析JSON字符串为可操作的JSON对象
            ObjectNode object = JSONUtils.parseObject(connectionParams);
            // 替换真实密码为挣蔣版密码，防止密码泄露
            object.put(Constants.PASSWORD, getHiddenPassword());
            // 将处理后的JSON对象转换回字符串并更新数据源对象
            dataSource.setConnectionParams(object.toString());
        }
    }

    /**
     * get hidden password (resolve the security hotspot)
     *
     * @return hidden password
     */
    private String getHiddenPassword() {
        // 返回摳蔣密码常量，用于隐藏真实密码信息，提高系统安全性
        return Constants.XXXXXX;
    }

    /**
     * query data resource list
     *
     * @param loginUser login user
     * @param type data source type
     * @return data source list page
     */
    @Override
    public List<DataSource> queryDataSourceList(User loginUser, Integer type) {

        // 声明数据源列表变量
        List<DataSource> datasourceList;
        // 根据用户类型采用不同的查询策略
        if (loginUser.getUserType().equals(UserType.ADMIN_USER)) {
            // 管理员权限：可以查询所有指定类型的数据源（userId=0表示不限制用户）
            datasourceList = dataSourceMapper.queryDataSourceByType(0, type);
        } else {
            // 普通用户权限控制：获取用户有权限的数据源ID集合
            Set<Integer> ids = resourcePermissionCheckService
                    .userOwnedResourceIdsAcquisition(AuthorizationType.DATASOURCE, loginUser.getId(), log);
            // 检查用户是否有任何数据源权限
            if (ids.isEmpty()) {
                // 无权限时返回空列表
                return Collections.emptyList();
            }
            // 根据权限ID列表查询数据源，并过滤出指定类型的数据源
            datasourceList = dataSourceMapper.selectBatchIds(ids).stream()
                    .filter(dataSource -> dataSource.getType().getCode() == type).collect(Collectors.toList());
        }

        // 返回过滤后的数据源列表
        return datasourceList;
    }

    /**
     * verify datasource exists
     *
     * @param name datasource name
     * @return true if data datasource not exists, otherwise return false
     */
    @Override
    public void verifyDataSourceName(String name) {
        // 名称唯一性验证：查询数据库中是否已存在同名数据源
        List<DataSource> dataSourceList = dataSourceMapper.queryDataSourceByName(name);
        // 检查查询结果
        if (dataSourceList != null && !dataSourceList.isEmpty()) {
            // 数据源名称已存在，抛出重复异常
            throw new ServiceException(Status.DATASOURCE_EXIST);
        }
    }

    /**
     * check connection
     *
     * @param type            data source type
     * @param connectionParam connectionParam
     * @return true if connect successfully, otherwise false
     * @return true if connect successfully, otherwise false
     */
    @Override
    public void checkConnection(DbType type, ConnectionParam connectionParam) {
        // 获取数据源处理器：根据数据库类型获取对应的连接处理器
        DataSourceProcessor sshDataSourceProcessor = DataSourceUtils.getDatasourceProcessor(type);
        // 执行连接测试：使用处理器测试数据源连接的可用性
        boolean connectivity = sshDataSourceProcessor.checkDataSourceConnectivity(connectionParam);
        // 检查连接结果
        if (connectivity) {
            // 连接成功，正常返回
            return;
        }
        // 连接失败，抛出连接测试失败异常
        throw new ServiceException(Status.CONNECTION_TEST_FAILURE);
    }

    /**
     * test connection
     *
     * @param id datasource id
     * @return connect result code
     */
    @Override
    public void connectionTest(int id) {
        // 数据源查询：根据ID获取数据源信息
        DataSource dataSource = dataSourceMapper.selectById(id);
        // 存在性验证：检查数据源是否存在
        if (dataSource == null) {
            // 数据源不存在，抛出资源不存在异常
            throw new ServiceException(Status.RESOURCE_NOT_EXIST);
        }
        // 执行连接测试：使用数据源的类型和连接参数进行连接测试
        checkConnection(dataSource.getType(),
                DataSourceUtils.buildConnectionParams(dataSource.getType(), dataSource.getConnectionParams()));
    }

    /**
     * delete datasource
     *
     * @param loginUser    login user
     * @param datasourceId data source id
     * @return delete result code
     */
    @Override
    @Transactional
    public void delete(User loginUser, int datasourceId) {
        // 数据源查询：根据ID查询要删除的数据源
        DataSource dataSource = dataSourceMapper.selectById(datasourceId);
        // 存在性验证：检查数据源是否存在
        if (dataSource == null) {
            // 数据源不存在，抛出资源不存在异常
            throw new ServiceException(Status.RESOURCE_NOT_EXIST);
        }
        // 权限验证：检查用户是否有删除指定数据源的权限
        if (!canOperatorPermissions(loginUser, new Object[]{dataSource.getId()}, AuthorizationType.DATASOURCE,
                DATASOURCE_DELETE)) {
            // 权限不足，抛出无操作权限异常
            throw new ServiceException(Status.USER_NO_OPERATION_PERM);
        }
        // 数据库事务操作：删除数据源主记录
        dataSourceMapper.deleteById(datasourceId);
        // 级联删除：同时删除数据源的用户关联关系，保证数据一致性
        datasourceUserMapper.deleteByDatasourceId(datasourceId);
    }

    /**
     * unauthorized datasource
     *
     * @param loginUser login user
     * @param userId user id
     * @return unauthed data source result code
     */
    @Override
    public List<DataSource> unAuthDatasource(User loginUser, Integer userId) {
        List<DataSource> datasourceList;
        if (canOperatorPermissions(loginUser, null, AuthorizationType.DATASOURCE, null)) {
            // admin gets all data sources except userId
            datasourceList = dataSourceMapper.queryDatasourceExceptUserId(userId);
        } else {
            // non-admins users get their own data sources
            datasourceList = dataSourceMapper.selectByMap(Collections.singletonMap("user_id", loginUser.getId()));
        }
        List<DataSource> resultList = new ArrayList<>();
        Set<DataSource> datasourceSet;
        if (datasourceList != null && !datasourceList.isEmpty()) {
            datasourceSet = new HashSet<>(datasourceList);

            List<DataSource> authedDataSourceList = dataSourceMapper.queryAuthedDatasource(userId);

            Set<DataSource> authedDataSourceSet;
            if (authedDataSourceList != null && !authedDataSourceList.isEmpty()) {
                authedDataSourceSet = new HashSet<>(authedDataSourceList);
                datasourceSet.removeAll(authedDataSourceSet);
            }
            resultList = new ArrayList<>(datasourceSet);
        }
        return resultList;
    }

    /**
     * authorized datasource
     *
     * @param loginUser login user
     * @param userId user id
     * @return authorized result code
     */
    @Override
    public List<DataSource> authedDatasource(User loginUser, Integer userId) {
        List<DataSource> authedDatasourceList = dataSourceMapper.queryAuthedDatasource(userId);
        return authedDatasourceList;
    }

    @Override
    public List<ParamsOptions> getTables(Integer datasourceId, String database) {
        DataSource dataSource = dataSourceMapper.selectById(datasourceId);

        List<String> tableList;
        BaseConnectionParam connectionParam =
                (BaseConnectionParam) DataSourceUtils.buildConnectionParams(
                        dataSource.getType(),
                        dataSource.getConnectionParams());

        if (null == connectionParam) {
            throw new ServiceException(Status.DATASOURCE_CONNECT_FAILED);
        }

        Connection connection =
                DataSourceUtils.getConnection(dataSource.getType(), connectionParam);
        ResultSet tables = null;

        try {

            if (null == connection) {
                throw new ServiceException(Status.DATASOURCE_CONNECT_FAILED);
            }

            DatabaseMetaData metaData = connection.getMetaData();
            String schema = null;
            try {
                schema = metaData.getConnection().getSchema();
            } catch (SQLException e) {
                log.error("Cant not get the schema, datasourceId:{}.", datasourceId, e);
                throw new ServiceException(Status.GET_DATASOURCE_TABLES_ERROR);
            }

            tables = metaData.getTables(
                    database,
                    getDbSchemaPattern(dataSource.getType(), schema, connectionParam),
                    "%", TABLE_TYPES);
            if (null == tables) {
                log.error("Get datasource tables error, datasourceId:{}.", datasourceId);
                throw new ServiceException(Status.GET_DATASOURCE_TABLES_ERROR);
            }

            tableList = new ArrayList<>();
            while (tables.next()) {
                String name = tables.getString(TABLE_NAME);
                tableList.add(name);
            }

        } catch (Exception e) {
            log.error("Get datasource tables error, datasourceId:{}.", datasourceId, e);
            throw new ServiceException(Status.GET_DATASOURCE_TABLES_ERROR);
        } finally {
            closeResult(tables);
            releaseConnection(connection);
        }

        List<ParamsOptions> options = getParamsOptions(tableList);
        return options;
    }

    @Override
    public List<ParamsOptions> getTableColumns(Integer datasourceId, String database, String tableName) {
        DataSource dataSource = dataSourceMapper.selectById(datasourceId);
        BaseConnectionParam connectionParam =
                (BaseConnectionParam) DataSourceUtils.buildConnectionParams(
                        dataSource.getType(),
                        dataSource.getConnectionParams());

        if (null == connectionParam) {
            throw new ServiceException(Status.DATASOURCE_CONNECT_FAILED);
        }

        Connection connection =
                DataSourceUtils.getConnection(dataSource.getType(), connectionParam);
        List<String> columnList = new ArrayList<>();
        ResultSet rs = null;

        try {
            if (null == connection) {
                throw new ServiceException(Status.DATASOURCE_CONNECT_FAILED);
            }

            DatabaseMetaData metaData = connection.getMetaData();

            if (dataSource.getType() == DbType.ORACLE) {
                database = null;
            }
            rs = metaData.getColumns(database, null, tableName, "%");
            if (rs == null) {
                throw new ServiceException(Status.DATASOURCE_CONNECT_FAILED);
            }
            while (rs.next()) {
                columnList.add(rs.getString(COLUMN_NAME));
            }
        } catch (Exception e) {
            log.error("Get datasource table columns error, datasourceId:{}.", dataSource.getId(), e);
            throw new ServiceException(Status.DATASOURCE_CONNECT_FAILED);
        } finally {
            closeResult(rs);
            releaseConnection(connection);
        }

        List<ParamsOptions> options = getParamsOptions(columnList);
        return options;
    }

    @Override
    public List<ParamsOptions> getDatabases(Integer datasourceId) {

        DataSource dataSource = dataSourceMapper.selectById(datasourceId);

        if (dataSource == null) {
            throw new ServiceException(Status.QUERY_DATASOURCE_ERROR);
        }

        List<String> tableList;
        BaseConnectionParam connectionParam =
                (BaseConnectionParam) DataSourceUtils.buildConnectionParams(
                        dataSource.getType(),
                        dataSource.getConnectionParams());

        if (null == connectionParam) {
            throw new ServiceException(Status.DATASOURCE_CONNECT_FAILED);
        }

        Connection connection =
                DataSourceUtils.getConnection(dataSource.getType(), connectionParam);
        ResultSet rs = null;

        try {
            if (null == connection) {
                throw new ServiceException(Status.DATASOURCE_CONNECT_FAILED);
            }
            if (dataSource.getType() == DbType.POSTGRESQL) {
                rs = connection.createStatement().executeQuery(Constants.DATABASES_QUERY_PG);
            } else {
                rs = connection.createStatement().executeQuery(Constants.DATABASES_QUERY);
            }
            tableList = new ArrayList<>();
            while (rs.next()) {
                String name = rs.getString(1);
                tableList.add(name);
            }
        } catch (Exception e) {
            log.error("Get databases error, datasourceId:{}.", datasourceId, e);
            throw new ServiceException(Status.GET_DATASOURCE_TABLES_ERROR);
        } finally {
            closeResult(rs);
            releaseConnection(connection);
        }

        List<ParamsOptions> options = getParamsOptions(tableList);
        return options;
    }

    private List<ParamsOptions> getParamsOptions(List<String> columnList) {
        List<ParamsOptions> options = null;
        if (CollectionUtils.isNotEmpty(columnList)) {
            options = new ArrayList<>();

            for (String column : columnList) {
                ParamsOptions childrenOption =
                        new ParamsOptions(column, column, false);
                options.add(childrenOption);
            }
        }
        return options;
    }

    private String getDbSchemaPattern(DbType dbType, String schema, BaseConnectionParam connectionParam) {
        if (dbType == null) {
            return null;
        }
        String schemaPattern = null;
        switch (dbType) {
            case HIVE:
                schemaPattern = connectionParam.getDatabase();
                break;
            case ORACLE:
                schemaPattern = connectionParam.getUser();
                if (null != schemaPattern) {
                    schemaPattern = schemaPattern.toUpperCase();
                }
                break;
            case SQLSERVER:
                schemaPattern = "dbo";
                break;
            case CLICKHOUSE:
            case DATABEND:
            case PRESTO:
                if (!StringUtils.isEmpty(schema)) {
                    schemaPattern = schema;
                }
                break;
            default:
                break;
        }
        return schemaPattern;
    }

    private static void releaseConnection(Connection connection) {
        if (null != connection) {
            try {
                connection.close();
            } catch (Exception e) {
                log.error("Connection release error", e);
            }
        }
    }

    private static void closeResult(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (Exception e) {
                log.error("ResultSet close error", e);
            }
        }
    }

}
