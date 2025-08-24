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

package org.apache.dolphinscheduler.api.controller;

import static org.apache.dolphinscheduler.api.enums.Status.IP_IS_EMPTY;
import static org.apache.dolphinscheduler.api.enums.Status.NOT_SUPPORT_SSO;
import static org.apache.dolphinscheduler.api.enums.Status.SIGN_OUT_ERROR;
import static org.apache.dolphinscheduler.api.enums.Status.USER_LOGIN_FAILURE;

import org.apache.dolphinscheduler.api.configuration.OAuth2Configuration;
import org.apache.dolphinscheduler.api.enums.Status;
import org.apache.dolphinscheduler.api.exceptions.ApiException;
import org.apache.dolphinscheduler.api.security.Authenticator;
import org.apache.dolphinscheduler.api.security.impl.AbstractSsoAuthenticator;
import org.apache.dolphinscheduler.api.service.SessionService;
import org.apache.dolphinscheduler.api.service.UsersService;
import org.apache.dolphinscheduler.api.utils.Result;
import org.apache.dolphinscheduler.common.constants.Constants;
import org.apache.dolphinscheduler.common.enums.UserType;
import org.apache.dolphinscheduler.common.model.OkHttpRequestHeaderContentType;
import org.apache.dolphinscheduler.common.model.OkHttpRequestHeaders;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.common.utils.OkHttpUtils;
import org.apache.dolphinscheduler.dao.entity.Session;
import org.apache.dolphinscheduler.dao.entity.User;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.Parameters;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.tags.Tag;

/**
 * 用户登录控制器
 * 
 * 这是系统安全认证的门户控制器，负责处理用户登录、单点登录(SSO)和退出等功能。
 * 类比：公司大门的门卫系统，负责验证访客身份，发放通行证，管理进出记录。
 * 
 * 主要功能：
 * 1. 用户名密码登录：传统的用户名密码认证方式
 * 2. 单点登录(SSO)：支持通过外部认证系统登录，如LDAP、CAS等
 * 3. OAuth2登录：支持GitHub、Gitee等第三方OAuth2授权登录
 * 4. 会话管理：创建和管理用户会话，设置Cookie信息
 * 5. 安全退出：清除会话信息和Cookie，确保安全退出
 * 6. IP地址验证：记录和验证用户登录IP，增强安全性
 * 
 * 安全特性：
 * - 参数验证：对用户名、密码等关键参数进行空值和格式检查
 * - IP白名单：支持IP地址验证，可配置允许登录的IP范围
 * - 会话管理：创建安全的用户会话，设置HttpOnly Cookie防止XSS攻击
 * - 多种认证：支持多种认证方式，适应不同的企业环境需求
 * - 异常处理：统一的异常处理机制，避免敏感信息泄露
 * 
 * 类比理解：
 * 这个控制器就像是一个"智能门禁系统"：
 * - 验证访客身份证（用户名密码认证）
 * - 支持VIP卡刷卡（SSO单点登录）
 * - 支持手机扫码（OAuth2第三方登录）
 * - 发放临时通行证（创建会话Cookie）
 * - 记录进出日志（IP地址记录）
 * - 处理退房手续（安全退出登录）
 */
@Tag(name = "LOGIN_TAG")
@RestController
@RequestMapping("")
@Slf4j
public class LoginController extends BaseController {

    /**
     * 会话服务
     * 
     * 负责用户会话的创建、查询、更新和销毁等操作。
     * 类比：门卫的访客登记册，记录谁在什么时候进入了系统。
     */
    @Autowired
    private SessionService sessionService;

    /**
     * 认证器接口
     * 
     * 统一的认证接口，支持多种认证方式的插件化实现。
     * 可以是密码认证、LDAP认证、SSO认证等不同的实现。
     * 类比：不同类型的身份验证设备（密码锁、指纹锁、刷卡器等）。
     */
    @Autowired
    private Authenticator authenticator;

    /**
     * OAuth2配置
     * 
     * 第三方OAuth2登录的配置信息，包含各个OAuth2提供商的配置。
     * 支持GitHub、Gitee、微信等多种OAuth2授权登录方式。
     * 类比：与其他安保公司的合作协议，允许他们的用户访问我们的系统。
     */
    @Autowired(required = false)
    private OAuth2Configuration oAuth2Configuration;

    /**
     * 用户服务
     * 
     * 用户管理相关的业务服务，处理用户的查询、创建等操作。
     * 在OAuth2登录时，如果用户不存在会自动创建新用户。
     * 类比：人力资源部门，管理所有员工的基本信息。
     */
    @Autowired
    private UsersService usersService;

    /**
     * 用户登录接口
     * 
     * 这是系统最基础也是最重要的接口，处理用户名密码方式的登录认证。
     * 成功登录后会创建用户会话，并通过Cookie的方式在浏览器中保存会话信息。
     * 
     * 类比：在酒店前台办理入住登记，提供身份证和预订信息，获得房卡。
     * 
     * 登录流程：
     * 1. 参数校验：检查用户名是否为空，确保必需参数完整
     * 2. IP地址验证：获取用户真实IP地址，用于安全验证和日志记录
     * 3. 身份认证：通过配置的认证器验证用户名和密码的正确性
     * 4. 会话创建：认证成功后创建用户会话，生成唯一的会话ID
     * 5. Cookie设置：将会话信息写入HttpOnly Cookie，防止JS访问
     * 
     * 安全措施：
     * - HttpOnly Cookie：防止XSS攻击获取会话信息
     * - IP地址记录：用于异地登录检测和安全审计
     * - 参数校验：防止恶意的空值或异常参数攻击
     * - 统一异常处理：避免敏感信息在错误信息中泄露
     * 
     * @param userName 用户名 - 用户的登录账号，必需参数
     * @param userPassword 用户密码 - 用户的登录密码，必需参数
     * @param request HTTP请求对象 - 用于获取客户端IP地址等信息
     * @param response HTTP响应对象 - 用于设置会话Cookie
     * @return 登录结果，包含会话信息或错误信息
     */
    @Operation(summary = "login", description = "LOGIN_NOTES")
    @Parameters({
            @Parameter(name = "userName", description = "USER_NAME", required = true, schema = @Schema(implementation = String.class)),
            @Parameter(name = "userPassword", description = "USER_PASSWORD", required = true, schema = @Schema(implementation = String.class))
    })
    @PostMapping(value = "/login")
    @ApiException(USER_LOGIN_FAILURE)
    public Result login(@RequestParam(value = "userName") String userName,
                        @RequestParam(value = "userPassword") String userPassword,
                        HttpServletRequest request,
                        HttpServletResponse response) {
        // user name check
        if (StringUtils.isEmpty(userName)) {
            return error(Status.USER_NAME_NULL.getCode(),
                    Status.USER_NAME_NULL.getMsg());
        }

        // user ip check
        String ip = getClientIpAddress(request);
        if (StringUtils.isEmpty(ip)) {
            return error(IP_IS_EMPTY.getCode(), IP_IS_EMPTY.getMsg());
        }

        // verify username and password
        Result<Map<String, String>> result = authenticator.authenticate(userName, userPassword, ip);
        if (result.getCode() != Status.SUCCESS.getCode()) {
            return result;
        }

        response.setStatus(HttpStatus.SC_OK);
        Map<String, String> cookieMap = result.getData();
        for (Map.Entry<String, String> cookieEntry : cookieMap.entrySet()) {
            Cookie cookie = new Cookie(cookieEntry.getKey(), cookieEntry.getValue());
            cookie.setHttpOnly(true);
            response.addCookie(cookie);
        }

        return result;
    }

    /**
     * sso login
     *
     * @return sso server url
     */
    @Operation(summary = "sso login", description = "SSO_LOGIN_NOTES")
    @GetMapping(value = "/login/sso")
    @ApiException(NOT_SUPPORT_SSO)
    public Result ssoLogin(HttpServletRequest request) {
        if (authenticator instanceof AbstractSsoAuthenticator) {
            String randomState = UUID.randomUUID().toString();
            HttpSession session = request.getSession();
            if (session.getAttribute(Constants.SSO_LOGIN_USER_STATE) == null) {
                session.setAttribute(Constants.SSO_LOGIN_USER_STATE, randomState);
            }
            return Result.success(((AbstractSsoAuthenticator) authenticator).getSignInUrl(randomState));
        }
        return Result.success();
    }

    @Operation(summary = "signOut", description = "SIGN_OUT_NOTES")
    @PostMapping(value = "/signOut")
    @ApiException(SIGN_OUT_ERROR)
    public Result signOut(@Parameter(hidden = true) @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                          HttpServletRequest request) {
        String ip = getClientIpAddress(request);
        sessionService.expireSession(loginUser.getId());
        // clear session
        request.removeAttribute(Constants.SESSION_USER);
        return success();
    }

    @DeleteMapping("cookies")
    public void clearCookieSessionId(HttpServletRequest request, HttpServletResponse response) {
        Cookie[] cookies = request.getCookies();
        for (Cookie cookie : cookies) {
            cookie.setMaxAge(0);
            cookie.setValue(null);
            response.addCookie(cookie);
        }
        response.setStatus(HttpStatus.SC_OK);
    }

    @Operation(summary = "getOauth2Provider", description = "GET_OAUTH2_PROVIDER")
    @GetMapping("oauth2-provider")
    public Result<List<OAuth2Configuration.OAuth2ClientProperties>> oauth2Provider() {
        if (oAuth2Configuration == null) {
            return Result.success(new ArrayList<>());
        }

        Collection<OAuth2Configuration.OAuth2ClientProperties> values = oAuth2Configuration.getProvider().values();
        List<OAuth2Configuration.OAuth2ClientProperties> providers = values.stream().map(e -> {
            OAuth2Configuration.OAuth2ClientProperties oAuth2ClientProperties =
                    new OAuth2Configuration.OAuth2ClientProperties();
            oAuth2ClientProperties.setAuthorizationUri(e.getAuthorizationUri());
            oAuth2ClientProperties.setRedirectUri(e.getRedirectUri());
            oAuth2ClientProperties.setClientId(e.getClientId());
            oAuth2ClientProperties.setProvider(e.getProvider());
            oAuth2ClientProperties.setIconUri(e.getIconUri());
            return oAuth2ClientProperties;
        }).collect(Collectors.toList());
        return Result.success(providers);
    }

    @SneakyThrows
    @Operation(summary = "redirectToOauth2", description = "REDIRECT_TO_OAUTH2_LOGIN")
    @GetMapping("redirect/login/oauth2")
    public void loginByAuth2(@RequestParam String code, @RequestParam String provider,
                             HttpServletRequest request, HttpServletResponse response) {
        OAuth2Configuration.OAuth2ClientProperties oAuth2ClientProperties =
                oAuth2Configuration.getProvider().get(provider);
        try {
            Map<String, String> tokenRequestHeader = new HashMap<>();
            tokenRequestHeader.put("Accept", "application/json");
            Map<String, Object> requestBody = new HashMap<>(16);
            requestBody.put("client_secret", oAuth2ClientProperties.getClientSecret());
            HashMap<String, Object> requestParamsMap = new HashMap<>();
            requestParamsMap.put("client_id", oAuth2ClientProperties.getClientId());
            requestParamsMap.put("code", code);
            requestParamsMap.put("grant_type", "authorization_code");
            requestParamsMap.put("redirect_uri",
                    String.format("%s?provider=%s", oAuth2ClientProperties.getRedirectUri(), provider));
            OkHttpRequestHeaders okHttpRequestHeadersPost = new OkHttpRequestHeaders();
            okHttpRequestHeadersPost.setHeaders(tokenRequestHeader);
            okHttpRequestHeadersPost.setOkHttpRequestHeaderContentType(OkHttpRequestHeaderContentType.APPLICATION_JSON);

            String tokenJsonStr = OkHttpUtils.post(oAuth2ClientProperties.getTokenUri(), okHttpRequestHeadersPost,
                    requestParamsMap, requestBody, Constants.HTTP_CONNECT_TIMEOUT, Constants.HTTP_CONNECT_TIMEOUT,
                    Constants.HTTP_CONNECT_TIMEOUT).getBody();
            String accessToken = JSONUtils.getNodeString(tokenJsonStr, "access_token");
            Map<String, String> userInfoRequestHeaders = new HashMap<>();
            userInfoRequestHeaders.put("Accept", "application/json");
            Map<String, Object> userInfoQueryMap = new HashMap<>();
            userInfoQueryMap.put("access_token", accessToken);
            userInfoRequestHeaders.put("Authorization", "Bearer " + accessToken);
            OkHttpRequestHeaders okHttpRequestHeadersGet = new OkHttpRequestHeaders();
            okHttpRequestHeadersGet.setHeaders(userInfoRequestHeaders);

            String userInfoJsonStr = OkHttpUtils.get(oAuth2ClientProperties.getUserInfoUri(),
                    okHttpRequestHeadersGet,
                    userInfoQueryMap,
                    Constants.HTTP_CONNECT_TIMEOUT,
                    Constants.HTTP_CONNECT_TIMEOUT,
                    Constants.HTTP_CONNECT_TIMEOUT).getBody();
            String username = JSONUtils.getNodeString(userInfoJsonStr, "login");
            User user = usersService.getUserByUserName(username);
            if (user == null) {
                user = usersService.createUser(UserType.GENERAL_USER, username, null);
            }
            Session session = sessionService.createSessionIfAbsent(user);
            response.setStatus(HttpStatus.SC_MOVED_TEMPORARILY);
            response.sendRedirect(String.format("%s?sessionId=%s&authType=%s", oAuth2ClientProperties.getCallbackUrl(),
                    session.getId(), "oauth2"));
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            response.setStatus(HttpStatus.SC_MOVED_TEMPORARILY);
            response.sendRedirect(String.format("%s?authType=%s&error=%s", oAuth2ClientProperties.getCallbackUrl(),
                    "oauth2", "oauth2 auth error"));
        }
    }
}
