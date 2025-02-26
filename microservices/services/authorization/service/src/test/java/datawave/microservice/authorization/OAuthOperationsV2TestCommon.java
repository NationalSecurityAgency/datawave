package datawave.microservice.authorization;

import static datawave.security.authorization.DatawaveUser.UserType.SERVER;
import static datawave.security.authorization.DatawaveUser.UserType.USER;
import static datawave.security.authorization.oauth.OAuthConstants.GRANT_AUTHORIZATION_CODE;
import static datawave.security.authorization.oauth.OAuthConstants.GRANT_REFRESH_TOKEN;
import static datawave.security.authorization.oauth.OAuthConstants.RESPONSE_TYPE_CODE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.net.ssl.SSLContext;

import org.junit.jupiter.api.Assertions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.autoconfigure.cache.CacheType;
import org.springframework.boot.test.autoconfigure.core.AutoConfigureCache;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.cloud.autoconfigure.RefreshAutoConfiguration;
import org.springframework.cloud.bus.BusProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.RequestEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.microservice.config.web.RestClientProperties;
import datawave.security.authorization.CachedDatawaveUserService;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.JWTTokenHandler;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.authorization.oauth.OAuthTokenResponse;
import datawave.security.authorization.oauth.OAuthUserInfo;
import datawave.security.util.ProxiedEntityUtils;

public class OAuthOperationsV2TestCommon {
    
    @LocalServerPort
    protected int webServicePort;
    
    @Autowired
    protected RestTemplateBuilder restTemplateBuilder;
    
    @Autowired
    protected JWTTokenHandler jwtTokenHandler;
    
    @Autowired
    protected SSLContext sslContext;
    
    @Autowired
    protected RestClientProperties restClientProperties;
    
    protected RestTemplate restTemplate;
    protected AuthorizationTestUtils testUtils;
    
    protected static Map<SubjectIssuerDNPair,DatawaveUser> userMap = new LinkedHashMap<>();
    protected final SubjectIssuerDNPair userDN = SubjectIssuerDNPair.of("cn=Test User, ou=testing, ou=development, o=testcorp, c=us",
                    "cn=testcorp ca, ou=security, o=testcorp, c=us");
    protected final SubjectIssuerDNPair notAllowedUserDN = SubjectIssuerDNPair.of(
                    "cn=notallowedcaller.testcorp.com, ou=microservices, ou=development, o=testcorp, c=us", "cn=testcorp ca, ou=security, o=testcorp, c=us");
    protected final SubjectIssuerDNPair serverDN = SubjectIssuerDNPair.of("cn=test.testcorp.com, ou=microservices, ou=development, o=testcorp, c=us",
                    "cn=testcorp ca, ou=security, o=testcorp, c=us");
    
    public DatawaveUser dwServer = new DatawaveUser(serverDN, SERVER, null, Arrays.asList("A", "B", "C", "D"), Arrays.asList("AuthorizedServer"), null,
                    System.currentTimeMillis());
    
    public DatawaveUser notAllowedDwUser = new DatawaveUser(notAllowedUserDN, USER, null, Arrays.asList("A", "B", "E"), Arrays.asList("AuthorizedUser"), null,
                    System.currentTimeMillis());
    
    public DatawaveUser dwUser = new DatawaveUser(userDN, USER, null, Arrays.asList("A", "B", "E"), Arrays.asList("AuthorizedUser"), null,
                    System.currentTimeMillis());
    
    private final String CLIENT_ID = "123456789";
    private final String CLIENT_SECRET = "secret";
    private final String REDIRECT_URI = "https://localhost/redirect";
    
    public enum AUTH_TYPE {
        TRUSTED_HEADER, JWT, NONE
    }
    
    public void TestCodeFlowValid(DatawaveUser dwUser, AUTH_TYPE authType) throws Exception {
        
        // Application redirects user's browser to the authorize endpoint (redirect not shown)
        // The user calls authorize with own credentials and gets redirected back to the application
        // This test is set up to not follow the redirect so that we can test the response
        String state = "randomstatestring";
        ResponseEntity<String> authorizeEntity = authorize(dwUser, authType, RESPONSE_TYPE_CODE, CLIENT_ID, REDIRECT_URI, state);
        assertEquals(302, authorizeEntity.getStatusCode().value(), "Expecting a 302 redirect");
        List<String> valueList = authorizeEntity.getHeaders().get("Location");
        assertEquals(1, valueList.size());
        assertTrue(valueList.get(0).startsWith(REDIRECT_URI), "Redirect location should start with redirect_uri");
        
        Map<String,List<String>> queryParams = splitQuery(new URL(valueList.get(0)));
        List<String> stateList = queryParams.get("state");
        assertEquals(1, stateList.size());
        assertEquals(state, stateList.get(0), "If state parameter is send, it should be returned");
        List<String> codeList = queryParams.get("code");
        assertEquals(1, codeList.size());
        
        // This is the short-lived code that an application needs to get a user's token
        String code = codeList.get(0);
        
        // After the user's browser gets redirected to the application, the application now has the code
        // and can call the token endpoint to get this user's token
        ResponseEntity<OAuthTokenResponse> tokenEntity = token(dwServer, GRANT_AUTHORIZATION_CODE, CLIENT_ID, CLIENT_SECRET, code, REDIRECT_URI, null);
        assertEquals(200, tokenEntity.getStatusCode().value());
        
        OAuthTokenResponse oAuthTokenResponse = tokenEntity.getBody();
        assertEquals(200, tokenEntity.getStatusCode().value());
        
        String access_token = oAuthTokenResponse.getAccess_token();
        Collection<DatawaveUser> usersFromToken = jwtTokenHandler.createUsersFromToken(access_token);
        // The DatawaveUser of both the user and the server should be in the token
        // The server is proxying for the user and must also be authenticated
        assertEquals(2, usersFromToken.size());
        
        // Call the user endpoint with the access_token to get the primary user
        ResponseEntity<OAuthUserInfo> userResponse = user(access_token, JWTTokenHandler.PRINCIPALS_CLAIM);
        OAuthUserInfo oAuthUserInfo = userResponse.getBody();
        assertEquals(ProxiedEntityUtils.getCommonName(dwUser.getDn().subjectDN()), oAuthUserInfo.getName());
        assertEquals(dwUser.getLogin(), oAuthUserInfo.getLogin());
        assertEquals(dwUser.getEmail(), oAuthUserInfo.getEmail());
        assertEquals(dwUser.getDn(), oAuthUserInfo.getDn());
        assertEquals(dwUser.getCreationTime(), oAuthUserInfo.getCreationTime());
        
        // Call the users endpoint with the access_token to get the all users
        ResponseEntity<OAuthUserInfo[]> usersResponse = users(access_token, JWTTokenHandler.PRINCIPALS_CLAIM);
        OAuthUserInfo[] users = usersResponse.getBody();
        assertNotNull(users);
        assertEquals(2, users.length, "Primary and proxying user should be returned");
        
        // Call the token endpoint with the refresh token id
        String refresh_token = oAuthTokenResponse.getRefresh_token();
        ResponseEntity<OAuthTokenResponse> refreshedTokenEntity = token(dwServer, GRANT_REFRESH_TOKEN, CLIENT_ID, CLIENT_SECRET, null, null, refresh_token);
        assertEquals(200, refreshedTokenEntity.getStatusCode().value());
    }
    
    public void TestCodeFlowInvalidClientId(DatawaveUser dwUser, AUTH_TYPE authType) {
        
        // Application redirects user's browser to the authorize endpoint (redirect not shown)
        // The user calls authorize with own credentials and gets redirected back to the application
        // This test is set up to not follow the redirect so that we can test the response
        try {
            authorize(dwUser, authType, RESPONSE_TYPE_CODE, "00000000", REDIRECT_URI, null);
        } catch (HttpStatusCodeException e) {
            assertEquals(HttpStatus.BAD_REQUEST, e.getStatusCode());
        }
        
        try {
            token(dwServer, GRANT_AUTHORIZATION_CODE, "00000000", CLIENT_SECRET, "wrongcode", REDIRECT_URI, null);
        } catch (HttpStatusCodeException e) {
            assertEquals(HttpStatus.BAD_REQUEST, e.getStatusCode());
        }
    }
    
    public void TestCodeFlowMissingResponseType(DatawaveUser dwUser, AUTH_TYPE authType) {
        
        // Application redirects user's browser to the authorize endpoint (redirect not shown)
        // The user calls authorize with own credentials and gets redirected back to the application
        // This test is set up to not follow the redirect so that we can test the response
        try {
            authorize(dwUser, authType, null, CLIENT_ID, REDIRECT_URI, null);
        } catch (HttpStatusCodeException e) {
            assertEquals(HttpStatus.BAD_REQUEST, e.getStatusCode());
        }
    }
    
    public void TestCodeFlowMissingRedirectUri(DatawaveUser dwUser, AUTH_TYPE authType) {
        
        // Application redirects user's browser to the authorize endpoint (redirect not shown)
        // The user calls authorize with own credentials and gets redirected back to the application
        // This test is set up to not follow the redirect so that we can test the response
        try {
            authorize(dwUser, authType, RESPONSE_TYPE_CODE, CLIENT_ID, null, null);
        } catch (HttpStatusCodeException e) {
            assertEquals(HttpStatus.BAD_REQUEST, e.getStatusCode());
        }
    }
    
    public void TestCodeFlowWrongCode() {
        
        try {
            token(dwServer, GRANT_AUTHORIZATION_CODE, CLIENT_ID, CLIENT_SECRET, "wrongcode", REDIRECT_URI, null);
        } catch (HttpStatusCodeException e) {
            assertEquals(HttpStatus.BAD_REQUEST, e.getStatusCode());
        }
    }
    
    public void TestCodeFlowRightCodeWrongOtherParameters(DatawaveUser dwUser, AUTH_TYPE authType) throws Exception {
        
        // Application redirects user's browser to the authorize endpoint (redirect not shown)
        // The user calls authorize with own credentials and gets redirected back to the application
        // This test is set up to not follow the redirect so that we can test the response
        ResponseEntity<String> authorizeEntity = authorize(dwUser, authType, RESPONSE_TYPE_CODE, CLIENT_ID, REDIRECT_URI, null);
        assertEquals(302, authorizeEntity.getStatusCode().value());
        List<String> valueList = authorizeEntity.getHeaders().get("Location");
        assertEquals(1, valueList.size());
        assertTrue(valueList.get(0).startsWith(REDIRECT_URI));
        
        Map<String,List<String>> queryParams = splitQuery(new URL(valueList.get(0)));
        List<String> codeList = queryParams.get("code");
        assertEquals(1, codeList.size());
        
        // This is the short-lived code that an application needs to get a user's token
        String code = codeList.get(0);
        
        try {
            token(dwServer, "invalid_grant_type", CLIENT_ID, CLIENT_SECRET, code, REDIRECT_URI, null);
        } catch (HttpStatusCodeException e) {
            assertEquals(HttpStatus.BAD_REQUEST, e.getStatusCode());
        }
        
        try {
            token(dwServer, GRANT_AUTHORIZATION_CODE, "invalidClientId", CLIENT_SECRET, code, REDIRECT_URI, null);
        } catch (HttpStatusCodeException e) {
            assertEquals(HttpStatus.BAD_REQUEST, e.getStatusCode());
        }
        
        try {
            token(dwServer, GRANT_AUTHORIZATION_CODE, CLIENT_ID, CLIENT_SECRET, code, REDIRECT_URI, null);
        } catch (HttpStatusCodeException e) {
            assertEquals(HttpStatus.BAD_REQUEST, e.getStatusCode());
        }
        
        try {
            token(dwServer, GRANT_AUTHORIZATION_CODE, CLIENT_ID, "wrongsecret", code, REDIRECT_URI, null);
        } catch (HttpStatusCodeException e) {
            assertEquals(HttpStatus.BAD_REQUEST, e.getStatusCode());
        }
        
        try {
            token(dwServer, GRANT_AUTHORIZATION_CODE, CLIENT_ID, CLIENT_SECRET, code, "https://different_redirect", null);
        } catch (HttpStatusCodeException e) {
            assertEquals(HttpStatus.BAD_REQUEST, e.getStatusCode());
        }
    }
    
    public void TestCodeFlowUseCodeTwice(DatawaveUser dwUser, AUTH_TYPE authType) throws Exception {
        
        // Application redirects user's browser to the authorize endpoint (redirect not shown)
        // The user calls authorize with own credentials and gets redirected back to the application
        // This test is set up to not follow the redirect so that we can test the response
        ResponseEntity<String> authorizeEntity = authorize(dwUser, authType, RESPONSE_TYPE_CODE, CLIENT_ID, REDIRECT_URI, null);
        assertEquals(302, authorizeEntity.getStatusCode().value());
        List<String> valueList = authorizeEntity.getHeaders().get("Location");
        assertEquals(1, valueList.size());
        assertTrue(valueList.get(0).startsWith(REDIRECT_URI));
        
        Map<String,List<String>> queryParams = splitQuery(new URL(valueList.get(0)));
        List<String> codeList = queryParams.get("code");
        assertEquals(1, codeList.size());
        
        // This is the short-lived code that an application needs to get a user's token
        String code = codeList.get(0);
        
        // After the user's browser gets redirected to the application, the application now has the code
        // and can call the token endpoint to get this user's token
        ResponseEntity<OAuthTokenResponse> tokenEntity = token(dwServer, GRANT_AUTHORIZATION_CODE, CLIENT_ID, CLIENT_SECRET, code, REDIRECT_URI, null);
        assertEquals(200, tokenEntity.getStatusCode().value());
        
        try {
            token(dwServer, GRANT_AUTHORIZATION_CODE, CLIENT_ID, CLIENT_SECRET, code, REDIRECT_URI, null);
        } catch (HttpStatusCodeException e) {
            assertEquals(HttpStatus.BAD_REQUEST, e.getStatusCode(), "Code should only be valid for one call");
        }
        
        try {
            token(dwServer, GRANT_AUTHORIZATION_CODE, "invalidClientId", CLIENT_SECRET, code, REDIRECT_URI, null);
        } catch (HttpStatusCodeException e) {
            assertEquals(HttpStatus.BAD_REQUEST, e.getStatusCode());
        }
        
        try {
            token(dwServer, GRANT_AUTHORIZATION_CODE, CLIENT_ID, CLIENT_SECRET, code, REDIRECT_URI, null);
        } catch (HttpStatusCodeException e) {
            assertEquals(HttpStatus.BAD_REQUEST, e.getStatusCode());
        }
        
        try {
            token(dwServer, GRANT_AUTHORIZATION_CODE, CLIENT_ID, "wrongsecret", code, REDIRECT_URI, null);
        } catch (HttpStatusCodeException e) {
            assertEquals(HttpStatus.BAD_REQUEST, e.getStatusCode());
        }
        
        try {
            token(dwServer, GRANT_AUTHORIZATION_CODE, CLIENT_ID, CLIENT_SECRET, code, "https://different_redirect", null);
        } catch (HttpStatusCodeException e) {
            assertEquals(HttpStatus.BAD_REQUEST, e.getStatusCode());
        }
    }
    
    private ResponseEntity<String> authorize(DatawaveUser dwUser, AUTH_TYPE authType, String response_type, String client_id, String redirect_uri,
                    String state) {
        
        DatawaveUserDetails trustedHeaderUser = authType.equals(AUTH_TYPE.TRUSTED_HEADER)
                        ? new DatawaveUserDetails(Collections.singleton(dwUser), dwUser.getCreationTime())
                        : null;
        DatawaveUserDetails jwtUser = authType.equals(AUTH_TYPE.JWT) ? new DatawaveUserDetails(Collections.singleton(dwUser), dwUser.getCreationTime()) : null;
        
        MultiValueMap<String,String> queryParams = new LinkedMultiValueMap<>();
        if (response_type != null) {
            queryParams.put("response_type", Collections.singletonList(response_type));
        }
        if (client_id != null) {
            queryParams.put("client_id", Collections.singletonList(client_id));
        }
        if (redirect_uri != null) {
            queryParams.put("redirect_uri", Collections.singletonList(redirect_uri));
        }
        if (state != null) {
            queryParams.put("state", Collections.singletonList(state));
        }
        UriComponents authorizeUri = UriComponentsBuilder.newInstance().scheme(testUtils.getScheme()).host("localhost").port(webServicePort)
                        .path("/authorization/v2/oauth/authorize").queryParams(queryParams).build();
        RequestEntity requestEntity = testUtils.createRequestEntity(trustedHeaderUser, jwtUser, HttpMethod.GET, authorizeUri);
        return restTemplate.exchange(requestEntity, String.class);
    }
    
    private ResponseEntity<OAuthTokenResponse> token(DatawaveUser dwServer, String grant_type, String client_id, String client_secret, String code,
                    String redirect_uri, String refresh_token) {
        DatawaveUserDetails authServer = new DatawaveUserDetails(Collections.singleton(dwServer), dwServer.getCreationTime());
        MultiValueMap<String,String> queryParams = new LinkedMultiValueMap<>();
        if (grant_type != null) {
            queryParams.put("grant_type", Collections.singletonList(grant_type));
        }
        if (client_id != null) {
            queryParams.put("client_id", Collections.singletonList(client_id));
        }
        if (client_secret != null) {
            queryParams.put("client_secret", Collections.singletonList(client_secret));
        }
        if (redirect_uri != null) {
            queryParams.put("redirect_uri", Collections.singletonList(redirect_uri));
        }
        if (code != null) {
            queryParams.put("code", Collections.singletonList(code));
        }
        if (refresh_token != null) {
            queryParams.put("refresh_token", Collections.singletonList(refresh_token));
        }
        UriComponents tokenUri = UriComponentsBuilder.newInstance().scheme(testUtils.getScheme()).host("localhost").port(webServicePort)
                        .path("/authorization/v2/oauth/token").queryParams(queryParams).build();
        
        // both http and https requests should find the JWT first even if calling with a client certificate
        RequestEntity requestEntity = testUtils.createRequestEntity(null, authServer, HttpMethod.POST, tokenUri);
        return restTemplate.exchange(requestEntity, OAuthTokenResponse.class);
    }
    
    private ResponseEntity<OAuthUserInfo> user(String token, String claim) {
        Collection<DatawaveUser> dwUsers = jwtTokenHandler.createUsersFromToken(token, claim);
        DatawaveUserDetails authUser = new DatawaveUserDetails(dwUsers, dwUsers.stream().findFirst().get().getCreationTime());
        UriComponents userUri = UriComponentsBuilder.newInstance().scheme(testUtils.getScheme()).host("localhost").port(webServicePort)
                        .path("/authorization/v2/oauth/user").build();
        RequestEntity requestEntity = testUtils.createRequestEntity(null, authUser, HttpMethod.GET, userUri);
        return restTemplate.exchange(requestEntity, OAuthUserInfo.class);
    }
    
    private ResponseEntity<OAuthUserInfo[]> users(String token, String claim) {
        Collection<DatawaveUser> dwUsers = jwtTokenHandler.createUsersFromToken(token, claim);
        DatawaveUserDetails authUser = new DatawaveUserDetails(dwUsers, dwUsers.stream().findFirst().get().getCreationTime());
        UriComponents userUri = UriComponentsBuilder.newInstance().scheme(testUtils.getScheme()).host("localhost").port(webServicePort)
                        .path("/authorization/v2/oauth/users").build();
        RequestEntity requestEntity = testUtils.createRequestEntity(null, authUser, HttpMethod.GET, userUri);
        return restTemplate.exchange(requestEntity, OAuthUserInfo[].class);
    }
    
    public static Map<String,List<String>> splitQuery(URL url) throws UnsupportedEncodingException {
        final Map<String,List<String>> query_pairs = new LinkedHashMap<>();
        final String[] pairs = url.getQuery().split("&");
        for (String pair : pairs) {
            final int idx = pair.indexOf("=");
            final String key = idx > 0 ? URLDecoder.decode(pair.substring(0, idx), "UTF-8") : pair;
            if (!query_pairs.containsKey(key)) {
                query_pairs.put(key, new LinkedList<>());
            }
            final String value = idx > 0 && pair.length() > idx + 1 ? URLDecoder.decode(pair.substring(idx + 1), "UTF-8") : null;
            query_pairs.get(key).add(value);
        }
        return query_pairs;
    }
    
    @ImportAutoConfiguration({RefreshAutoConfiguration.class})
    @AutoConfigureCache(cacheProvider = CacheType.HAZELCAST)
    @ComponentScan(basePackages = "datawave.microservice")
    @Profile("OAuthServiceTest")
    @Configuration
    public static class AuthorizationServiceTestConfiguration {
        @Bean
        public CachedDatawaveUserService oauthCachedDatawaveUserService() {
            return new AuthorizationTestUserService(OAuthOperationsV2TestCommon.userMap, false);
        }
        
        @Bean
        public HazelcastInstance testHazelcastInstance() {
            Config config = new Config();
            config.setClusterName(UUID.randomUUID().toString());
            config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
            return Hazelcast.newHazelcastInstance(config);
        }
        
        @Bean
        public BusProperties busProperties() {
            return new BusProperties();
        }
    }
    
    /*
     * Only used for the OAuth tests so that we can ignore redirect responses and check the response values at each step of the OAuth process
     */
    // public class OAuthRestTemplateCustomizer implements RestTemplateCustomizer {
    //
    // private final SSLContext sslContext;
    // private final int maxConnectionsTotal;
    // private final int maxConnectionsPerRoute;
    //
    // public OAuthRestTemplateCustomizer(SSLContext sslContext, RestClientProperties restClientProperties) {
    // this.sslContext = sslContext;
    // this.maxConnectionsTotal = restClientProperties.getMaxConnectionsTotal();
    // this.maxConnectionsPerRoute = restClientProperties.getMaxConnectionsPerRoute();
    // }
    //
    // @Override
    // public void customize(RestTemplate restTemplate) {
    // restTemplate.setRequestFactory(clientHttpRequestFactory());
    // }
    //
    // protected ClientHttpRequestFactory clientHttpRequestFactory() {
    // HttpClient httpClient = customizeHttpClient(HttpClients.custom(), sslContext).build();
    // return new HttpComponentsClientHttpRequestFactory(httpClient);
    // }
    //
    // protected HttpClientBuilder customizeHttpClient(HttpClientBuilder httpClientBuilder, SSLContext sslContext) {
    // if (sslContext != null) {
    // httpClientBuilder.setSSLContext(sslContext);
    // }
    // httpClientBuilder.setMaxConnTotal(maxConnectionsTotal);
    // httpClientBuilder.setMaxConnPerRoute(maxConnectionsPerRoute);
    // httpClientBuilder.disableRedirectHandling();
    // // TODO: We're allowing all hosts, since the cert presented by the service we're calling likely won't match its hostname (e.g., a docker host name)
    // // Instead, we could list the expected cert as a property (or use our server cert), and verify that the presented name matches.
    // return httpClientBuilder.setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE);
    // }
    // }
}
