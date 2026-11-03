package datawave.security.authorization.remote;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;

import javax.annotation.PostConstruct;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.ConnectTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.annotation.EnableCaching;

import com.fasterxml.jackson.databind.ObjectReader;

import datawave.authorization.remote.RemoteAuthorizationException;
import datawave.security.authorization.AuthorizationException;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.ProxiedUserDetails;
import datawave.security.authorization.RemoteUserOperationsProperties;
import datawave.security.authorization.UserOperations;
import datawave.security.util.SecurityConstants;
import datawave.user.AuthorizationsListBase;
import datawave.webservice.common.remote.RemoteHttpService;
import datawave.webservice.result.GenericResponse;

@EnableCaching
public class WSRemoteUserOperations extends RemoteHttpService implements UserOperations {
    private static final Logger log = LoggerFactory.getLogger(WSRemoteUserOperations.class);

    public static final String PROXIED_ENTITIES_HEADER = SecurityConstants.PROXIED_ENTITIES_HEADER;
    public static final String PROXIED_ISSUERS_HEADER = SecurityConstants.PROXIED_ISSUERS_HEADER;

    private static final String LIST_EFFECTIVE_AUTHS = "listEffectiveAuthorizations";

    private static final String FLUSH_CREDS = "flushCachedCredentials";

    public static final String INCLUDE_REMOTE_SERVICES = "includeRemoteServices";

    private ObjectReader genericResponseReader;

    private ObjectReader authResponseReader;

    private RemoteUserOperationsProperties remoteUserOperationsProperties;

    private boolean initialized = false;

    @Override
    @PostConstruct
    public void init() {
        if (!initialized) {
            super.init();
            genericResponseReader = objectMapper.readerFor(GenericResponse.class);
            authResponseReader = objectMapper.readerFor(responseObjectFactory.getAuthorizationsList().getClass());
            if (remoteUserOperationsProperties == null) {
                throw new IllegalStateException("RemoteUserOperationsProperties must be set before calling init()");
            }
            URI serviceUri = URI.create(remoteUserOperationsProperties.getAuthorizationServiceUri());
            setQueryServiceScheme(serviceUri.getScheme());
            setQueryServiceHost(serviceUri.getHost());
            setQueryServicePort(serviceUri.getPort());
            setQueryServiceURI(serviceUri.getPath());
            initialized = true;
        }
    }

    @Override
    @Cacheable(value = "getRemoteUser", key = "{#currentUser}", cacheManager = "remoteOperationsCacheManager")
    public ProxiedUserDetails getRemoteUser(ProxiedUserDetails currentUser) throws AuthorizationException {
        log.info("Cache fault: Retrieving user for " + currentUser.getPrimaryUser().getDn());
        return UserOperations.super.getRemoteUser(currentUser);
    }

    @Override
    @Cacheable(value = "listEffectiveAuthorizations", key = "{#callerObject}", cacheManager = "remoteOperationsCacheManager")
    public AuthorizationsListBase listEffectiveAuthorizations(ProxiedUserDetails callerObject) throws AuthorizationException {
        init();
        final DatawavePrincipal principal = getDatawavePrincipal(callerObject);
        log.info("Cache fault: Retrieving effective auths for " + principal.getPrimaryUser().getDn());
        final String suffix = LIST_EFFECTIVE_AUTHS;
        // this sets the values in the retry handler for this call, which is used in the executeGetMethod() call below.
        // This is done here because the HttpClient is only created once and the RetryHandler is set at creation time.
        RemoteUserOperationsProperties.RetryTimeoutProperties retryTimeoutProperties = remoteUserOperationsProperties.getListEffectiveAuthorizationsRetry();
        getRetryHandler().setUnavailableRetryCount(retryTimeoutProperties.getRetries());
        getRetryHandler().setUnavailableRetryDelay(Long.valueOf(retryTimeoutProperties.getRetryDelayMillis()).intValue());
        RequestConfig reqConfig = RequestConfig.custom().setConnectionRequestTimeout(getConfig().getConnectionPoolTimeout())
                        .setConnectTimeout(getConfig().getConnectTimeout()).setSocketTimeout(Long.valueOf(retryTimeoutProperties.getTimeoutMillis()).intValue())
                        .build();
        // includeRemoteServices=false to avoid any loops
        try {
            return executeGetMethod(suffix, uriBuilder -> {
                uriBuilder.addParameter(INCLUDE_REMOTE_SERVICES, "false");
            }, httpGet -> {
                httpGet.setConfig(reqConfig);
                httpGet.setHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON);
                httpGet.setHeader(PROXIED_ENTITIES_HEADER, getProxiedEntities(principal));
                httpGet.setHeader(PROXIED_ISSUERS_HEADER, getProxiedIssuers(principal));
            }, entity -> {
                return readResponse(entity, authResponseReader);
            }, () -> suffix);
        } catch (SocketTimeoutException | ConnectTimeoutException e) {
            // also covers ConnectionPoolTimeoutException as a subclass of ConnectTimeoutException
            throw new RemoteAuthorizationException("RemoteQueryService timed out", e);
        } catch (URISyntaxException | IOException e) {
            // this mimics what happens up inside RemoteQueryService
            throw new RuntimeException(e);
        }
    }

    @Override
    public GenericResponse<String> flushCachedCredentials(ProxiedUserDetails callerObject) throws AuthorizationException {
        init();
        final DatawavePrincipal principal = getDatawavePrincipal(callerObject);
        final String suffix = FLUSH_CREDS;
        // this sets the values in the retry handler for this call, which is used in the executeGetMethod() call below.
        // This is done here because the HttpClient is only created once and the RetryHandler is set at creation time.
        RemoteUserOperationsProperties.RetryTimeoutProperties retryTimeoutProperties = remoteUserOperationsProperties.getFlushCachedCredentialsRetry();
        getRetryHandler().setUnavailableRetryCount(retryTimeoutProperties.getRetries());
        getRetryHandler().setUnavailableRetryDelay(Long.valueOf(retryTimeoutProperties.getRetryDelayMillis()).intValue());
        RequestConfig reqConfig = RequestConfig.custom().setConnectionRequestTimeout(getConfig().getConnectionPoolTimeout())
                        .setConnectTimeout(getConfig().getConnectTimeout()).setSocketTimeout(Long.valueOf(retryTimeoutProperties.getTimeoutMillis()).intValue())
                        .build();
        // includeRemoteServices=false to avoid any loops
        try {
            return executeGetMethod(suffix, uriBuilder -> {
                uriBuilder.addParameter(INCLUDE_REMOTE_SERVICES, "false");
            }, httpGet -> {
                httpGet.setConfig(reqConfig);
                httpGet.setHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON);
                httpGet.setHeader(PROXIED_ENTITIES_HEADER, getProxiedEntities(principal));
                httpGet.setHeader(PROXIED_ISSUERS_HEADER, getProxiedIssuers(principal));
            }, entity -> {
                return readResponse(entity, genericResponseReader);
            }, () -> suffix);
        } catch (SocketTimeoutException | ConnectTimeoutException e) {
            // also covers ConnectionPoolTimeoutException as a subclass of ConnectTimeoutException
            throw new RemoteAuthorizationException("RemoteQueryService timed out", e);
        } catch (URISyntaxException | IOException e) {
            // this mimics what happens up inside RemoteQueryService
            throw new RuntimeException(e);
        }
    }

    private DatawavePrincipal getDatawavePrincipal(Object callerObject) {
        if (callerObject instanceof DatawavePrincipal) {
            return (DatawavePrincipal) callerObject;
        }
        throw new RuntimeException("Cannot handle a " + callerObject.getClass() + ". Only DatawavePrincipal is accepted");
    }

    public void setRemoteUserOperationsProperties(RemoteUserOperationsProperties remoteUserOperationsProperties) {
        this.remoteUserOperationsProperties = remoteUserOperationsProperties;
    }
}
