package datawave.security.authorization.remote;

import com.fasterxml.jackson.databind.ObjectReader;
import datawave.security.auth.DatawaveAuthenticationMechanism;
import datawave.security.authorization.AuthorizationException;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.UserOperations;
import datawave.user.AuthorizationsListBase;
import datawave.webservice.common.remote.RemoteHttpService;
import datawave.webservice.result.GenericResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.annotation.EnableCaching;

import javax.annotation.PostConstruct;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;

@EnableCaching
public class RemoteUserOperationsImpl extends RemoteHttpService implements UserOperations {
    private static final Logger log = LoggerFactory.getLogger(RemoteUserOperationsImpl.class);
    
    public static final String PROXIED_ENTITIES_HEADER = DatawaveAuthenticationMechanism.PROXIED_ENTITIES_HEADER;
    public static final String PROXIED_ISSUERS_HEADER = DatawaveAuthenticationMechanism.PROXIED_ISSUERS_HEADER;
    
    private static final String LIST_EFFECTIVE_AUTHS = "listEffectiveAuthorizations";
    
    private static final String FLUSH_CREDS = "flushCachedCredentials";
    
    private static final String INCLUDE_REMOTE_SERVICES = "includeRemoteServices";
    
    private ObjectReader genericResponseReader;
    
    private ObjectReader authResponseReader;
    
    private boolean initialized = false;
    
    @Override
    @PostConstruct
    public void init() {
        if (!initialized) {
            super.init();
            genericResponseReader = objectMapper.readerFor(GenericResponse.class);
            authResponseReader = objectMapper.readerFor(responseObjectFactory.getAuthorizationsList().getClass());
            initialized = true;
        }
    }
    
    @Override
    public AuthorizationsListBase listEffectiveAuthorizations(Object callerObject) throws AuthorizationException {
        init();
        final DatawavePrincipal principal = getDatawavePrincipal(callerObject);
        final String suffix = LIST_EFFECTIVE_AUTHS;
        // includeRemoteServices=false to avoid any loops
        return executeGetMethodWithRuntimeException(suffix, uriBuilder -> {
            uriBuilder.addParameter(INCLUDE_REMOTE_SERVICES, "false");
        }, httpGet -> {
            httpGet.setHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON);
            httpGet.setHeader(PROXIED_ENTITIES_HEADER, getProxiedEntities(principal));
            httpGet.setHeader(PROXIED_ISSUERS_HEADER, getProxiedIssuers(principal));
        }, entity -> {
            return readResponse(entity, authResponseReader);
        }, () -> suffix);
    }
    
    @Override
    public GenericResponse<String> flushCachedCredentials(Object callerObject) throws AuthorizationException {
        init();
        final DatawavePrincipal principal = getDatawavePrincipal(callerObject);
        final String suffix = FLUSH_CREDS;
        // includeRemoteServices=false to avoid any loops
        return executeGetMethodWithRuntimeException(suffix, uriBuilder -> {
            uriBuilder.addParameter(INCLUDE_REMOTE_SERVICES, "false");
        }, httpGet -> {
            httpGet.setHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON);
            httpGet.setHeader(PROXIED_ENTITIES_HEADER, getProxiedEntities(principal));
            httpGet.setHeader(PROXIED_ISSUERS_HEADER, getProxiedIssuers(principal));
        }, entity -> {
            return readResponse(entity, genericResponseReader);
        }, () -> suffix);
    }
    
    @Override
    @Cacheable(value = "remoteUser", key = "{#principal}", cacheManager = "remoteUserOperationsCacheManager")
    public DatawavePrincipal getRemoteUser(DatawavePrincipal principal) throws AuthorizationException {
        log.info("Cache fault: Retrieving user for " + principal.getPrimaryUser().getDn());
        return UserOperations.super.getRemoteUser(principal);
    }
    
    private DatawavePrincipal getDatawavePrincipal(Object callerObject) {
        if (callerObject instanceof DatawavePrincipal) {
            return (DatawavePrincipal) callerObject;
        }
        throw new RuntimeException("Cannot handle a " + callerObject.getClass() + ". Only DatawavePrincipal is accepted");
    }
    
}
