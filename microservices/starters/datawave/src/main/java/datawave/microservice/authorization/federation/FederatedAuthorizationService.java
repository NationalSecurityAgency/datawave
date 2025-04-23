package datawave.microservice.authorization.federation;

import static datawave.microservice.authorization.preauth.ProxiedEntityX509Filter.ENTITIES_HEADER;
import static datawave.microservice.authorization.preauth.ProxiedEntityX509Filter.ISSUERS_HEADER;

import java.time.Duration;
import java.util.function.Function;

import org.apache.http.HttpHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.reactive.function.client.ExchangeStrategies;
import org.springframework.web.reactive.function.client.WebClient;

import datawave.microservice.authorization.config.AuthorizationsListSupplier;
import datawave.microservice.authorization.federation.config.FederatedAuthorizationServiceProperties;
import datawave.microservice.authorization.federation.config.FederatedAuthorizationServiceProperties.RetryTimeoutProperties;
import datawave.security.authorization.AuthorizationException;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.ProxiedUserDetails;
import datawave.security.authorization.UserOperations;
import datawave.user.AuthorizationsListBase;
import datawave.webservice.result.GenericResponse;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

public class FederatedAuthorizationService implements UserOperations {
    private static final Logger log = LoggerFactory.getLogger(FederatedAuthorizationService.class);
    
    public static final String INCLUDE_REMOTE_SERVICES = "includeRemoteServices";
    
    private FederatedAuthorizationServiceProperties federatedAuthorizationProperties;
    private final WebClient webClient;
    private AuthorizationsListSupplier authorizationsListSupplier;
    
    public FederatedAuthorizationService(FederatedAuthorizationServiceProperties federatedAuthorizationProperties, WebClient.Builder webClientBuilder,
                    AuthorizationsListSupplier authorizationsListSupplier) {
        this.federatedAuthorizationProperties = federatedAuthorizationProperties;
        // @formatter:off
        this.webClient = webClientBuilder
                .baseUrl(federatedAuthorizationProperties.getFederatedAuthorizationUri())
                .exchangeStrategies(ExchangeStrategies.builder()
                        .codecs(clientCodecConfigurer -> clientCodecConfigurer
                                .defaultCodecs()
                                .maxInMemorySize(federatedAuthorizationProperties.getMaxBytesToBuffer()))
                        .build())
                .build();
        // @formatter:on
        this.authorizationsListSupplier = authorizationsListSupplier;
    }
    
    private String getProxiedEntities(ProxiedUserDetails currentUser) {
        return getProxiedDN(currentUser, (user) -> user.getDn().subjectDN());
    }
    
    private String getProxiedIssuers(ProxiedUserDetails currentUser) {
        return getProxiedDN(currentUser, (user) -> user.getDn().issuerDN());
    }
    
    private String getProxiedDN(ProxiedUserDetails currentUser, Function<DatawaveUser,String> getDN) {
        StringBuilder builder = new StringBuilder();
        for (DatawaveUser user : currentUser.getProxiedUsers()) {
            builder.append('<').append(getDN.apply(user)).append('>');
        }
        return builder.toString();
    }
    
    @Override
    @Cacheable(value = "getRemoteUser", key = "{#currentUser}", cacheManager = "remoteOperationsCacheManager")
    public <T extends ProxiedUserDetails> T getRemoteUser(T currentUser) throws AuthorizationException {
        return UserOperations.super.getRemoteUser(currentUser);
    }
    
    @Override
    @Cacheable(value = "listEffectiveAuthorizations", key = "{#currentUser}", cacheManager = "remoteOperationsCacheManager")
    public AuthorizationsListBase listEffectiveAuthorizations(ProxiedUserDetails currentUser) throws AuthorizationException {
        return listEffectiveAuthorizations(currentUser, true);
    }
    
    public AuthorizationsListBase listEffectiveAuthorizations(ProxiedUserDetails currentUser, boolean federate) throws AuthorizationException {
        log.info("FederatedAuthorizationService listEffectiveAuthorizations (federate: {}) for {}", federate, currentUser.getPrimaryUser());
        
        try {
            // @formatter:off
            //noinspection rawtypes,unchecked
            ResponseEntity<AuthorizationsListBase> authorizationsListBaseResponseEntity = (ResponseEntity<AuthorizationsListBase>) getResponseEntity(
                    currentUser,
                    federate,
                    federatedAuthorizationProperties.getListEffectiveAuthorizationsRetry(),
                    "listEffectiveAuthorizations",
                    authorizationsListSupplier.get().getClass());
            // @formatter:on
            
            AuthorizationException authorizationException;
            if (authorizationsListBaseResponseEntity != null) {
                AuthorizationsListBase authorizationsListBase = authorizationsListBaseResponseEntity.getBody();
                
                if (authorizationsListBaseResponseEntity.getStatusCode() == HttpStatus.OK) {
                    return authorizationsListBase;
                } else {
                    authorizationException = new AuthorizationException("Unknown error occurred while calling listEffectiveAuthorizations for "
                                    + currentUser.getPrimaryUser() + ", Status Code: " + authorizationsListBaseResponseEntity.getStatusCodeValue());
                }
            } else {
                authorizationException = new AuthorizationException(
                                "Unknown error occurred while calling listEffectiveAuthorizations for " + currentUser.getPrimaryUser());
            }
            throw authorizationException;
        } catch (ServiceException e) {
            log.error("Timed out waiting for federated listEffectiveAuthorizations response");
            throw new AuthorizationException("Timed out waiting for federated listEffectiveAuthorizations response", e);
        }
    }
    
    @Override
    public GenericResponse<String> flushCachedCredentials(ProxiedUserDetails currentUser) throws AuthorizationException {
        return flushCachedCredentials(currentUser, true);
    }
    
    public GenericResponse<String> flushCachedCredentials(ProxiedUserDetails currentUser, boolean federate) throws AuthorizationException {
        log.info("FederatedAuthorizationService flushCachedCredentials (federate: {}) for {}", federate, currentUser.getPrimaryUser());
        
        try {
            // @formatter:off
            //noinspection rawtypes,unchecked
            ResponseEntity<GenericResponse> genericResponseEntity = (ResponseEntity<GenericResponse>)getResponseEntity(
                    currentUser,
                    federate,
                    federatedAuthorizationProperties.getFlushCachedCredentialsRetry(),
                    "flushCachedCredentials",
                    GenericResponse.class);
            // @formatter:on
            
            AuthorizationException authorizationException;
            if (genericResponseEntity != null) {
                GenericResponse genericResponse = genericResponseEntity.getBody();
                
                if (genericResponseEntity.getStatusCode() == HttpStatus.OK) {
                    return genericResponse;
                } else {
                    authorizationException = new AuthorizationException("Unknown error occurred while calling flushCachedCredentials for "
                                    + currentUser.getPrimaryUser() + ", Status Code: " + genericResponseEntity.getStatusCodeValue());
                }
            } else {
                authorizationException = new AuthorizationException(
                                "Unknown error occurred while calling flushCachedCredentials for " + currentUser.getPrimaryUser());
            }
            throw authorizationException;
        } catch (ServiceException e) {
            log.error("Timed out waiting for federated flushCachedCredentials response");
            throw new AuthorizationException("Timed out waiting for federated flushCachedCredentials response", e);
        }
    }
    
    protected ResponseEntity<?> getResponseEntity(ProxiedUserDetails currentUser, boolean federate, RetryTimeoutProperties retry, String endpoint,
                    Class entityClass) {
        // @formatter:off
        return (ResponseEntity<?>) webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path(endpoint)
                        .queryParam("includeRemoteServices", federate)
                        .build())
                .header(ENTITIES_HEADER, getProxiedEntities(currentUser))
                .header(ISSUERS_HEADER, getProxiedIssuers(currentUser))
                .header(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE)
                .retrieve()
                // don't retry on 4xx errors
                .onStatus(HttpStatus::is5xxServerError,
                        clientResponse -> Mono.error(new ServiceException("Service Error", clientResponse.rawStatusCode())))
                .toEntity(entityClass)
                .retryWhen(Retry
                        .fixedDelay(retry.getRetries(), Duration.ofMillis(retry.getRetryDelayMillis()))
                        .filter(throwable -> throwable instanceof ServiceException)
                        .onRetryExhaustedThrow(((retryBackoffSpec, retrySignal) -> {
                            throw new ServiceException("External Service failed to process after max retries",
                                    HttpStatus.SERVICE_UNAVAILABLE.value());
                        })))
                .block(Duration.ofMillis(retry.getTimeoutMillis()));
        // @formatter:on
    }
    
    public class ServiceException extends RuntimeException {
        int statusCode;
        
        public ServiceException(String message, int statusCode) {
            super(message);
            this.statusCode = statusCode;
        }
    }
}
