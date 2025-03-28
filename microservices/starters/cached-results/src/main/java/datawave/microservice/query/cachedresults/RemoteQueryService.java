package datawave.microservice.query.cachedresults;

import java.text.MessageFormat;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.ExchangeStrategies;
import org.springframework.web.reactive.function.client.WebClient;

import datawave.microservice.query.cachedresults.config.CachedResultsQueryProperties;
import datawave.security.authorization.JWTTokenHandler;
import datawave.security.authorization.ProxiedUserDetails;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.NoResultsQueryException;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.exception.QueryExceptionType;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.BaseResponse;
import datawave.webservice.result.GenericResponse;
import datawave.webservice.result.VoidResponse;

@Service
@ConditionalOnProperty(name = "datawave.query.cached-results.enabled", havingValue = "true", matchIfMissing = true)
public class RemoteQueryService implements QueryService {
    
    private static final Logger log = LoggerFactory.getLogger(RemoteQueryService.class);
    
    private final WebClient webClient;
    private final JWTTokenHandler jwtTokenHandler;
    private final CachedResultsQueryProperties.RemoteQuery remoteQueryProperties;
    
    public RemoteQueryService(CachedResultsQueryProperties cachedResultsQueryProperties, WebClient.Builder webClientBuilder, JWTTokenHandler jwtTokenHandler) {
        remoteQueryProperties = cachedResultsQueryProperties.getRemoteQuery();
        
        // @formatter:off
        this.webClient = webClientBuilder
                .baseUrl(cachedResultsQueryProperties.getRemoteQuery().getQueryServiceUri())
                .exchangeStrategies(ExchangeStrategies.builder()
                        .codecs(clientCodecConfigurer -> clientCodecConfigurer
                                .defaultCodecs()
                                .maxInMemorySize(cachedResultsQueryProperties.getRemoteQuery().getMaxBytesToBuffer()))
                        .build())
                .build();
        // @formatter:on
        this.jwtTokenHandler = jwtTokenHandler;
    }
    
    private String createBearerHeader(ProxiedUserDetails currentUser) {
        return "Bearer " + jwtTokenHandler.createTokenFromUsers(currentUser.getPrimaryUser().getName(), currentUser.getProxiedUsers());
    }
    
    @Override
    public GenericResponse<String> duplicate(String queryId, ProxiedUserDetails currentUser) throws QueryException {
        log.info("RemoteQueryService duplicate {} for {}", queryId, currentUser.getPrimaryUser());
        
        try {
            // @formatter:off
            ResponseEntity<BaseResponse> baseResponseEntity = webClient.post()
                    .uri(uriBuilder -> uriBuilder
                            .path("/" + queryId + "/duplicate")
                            .build())
                    .header("Authorization", createBearerHeader(currentUser))
                    .header(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE)
                    .retrieve()
                    .toEntity(BaseResponse.class)
                    .block(Duration.ofMillis(remoteQueryProperties.getDuplicateTimeoutMillis()));
            // @formatter:on
            
            QueryException queryException;
            if (baseResponseEntity != null) {
                BaseResponse baseResponse = baseResponseEntity.getBody();
                
                if (baseResponse instanceof GenericResponse && baseResponseEntity.getStatusCode() == HttpStatus.OK) {
                    return (GenericResponse<String>) baseResponse;
                } else {
                    if (baseResponse != null && baseResponse.getExceptions().size() > 0) {
                        QueryExceptionType exceptionType = baseResponse.getExceptions().get(0);
                        queryException = new QueryException(exceptionType.getCode(), exceptionType.getCause(), exceptionType.getMessage());
                    } else {
                        queryException = new QueryException("Unknown error occurred while calling duplicate for " + queryId,
                                        baseResponseEntity.getStatusCodeValue());
                    }
                }
            } else {
                queryException = new QueryException("Unknown error occurred while calling duplicate for " + queryId);
            }
            throw queryException;
        } catch (RuntimeException e) {
            log.error("Timed out waiting for remote query duplicate response");
            throw new QueryException("Timed out waiting for remote query duplicate response", e);
        }
    }
    
    @Override
    public BaseQueryResponse next(String queryId, ProxiedUserDetails currentUser) throws QueryException {
        log.info("RemoteQueryService next {} for {}", queryId, currentUser.getPrimaryUser());
        
        try {
            // @formatter:off
            ResponseEntity<BaseResponse> baseResponseEntity = webClient.get()
                    .uri(uriBuilder -> uriBuilder
                            .path("/" + queryId + "/next")
                            .build())
                    .header("Authorization", createBearerHeader(currentUser))
                    .header(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE)
                    .retrieve()
                    .toEntity(BaseResponse.class)
                    .block(Duration.ofMillis(remoteQueryProperties.getNextTimeoutMillis()));
            // @formatter:on
            
            QueryException queryException;
            if (baseResponseEntity != null) {
                BaseResponse baseResponse = baseResponseEntity.getBody();
                
                // if we got what we were looking for, return it
                if (baseResponse instanceof BaseQueryResponse) {
                    return (BaseQueryResponse) baseResponse;
                } else {
                    if (baseResponseEntity.getStatusCode() == HttpStatus.NO_CONTENT) {
                        queryException = new NoResultsQueryException(DatawaveErrorCode.NO_QUERY_RESULTS_FOUND, MessageFormat.format("{0}", queryId));
                    } else if (baseResponse != null && baseResponse.getExceptions().size() > 0) {
                        QueryExceptionType exceptionType = baseResponse.getExceptions().get(0);
                        queryException = new QueryException(exceptionType.getCode(), exceptionType.getCause(), exceptionType.getMessage());
                    } else {
                        queryException = new QueryException("Unknown error occurred while calling next for " + queryId,
                                        baseResponseEntity.getStatusCodeValue());
                    }
                }
            } else {
                queryException = new QueryException("Unknown error occurred while calling next for " + queryId);
            }
            throw queryException;
        } catch (RuntimeException e) {
            log.error("Timed out waiting for remote query next response");
            throw new QueryException("Timed out waiting for remote query next response", e);
        }
    }
    
    @Override
    public VoidResponse close(String queryId, ProxiedUserDetails currentUser) throws QueryException {
        log.info("RemoteQueryService close {} for {}", queryId, currentUser.getPrimaryUser());
        
        try {
            // @formatter:off
            ResponseEntity<VoidResponse> voidResponseEntity = webClient.put()
                    .uri(uriBuilder -> uriBuilder
                            .path("/" + queryId + "/close")
                            .build())
                    .header("Authorization", createBearerHeader(currentUser))
                    .header(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE)
                    .retrieve()
                    .toEntity(VoidResponse.class)
                    .block(Duration.ofMillis(remoteQueryProperties.getCloseTimeoutMillis()));
            // @formatter:on
            
            QueryException queryException;
            if (voidResponseEntity != null) {
                VoidResponse voidResponse = voidResponseEntity.getBody();
                
                if (voidResponseEntity.getStatusCode() == HttpStatus.OK) {
                    return voidResponse;
                } else {
                    if (voidResponse != null && voidResponse.getExceptions().size() > 0) {
                        QueryExceptionType exceptionType = voidResponse.getExceptions().get(0);
                        queryException = new QueryException(exceptionType.getCode(), exceptionType.getCause(), exceptionType.getMessage());
                    } else {
                        queryException = new QueryException("Unknown error occurred while calling close for " + queryId,
                                        voidResponseEntity.getStatusCodeValue());
                    }
                }
            } else {
                queryException = new QueryException("Unknown error occurred while calling close for " + queryId);
            }
            throw queryException;
        } catch (RuntimeException e) {
            log.error("Timed out waiting for remote query close response");
            throw new QueryException("Timed out waiting for remote query close response", e);
        }
    }
    
    @Override
    public VoidResponse cancel(String queryId, ProxiedUserDetails currentUser) throws QueryException {
        log.info("RemoteQueryService cancel {} for {}", queryId, currentUser.getPrimaryUser());
        
        try {
            // @formatter:off
            ResponseEntity<VoidResponse> voidResponseEntity = webClient.put()
                    .uri(uriBuilder -> uriBuilder
                            .path("/" + queryId + "/cancel")
                            .build())
                    .header("Authorization", createBearerHeader(currentUser))
                    .header(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE)
                    .retrieve()
                    .toEntity(VoidResponse.class)
                    .block(Duration.ofMillis(remoteQueryProperties.getCancelTimeoutMillis()));
            // @formatter:on
            
            QueryException queryException;
            if (voidResponseEntity != null) {
                VoidResponse voidResponse = voidResponseEntity.getBody();
                
                if (voidResponseEntity.getStatusCode() == HttpStatus.OK) {
                    return voidResponse;
                } else {
                    if (voidResponse != null && voidResponse.getExceptions().size() > 0) {
                        QueryExceptionType exceptionType = voidResponse.getExceptions().get(0);
                        queryException = new QueryException(exceptionType.getCode(), exceptionType.getCause(), exceptionType.getMessage());
                    } else {
                        queryException = new QueryException("Unknown error occurred while calling cancel for " + queryId,
                                        voidResponseEntity.getStatusCodeValue());
                    }
                }
            } else {
                queryException = new QueryException("Unknown error occurred while calling cancel for " + queryId);
            }
            throw queryException;
        } catch (RuntimeException e) {
            log.error("Timed out waiting for remote query cancel response");
            throw new QueryException("Timed out waiting for remote query cancel response", e);
        }
    }
    
    @Override
    public VoidResponse remove(String queryId, ProxiedUserDetails currentUser) throws QueryException {
        log.info("RemoteQueryService remove {} for {}", queryId, currentUser.getPrimaryUser());
        
        try {
            // @formatter:off
            ResponseEntity<VoidResponse> voidResponseEntity = webClient.put()
                    .uri(uriBuilder -> uriBuilder
                            .path("/" + queryId + "/remove")
                            .build())
                    .header("Authorization", createBearerHeader(currentUser))
                    .header(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE)
                    .retrieve()
                    .toEntity(VoidResponse.class)
                    .block(Duration.ofMillis(remoteQueryProperties.getRemoveTimeoutMillis()));
            // @formatter:on
            
            QueryException queryException;
            if (voidResponseEntity != null) {
                VoidResponse voidResponse = voidResponseEntity.getBody();
                
                if (voidResponseEntity.getStatusCode() == HttpStatus.OK) {
                    return voidResponse;
                } else {
                    if (voidResponse != null && voidResponse.getExceptions().size() > 0) {
                        QueryExceptionType exceptionType = voidResponse.getExceptions().get(0);
                        queryException = new QueryException(exceptionType.getCode(), exceptionType.getCause(), exceptionType.getMessage());
                    } else {
                        queryException = new QueryException("Unknown error occurred while calling remove for " + queryId,
                                        voidResponseEntity.getStatusCodeValue());
                    }
                }
            } else {
                queryException = new QueryException("Unknown error occurred while calling remove for " + queryId);
            }
            throw queryException;
        } catch (RuntimeException e) {
            log.error("Timed out waiting for remote query remove response");
            throw new QueryException("Timed out waiting for remote query remove response", e);
        }
    }
}
