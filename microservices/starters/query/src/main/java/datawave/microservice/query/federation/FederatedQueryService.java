package datawave.microservice.query.federation;

import static datawave.microservice.authorization.preauth.ProxiedEntityX509Filter.ENTITIES_HEADER;
import static datawave.microservice.authorization.preauth.ProxiedEntityX509Filter.ISSUERS_HEADER;

import java.net.URI;
import java.text.MessageFormat;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.ExchangeStrategies;
import org.springframework.web.reactive.function.client.WebClient;

import datawave.core.query.remote.RemoteQueryService;
import datawave.microservice.query.federation.config.FederatedQueryProperties;
import datawave.security.authorization.DatawaveUser;
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

public class FederatedQueryService implements RemoteQueryService {
    
    private static final Logger log = LoggerFactory.getLogger(FederatedQueryService.class);
    
    private FederatedQueryProperties federatedQueryProperties;
    private final WebClient webClient;
    
    public FederatedQueryService(FederatedQueryProperties federatedQueryProperties, WebClient.Builder webClientBuilder) {
        this.federatedQueryProperties = federatedQueryProperties;
        // @formatter:off
        this.webClient = webClientBuilder
                .baseUrl(federatedQueryProperties.getQueryServiceUri())
                .exchangeStrategies(ExchangeStrategies.builder()
                        .codecs(clientCodecConfigurer -> clientCodecConfigurer
                                .defaultCodecs()
                                .maxInMemorySize(federatedQueryProperties.getMaxBytesToBuffer()))
                        .build())
                .build();
        // @formatter:on
    }
    
    private String getProxiedEntities(ProxiedUserDetails currentUser) {
        StringBuilder builder = new StringBuilder();
        for (DatawaveUser user : currentUser.getProxiedUsers()) {
            builder.append('<').append(user.getDn().subjectDN()).append('>');
        }
        return builder.toString();
    }
    
    private String getProxiedIssuers(ProxiedUserDetails currentUser) {
        StringBuilder builder = new StringBuilder();
        for (DatawaveUser user : currentUser.getProxiedUsers()) {
            builder.append('<').append(user.getDn().issuerDN()).append('>');
        }
        return builder.toString();
    }
    
    @Override
    public GenericResponse<String> createQuery(String queryLogicName, Map<String,List<String>> queryParameters, ProxiedUserDetails currentUser)
                    throws QueryException {
        log.info("FederatedQueryService {}/create for {}", queryLogicName, currentUser.getPrimaryUser());
        
        try {
            // @formatter:off
            ResponseEntity<GenericResponse> genericResponseEntity = webClient.post()
                    .uri(uriBuilder -> uriBuilder
                            .path(queryLogicName + "/create")
                            .build())
                    .body(BodyInserters.fromValue(queryParameters))
                    .header(ENTITIES_HEADER, getProxiedEntities(currentUser))
                    .header(ISSUERS_HEADER, getProxiedIssuers(currentUser))
                    .header(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE)
                    .retrieve()
                    .toEntity(GenericResponse.class)
                    .block(Duration.ofMillis(federatedQueryProperties.getCreateTimeoutMillis()));
            // @formatter:on
            
            QueryException queryException;
            if (genericResponseEntity != null) {
                GenericResponse<String> genericResponse = (GenericResponse<String>) genericResponseEntity.getBody();
                
                if (genericResponseEntity.getStatusCode() == HttpStatus.OK) {
                    return genericResponse;
                } else {
                    if (genericResponse != null && !genericResponse.getExceptions().isEmpty()) {
                        QueryExceptionType exceptionType = genericResponse.getExceptions().get(0);
                        queryException = new QueryException(exceptionType.getCode(), exceptionType.getCause(), exceptionType.getMessage());
                    } else {
                        queryException = new QueryException(
                                        "Unknown error occurred while calling " + queryLogicName + "/create for " + currentUser.getPrimaryUser(),
                                        genericResponseEntity.getStatusCodeValue());
                    }
                }
            } else {
                queryException = new QueryException("Unknown error occurred while calling " + queryLogicName + "/create for " + currentUser.getPrimaryUser());
            }
            throw queryException;
        } catch (RuntimeException e) {
            log.error("Timed out waiting for remote query create response");
            throw new QueryException("Timed out waiting for remote query create response", e);
        }
    }
    
    @Override
    public BaseQueryResponse next(String queryId, ProxiedUserDetails currentUser) throws QueryException {
        log.info("FederatedQueryService next {} for {}", queryId, currentUser.getPrimaryUser());
        
        try {
            // @formatter:off
            ResponseEntity<BaseResponse> baseResponseEntity = webClient.get()
                    .uri(uriBuilder -> uriBuilder
                            .path("/" + queryId + "/next")
                            .build())
                    .header(ENTITIES_HEADER, getProxiedEntities(currentUser))
                    .header(ISSUERS_HEADER, getProxiedIssuers(currentUser))
                    .header(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE)
                    .retrieve()
                    .toEntity(BaseResponse.class)
                    .block(Duration.ofMillis(federatedQueryProperties.getNextTimeoutMillis()));
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
                    } else if (baseResponse != null && !baseResponse.getExceptions().isEmpty()) {
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
    
    // @Override
    public void setNextQueryResponseClass(Class<? extends BaseQueryResponse> nextQueryResponseClass) {
        // noop, required by interface.
    }
    
    @Override
    public VoidResponse close(String queryId, ProxiedUserDetails currentUser) throws QueryException {
        log.info("FederatedQueryService close {} for {}", queryId, currentUser.getPrimaryUser());
        
        try {
            // @formatter:off
            ResponseEntity<VoidResponse> voidResponseEntity = webClient.put()
                    .uri(uriBuilder -> uriBuilder
                            .path("/" + queryId + "/close")
                            .build())
                    .header(ENTITIES_HEADER, getProxiedEntities(currentUser))
                    .header(ISSUERS_HEADER, getProxiedIssuers(currentUser))
                    .header(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE)
                    .retrieve()
                    .toEntity(VoidResponse.class)
                    .block(Duration.ofMillis(federatedQueryProperties.getCloseTimeoutMillis()));
            // @formatter:on
            
            QueryException queryException;
            if (voidResponseEntity != null) {
                VoidResponse voidResponse = voidResponseEntity.getBody();
                
                if (voidResponseEntity.getStatusCode() == HttpStatus.OK) {
                    return voidResponse;
                } else {
                    if (voidResponse != null && !voidResponse.getExceptions().isEmpty()) {
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
    public GenericResponse<String> planQuery(String queryLogicName, Map<String,List<String>> queryParameters, ProxiedUserDetails currentUser)
                    throws QueryException {
        log.info("FederatedQueryService {}/plan for {}", queryLogicName, currentUser.getPrimaryUser());
        
        try {
            // @formatter:off
            ResponseEntity<GenericResponse> genericResponseEntity = webClient.post()
                    .uri(uriBuilder -> uriBuilder
                            .path(queryLogicName + "/plan")
                            .build())
                    .body(BodyInserters.fromValue(queryParameters))
                    .header(ENTITIES_HEADER, getProxiedEntities(currentUser))
                    .header(ISSUERS_HEADER, getProxiedIssuers(currentUser))
                    .header(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE)
                    .retrieve()
                    .toEntity(GenericResponse.class)
                    .block(Duration.ofMillis(federatedQueryProperties.getPlanTimeoutMillis()));
            // @formatter:on
            
            QueryException queryException;
            if (genericResponseEntity != null) {
                GenericResponse<String> genericResponse = (GenericResponse<String>) genericResponseEntity.getBody();
                
                if (genericResponseEntity.getStatusCode() == HttpStatus.OK) {
                    return genericResponse;
                } else {
                    if (genericResponse != null && !genericResponse.getExceptions().isEmpty()) {
                        QueryExceptionType exceptionType = genericResponse.getExceptions().get(0);
                        queryException = new QueryException(exceptionType.getCode(), exceptionType.getCause(), exceptionType.getMessage());
                    } else {
                        queryException = new QueryException(
                                        "Unknown error occurred while calling " + queryLogicName + "/plan for " + currentUser.getPrimaryUser(),
                                        genericResponseEntity.getStatusCodeValue());
                    }
                }
            } else {
                queryException = new QueryException("Unknown error occurred while calling " + queryLogicName + "/plan for " + currentUser.getPrimaryUser());
            }
            throw queryException;
        } catch (RuntimeException e) {
            log.error("Timed out waiting for remote query plan response");
            throw new QueryException("Timed out waiting for remote query plan response", e);
        }
    }
    
    @Override
    public GenericResponse<String> planQuery(String queryId, ProxiedUserDetails currentUser) throws QueryException {
        log.info("FederatedQueryService plan {} for {}", queryId, currentUser.getPrimaryUser());
        
        try {
            // @formatter:off
            ResponseEntity<GenericResponse> genericResponseEntity = webClient.get()
                    .uri(uriBuilder -> uriBuilder
                            .path("/" + queryId + "/plan")
                            .build())
                    .header(ENTITIES_HEADER, getProxiedEntities(currentUser))
                    .header(ISSUERS_HEADER, getProxiedIssuers(currentUser))
                    .header(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE)
                    .retrieve()
                    .toEntity(GenericResponse.class)
                    .block(Duration.ofMillis(federatedQueryProperties.getPlanTimeoutMillis()));
            // @formatter:on
            
            QueryException queryException;
            if (genericResponseEntity != null) {
                GenericResponse<String> genericResponse = (GenericResponse<String>) genericResponseEntity.getBody();
                
                if (genericResponseEntity.getStatusCode() == HttpStatus.OK) {
                    return genericResponse;
                } else {
                    if (genericResponse != null && !genericResponse.getExceptions().isEmpty()) {
                        QueryExceptionType exceptionType = genericResponse.getExceptions().get(0);
                        queryException = new QueryException(exceptionType.getCode(), exceptionType.getCause(), exceptionType.getMessage());
                    } else {
                        queryException = new QueryException("Unknown error occurred while calling plan for " + queryId,
                                        genericResponseEntity.getStatusCodeValue());
                    }
                }
            } else {
                queryException = new QueryException("Unknown error occurred while calling plan for " + queryId);
            }
            throw queryException;
        } catch (RuntimeException e) {
            log.error("Timed out waiting for remote query plan response");
            throw new QueryException("Timed out waiting for remote query plan response", e);
        }
    }
    
    @Override
    public URI getQueryMetricsURI(String queryId) {
        return URI.create(String.join("/", federatedQueryProperties.getQueryMetricServiceUri(), "queryId"));
    }
}
