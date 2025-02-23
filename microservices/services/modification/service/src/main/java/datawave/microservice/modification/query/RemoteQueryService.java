package datawave.microservice.modification.query;

import java.text.ParseException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClient;

import datawave.microservice.modification.config.ModificationQueryProperties;
import datawave.microservice.query.DefaultQueryParameters;
import datawave.microservice.query.QueryParameters;
import datawave.modification.query.ModificationQueryService;
import datawave.query.exceptions.DatawaveQueryException;
import datawave.security.authorization.JWTTokenHandler;
import datawave.security.authorization.ProxiedUserDetails;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.GenericResponse;
import datawave.webservice.result.VoidResponse;

public class RemoteQueryService implements ModificationQueryService {
    private static final Logger log = LoggerFactory.getLogger(RemoteQueryService.class);
    
    private final ModificationQueryProperties modificationProperties;
    private final WebClient webClient;
    private final JWTTokenHandler jwtTokenHandler;
    
    private final ProxiedUserDetails userDetails;
    
    public static class RemoteQueryServiceFactory implements ModificationQueryServiceFactory {
        private final ModificationQueryProperties modificationProperties;
        private final WebClient.Builder webClientBuilder;
        private final JWTTokenHandler jwtTokenHandler;
        
        public RemoteQueryServiceFactory(ModificationQueryProperties modificationProperties, WebClient.Builder webClientBuilder,
                        JWTTokenHandler jwtTokenHandler) {
            this.modificationProperties = modificationProperties;
            this.webClientBuilder = webClientBuilder;
            this.jwtTokenHandler = jwtTokenHandler;
        }
        
        @Override
        public ModificationQueryService createService(ProxiedUserDetails userDetails) {
            return new RemoteQueryService(modificationProperties, webClientBuilder, jwtTokenHandler, userDetails);
        }
    }
    
    public RemoteQueryService(ModificationQueryProperties modificationProperties, WebClient.Builder webClientBuilder, JWTTokenHandler jwtTokenHandler,
                    ProxiedUserDetails user) {
        this.modificationProperties = modificationProperties;
        String uri = modificationProperties.getQueryURI();
        if (!uri.endsWith("/")) {
            uri = uri + '/';
        }
        this.webClient = webClientBuilder.baseUrl(uri).build();
        this.jwtTokenHandler = jwtTokenHandler;
        this.userDetails = user;
    }
    
    protected String createBearerHeader() {
        return "Bearer " + jwtTokenHandler.createTokenFromUsers(userDetails.getPrimaryUser().getName(), userDetails.getProxiedUsers());
    }
    
    @Override
    public GenericResponse<String> createQuery(String logicName, Map<String,List<String>> paramsToMap) throws DatawaveQueryException {
        String bearerHeader = createBearerHeader();
        
        Collection<String> userAuths = new ArrayList<>(userDetails.getPrimaryUser().getAuths());
        
        try {
            DefaultQueryParameters params = new DefaultQueryParameters();
            params.validate(paramsToMap);
            
            String beginDate = DefaultQueryParameters.formatDate(params.getBeginDate());
            String endDate = DefaultQueryParameters.formatDate(params.getEndDate());
            String expirationDate = DefaultQueryParameters.formatDate(params.getExpirationDate());
            
            return webClient.post().uri(uriBuilder -> uriBuilder.path(logicName + "/create")
                            .queryParam(QueryParameters.QUERY_POOL, modificationProperties.getQueryPool()).queryParam(QueryParameters.QUERY_BEGIN, beginDate)
                            .queryParam(QueryParameters.QUERY_END, endDate).queryParam(QueryParameters.QUERY_LOGIC_NAME, logicName)
                            .queryParam(QueryParameters.QUERY_STRING, params.getQuery()).queryParam(QueryParameters.QUERY_NAME, params.getQueryName())
                            .queryParam(QueryParameters.QUERY_VISIBILITY, params.getVisibility())
                            .queryParam(QueryParameters.QUERY_AUTHORIZATIONS, String.join(",", userAuths))
                            .queryParam(QueryParameters.QUERY_EXPIRATION, expirationDate).queryParam(QueryParameters.QUERY_PAGESIZE, params.getPagesize())
                            .queryParam(QueryParameters.QUERY_PERSISTENCE, params.getPersistenceMode())
                            .queryParam(QueryParameters.QUERY_TRACE, params.isTrace())
                            .queryParam(QueryParameters.QUERY_PARAMS, paramsToMap.get(QueryParameters.QUERY_PARAMS)).build())
                            .header("Authorization", bearerHeader).header(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE).retrieve()
                            .bodyToMono(GenericResponse.class).block(Duration.ofMillis(modificationProperties.getRemoteQueryTimeoutMillis()));
            // @formatter:on
        } catch (ParseException pe) {
            log.error("Date parse exception for remote query create", pe);
            throw new IllegalStateException("Date parse exception for remote query create", pe);
        } catch (IllegalStateException e) {
            log.error("Timed out waiting for remote query createAndNext response");
            throw new IllegalStateException("Timed out waiting for remote query createAndNext response", e);
        }
    }
    
    public BaseQueryResponse next(String queryId) {
        String bearerHeader = createBearerHeader();
        try {
            // @formatter:off
            return webClient.get()
                    .uri(uriBuilder -> uriBuilder
                            .path(queryId + "/next")
                            .build())
                    .header("Authorization", bearerHeader)
                    .header(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE)
                    .retrieve()
                    .bodyToMono(BaseQueryResponse.class)
                    .block(Duration.ofMillis(modificationProperties.getRemoteQueryTimeoutMillis()));
            // @formatter:on
        } catch (IllegalStateException e) {
            log.error("Timed out waiting for remote query next response");
            throw new IllegalStateException("Timed out waiting for remote query next response", e);
        }
    }
    
    @Override
    public void close(String queryId) {
        String bearerHeader = createBearerHeader();
        try {
            // @formatter:off
            webClient.put()
                    .uri(uriBuilder -> uriBuilder
                            .path(queryId + "/close")
                            .build())
                    .header("Authorization", bearerHeader)
                    .header(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE)
                    .retrieve()
                    .bodyToMono(VoidResponse.class)
                    .block(Duration.ofMillis(modificationProperties.getRemoteQueryTimeoutMillis()));
            // @formatter:on
        } catch (IllegalStateException e) {
            log.error("Timed out waiting for remote query close response");
            throw new IllegalStateException("Timed out waiting for remote query close response", e);
        }
    }
}
