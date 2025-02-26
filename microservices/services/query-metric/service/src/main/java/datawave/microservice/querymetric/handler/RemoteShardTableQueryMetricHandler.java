package datawave.microservice.querymetric.handler;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClient;

import datawave.core.common.connection.AccumuloClientPool;
import datawave.marking.MarkingFunctions;
import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.microservice.query.DefaultQueryParameters;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryParameters;
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.QueryMetricFactory;
import datawave.microservice.querymetric.config.QueryMetricHandlerProperties;
import datawave.microservice.querymetric.factory.QueryMetricQueryLogicFactory;
import datawave.microservice.security.util.DnUtils;
import datawave.query.language.parser.jexl.LuceneToJexlQueryParser;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.JWTTokenHandler;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.result.BaseQueryResponse;

public class RemoteShardTableQueryMetricHandler<T extends BaseQueryMetric> extends ShardTableQueryMetricHandler<T> {
    private static final Logger log = LoggerFactory.getLogger(RemoteShardTableQueryMetricHandler.class);
    
    private DatawaveUserDetails userDetails;
    private final ResponseObjectFactory responseObjectFactory;
    private final WebClient webClient;
    private final WebClient authWebClient;
    private final JWTTokenHandler jwtTokenHandler;
    
    public RemoteShardTableQueryMetricHandler(QueryMetricHandlerProperties queryMetricHandlerProperties, @Qualifier("warehouse") AccumuloClientPool clientPool,
                    QueryMetricQueryLogicFactory logicFactory, QueryMetricFactory metricFactory, MarkingFunctions markingFunctions,
                    QueryMetricCombiner queryMetricCombiner, LuceneToJexlQueryParser luceneToJexlQueryParser, ResponseObjectFactory responseObjectFactory,
                    WebClient.Builder webClientBuilder, JWTTokenHandler jwtTokenHandler, DnUtils dnUtils) {
        super(queryMetricHandlerProperties, clientPool, logicFactory, metricFactory, markingFunctions, queryMetricCombiner, luceneToJexlQueryParser, dnUtils);
        
        this.responseObjectFactory = responseObjectFactory;
        
        this.webClient = webClientBuilder.baseUrl(queryMetricHandlerProperties.getQueryServiceUri()).build();
        this.jwtTokenHandler = jwtTokenHandler;
        
        this.authWebClient = webClientBuilder.baseUrl(queryMetricHandlerProperties.getAuthServiceUri()).build();
    }
    
    protected String createBearerHeader() {
        if (userDetails == null) {
            final String jwt;
            try {
                // @formatter:off
                jwt = authWebClient.get()
                        .retrieve()
                        .bodyToMono(String.class)
                        .block(Duration.ofMillis(queryMetricHandlerProperties.getRemoteQueryTimeoutMillis()));
                // @formatter:on
            } catch (IllegalStateException e) {
                log.error("Timed out waiting for remote authorization response");
                throw new IllegalStateException("Timed out waiting for remote authorization response", e);
            }
            
            Collection<DatawaveUser> principals = jwtTokenHandler.createUsersFromToken(jwt);
            long createTime = principals.stream().map(DatawaveUser::getCreationTime).min(Long::compareTo).orElse(System.currentTimeMillis());
            userDetails = new DatawaveUserDetails(principals, createTime);
        }
        
        return "Bearer " + jwtTokenHandler.createTokenFromUsers(userDetails.getPrimaryUser().getName(), userDetails.getProxiedUsers());
    }
    
    protected BaseQueryResponse createAndNext(Query query) throws Exception {
        String bearerHeader = createBearerHeader();
        String beginDate = DefaultQueryParameters.formatDate(query.getBeginDate());
        String endDate = DefaultQueryParameters.formatDate(query.getEndDate());
        String expirationDate = DefaultQueryParameters.formatDate(query.getExpirationDate());
        
        Collection<String> userAuths = new ArrayList<>(userDetails.getPrimaryUser().getAuths());
        if (clientAuthorizations != null) {
            Collection<String> connectorAuths = Arrays.asList(StringUtils.split(clientAuthorizations, ','));
            userAuths.retainAll(connectorAuths);
        }
        
        try {
            // @formatter:off
            return webClient.post()
                    .uri(uriBuilder -> uriBuilder
                            .path("/" + queryMetricHandlerProperties.getQueryMetricsLogic() + "/createAndNext")
                            .queryParam(QueryParameters.QUERY_BEGIN, beginDate)
                            .queryParam(QueryParameters.QUERY_END, endDate)
                            .queryParam(QueryParameters.QUERY_LOGIC_NAME, query.getQueryLogicName())
                            .queryParam(QueryParameters.QUERY_STRING, query.getQuery())
                            .queryParam(QueryParameters.QUERY_NAME, queryMetricHandlerProperties.getQueryMetricsLogic())
                            .queryParam(QueryParameters.QUERY_VISIBILITY, query.getColumnVisibility())
                            .queryParam(QueryParameters.QUERY_AUTHORIZATIONS, String.join(",", userAuths))
                            .queryParam(QueryParameters.QUERY_EXPIRATION, expirationDate)
                            .queryParam(QueryParameters.QUERY_PAGESIZE, query.getPagesize())
                            .queryParam(QueryParameters.QUERY_PARAMS, query.getParameters().stream().map(p -> String.join(":", p.getParameterName(), p.getParameterValue())).collect(Collectors.joining(";")))
                            .build())
                    .header("Authorization", bearerHeader)
                    .header("Pool", queryMetricHandlerProperties.getQueryPool())
                    .header(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE)
                    .retrieve()
                    .bodyToMono(responseObjectFactory.getEventQueryResponse().getClass())
                    .block(Duration.ofMillis(queryMetricHandlerProperties.getRemoteQueryTimeoutMillis()));
            // @formatter:on
        } catch (IllegalStateException e) {
            log.error("Timed out waiting for remote query createAndNext response");
            throw new IllegalStateException("Timed out waiting for remote query createAndNext response", e);
        }
    }
    
    protected BaseQueryResponse next(String queryId) throws Exception {
        String bearerHeader = createBearerHeader();
        try {
            // @formatter:off
            return webClient.get()
                    .uri(uriBuilder -> uriBuilder
                            .path("/" + queryId + "/next")
                            .build())
                    .header("Authorization", bearerHeader)
                    .header(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE)
                    .retrieve()
                    .bodyToMono(responseObjectFactory.getEventQueryResponse().getClass())
                    .block(Duration.ofMillis(queryMetricHandlerProperties.getRemoteQueryTimeoutMillis()));
            // @formatter:on
        } catch (IllegalStateException e) {
            log.error("Timed out waiting for remote query next response");
            throw new IllegalStateException("Timed out waiting for remote query next response", e);
        }
    }
    
    @Override
    protected void close(String queryId) {
        // do nothing
    }
}
