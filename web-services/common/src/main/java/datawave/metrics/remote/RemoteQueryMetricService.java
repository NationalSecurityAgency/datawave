package datawave.metrics.remote;

import com.codahale.metrics.Counter;
import com.codahale.metrics.annotation.Metric;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectReader;
import datawave.configuration.RefreshableScope;
import datawave.microservice.querymetric.QueryMetricsSummaryResponse;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.system.CallerPrincipal;
import datawave.webservice.common.remote.RemoteHttpService;
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.BaseQueryMetricListResponse;
import datawave.webservice.query.map.QueryGeometryResponse;
import datawave.webservice.result.VoidResponse;
import org.apache.deltaspike.core.api.config.ConfigProperty;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;

import javax.annotation.PostConstruct;
import javax.annotation.Priority;
import javax.enterprise.inject.Alternative;
import javax.inject.Inject;
import javax.interceptor.Interceptor;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Sends query metric updates to a remote microservice.
 */
@RefreshableScope
@Alternative
// Make this alternative active for the entire application per the CDI 1.2 specification
@Priority(Interceptor.Priority.APPLICATION)
public class RemoteQueryMetricService extends RemoteHttpService {
    
    private static final String UPDATE_METRIC_SUFFIX = "updateMetric";
    private static final String UPDATE_METRICS_SUFFIX = "updateMetrics";
    private static final String ID_METRIC_SUFFIX = "id/%s";
    private static final String MAP_METRIC_SUFFIX = "id/map/%s";
    private static final String SUMMARY_ALL_SUFFIX = "summary/all";
    private static final String SUMMARY_USER_SUFFIX = "summary/user";
    private static final String AUTH_HEADER_NAME = "Authorization";
    private ObjectReader voidResponseReader;
    private ObjectReader baseQueryMetricListResponseReader;
    private ObjectReader queryGeometryResponseReader;
    private ObjectReader queryMetricsSummaryResponseReader;
    
    @Inject
    @ConfigProperty(name = "dw.remoteQueryMetricService.useSrvDnsLookup", defaultValue = "false")
    private boolean useSrvDNS;
    
    @Inject
    @ConfigProperty(name = "dw.remoteQueryMetricService.srvDnsServers", defaultValue = "127.0.0.1")
    private List<String> srvDnsServers;
    
    @Inject
    @ConfigProperty(name = "dw.remoteQueryMetricService.srvDnsPort", defaultValue = "8600")
    private int srvDnsPort;
    
    @Inject
    @ConfigProperty(name = "dw.remoteQueryMetricService.scheme", defaultValue = "https")
    private String serviceScheme;
    
    @Inject
    @ConfigProperty(name = "dw.remoteQueryMetricService.host", defaultValue = "localhost")
    private String serviceHost;
    
    @Inject
    @ConfigProperty(name = "dw.remoteQueryMetricService.port", defaultValue = "8443")
    private int servicePort;
    
    @Inject
    @ConfigProperty(name = "dw.remoteQueryMetricService.uri", defaultValue = "/querymetric/v1/")
    private String serviceURI;
    
    @Inject
    @ConfigProperty(name = "dw.remoteQueryMetricService.maxConnections", defaultValue = "100")
    private int maxConnections;
    
    @Inject
    @ConfigProperty(name = "dw.remoteQueryMetricService.retryCount", defaultValue = "5")
    private int retryCount;
    
    @Inject
    @ConfigProperty(name = "dw.remoteQueryMetricService.unavailableRetryCount", defaultValue = "15")
    private int unavailableRetryCount;
    
    @Inject
    @ConfigProperty(name = "dw.remoteQueryMetricService.unavailableRetryDelayMS", defaultValue = "2000")
    private int unavailableRetryDelay;
    
    @Inject
    @Metric(name = "dw.remoteQueryMetricService.retries", absolute = true)
    private Counter retryCounter;
    
    @Inject
    @Metric(name = "dw.remoteQueryMetricService.failures", absolute = true)
    private Counter failureCounter;
    
    @Inject
    @CallerPrincipal
    protected DatawavePrincipal callerPrincipal;
    
    @Override
    @PostConstruct
    public void init() {
        super.init();
        voidResponseReader = objectMapper.readerFor(VoidResponse.class);
        baseQueryMetricListResponseReader = objectMapper.readerFor(BaseQueryMetricListResponse.class);
        queryGeometryResponseReader = objectMapper.readerFor(QueryGeometryResponse.class);
        queryMetricsSummaryResponseReader = objectMapper.readerFor(QueryMetricsSummaryResponse.class);
    }
    
    @Timed(name = "dw.remoteQueryMetricService.updateMetric", absolute = true)
    public VoidResponse updateMetric(BaseQueryMetric metric) throws JsonProcessingException {
        return update(UPDATE_METRIC_SUFFIX, metric);
    }
    
    @Timed(name = "dw.remoteQueryMetricService.updateMetrics", absolute = true)
    public VoidResponse updateMetrics(Collection<BaseQueryMetric> metrics) throws JsonProcessingException {
        return update(UPDATE_METRICS_SUFFIX, metrics);
    }
    
    private VoidResponse update(String suffix, Object body) throws JsonProcessingException {
        HttpEntity postBody = new StringEntity(objectMapper.writeValueAsString(body), "UTF-8");
        // @formatter:off
        return executePostMethodWithRuntimeException(
                        suffix,
                        uriBuilder -> {
			    uriBuilder.addParameter("metricType", "COMPLETE");
			},
                        httpPost -> {
                            httpPost.setEntity(postBody);
                            httpPost.setHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON);
                            httpPost.setHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON);
                        },
                        entity -> voidResponseReader.readValue(entity.getContent()),
                        () -> suffix);
        // @formatter:on
    }
    
    public BaseQueryMetricListResponse id(String queryId) {
        String suffix = String.format(ID_METRIC_SUFFIX, queryId);
        // @formatter:off
        return executeGetMethodWithRuntimeException(
                        suffix,
                        uriBuilder -> {},
                        httpGet -> {
                            httpGet.setHeader(AUTH_HEADER_NAME, getBearer());
                            httpGet.setHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON);
                        },
                        entity -> baseQueryMetricListResponseReader.readValue(entity.getContent()),
                        () -> suffix);
        // @formatter:on
    }
    
    public QueryGeometryResponse map(String queryId) {
        String suffix = String.format(MAP_METRIC_SUFFIX, queryId);
        // @formatter:off
        return executeGetMethodWithRuntimeException(
                suffix,
                uriBuilder -> {},
                httpGet -> {
                    httpGet.setHeader(AUTH_HEADER_NAME, getBearer());
                    httpGet.setHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON);
                },
                entity -> queryGeometryResponseReader.readValue(entity.getContent()),
                () -> suffix);
        // @formatter:on
    }
    
    public QueryMetricsSummaryResponse summaryAll(Date begin, Date end) {
        String suffix = SUMMARY_ALL_SUFFIX;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HHmmss.SSS");
        // @formatter:off
        return executeGetMethodWithRuntimeException(
                suffix,
                uriBuilder -> {
                    if (begin != null) {
                        uriBuilder.addParameter("begin", sdf.format(begin));
                    }
                    if (end != null) {
                        uriBuilder.addParameter("end", sdf.format(end));
                    }
                },
                httpGet -> {
                    httpGet.setHeader(AUTH_HEADER_NAME, getBearer());
                    httpGet.setHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON);
                },
                entity -> queryMetricsSummaryResponseReader.readValue(entity.getContent()),
                () -> suffix);
        // @formatter:on
    }
    
    public QueryMetricsSummaryResponse summaryUser(Date begin, Date end) {
        String suffix = SUMMARY_USER_SUFFIX;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HHmmss.SSS");
        // @formatter:off
        return executeGetMethodWithRuntimeException(
                suffix,
                uriBuilder -> {
                    if (begin != null) {
                        uriBuilder.addParameter("begin", sdf.format(begin));
                    }
                    if (end != null) {
                        uriBuilder.addParameter("end", sdf.format(end));
                    }
                },
                httpGet -> {
                    httpGet.setHeader(AUTH_HEADER_NAME, getBearer());
                    httpGet.setHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON);
                },
                entity -> queryMetricsSummaryResponseReader.readValue(entity.getContent()),
                () -> suffix);
        // @formatter:on
    }
    
    protected String getBearer() {
        return "Bearer " + jwtTokenHandler.createTokenFromUsers(callerPrincipal.getName(), callerPrincipal.getProxiedUsers());
    }
    
    private <T> T executePostMethodWithRuntimeException(String uriSuffix, Consumer<URIBuilder> uriCustomizer, Consumer<HttpPost> requestCustomizer,
                    IOFunction<T> resultConverter, Supplier<String> errorSupplier) {
        try {
            return executePostMethod(uriSuffix, uriCustomizer, requestCustomizer, resultConverter, errorSupplier);
        } catch (URISyntaxException e) {
            throw new RuntimeException("Invalid URI: " + e.getMessage(), e);
        } catch (IOException e) {
            failureCounter.inc();
            throw new RuntimeException(e.getMessage(), e);
        }
    }
    
    private <T> T executeGetMethodWithRuntimeException(String uriSuffix, Consumer<URIBuilder> uriCustomizer, Consumer<HttpGet> requestCustomizer,
                    IOFunction<T> resultConverter, Supplier<String> errorSupplier) {
        try {
            return executeGetMethod(uriSuffix, uriCustomizer, requestCustomizer, resultConverter, errorSupplier);
        } catch (URISyntaxException e) {
            throw new RuntimeException("Invalid URI: " + e.getMessage(), e);
        } catch (IOException e) {
            failureCounter.inc();
            throw new RuntimeException(e.getMessage(), e);
        }
    }
    
    @Override
    protected String serviceHost() {
        return serviceHost;
    }
    
    @Override
    protected int servicePort() {
        return servicePort;
    }
    
    @Override
    protected String serviceURI() {
        return serviceURI;
    }
    
    @Override
    protected boolean useSrvDns() {
        return useSrvDNS;
    }
    
    @Override
    protected List<String> srvDnsServers() {
        return srvDnsServers;
    }
    
    @Override
    protected int srvDnsPort() {
        return srvDnsPort;
    }
    
    @Override
    protected String serviceScheme() {
        return serviceScheme;
    }
    
    @Override
    protected int maxConnections() {
        return maxConnections;
    }
    
    @Override
    protected int retryCount() {
        return retryCount;
    }
    
    @Override
    protected int unavailableRetryCount() {
        return unavailableRetryCount;
    }
    
    @Override
    protected int unavailableRetryDelay() {
        return unavailableRetryDelay;
    }
    
    @Override
    protected Counter retryCounter() {
        return retryCounter;
    }
}
