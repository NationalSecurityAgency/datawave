package datawave.metrics.remote;

import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;

import org.apache.http.HttpEntity;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.xbill.DNS.TextParseException;

import com.codahale.metrics.Counter;
import com.codahale.metrics.annotation.Metric;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectReader;

import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.BaseQueryMetricListResponse;
import datawave.microservice.querymetric.QueryGeometryResponse;
import datawave.microservice.querymetric.QueryMetricsSummaryResponse;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.system.CallerPrincipal;
import datawave.webservice.common.remote.RemoteHttpService;
import datawave.webservice.result.VoidResponse;

/**
 * Sends query metric updates to a remote microservice.
 */
public class RemoteQueryMetricService extends RemoteHttpService {

    public static final String UPDATE_METRIC_SUFFIX = "updateMetric";
    public static final String UPDATE_METRICS_SUFFIX = "updateMetrics";
    public static final String ID_METRIC_SUFFIX = "id/%s";
    public static final String MAP_METRIC_SUFFIX = "id/%s/map";
    public static final String SUMMARY_ALL_SUFFIX = "summary/all";
    public static final String SUMMARY_USER_SUFFIX = "summary/user";
    public static final String AUTH_HEADER_NAME = "Authorization";
    private ObjectReader baseQueryMetricListResponseReader;
    private ObjectReader queryGeometryResponseReader;
    private ObjectReader queryMetricsSummaryResponseReader;

    private boolean useConfiguredURIForRedirect = false;
    private String serviceURI = "/querymetric/v1/";

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
        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
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

    public URIBuilder buildRedirectURI(String suffix, URI baseURI) throws TextParseException {
        return buildRedirectURI(suffix, baseURI, useConfiguredURIForRedirect);
    }

    protected String getBearer() {
        return "Bearer " + jwtTokenHandler.createTokenFromUsers(callerPrincipal.getName(), callerPrincipal.getProxiedUsers());
    }

    @Override
    protected String serviceURI() {
        return serviceURI;
    }

    public void setUseConfiguredURIForRedirect(boolean useConfiguredURIForRedirect) {
        this.useConfiguredURIForRedirect = useConfiguredURIForRedirect;
    }

    @Override
    protected Counter retryCounter() {
        return retryCounter;
    }

    @Override
    protected Counter failureCounter() {
        return failureCounter;
    }
}
