package datawave.webservice.query.remote;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;

import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.message.BasicNameValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectReader;

import datawave.core.query.remote.RemoteQueryService;
import datawave.core.query.remote.RemoteQueryServiceProperties;
import datawave.core.query.remote.RemoteTimeoutQueryException;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.ProxiedUserDetails;
import datawave.security.util.SecurityConstants;
import datawave.webservice.common.remote.RemoteHttpService;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.GenericResponse;
import datawave.webservice.result.VoidResponse;

public class WSRemoteQueryService extends RemoteHttpService implements RemoteQueryService {
    private static final Logger log = LoggerFactory.getLogger(WSRemoteQueryService.class);

    public static final String PROXIED_ENTITIES_HEADER = SecurityConstants.PROXIED_ENTITIES_HEADER;
    public static final String PROXIED_ISSUERS_HEADER = SecurityConstants.PROXIED_ISSUERS_HEADER;

    private static final String CREATE = "%s/create";

    private static final String NEXT = "%s/next";

    private static final String CLOSE = "%s/close";

    private static final String PLAN = "%s/plan";

    private ObjectReader genericResponseReader;

    private ObjectReader baseQueryResponseReader;

    private ObjectReader nextQueryResponseReader;

    private boolean initialized = false;

    private Class<? extends BaseQueryResponse> nextQueryResponseClass;

    private RemoteQueryServiceProperties remoteQueryServiceProperties;

    @Override
    @PostConstruct
    public void init() {
        if (!initialized) {
            super.init();
            if (remoteQueryServiceProperties == null) {
                throw new IllegalStateException("RemoteQueryServiceProperties must be set before calling init()");
            }
            URI serviceUri = URI.create(remoteQueryServiceProperties.getQueryServiceUri());
            setQueryServiceScheme(serviceUri.getScheme());
            setQueryServiceHost(serviceUri.getHost());
            setQueryServicePort(serviceUri.getPort());
            setQueryServiceURI(serviceUri.getPath());
            String responseClassName = remoteQueryServiceProperties.getNextQueryResponseClass();
            if (responseClassName != null) {
                try {
                    this.nextQueryResponseClass = Class.forName(responseClassName).asSubclass(BaseQueryResponse.class);
                } catch (ClassNotFoundException e) {
                    throw new IllegalArgumentException("Unable to load next query response class: " + responseClassName, e);
                }
            }

            genericResponseReader = objectMapper.readerFor(GenericResponse.class);
            baseQueryResponseReader = objectMapper.readerFor(BaseQueryResponse.class);
            if (nextQueryResponseClass == null) {
                nextQueryResponseReader = objectMapper.readerFor(responseObjectFactory.getEventQueryResponse().getClass());
            } else {
                nextQueryResponseReader = objectMapper.readerFor(nextQueryResponseClass);
            }

            initialized = true;
        }
    }

    @Override
    public GenericResponse<String> createQuery(String queryLogicName, Map<String,List<String>> queryParameters, ProxiedUserDetails callerObject)
                    throws QueryException {
        return query(CREATE, queryLogicName, queryParameters, callerObject, Long.valueOf(remoteQueryServiceProperties.getCreateTimeout()).intValue());
    }

    @Override
    public GenericResponse<String> planQuery(String queryLogicName, Map<String,List<String>> queryParameters, ProxiedUserDetails callerObject)
                    throws QueryException {
        return query(PLAN, queryLogicName, queryParameters, callerObject, Long.valueOf(remoteQueryServiceProperties.getPlanTimeoutMillis()).intValue());
    }

    private GenericResponse<String> query(String endPoint, String queryLogicName, Map<String,List<String>> queryParameters, ProxiedUserDetails callerObject,
                    int timeout) throws QueryException {
        init();
        final DatawavePrincipal principal = getDatawavePrincipal(callerObject);

        final List<NameValuePair> nameValuePairs = new ArrayList<>();
        queryParameters.entrySet().stream().forEach(e -> e.getValue().stream().forEach(v -> nameValuePairs.add(new BasicNameValuePair(e.getKey(), v))));

        final HttpEntity postBody;
        try {
            postBody = new UrlEncodedFormEntity(nameValuePairs, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }

        log.info("query Parameters : " + queryParameters);
        log.info("post body : " + postBody);

        final String suffix = String.format(endPoint, queryLogicName);
        RequestConfig reqConfig = RequestConfig.custom().setConnectionRequestTimeout(getConfig().getConnectionPoolTimeout())
                        .setConnectTimeout(getConfig().getConnectTimeout()).setSocketTimeout(timeout).build();

        try {
            // @formatter:off
            return executePostMethod(
                    suffix,
                    uriBuilder -> { },
                    httpPost -> {
                        httpPost.setConfig(reqConfig);
                        httpPost.setEntity(postBody);
                        httpPost.setHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON);
                        httpPost.setHeader(PROXIED_ENTITIES_HEADER, RemoteHttpService.getProxiedEntities(principal));
                        httpPost.setHeader(PROXIED_ISSUERS_HEADER, RemoteHttpService.getProxiedIssuers(principal));
                    },
                    entity -> {
                        return readResponse(entity, genericResponseReader);
                    },
                    () -> suffix);
            // @formatter:on
        } catch (SocketTimeoutException | ConnectTimeoutException e) {
            // also covers ConnectionPoolTimeoutException as a subclass of ConnectTimeoutException
            throw new RemoteTimeoutQueryException("RemoteQueryService timed out", e);
        } catch (URISyntaxException | IOException e) {
            // this mimics what happens up inside RemoteQueryService
            throw new RuntimeException(e);
        }
    }

    @Override
    public BaseQueryResponse next(String id, ProxiedUserDetails callerObject) throws QueryException {
        init();
        final DatawavePrincipal principal = getDatawavePrincipal(callerObject);

        final String suffix = String.format(NEXT, id);
        RequestConfig reqConfig = RequestConfig.custom().setConnectionRequestTimeout(getConfig().getConnectionPoolTimeout())
                        .setConnectTimeout(getConfig().getConnectTimeout())
                        .setSocketTimeout(Long.valueOf(remoteQueryServiceProperties.getNextTimeoutMillis()).intValue()).build();

        try {
            return executeGetMethod(suffix, uriBuilder -> {}, httpGet -> {
                httpGet.setConfig(reqConfig);
                httpGet.setHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON);
                httpGet.setHeader(PROXIED_ENTITIES_HEADER, RemoteHttpService.getProxiedEntities(principal));
                httpGet.setHeader(PROXIED_ISSUERS_HEADER, RemoteHttpService.getProxiedIssuers(principal));
            }, entity -> readResponse(entity, nextQueryResponseReader, baseQueryResponseReader), () -> suffix);
        } catch (SocketTimeoutException | ConnectTimeoutException e) {
            // also covers ConnectionPoolTimeoutException as a subclass of ConnectTimeoutException
            throw new RemoteTimeoutQueryException("RemoteQueryService timed out", e);
        } catch (URISyntaxException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public VoidResponse close(String id, ProxiedUserDetails callerObject) throws QueryException {
        init();
        final DatawavePrincipal principal = getDatawavePrincipal(callerObject);

        final String suffix = String.format(CLOSE, id);
        RequestConfig reqConfig = RequestConfig.custom().setConnectionRequestTimeout(getConfig().getConnectionPoolTimeout())
                        .setConnectTimeout(getConfig().getConnectTimeout())
                        .setSocketTimeout(Long.valueOf(remoteQueryServiceProperties.getCloseTimeoutMillis()).intValue()).build();

        try {
            return executePostMethod(suffix, uriBuilder -> {}, httpPost -> {
                httpPost.setConfig(reqConfig);
                httpPost.setHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON);
                httpPost.setHeader(PROXIED_ENTITIES_HEADER, RemoteHttpService.getProxiedEntities(principal));
                httpPost.setHeader(PROXIED_ISSUERS_HEADER, RemoteHttpService.getProxiedIssuers(principal));
            }, entity -> readVoidResponse(entity), () -> suffix);
        } catch (SocketTimeoutException | ConnectTimeoutException e) {
            // also covers ConnectionPoolTimeoutException as a subclass of ConnectTimeoutException
            throw new RemoteTimeoutQueryException("RemoteQueryService timed out", e);
        } catch (URISyntaxException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void setRemoteQueryServiceProperties(RemoteQueryServiceProperties remoteQueryServiceProperties) {
        this.remoteQueryServiceProperties = remoteQueryServiceProperties;
    }

    @Override
    public GenericResponse<String> planQuery(String id, ProxiedUserDetails callerObject) throws QueryException {
        init();
        final DatawavePrincipal principal = getDatawavePrincipal(callerObject);

        final String suffix = String.format(PLAN, id);
        RequestConfig reqConfig = RequestConfig.custom().setConnectionRequestTimeout(getConfig().getConnectionPoolTimeout())
                        .setConnectTimeout(getConfig().getConnectTimeout())
                        .setSocketTimeout(Long.valueOf(remoteQueryServiceProperties.getPlanTimeoutMillis()).intValue()).build();

        try {
            return executePostMethod(suffix, uriBuilder -> {}, httpPost -> {
                httpPost.setConfig(reqConfig);
                httpPost.setHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON);
                httpPost.setHeader(PROXIED_ENTITIES_HEADER, RemoteHttpService.getProxiedEntities(principal));
                httpPost.setHeader(PROXIED_ISSUERS_HEADER, RemoteHttpService.getProxiedIssuers(principal));
            }, entity -> readResponse(entity, genericResponseReader), () -> suffix);
        } catch (SocketTimeoutException | ConnectTimeoutException e) {
            // also covers ConnectionPoolTimeoutException as a subclass of ConnectTimeoutException
            throw new RemoteTimeoutQueryException("RemoteQueryService timed out", e);
        } catch (URISyntaxException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public URI getQueryMetricsURI(String id) {
        return URI.create(String.join("/", remoteQueryServiceProperties.getQueryMetricServiceUri(), id));
    }

    private DatawavePrincipal getDatawavePrincipal(ProxiedUserDetails callerObject) {
        if (callerObject instanceof DatawavePrincipal) {
            return (DatawavePrincipal) callerObject;
        }
        throw new RuntimeException("Cannot handle a " + callerObject.getClass() + ". Only DatawavePrincipal is accepted");
    }

    public Class<? extends BaseQueryResponse> getNextQueryResponseClass() {
        return nextQueryResponseClass;
    }

    public void setNextQueryResponseClass(Class<? extends BaseQueryResponse> nextQueryResponseClass) {
        this.nextQueryResponseClass = nextQueryResponseClass;
    }

    @Override
    public RemoteQueryServiceProperties getRemoteQueryServiceProperties() {
        return remoteQueryServiceProperties;
    }
}
