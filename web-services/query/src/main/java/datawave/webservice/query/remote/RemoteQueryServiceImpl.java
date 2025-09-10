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
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.message.BasicNameValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xbill.DNS.TextParseException;

import com.fasterxml.jackson.databind.ObjectReader;

import datawave.core.query.remote.RemoteQueryService;
import datawave.core.query.remote.RemoteTimeoutQueryException;
import datawave.security.auth.DatawaveAuthenticationMechanism;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.ProxiedUserDetails;
import datawave.webservice.common.remote.RemoteHttpService;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.GenericResponse;
import datawave.webservice.result.VoidResponse;

public class RemoteQueryServiceImpl extends RemoteHttpService implements RemoteQueryService {
    private static final Logger log = LoggerFactory.getLogger(RemoteQueryServiceImpl.class);

    public static final String PROXIED_ENTITIES_HEADER = DatawaveAuthenticationMechanism.PROXIED_ENTITIES_HEADER;
    public static final String PROXIED_ISSUERS_HEADER = DatawaveAuthenticationMechanism.PROXIED_ISSUERS_HEADER;

    private static final String CREATE = "%s/create";

    private static final String NEXT = "%s/next";

    private static final String CLOSE = "%s/close";

    private static final String PLAN = "%s/plan";

    private static final String METRICS = "Metrics/id/%s";

    private ObjectReader genericResponseReader;

    private ObjectReader baseQueryResponseReader;

    private ObjectReader nextQueryResponseReader;

    private boolean initialized = false;

    private Class<? extends BaseQueryResponse> nextQueryResponseClass;

    @Override
    @PostConstruct
    public void init() {
        if (!initialized) {
            super.init();
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
        return query(CREATE, queryLogicName, queryParameters, callerObject);
    }

    @Override
    public GenericResponse<String> planQuery(String queryLogicName, Map<String,List<String>> queryParameters, ProxiedUserDetails callerObject)
                    throws QueryException {
        return query(PLAN, queryLogicName, queryParameters, callerObject);
    }

    private GenericResponse<String> query(String endPoint, String queryLogicName, Map<String,List<String>> queryParameters, ProxiedUserDetails callerObject)
                    throws QueryException {
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
        try {
            // @formatter:off
            return executePostMethod(
                    suffix,
                    uriBuilder -> { },
                    httpPost -> {
                        httpPost.setEntity(postBody);
                        httpPost.setHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON);
                        httpPost.setHeader(PROXIED_ENTITIES_HEADER, getProxiedEntities(principal));
                        httpPost.setHeader(PROXIED_ISSUERS_HEADER, getProxiedIssuers(principal));
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
        try {
            return executeGetMethod(suffix, uriBuilder -> {}, httpGet -> {
                httpGet.setHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON);
                httpGet.setHeader(PROXIED_ENTITIES_HEADER, getProxiedEntities(principal));
                httpGet.setHeader(PROXIED_ISSUERS_HEADER, getProxiedIssuers(principal));
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
        try {
            return executePostMethod(suffix, uriBuilder -> {}, httpPost -> {
                httpPost.setHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON);
                httpPost.setHeader(PROXIED_ENTITIES_HEADER, getProxiedEntities(principal));
                httpPost.setHeader(PROXIED_ISSUERS_HEADER, getProxiedIssuers(principal));
            }, entity -> readVoidResponse(entity), () -> suffix);
        } catch (SocketTimeoutException | ConnectTimeoutException e) {
            // also covers ConnectionPoolTimeoutException as a subclass of ConnectTimeoutException
            throw new RemoteTimeoutQueryException("RemoteQueryService timed out", e);
        } catch (URISyntaxException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public GenericResponse<String> planQuery(String id, ProxiedUserDetails callerObject) throws QueryException {
        init();
        final DatawavePrincipal principal = getDatawavePrincipal(callerObject);

        final String suffix = String.format(PLAN, id);
        try {
            return executePostMethod(suffix, uriBuilder -> {}, httpPost -> {
                httpPost.setHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON);
                httpPost.setHeader(PROXIED_ENTITIES_HEADER, getProxiedEntities(principal));
                httpPost.setHeader(PROXIED_ISSUERS_HEADER, getProxiedIssuers(principal));
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
        try {
            URIBuilder builder = buildURI();
            builder.setPath(serviceURI() + String.format(METRICS, id));
            return builder.build();
        } catch (TextParseException | URISyntaxException e) {
            throw new RuntimeException(e);
        }

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
}
