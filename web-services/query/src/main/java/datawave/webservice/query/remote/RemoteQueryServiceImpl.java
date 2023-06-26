package datawave.webservice.query.remote;

import com.fasterxml.jackson.databind.ObjectReader;
import datawave.core.query.remote.RemoteQueryService;
import datawave.security.auth.DatawaveAuthenticationMechanism;
import datawave.security.authorization.DatawavePrincipal;
import datawave.webservice.common.remote.RemoteHttpService;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.GenericResponse;
import datawave.webservice.result.VoidResponse;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xbill.DNS.TextParseException;

import javax.annotation.PostConstruct;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

    private ObjectReader eventQueryResponseReader;

    private boolean initialized = false;

    @Override
    @PostConstruct
    public void init() {
        if (!initialized) {
            super.init();
            genericResponseReader = objectMapper.readerFor(GenericResponse.class);
            baseQueryResponseReader = objectMapper.readerFor(BaseQueryResponse.class);
            eventQueryResponseReader = objectMapper.readerFor(responseObjectFactory.getEventQueryResponse().getClass());
            initialized = true;
        }
    }

    @Override
    public GenericResponse<String> createQuery(String queryLogicName, Map<String,List<String>> queryParameters, Object callerObject) {
        return query(CREATE, queryLogicName, queryParameters, callerObject);
    }

    @Override
    public GenericResponse<String> planQuery(String queryLogicName, Map<String,List<String>> queryParameters, Object callerObject) {
        return query(PLAN, queryLogicName, queryParameters, callerObject);
    }

    private GenericResponse<String> query(String endPoint, String queryLogicName, Map<String,List<String>> queryParameters, Object callerObject) {
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
        // @formatter:off
        return executePostMethodWithRuntimeException(
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
    }

    @Override
    public BaseQueryResponse next(String id, Object callerObject) {
        init();
        final DatawavePrincipal principal = getDatawavePrincipal(callerObject);

        final String suffix = String.format(NEXT, id);
        return executeGetMethodWithRuntimeException(suffix, uriBuilder -> {}, httpGet -> {
            httpGet.setHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON);
            httpGet.setHeader(PROXIED_ENTITIES_HEADER, getProxiedEntities(principal));
            httpGet.setHeader(PROXIED_ISSUERS_HEADER, getProxiedIssuers(principal));
        }, entity -> {
            return readResponse(entity, eventQueryResponseReader, baseQueryResponseReader);
        }, () -> suffix);
    }

    @Override
    public VoidResponse close(String id, Object callerObject) {
        init();
        final DatawavePrincipal principal = getDatawavePrincipal(callerObject);

        final String suffix = String.format(CLOSE, id);
        return executePostMethodWithRuntimeException(suffix, uriBuilder -> {}, httpPost -> {
            httpPost.setHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON);
            httpPost.setHeader(PROXIED_ENTITIES_HEADER, getProxiedEntities(principal));
            httpPost.setHeader(PROXIED_ISSUERS_HEADER, getProxiedIssuers(principal));
        }, entity -> {
            return readVoidResponse(entity);
        }, () -> suffix);
    }

    @Override
    public GenericResponse<String> planQuery(String id, Object callerObject) {
        init();
        final DatawavePrincipal principal = getDatawavePrincipal(callerObject);

        final String suffix = String.format(PLAN, id);
        return executePostMethodWithRuntimeException(suffix, uriBuilder -> {}, httpPost -> {
            httpPost.setHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON);
            httpPost.setHeader(PROXIED_ENTITIES_HEADER, getProxiedEntities(principal));
            httpPost.setHeader(PROXIED_ISSUERS_HEADER, getProxiedIssuers(principal));
        }, entity -> {
            return readResponse(entity, genericResponseReader);
        }, () -> suffix);
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

    private DatawavePrincipal getDatawavePrincipal(Object callerObject) {
        if (callerObject instanceof DatawavePrincipal) {
            return (DatawavePrincipal) callerObject;
        }
        throw new RuntimeException("Cannot handle a " + callerObject.getClass() + ". Only DatawavePrincipal is accepted");
    }

}
