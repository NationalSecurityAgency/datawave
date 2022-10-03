package datawave.webservice.query.remote;

import com.codahale.metrics.Counter;
import com.fasterxml.jackson.databind.ObjectReader;
import datawave.security.authorization.DatawavePrincipal;
import datawave.webservice.common.remote.RemoteHttpService;
import datawave.webservice.common.remote.RemoteHttpServiceConfiguration;
import datawave.webservice.common.remote.RemoteQueryService;
import datawave.webservice.query.result.event.DefaultResponseObjectFactory;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.GenericResponse;
import datawave.webservice.result.VoidResponse;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class RemoteQueryServiceImpl extends RemoteHttpService implements RemoteQueryService {
    private static final Logger log = LoggerFactory.getLogger(RemoteQueryServiceImpl.class);
    
    private static final String CREATE = "%s/create";
    
    private static final String NEXT = "%s/next";
    
    private static final String CLOSE = "%s/close";
    
    private static final String PLAN = "%s/plan";
    
    private ObjectReader voidResponseReader;
    
    private ObjectReader genericResponseReader;
    
    private ObjectReader baseQueryResponseReader;
    
    private ObjectReader eventQueryResponseReader;
    
    private ResponseObjectFactory responseObjectFactory = new DefaultResponseObjectFactory();
    
    private RemoteHttpServiceConfiguration config = new RemoteHttpServiceConfiguration();
    
    private boolean initialized = false;
    
    @Override
    @PostConstruct
    public void init() {
        if (!initialized) {
            super.init();
            voidResponseReader = objectMapper.readerFor(VoidResponse.class);
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
        final String postBody;
        final StringEntity post;
        try {
            URIBuilder uriBuilder = new URIBuilder();
            queryParameters.entrySet().stream().forEach(e -> e.getValue().stream().forEach(v -> uriBuilder.addParameter(e.getKey(), v)));
            postBody = uriBuilder.build().getQuery();
            post = new StringEntity(postBody, ContentType.APPLICATION_FORM_URLENCODED);
        } catch (URISyntaxException e) {
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
                    httpPost.setEntity(post);
                    httpPost.setHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON);
                    httpPost.setHeader(HttpHeaders.AUTHORIZATION, getBearer(callerObject));
                    httpPost.setHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_FORM_URLENCODED);
                },
                entity -> {
                    return readResponse(entity, genericResponseReader);
                },
                () -> suffix);
        // @formatter:on
    }
    
    @Override
    public BaseQueryResponse next(String id, Object callerObject) {
        final String suffix = String.format(NEXT, id);
        return executeGetMethodWithRuntimeException(suffix, uriBuilder -> {}, httpGet -> {
            httpGet.setHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON);
            httpGet.setHeader(HttpHeaders.AUTHORIZATION, getBearer(callerObject));
        }, entity -> {
            return readResponse(entity, eventQueryResponseReader, baseQueryResponseReader);
        }, () -> suffix);
    }
    
    @Override
    public VoidResponse close(String id, Object callerObject) {
        final String suffix = String.format(CLOSE, id);
        return executePostMethodWithRuntimeException(suffix, uriBuilder -> {}, httpPost -> {
            httpPost.setHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON);
            httpPost.setHeader(HttpHeaders.AUTHORIZATION, getBearer(callerObject));
        }, entity -> {
            return readVoidResponse(entity);
        }, () -> suffix);
    }
    
    @Override
    public GenericResponse<String> planQuery(String id, Object callerObject) {
        final String suffix = String.format(PLAN, id);
        return executePostMethodWithRuntimeException(suffix, uriBuilder -> {}, httpPost -> {
            httpPost.setHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON);
            httpPost.setHeader(HttpHeaders.AUTHORIZATION, getBearer(callerObject));
        }, entity -> {
            return readResponse(entity, genericResponseReader);
        }, () -> suffix);
    }
    
    public <T> T readResponse(HttpEntity entity, ObjectReader reader) throws IOException {
        if (entity == null) {
            return null;
        } else {
            String content = getContent(entity.getContent());
            try {
                return reader.readValue(content);
            } catch (IOException ioe) {
                log.error("Failed to read entity content.  Trying as a VoidResponse.", ioe);
                log.error(content);
                VoidResponse response = voidResponseReader.readValue(content);
                throw new RuntimeException(response.getMessages().toString());
            }
        }
    }
    
    public <T> T readResponse(HttpEntity entity, ObjectReader reader1, ObjectReader reader2) throws IOException {
        if (entity == null) {
            return null;
        } else {
            String content = getContent(entity.getContent());
            try {
                return reader1.readValue(content);
            } catch (IOException ioe1) {
                try {
                    return reader2.readValue(content);
                } catch (IOException ioe) {
                    log.error("Failed to read entity content.  Trying as a VoidResponse.", ioe);
                    log.error(content);
                    VoidResponse response = voidResponseReader.readValue(content);
                    throw new RuntimeException(response.getMessages().toString());
                }
            }
        }
    }
    
    private String getContent(InputStream content) throws IOException {
        StringBuilder builder = new StringBuilder();
        InputStreamReader reader = new InputStreamReader(content, "UTF8");
        char[] buffer = new char[1024];
        int chars = reader.read(buffer);
        while (chars >= 0) {
            builder.append(buffer, 0, chars);
            chars = reader.read(buffer);
        }
        return builder.toString();
    }
    
    public VoidResponse readVoidResponse(HttpEntity entity) throws IOException {
        if (entity == null) {
            return null;
        } else {
            VoidResponse response = voidResponseReader.readValue(entity.getContent());
            if (response.getHasResults()) {
                return response;
            } else {
                throw new RuntimeException(response.getMessages().toString());
            }
        }
    }
    
    protected String getBearer(Object callerObject) {
        init();
        if (callerObject instanceof DatawavePrincipal) {
            DatawavePrincipal callerPrincipal = (DatawavePrincipal) callerObject;
            return "Bearer " + jwtTokenHandler.createTokenFromUsers(callerPrincipal.getName(), callerPrincipal.getProxiedUsers());
        } else {
            throw new RuntimeException("Cannot use " + callerObject.getClass().getName() + " as a caller object");
        }
    }
    
    private <T> T executePostMethodWithRuntimeException(String uriSuffix, Consumer<URIBuilder> uriCustomizer, Consumer<HttpPost> requestCustomizer,
                    IOFunction<T> resultConverter, Supplier<String> errorSupplier) {
        init();
        try {
            return executePostMethod(uriSuffix, uriCustomizer, requestCustomizer, resultConverter, errorSupplier);
        } catch (URISyntaxException e) {
            throw new RuntimeException("Invalid URI: " + e.getMessage(), e);
        } catch (IOException e) {
            config.getFailureCounter().inc();
            throw new RuntimeException(e.getMessage(), e);
        }
    }
    
    private <T> T executeGetMethodWithRuntimeException(String uriSuffix, Consumer<URIBuilder> uriCustomizer, Consumer<HttpGet> requestCustomizer,
                    IOFunction<T> resultConverter, Supplier<String> errorSupplier) {
        init();
        try {
            return executeGetMethod(uriSuffix, uriCustomizer, requestCustomizer, resultConverter, errorSupplier);
        } catch (URISyntaxException e) {
            throw new RuntimeException("Invalid URI: " + e.getMessage(), e);
        } catch (IOException e) {
            config.getFailureCounter().inc();
            throw new RuntimeException(e.getMessage(), e);
        }
    }
    
    @Override
    protected String serviceHost() {
        return config.getServiceHost();
    }
    
    @Override
    protected int servicePort() {
        return config.getServicePort();
    }
    
    @Override
    protected String serviceURI() {
        return config.getServiceURI();
    }
    
    @Override
    protected boolean useSrvDns() {
        return config.isUseSrvDNS();
    }
    
    @Override
    protected List<String> srvDnsServers() {
        return config.getSrvDnsServers();
    }
    
    @Override
    protected int srvDnsPort() {
        return config.getSrvDnsPort();
    }
    
    @Override
    protected String serviceScheme() {
        return config.getServiceScheme();
    }
    
    @Override
    protected int maxConnections() {
        return config.getMaxConnections();
    }
    
    @Override
    protected int retryCount() {
        return config.getRetryCount();
    }
    
    @Override
    protected int unavailableRetryCount() {
        return config.getUnavailableRetryCount();
    }
    
    @Override
    protected int unavailableRetryDelay() {
        return config.getUnavailableRetryDelay();
    }
    
    @Override
    protected Counter retryCounter() {
        return config.getRetryCounter();
    }
    
    public void setUseSrvDNS(boolean useSrvDNS) {
        config.setUseSrvDNS(useSrvDNS);
    }
    
    public void setSrvDnsServers(List<String> srvDnsServers) {
        config.setSrvDnsServers(srvDnsServers);
    }
    
    public void setSrvDnsPort(int srvDnsPort) {
        config.setSrvDnsPort(srvDnsPort);
    }
    
    public void setQueryServiceScheme(String queryServiceScheme) {
        config.setServiceScheme(queryServiceScheme);
    }
    
    public void setQueryServiceHost(String queryServiceHost) {
        config.setServiceHost(queryServiceHost);
    }
    
    public void setQueryServicePort(int queryServicePort) {
        config.setServicePort(queryServicePort);
    }
    
    public void setQueryServiceURI(String queryServiceURI) {
        config.setServiceURI(queryServiceURI);
    }
    
    public void setMaxConnections(int maxConnections) {
        config.setMaxConnections(maxConnections);
    }
    
    public void setRetryCount(int retryCount) {
        config.setRetryCount(retryCount);
    }
    
    public void setUnavailableRetryCount(int unavailableRetryCount) {
        config.setUnavailableRetryCount(unavailableRetryCount);
    }
    
    public void setUnavailableRetryDelay(int unavailableRetryDelay) {
        config.setUnavailableRetryDelay(unavailableRetryDelay);
    }
    
    public ResponseObjectFactory getResponseObjectFactory() {
        return responseObjectFactory;
    }
    
    public void setResponseObjectFactory(ResponseObjectFactory responseObjectFactory) {
        this.responseObjectFactory = responseObjectFactory;
    }
    
    public RemoteHttpServiceConfiguration getConfig() {
        return config;
    }
    
    public void setConfig(RemoteHttpServiceConfiguration config) {
        this.config = config;
    }
}
