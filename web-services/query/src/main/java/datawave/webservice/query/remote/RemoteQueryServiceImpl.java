
package datawave.webservice.query.remote;

import com.codahale.metrics.Counter;
import com.fasterxml.jackson.databind.ObjectReader;
import datawave.core.query.remote.RemoteQueryService;
import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.security.authorization.DatawavePrincipal;
import datawave.webservice.common.remote.RemoteHttpService;
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
import org.springframework.util.StreamUtils;

import javax.annotation.PostConstruct;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collections;
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
    
    private boolean useSrvDNS = false;
    
    private List<String> srvDnsServers = Collections.singletonList("127.0.0.1");
    
    private int srvDnsPort = 8600;
    
    private String queryServiceScheme = "https";
    
    private String queryServiceHost = "localhost";
    
    private int queryServicePort = 8443;
    
    private String queryServiceURI = "/query/v1/";
    
    private int maxConnections = 100;
    
    private int retryCount = 5;
    
    private int unavailableRetryCount = 15;
    
    private int unavailableRetryDelay = 2000;
    
    private Counter retryCounter = new Counter();
    
    private Counter failureCounter = new Counter();
    
    private boolean initialized = false;
    
    @Override
    @PostConstruct
    public void init() {
        if (!initialized) {
            super.init();
            voidResponseReader = objectMapper.readerFor(VoidResponse.class);
            genericResponseReader = objectMapper.readerFor(GenericResponse.class);
            baseQueryResponseReader = objectMapper.readerFor(BaseQueryResponse.class);
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
            return readResponse(entity, baseQueryResponseReader);
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
        } else if (entity.isRepeatable()) {
            try {
                return reader.readValue(entity.getContent());
            } catch (IOException ioe) {
                log.error("Failed to read entity content with " + reader.getValueType() + ".  Trying as a VoidResponse.", ioe);
                VoidResponse response = voidResponseReader.readValue(entity.getContent());
                throw new RuntimeException(response.getMessages().toString());
            }
        } else {
            byte[] data = StreamUtils.copyToByteArray(entity.getContent());
            try {
                return reader.readValue(new ByteArrayInputStream(data));
            } catch (IOException ioe) {
                log.error("Failed to read entity content with " + reader.getValueType() + ".  Trying as a VoidResponse.", ioe);
                VoidResponse response = voidResponseReader.readValue(entity.getContent());
                throw new RuntimeException(response.getMessages().toString());
            }
        }
    }
    
    public VoidResponse readVoidResponse(HttpEntity entity) throws IOException {
        if (entity == null) {
            return null;
        } else if (entity.isRepeatable()) {
            VoidResponse response = voidResponseReader.readValue(entity.getContent());
            if (response.getHasResults()) {
                return response;
            } else {
                throw new RuntimeException(response.getMessages().toString());
            }
        } else {
            byte[] data = StreamUtils.copyToByteArray(entity.getContent());
            VoidResponse response = voidResponseReader.readValue(new ByteArrayInputStream(data));
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
        } else if (callerObject instanceof DatawaveUserDetails) {
            DatawaveUserDetails callerDetails = (DatawaveUserDetails) callerObject;
            return "Bearer " + jwtTokenHandler.createTokenFromUsers(callerDetails.getUsername(), callerDetails.getProxiedUsers());
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
            failureCounter.inc();
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
            failureCounter.inc();
            throw new RuntimeException(e.getMessage(), e);
        }
    }
    
    @Override
    protected String serviceHost() {
        return queryServiceHost;
    }
    
    @Override
    protected int servicePort() {
        return queryServicePort;
    }
    
    @Override
    protected String serviceURI() {
        return queryServiceURI;
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
        return queryServiceScheme;
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
    
    public void setUseSrvDNS(boolean useSrvDNS) {
        this.useSrvDNS = useSrvDNS;
    }
    
    public void setSrvDnsServers(List<String> srvDnsServers) {
        this.srvDnsServers = srvDnsServers;
    }
    
    public void setSrvDnsPort(int srvDnsPort) {
        this.srvDnsPort = srvDnsPort;
    }
    
    public void setQueryServiceScheme(String queryServiceScheme) {
        this.queryServiceScheme = queryServiceScheme;
    }
    
    public void setQueryServiceHost(String queryServiceHost) {
        this.queryServiceHost = queryServiceHost;
    }
    
    public void setQueryServicePort(int queryServicePort) {
        this.queryServicePort = queryServicePort;
    }
    
    public void setQueryServiceURI(String queryServiceURI) {
        this.queryServiceURI = queryServiceURI;
    }
    
    public void setMaxConnections(int maxConnections) {
        this.maxConnections = maxConnections;
    }
    
    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }
    
    public void setUnavailableRetryCount(int unavailableRetryCount) {
        this.unavailableRetryCount = unavailableRetryCount;
    }
    
    public void setUnavailableRetryDelay(int unavailableRetryDelay) {
        this.unavailableRetryDelay = unavailableRetryDelay;
    }
}
