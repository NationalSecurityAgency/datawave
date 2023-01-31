package datawave.security.authorization.remote;

import com.codahale.metrics.Counter;
import com.fasterxml.jackson.databind.ObjectReader;
import datawave.security.auth.DatawaveAuthenticationMechanism;
import datawave.security.authorization.AuthorizationException;
import datawave.security.authorization.DatawavePrincipal;
import datawave.user.AuthorizationsListBase;
import datawave.webservice.common.remote.RemoteHttpService;
import datawave.webservice.common.remote.RemoteHttpServiceConfiguration;
import datawave.security.authorization.RemoteUserOperations;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.result.GenericResponse;
import datawave.webservice.result.VoidResponse;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
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
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class RemoteUserOperationsImpl extends RemoteHttpService implements RemoteUserOperations {
    private static final Logger log = LoggerFactory.getLogger(RemoteUserOperationsImpl.class);
    
    public static final String PROXIED_ENTITIES_HEADER = DatawaveAuthenticationMechanism.PROXIED_ENTITIES_HEADER;
    public static final String PROXIED_ISSUERS_HEADER = DatawaveAuthenticationMechanism.PROXIED_ISSUERS_HEADER;
    
    // set includeRemoteServices off to avoid any subsequent hops
    private static final String LIST_EFFECTIVE_AUTHS = "listEffectiveAuthorizations";
    
    private static final String FLUSH_CREDS = "flushCachedCredentials";
    
    private ObjectReader voidResponseReader;
    
    private ObjectReader genericResponseReader;
    
    private ObjectReader authResponseReader;
    
    private ResponseObjectFactory responseObjectFactory;
    
    private RemoteHttpServiceConfiguration config = new RemoteHttpServiceConfiguration();
    
    private boolean initialized = false;
    
    @Override
    @PostConstruct
    public void init() {
        if (!initialized) {
            super.init();
            voidResponseReader = objectMapper.readerFor(VoidResponse.class);
            genericResponseReader = objectMapper.readerFor(GenericResponse.class);
            authResponseReader = objectMapper.readerFor(responseObjectFactory.getAuthorizationsList().getClass());
            initialized = true;
        }
    }
    
    @Override
    public AuthorizationsListBase listEffectiveAuthorizations(Object callerObject) throws AuthorizationException {
        final String suffix = LIST_EFFECTIVE_AUTHS;
        // includeRemoteServices=false to avoid any loops
        return executeGetMethodWithRuntimeException(suffix, uriBuilder -> {
            uriBuilder.addParameter("includeRemoteServices", "false");
        }, httpGet -> {
            httpGet.setHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON);
            httpGet.setHeader(PROXIED_ENTITIES_HEADER, getProxiedEntities(callerObject));
            httpGet.setHeader(PROXIED_ISSUERS_HEADER, getProxiedIssuers(callerObject));
        }, entity -> {
            return readResponse(entity, authResponseReader);
        }, () -> suffix);
    }
    
    @Override
    public GenericResponse<String> flushCachedCredentials(Object callerObject) {
        final String suffix = FLUSH_CREDS;
        // includeRemoteServices=false to avoid any loops
        return executeGetMethodWithRuntimeException(suffix, uriBuilder -> {
            uriBuilder.addParameter("includeRemoteServices", "false");
        }, httpGet -> {
            httpGet.setHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON);
            httpGet.setHeader(PROXIED_ENTITIES_HEADER, getProxiedEntities(callerObject));
            httpGet.setHeader(PROXIED_ISSUERS_HEADER, getProxiedIssuers(callerObject));
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
    
    public static String getProxiedEntities(Object callerObject) {
        if (callerObject instanceof DatawavePrincipal) {
            DatawavePrincipal callerPrincipal = (DatawavePrincipal) callerObject;
            return callerPrincipal.getProxiedUsers().stream().map(u -> new StringBuilder().append('<').append(u.getDn().subjectDN()).append('>'))
                            .collect(Collectors.joining());
        } else {
            throw new RuntimeException("Cannot use " + callerObject.getClass().getName() + " as a caller object");
        }
    }
    
    public static String getProxiedIssuers(Object callerObject) {
        if (callerObject instanceof DatawavePrincipal) {
            DatawavePrincipal callerPrincipal = (DatawavePrincipal) callerObject;
            return callerPrincipal.getProxiedUsers().stream().map(u -> new StringBuilder().append('<').append(u.getDn().issuerDN()).append('>'))
                            .collect(Collectors.joining());
        } else {
            throw new RuntimeException("Cannot use " + callerObject.getClass().getName() + " as a caller object");
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
