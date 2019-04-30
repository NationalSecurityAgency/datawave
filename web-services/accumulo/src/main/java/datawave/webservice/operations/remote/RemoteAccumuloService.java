package datawave.webservice.operations.remote;

import com.codahale.metrics.Counter;
import com.codahale.metrics.annotation.Metric;
import com.fasterxml.jackson.databind.ObjectReader;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.system.CallerPrincipal;
import datawave.webservice.common.remote.RemoteHttpService;
import org.apache.commons.lang.StringUtils;
import org.apache.deltaspike.core.api.config.ConfigProperty;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.utils.URIBuilder;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

public abstract class RemoteAccumuloService extends RemoteHttpService {
    
    protected static final String AUTH_HEADER_NAME = "Authorization";
    
    @Inject
    @ConfigProperty(name = "dw.remoteAccumuloService.uri", defaultValue = "/accumulo/v1/")
    private String serviceURI;
    
    @Inject
    @ConfigProperty(name = "dw.remoteAccumuloService.useSrvDnsLookup", defaultValue = "false")
    private boolean useSrvDNS;
    
    @Inject
    @ConfigProperty(name = "dw.remoteAccumuloService.srvDnsServers", defaultValue = "127.0.0.1")
    private List<String> srvDnsServers;
    
    @Inject
    @ConfigProperty(name = "dw.remoteAccumuloService.srvDnsPort", defaultValue = "8600")
    private int srvDnsPort;
    
    @Inject
    @ConfigProperty(name = "dw.remoteAccumuloService.scheme", defaultValue = "https")
    private String serviceScheme;
    
    @Inject
    @ConfigProperty(name = "dw.remoteAccumuloService.host", defaultValue = "localhost")
    private String serviceHost;
    
    @Inject
    @ConfigProperty(name = "dw.remoteAccumuloService.port", defaultValue = "8843")
    private int servicePort;
    
    @Inject
    @ConfigProperty(name = "dw.remoteAccumuloService.maxConnections", defaultValue = "100")
    private int maxConnections;
    
    @Inject
    @ConfigProperty(name = "dw.remoteAccumuloService.retryCount", defaultValue = "5")
    private int retryCount;
    
    @Inject
    @ConfigProperty(name = "dw.remoteAccumuloService.unavailableRetryCount", defaultValue = "15")
    private int unavailableRetryCount;
    
    @Inject
    @ConfigProperty(name = "dw.remoteAccumuloService.unavailableRetryDelayMS", defaultValue = "2000")
    private int unavailableRetryDelay;
    
    @Inject
    @Metric(name = "dw.remoteAccumuloService.retries", absolute = true)
    private Counter retryCounter;
    
    @Inject
    @Metric(name = "dw.remoteAccumuloService.failures", absolute = true)
    private Counter failureCounter;
    
    @Inject
    @CallerPrincipal
    protected DatawavePrincipal callerPrincipal;
    
    @Override
    @PostConstruct
    public void init() {
        super.init();
    }
    
    protected <T> T executeGetMethodWithRuntimeException(String uriSuffix, Consumer<URIBuilder> uriCustomizer, Consumer<HttpGet> requestCustomizer,
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
    
    protected <T> T executePostMethodWithRuntimeException(String uriSuffix, Consumer<URIBuilder> uriCustomizer, Consumer<HttpPost> requestCustomizer,
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
    
    protected <T> T executePutMethodWithRuntimeException(String uriSuffix, Consumer<URIBuilder> uriCustomizer, Consumer<HttpPut> requestCustomizer,
                    IOFunction<T> resultConverter, Supplier<String> errorSupplier) {
        try {
            return executePutMethod(uriSuffix, uriCustomizer, requestCustomizer, resultConverter, errorSupplier);
        } catch (URISyntaxException e) {
            throw new RuntimeException("Invalid URI: " + e.getMessage(), e);
        } catch (IOException e) {
            failureCounter.inc();
            throw new RuntimeException(e.getMessage(), e);
        }
    }
    
    protected String getBearer() {
        return "Bearer " + jwtTokenHandler.createTokenFromUsers(callerPrincipal.getName(), callerPrincipal.getProxiedUsers());
    }
    
    protected <T> T execGet(String suffix, ObjectReader reader) {
        // @formatter:off
        return executeGetMethodWithRuntimeException(
                suffix,
                uriBuilder -> {},
                httpGet -> {
                    httpGet.setHeader(AUTH_HEADER_NAME, getBearer());
                },
                entity -> reader.readValue(entity.getContent()),
                () -> StringUtils.isEmpty(suffix) ? serviceURI() : suffix
        );
        // @formatter:on
    }
    
    protected <T> T execPost(String suffix, ObjectReader reader, HttpEntity postBody) {
        return execPost(suffix, reader, postBody, () -> StringUtils.isEmpty(suffix) ? serviceURI() : suffix);
    }
    
    protected <T> T execPost(String suffix, ObjectReader reader, HttpEntity postBody, Supplier<String> errorSupplier) {
        // @formatter:off
        return executePostMethodWithRuntimeException(
                suffix,
                uriBuilder -> {},
                httpPost -> {
                    httpPost.setEntity(postBody);
                    httpPost.setHeader(AUTH_HEADER_NAME, getBearer());
                },
                entity -> reader.readValue(entity.getContent()),
                errorSupplier
        );
        // @formatter:on
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
    
    @Override
    protected String serviceURI() {
        return serviceURI;
    }
}
