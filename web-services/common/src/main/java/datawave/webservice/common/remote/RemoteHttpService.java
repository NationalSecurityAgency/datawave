package datawave.webservice.common.remote;

import com.codahale.metrics.Counter;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationModule;
import com.spotify.dns.DnsSrvResolver;
import com.spotify.dns.DnsSrvResolvers;
import com.spotify.dns.LookupResult;
import datawave.security.authorization.JWTTokenHandler;
import datawave.security.authorization.JWTTokenHandler.TtlMode;
import datawave.security.util.DnUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.DefaultServiceUnavailableRetryStrategy;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.jboss.security.JSSESecurityDomain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xbill.DNS.DClass;
import org.xbill.DNS.ExtendedResolver;
import org.xbill.DNS.Lookup;
import org.xbill.DNS.Record;
import org.xbill.DNS.TextParseException;
import org.xbill.DNS.Type;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import javax.enterprise.concurrent.ManagedExecutorService;
import javax.inject.Inject;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.X509KeyManager;
import java.io.IOException;
import java.net.ConnectException;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.security.Key;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * A base class for services that need to use HTTPClient to make remote calls to a microservice.
 */
public abstract class RemoteHttpService {
    protected final Logger log = LoggerFactory.getLogger(getClass());
    
    protected JWTTokenHandler jwtTokenHandler;
    protected DnsSrvResolver dnsSrvResolver;
    protected ObjectMapper objectMapper;
    private CloseableHttpClient client;
    private AtomicInteger activeExecutions = new AtomicInteger(0);
    
    @Inject
    private JSSESecurityDomain jsseSecurityDomain;
    
    @Resource
    private ManagedExecutorService executorService;
    
    protected <T> T execute(HttpRequestBase request, IOFunction<T> resultConverter, Supplier<String> errorSupplier) throws IOException {
        try {
            activeExecutions.incrementAndGet();
            return client.execute(
                            request,
                            r -> {
                                if (r.getStatusLine().getStatusCode() != 200) {
                                    throw new ClientProtocolException("Unable to " + errorSupplier.get() + ": " + r.getStatusLine() + " "
                                                    + EntityUtils.toString(r.getEntity()));
                                } else {
                                    return resultConverter.apply(r.getEntity());
                                }
                            });
        } finally {
            activeExecutions.decrementAndGet();
        }
    }
    
    @PostConstruct
    protected void init() {
        objectMapper = new ObjectMapper();
        objectMapper.enable(MapperFeature.USE_WRAPPER_NAME_AS_PROPERTY_NAME);
        objectMapper.registerModule(new GuavaModule());
        objectMapper.registerModule(new JaxbAnnotationModule());
        
        if (useSrvDns()) {
            if (srvDnsServers() != null && !srvDnsServers().isEmpty()) {
                try {
                    ExtendedResolver resolver = new ExtendedResolver(srvDnsServers().toArray(new String[0]));
                    resolver.setLoadBalance(true);
                    resolver.setPort(srvDnsPort());
                    Lookup.setDefaultResolver(resolver);
                } catch (UnknownHostException e) {
                    throw new IllegalArgumentException("Unable to resolve SRV DNS hosts: " + srvDnsServers() + ": " + e.getMessage(), e);
                }
            }
            dnsSrvResolver = DnsSrvResolvers.newBuilder().build();
        }
        
        try {
            SSLContext ctx = SSLContext.getInstance("TLSv1.2");
            ctx.init(jsseSecurityDomain.getKeyManagers(), jsseSecurityDomain.getTrustManagers(), null);
            
            String alias = jsseSecurityDomain.getKeyStore().aliases().nextElement();
            X509KeyManager keyManager = (X509KeyManager) jsseSecurityDomain.getKeyManagers()[0];
            X509Certificate[] certs = keyManager.getCertificateChain(alias);
            Key signingKey = keyManager.getPrivateKey(alias);
            
            jwtTokenHandler = new JWTTokenHandler(certs[0], signingKey, 24, TimeUnit.HOURS, TtlMode.RELATIVE_TO_CURRENT_TIME, objectMapper);
            
            ArrayList<Header> defaultHeaders = new ArrayList<>();
            defaultHeaders.add(new BasicHeader(HttpHeaders.ACCEPT, ContentType.APPLICATION_JSON.getMimeType()));
            // If we're using HTTP, then add our cert in as a header so the authorization service knows who we are.
            if ("http".equals(serviceScheme())) {
                defaultHeaders.add(new BasicHeader("X-SSL-clientcert-subject", DnUtils.normalizeDN(certs[0].getSubjectX500Principal().getName())));
                defaultHeaders.add(new BasicHeader("X-SSL-clientcert-issuer", DnUtils.normalizeDN(certs[0].getIssuerX500Principal().getName())));
            }
            
            // @formatter:off
            client = HttpClients.custom()
                    .setSSLContext(ctx)
                    .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                    .setDefaultHeaders(defaultHeaders)
                    .setMaxConnTotal(maxConnections())
                    .setMaxConnPerRoute(maxConnections())
                    .setRetryHandler(new DatawaveRetryHandler(retryCount(), unavailableRetryCount(), unavailableRetryDelay(), retryCounter()))
                    .setServiceUnavailableRetryStrategy(new DatawaveUnavailableRetryStrategy(unavailableRetryCount(), unavailableRetryDelay(), retryCounter()))
                    .build();
            // @formatter:on
        } catch (NoSuchAlgorithmException | KeyManagementException e) {
            log.error("Unable to instantiate SSL context: " + e);
            throw new IllegalStateException(e);
        } catch (KeyStoreException e) {
            log.error("Unable to retrieve aliases from KeyStore.");
            throw new IllegalStateException(e);
        }
    }
    
    @PreDestroy
    protected void shutdown() {
        executorService.submit(() -> {
            long waitStart = System.currentTimeMillis();
            long totalWait = 0;
            while (activeExecutions.get() > 0 && totalWait < 60000L) {
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e) {
                    break;
                }
                totalWait = System.currentTimeMillis() - waitStart;
            }
            try {
                client.close();
            } catch (IOException e) {
                log.warn("Exception while shutting down HttpClient: " + e.getMessage(), e);
            }
            
        });
    }
    
    protected URIBuilder buildURI() throws TextParseException {
        final String host = serviceHost();
        final int port = servicePort();
        URIBuilder builder = new URIBuilder();
        builder.setScheme(serviceScheme());
        if (useSrvDns()) {
            List<LookupResult> results = dnsSrvResolver.resolve(host);
            if (results != null && !results.isEmpty()) {
                LookupResult result = results.get(0);
                builder.setHost(result.host());
                builder.setPort(result.port());
                // Consul sends the hostname back in its own namespace. Although the A record is included in the
                // "ADDITIONAL SECTION", Spotify SRV lookup doesn't translate, so we need to do the lookup manually.
                if (result.host().endsWith(".consul.")) {
                    Record[] newResults = new Lookup(result.host(), Type.A, DClass.IN).run();
                    if (newResults != null && newResults.length > 0) {
                        builder.setHost(newResults[0].rdataToString());
                    } else {
                        throw new IllegalArgumentException("Unable to resolve service host " + host + " -> " + result.host() + " -> ???");
                    }
                }
            } else {
                throw new IllegalArgumentException("Unable to resolve service host: " + host);
            }
        } else {
            builder.setHost(host);
            builder.setPort(port);
        }
        return builder;
    }
    
    public URIBuilder buildURI(String suffix) throws TextParseException {
        if (suffix == null)
            suffix = "";
        return buildURI().setPath(serviceURI() + suffix);
    }
    
    protected <T> T executeGetMethod(Consumer<URIBuilder> uriCustomizer, Consumer<HttpGet> requestCustomizer, IOFunction<T> resultConverter,
                    Supplier<String> errorSupplier) throws URISyntaxException, IOException {
        return executeGetMethod("", uriCustomizer, requestCustomizer, resultConverter, errorSupplier);
    }
    
    protected <T> T executeGetMethod(String uriSuffix, Consumer<URIBuilder> uriCustomizer, Consumer<HttpGet> requestCustomizer, IOFunction<T> resultConverter,
                    Supplier<String> errorSupplier) throws URISyntaxException, IOException {
        URIBuilder builder = buildURI();
        builder.setPath(serviceURI() + uriSuffix);
        uriCustomizer.accept(builder);
        HttpGet getRequest = new HttpGet(builder.build());
        requestCustomizer.accept(getRequest);
        return execute(getRequest, resultConverter, errorSupplier);
    }
    
    protected <T> T executePostMethod(Consumer<URIBuilder> uriCustomizer, Consumer<HttpPost> requestCustomizer, IOFunction<T> resultConverter,
                    Supplier<String> errorSupplier) throws URISyntaxException, IOException {
        return executePostMethod("", uriCustomizer, requestCustomizer, resultConverter, errorSupplier);
    }
    
    protected <T> T executePostMethod(String uriSuffix, Consumer<URIBuilder> uriCustomizer, Consumer<HttpPost> requestCustomizer,
                    IOFunction<T> resultConverter, Supplier<String> errorSupplier) throws URISyntaxException, IOException {
        URIBuilder builder = buildURI();
        builder.setPath(serviceURI() + uriSuffix);
        uriCustomizer.accept(builder);
        HttpPost postRequest = new HttpPost(builder.build());
        requestCustomizer.accept(postRequest);
        return execute(postRequest, resultConverter, errorSupplier);
    }
    
    protected <T> T executePutMethod(String uriSuffix, Consumer<URIBuilder> uriCustomizer, Consumer<HttpPut> requestCustomizer, IOFunction<T> resultConverter,
                    Supplier<String> errorSupplier) throws URISyntaxException, IOException {
        URIBuilder builder = buildURI();
        builder.setPath(serviceURI() + uriSuffix);
        uriCustomizer.accept(builder);
        HttpPut putRequest = new HttpPut(builder.build());
        requestCustomizer.accept(putRequest);
        return execute(putRequest, resultConverter, errorSupplier);
    }
    
    protected abstract String serviceHost();
    
    protected abstract int servicePort();
    
    protected abstract String serviceURI();
    
    protected abstract boolean useSrvDns();
    
    protected abstract List<String> srvDnsServers();
    
    protected abstract int srvDnsPort();
    
    protected abstract String serviceScheme();
    
    protected abstract int maxConnections();
    
    protected abstract int retryCount();
    
    protected abstract int unavailableRetryCount();
    
    protected abstract int unavailableRetryDelay();
    
    protected abstract Counter retryCounter();
    
    private static class DatawaveRetryHandler extends DefaultHttpRequestRetryHandler {
        private final int unavailableRetryCount;
        private final int unavailableRetryDelay;
        private final Counter retryCounter;
        
        public DatawaveRetryHandler(int retryCount, int unavailableRetryCount, int unavailableRetryDelay, Counter retryCounter) {
            super(retryCount, false, Arrays.asList(UnknownHostException.class, SSLException.class));
            this.unavailableRetryCount = unavailableRetryCount;
            this.unavailableRetryDelay = unavailableRetryDelay;
            this.retryCounter = retryCounter;
        }
        
        @Override
        public boolean retryRequest(IOException exception, int executionCount, HttpContext context) {
            boolean shouldRetry = super.retryRequest(exception, executionCount, context);
            if (exception instanceof ConnectException) {
                shouldRetry = (executionCount <= unavailableRetryCount);
                if (shouldRetry) {
                    try {
                        Thread.sleep(unavailableRetryDelay);
                    } catch (InterruptedException e) {
                        // Ignore -- we'll just end up retrying a little too fast
                    }
                }
            }
            if (shouldRetry) {
                retryCounter.inc();
            }
            return shouldRetry;
        }
    }
    
    private static class DatawaveUnavailableRetryStrategy extends DefaultServiceUnavailableRetryStrategy {
        private final int maxRetries;
        private final Counter retryCounter;
        
        private DatawaveUnavailableRetryStrategy(int maxRetries, int retryInterval, Counter retryCounter) {
            super(maxRetries, retryInterval);
            this.maxRetries = maxRetries;
            this.retryCounter = retryCounter;
        }
        
        @Override
        public boolean retryRequest(HttpResponse response, int executionCount, HttpContext context) {
            // Note that a 404 can happen during service startup, so we want to retry.
            boolean shouldRetry = executionCount <= maxRetries
                            && (response.getStatusLine().getStatusCode() == HttpStatus.SC_SERVICE_UNAVAILABLE || response.getStatusLine().getStatusCode() == HttpStatus.SC_NOT_FOUND);
            if (shouldRetry) {
                retryCounter.inc();
            }
            return shouldRetry;
        }
    }
    
    protected interface IOFunction<T> {
        T apply(HttpEntity entity) throws IOException;
    }
}
