package datawave.webservice.common.audit.remote;

import com.codahale.metrics.Counter;
import com.codahale.metrics.annotation.Metric;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.spotify.dns.DnsSrvResolver;
import com.spotify.dns.DnsSrvResolvers;
import com.spotify.dns.LookupResult;
import datawave.configuration.RefreshableScope;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.util.DnUtils;
import datawave.webservice.common.audit.AuditParameters;
import datawave.webservice.common.audit.Auditor;
import datawave.webservice.security.JWTTokenHandler;
import org.apache.deltaspike.core.api.config.ConfigProperty;
import org.apache.deltaspike.core.api.exclude.Exclude;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpPost;
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
import org.xbill.DNS.Type;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Priority;
import javax.annotation.Resource;
import javax.ejb.Asynchronous;
import javax.ejb.EJBContext;
import javax.enterprise.inject.Alternative;
import javax.inject.Inject;
import javax.interceptor.Interceptor;
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
import java.security.Principal;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * This default auditor delegates all methods to a remote audit microservice.
 */
@RefreshableScope
@Alternative
// Make this alternative active for the entire application per the CDI 1.2 specification
@Priority(Interceptor.Priority.APPLICATION)
@Exclude(onExpression = "dw.audit.use.remoteauditservice!=true")
public class RemoteAuditor implements Auditor {
    private final Logger log = LoggerFactory.getLogger(getClass());
    
    private CloseableHttpClient client;
    private JWTTokenHandler jwtTokenHandler;
    
    @Inject
    private JSSESecurityDomain jsseSecurityDomain;
    
    @Inject
    @ConfigProperty(name = "dw.remoteDatawaveAuditService.useSrvDnsLookup", defaultValue = "false")
    private boolean useSrvDNS;
    
    @Inject
    @ConfigProperty(name = "dw.remoteDatawaveAuditService.srvDnsServers", defaultValue = "127.0.0.1")
    private List<String> srvDnsServers;
    
    @Inject
    @ConfigProperty(name = "dw.remoteDatawaveAuditService.srvDnsPort", defaultValue = "8600")
    private int srvDnsPort;
    
    @Inject
    @ConfigProperty(name = "dw.remoteDatawaveAuditService.scheme", defaultValue = "https")
    private String auditServiceScheme;
    
    @Inject
    @ConfigProperty(name = "dw.remoteDatawaveAuditService.host", defaultValue = "localhost")
    private String auditServiceHost;
    
    @Inject
    @ConfigProperty(name = "dw.remoteDatawaveAuditService.port", defaultValue = "8443")
    private int auditServicePort;
    
    @Inject
    @ConfigProperty(name = "dw.remoteDatawaveAuditService.uri", defaultValue = "/datawave/")
    private String auditServiceURI;
    
    @Inject
    @ConfigProperty(name = "dw.remoteDatawaveAuditService.maxConnections", defaultValue = "100")
    private int maxConnections;
    
    @Inject
    @ConfigProperty(name = "dw.remoteDatawaveAuditService.retryCount", defaultValue = "5")
    private int retryCount;
    
    @Inject
    @ConfigProperty(name = "dw.remoteDatawaveAuditService.unavailableRetryCount", defaultValue = "15")
    private int unavailableRetryCount;
    
    @Inject
    @ConfigProperty(name = "dw.remoteDatawaveAuditService.unavailableRetryDelayMS", defaultValue = "2000")
    private int unavailableRetryDelay;
    
    @Inject
    @Metric(name = "dw.remoteDatawaveAuditService.retries", absolute = true)
    private Counter retryCounter;
    
    @Inject
    @Metric(name = "dw.remoteDatawaveAuditService.failures", absolute = true)
    private Counter failureCounter;
    
    private DnsSrvResolver dnsSrvResolver;
    
    @Resource
    private EJBContext ctx;
    
    @Override
    @Timed(name = "dw.remoteAuditService.audit", absolute = true)
    public void audit(AuditParameters msg) throws Exception {
        Principal p = ctx.getCallerPrincipal();
        DatawavePrincipal dp = null;
        if (p instanceof DatawavePrincipal)
            dp = (DatawavePrincipal) p;
        
        final String bearerHeader = "Bearer " + jwtTokenHandler.createTokenFromUsers(dp.getName(), dp.getProxiedUsers());
        // @formatter:off
        executePostMethodAsyncWithRuntimeException(
                "audit",
                uriBuilder -> {
                    Map<String, String> params = msg.toMap();
                    for (String param : params.keySet())
                        uriBuilder.addParameter(param, params.get(param));
                },
                httpPost -> {
                    httpPost.setHeader("Authorization", bearerHeader);
                },
                EntityUtils::toString,
                () -> "audit [" + msg.toString() + "]");
        // @formatter:on
    }
    
    @Asynchronous
    protected <T> void executePostMethodAsyncWithRuntimeException(String uriSuffix, Consumer<URIBuilder> uriCustomizer, Consumer<HttpPost> requestCustomizer,
                    IOFunction<T> resultConverter, Supplier<String> errorSupplier) {
        T response = executePostMethodWithRuntimeException(uriSuffix, uriCustomizer, requestCustomizer, resultConverter, errorSupplier);
        log.debug(response.toString());
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
    
    protected <T> T executePostMethod(String uriSuffix, Consumer<URIBuilder> uriCustomizer, Consumer<HttpPost> requestCustomizer,
                    IOFunction<T> resultConverter, Supplier<String> errorSupplier) throws URISyntaxException, IOException {
        URIBuilder builder = new URIBuilder();
        builder.setScheme(auditServiceScheme);
        if (useSrvDNS) {
            List<LookupResult> results = dnsSrvResolver.resolve(auditServiceHost);
            if (results != null && !results.isEmpty()) {
                LookupResult result = results.get(0);
                builder.setHost(result.host());
                builder.setPort(result.port());
                // Consul sends the hostname back in its own namespace. Although the A record is invluded in the
                // "ADDITIONAL SECTION". Spotify SRV doesn't translate, so we need to do the lookup manually.
                if (result.host().endsWith(".consul.")) {
                    Record[] newResults = new Lookup(result.host(), Type.A, DClass.IN).run();
                    if (newResults != null && newResults.length > 0) {
                        builder.setHost(newResults[0].rdataToString());
                    } else {
                        throw new IllegalArgumentException("Unable to resolve audit service host " + auditServiceHost + " -> " + result.host() + " -> ???");
                    }
                }
            } else {
                throw new IllegalArgumentException("Unable to resolve audit service host: " + auditServiceHost);
            }
        } else {
            builder.setHost(auditServiceHost);
            builder.setPort(auditServicePort);
        }
        builder.setPath(auditServiceURI + uriSuffix);
        uriCustomizer.accept(builder);
        HttpPost postRequest = new HttpPost(builder.build());
        requestCustomizer.accept(postRequest);
        return client.execute(postRequest, r -> {
            if (r.getStatusLine().getStatusCode() != 200) {
                throw new ClientProtocolException("Unable to " + errorSupplier.get() + ": " + r.getStatusLine() + " " + EntityUtils.toString(r.getEntity()));
            } else {
                return resultConverter.apply(r.getEntity());
            }
        });
    }
    
    @PostConstruct
    protected void init() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new GuavaModule());
        
        if (useSrvDNS) {
            if (srvDnsServers != null && !srvDnsServers.isEmpty()) {
                try {
                    ExtendedResolver resolver = new ExtendedResolver(srvDnsServers.toArray(new String[srvDnsServers.size()]));
                    resolver.setLoadBalance(true);
                    resolver.setPort(srvDnsPort);
                    Lookup.setDefaultResolver(resolver);
                } catch (UnknownHostException e) {
                    throw new IllegalArgumentException("Unable to resolve SRV DNS hosts: " + srvDnsServers + ": " + e.getMessage(), e);
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
            
            jwtTokenHandler = new JWTTokenHandler(certs[0], signingKey, 24, TimeUnit.HOURS, objectMapper);
            
            ArrayList<Header> defaultHeaders = new ArrayList<>();
            defaultHeaders.add(new BasicHeader(HttpHeaders.ACCEPT, ContentType.APPLICATION_JSON.getMimeType()));
            // If we're using HTTP, then add our cert in as a header so the authorization service knows who we are.
            if ("http".equals(auditServiceScheme)) {
                defaultHeaders.add(new BasicHeader("X-SSL-clientcert-subject", DnUtils.normalizeDN(certs[0].getSubjectX500Principal().getName())));
                defaultHeaders.add(new BasicHeader("X-SSL-clientcert-issuer", DnUtils.normalizeDN(certs[0].getIssuerX500Principal().getName())));
            }
            
            // @formatter:off
            client = HttpClients.custom()
                    .setSSLContext(ctx)
                    .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                    .setDefaultHeaders(defaultHeaders)
                    .setMaxConnTotal(maxConnections)
                    .setRetryHandler(new DatawaveRetryHandler(retryCount, unavailableRetryCount, unavailableRetryDelay, retryCounter))
                    .setServiceUnavailableRetryStrategy(new DatawaveUnavailableRetryStrategy(unavailableRetryCount, unavailableRetryDelay, retryCounter))
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
        try {
            client.close();
        } catch (IOException e) {
            log.warn("Exception while shutting down HttpClient: " + e.getMessage(), e);
        }
    }
    
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
    
    private interface IOFunction<T> {
        T apply(HttpEntity entity) throws IOException;
    }
}
