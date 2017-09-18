package datawave.security.authorization.remote;

import com.codahale.metrics.Counter;
import com.codahale.metrics.annotation.Metric;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import datawave.configuration.RefreshableScope;
import datawave.security.authorization.AuthorizationException;
import datawave.security.authorization.CachedDatawaveUserService;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.webservice.security.JWTTokenHandler;
import org.apache.deltaspike.core.api.config.ConfigProperty;
import org.apache.deltaspike.core.api.exclude.Exclude;
import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.jboss.security.JSSESecurityDomain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Priority;
import javax.enterprise.inject.Alternative;
import javax.inject.Inject;
import javax.interceptor.Interceptor;
import javax.net.ssl.SSLContext;
import javax.net.ssl.X509KeyManager;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.Key;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * A {@link CachedDatawaveUserService} that delegates all methods to a remote authorization microservice.
 */
@RefreshableScope
@Alternative
// Make this alternative active for the entire application per the CDI 1.2 specification
@Priority(Interceptor.Priority.APPLICATION)
@Exclude(onExpression = "dw.security.use.remoteuserservice!=true")
public class RemoteDatawaveUserService implements CachedDatawaveUserService {
    private final Logger log = LoggerFactory.getLogger(getClass());
    
    private ObjectReader datawaveUserReader;
    private ObjectReader datawaveUserListReader;
    private CloseableHttpClient client;
    private JWTTokenHandler jwtTokenHandler;
    
    @Inject
    private JSSESecurityDomain jsseSecurityDomain;
    
    @Inject
    @ConfigProperty(name = "dw.remoteDatawaveUserService.uri", defaultValue = "https://localhost:8543/datawave/")
    private String authServiceURI;
    
    @Inject
    @ConfigProperty(name = "dw.remoteDatawaveUserService.maxConnections", defaultValue = "100")
    private int maxConnections;
    
    @Inject
    @ConfigProperty(name = "dw.remoteDatawaveUserService.retryCount", defaultValue = "3")
    private int retryCount;
    
    @Inject
    @Metric(name = "dw.remoteDatawaveUserService.retries", absolute = true)
    private Counter retryCounter;
    
    @Inject
    @Metric(name = "dw.remoteDatawaveUserService.failures", absolute = true)
    private Counter failureCounter;
    
    @Override
    @Timed(name = "dw.remoteDatawaveUserService.lookup", absolute = true)
    public Collection<DatawaveUser> lookup(Collection<SubjectIssuerDNPair> dns) throws AuthorizationException {
        final String enttiesHeader = "<" + dns.stream().map(SubjectIssuerDNPair::subjectDN).collect(Collectors.joining("><")) + ">";
        final String issuersHeader = "<" + dns.stream().map(SubjectIssuerDNPair::issuerDN).collect(Collectors.joining("><")) + ">";
        // @formatter:off
        String jwtString = executeGetMethodWithAuthorizationException("authorize",
                uriBuilder -> encodeDNSparams(uriBuilder, dns),
                httpGet -> {
                    httpGet.setHeader("X-ProxiedEntitiesChain", enttiesHeader);
                    httpGet.setHeader("X-ProxiedIssuersChain", issuersHeader);
                },
                EntityUtils::toString,
                () -> "lookup " + dns);
        // @formatter:on
        return jwtTokenHandler.createUsersFromToken(jwtString);
    }
    
    @Override
    @Timed(name = "dw.remoteDatawaveUserService.reload", absolute = true)
    public Collection<DatawaveUser> reload(Collection<SubjectIssuerDNPair> dns) throws AuthorizationException {
        // @formatter:off
        return executeGetMethodWithAuthorizationException("admin/reloadUsers",
                uriBuilder -> encodeDNSparams(uriBuilder, dns),
                httpGet -> {},
                entity -> datawaveUserListReader.readValue(entity.getContent()),
                () -> "reload " + dns);
        // @formatter:on
    }
    
    private static void encodeDNSparams(URIBuilder uriBuilder, Collection<SubjectIssuerDNPair> dns) {
        // We need to base64 encode each parameter as a work-around since DNs contain
        // commas, which are used as a separator for a multi-valued parameter.
        // @formatter:off
        Base64.Encoder encoder = Base64.getEncoder();
        dns.stream()
                .map(SubjectIssuerDNPair::toString)
                .map(s -> encoder.encodeToString(s.getBytes()))
                .forEach(s -> uriBuilder.addParameter("dns", s));
        // @formatter:on
    }
    
    @Override
    @Timed(name = "dw.remoteDatawaveUserService.list", absolute = true)
    public DatawaveUser list(String name) {
        // @formatter:off
        return executeGetMethodWithRuntimeException("admin/listUser",
                uriBuilder -> uriBuilder.addParameter("name", name),
                entity -> datawaveUserReader.readValue(entity.getContent()),
                () -> "list");
        // @formatter:on
    }
    
    @Override
    @Timed(name = "dw.remoteDatawaveUserService.listAll", absolute = true)
    public Collection<? extends DatawaveUser> listAll() {
        // @formatter:off
        return executeGetMethodWithRuntimeException("admin/listUsers",
                uriBuilder -> {
                },
                entity -> datawaveUserListReader.readValue(entity.getContent()),
                () -> "list all users");
        // @formatter:on
    }
    
    @Override
    @Timed(name = "dw.remoteDatawaveUserService.listMatching", absolute = true)
    public Collection<? extends DatawaveUser> listMatching(String substring) {
        // @formatter:off
        return executeGetMethodWithRuntimeException("admin/listUsersMatching",
                uriBuilder -> uriBuilder.addParameter("substring", substring),
                entity -> datawaveUserListReader.readValue(entity.getContent()),
                () -> "list all users matching " + substring);
        // @formatter:on
    }
    
    @Override
    @Timed(name = "dw.remoteDatawaveUserService.evict", absolute = true)
    public String evict(String name) {
        // @formatter:off
        return executeGetMethodWithRuntimeException("admin/evictUser",
                uriBuilder -> uriBuilder.addParameter("name", name),
                EntityUtils::toString,
                () -> "evict " + name);
        // @formatter:on
    }
    
    @Override
    @Timed(name = "dw.remoteDatawaveUserService.evictMatching", absolute = true)
    public String evictMatching(String substring) {
        // @formatter:off
        return executeGetMethodWithRuntimeException("admin/evictUsersMatching",
                uriBuilder -> uriBuilder.addParameter("substring", substring),
                EntityUtils::toString,
                () -> "evict users matching " + substring);
        // @formatter:on
    }
    
    @Override
    @Timed(name = "dw.remoteDatawaveUserService.evictAll", absolute = true)
    public String evictAll() {
        // @formatter:off
        return executeGetMethodWithRuntimeException("admin/evictUsersMatching",
                b -> {
                },
                EntityUtils::toString,
                () -> "evict all users");
        // @formatter:on
    }
    
    protected <T> T executeGetMethodWithRuntimeException(String uriSuffix, Consumer<URIBuilder> params, IOFunction<T> resultConverter, Supplier<String> errorStr) {
        try {
            return executeGetMethod(uriSuffix, params, resultConverter, errorStr);
        } catch (URISyntaxException e) {
            throw new RuntimeException("Invalid URI: " + e.getMessage(), e);
        } catch (IOException e) {
            failureCounter.inc();
            throw new RuntimeException(e.getMessage(), e);
        }
    }
    
    protected <T> T executeGetMethodWithAuthorizationException(String uriSuffix, Consumer<URIBuilder> params, Consumer<HttpGet> requestCustomizer,
                    IOFunction<T> resultConverter, Supplier<String> errorStr) throws AuthorizationException {
        try {
            return executeGetMethod(uriSuffix, params, resultConverter, errorStr);
        } catch (URISyntaxException e) {
            throw new AuthorizationException("Invalid URI: " + e.getMessage(), e);
        } catch (IOException e) {
            failureCounter.inc();
            throw new AuthorizationException(e.getMessage(), e);
        }
    }
    
    protected <T> T executeGetMethod(String uriSuffix, Consumer<URIBuilder> uriCustomizer, IOFunction<T> resultConverter, Supplier<String> errorSupplier)
                    throws URISyntaxException, IOException {
        return executeGetMethod(uriSuffix, uriCustomizer, httpGet -> {}, resultConverter, errorSupplier);
    }
    
    protected <T> T executeGetMethod(String uriSuffix, Consumer<URIBuilder> uriCustomizer, Consumer<HttpGet> requestCustomizer, IOFunction<T> resultConverter,
                    Supplier<String> errorSupplier) throws URISyntaxException, IOException {
        URIBuilder builder = new URIBuilder(authServiceURI + uriSuffix);
        uriCustomizer.accept(builder);
        HttpGet getRequest = new HttpGet(builder.build());
        requestCustomizer.accept(getRequest);
        return client.execute(getRequest, r -> {
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
        datawaveUserReader = objectMapper.readerFor(DatawaveUser.class);
        datawaveUserListReader = objectMapper.readerFor(objectMapper.getTypeFactory().constructCollectionType(Collection.class, DatawaveUser.class));
        
        try {
            SSLContext ctx = SSLContext.getInstance("TLSv1.2");
            ctx.init(jsseSecurityDomain.getKeyManagers(), jsseSecurityDomain.getTrustManagers(), null);
            
            String alias = jsseSecurityDomain.getKeyStore().aliases().nextElement();
            X509KeyManager keyManager = (X509KeyManager) jsseSecurityDomain.getKeyManagers()[0];
            X509Certificate[] certs = keyManager.getCertificateChain(alias);
            Key signingKey = keyManager.getPrivateKey(alias);
            
            jwtTokenHandler = new JWTTokenHandler(certs[0], signingKey, 24, TimeUnit.HOURS, objectMapper);
            
            // @formatter:off
            client = HttpClients.custom()
                    .setSSLContext(ctx)
                    .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                    .setMaxConnTotal(maxConnections)
                    .setRetryHandler(new RemoteAuthRetryHandler(retryCount, false, retryCounter))
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
    
    private static class RemoteAuthRetryHandler extends DefaultHttpRequestRetryHandler {
        private Counter retryCounter;
        
        public RemoteAuthRetryHandler(int retryCount, boolean requestSentRetryEnabled, Counter retryCounter) {
            super(retryCount, requestSentRetryEnabled);
            this.retryCounter = retryCounter;
        }
        
        @Override
        public boolean retryRequest(IOException exception, int executionCount, HttpContext context) {
            boolean shouldRetry = super.retryRequest(exception, executionCount, context);
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
