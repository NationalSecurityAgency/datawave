package datawave.webservice.common.remote;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.URI;
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
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import javax.enterprise.concurrent.ManagedExecutorService;
import javax.inject.Inject;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.X509KeyManager;

import org.apache.commons.collections4.ListUtils;
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

import com.codahale.metrics.Counter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.spotify.dns.DnsSrvResolver;
import com.spotify.dns.DnsSrvResolvers;
import com.spotify.dns.LookupResult;

import datawave.security.authorization.AuthorizationException;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.JWTTokenHandler;
import datawave.security.authorization.JWTTokenHandler.TtlMode;
import datawave.security.util.DnUtils;
import datawave.webservice.common.exception.DatawaveWebApplicationException;
import datawave.webservice.common.json.ObjectMapperDecorator;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.exception.QueryExceptionType;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.result.VoidResponse;

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

    @Inject
    protected ObjectMapperDecorator objectMapperDecorator;

    protected ResponseObjectFactory responseObjectFactory;

    protected ObjectReader voidResponseReader;

    private RemoteHttpServiceConfiguration config = new RemoteHttpServiceConfiguration();

    public void setJsseSecurityDomain(JSSESecurityDomain jsseSecurityDomain) {
        this.jsseSecurityDomain = jsseSecurityDomain;
    }

    public void setExecutorService(ManagedExecutorService executorService) {
        this.executorService = executorService;
    }

    public void setObjectMapperDecorator(ObjectMapperDecorator objectMapperDecorator) {
        this.objectMapperDecorator = objectMapperDecorator;
    }

    protected <T> T execute(HttpRequestBase request, IOFunction<T> resultConverter, Supplier<String> errorSupplier) throws IOException {
        if (log.isTraceEnabled()) {
            log.trace("Executing " + request.getClass().getSimpleName() + " against " + request.getURI());
        }
        try {
            activeExecutions.incrementAndGet();
            return client.execute(request, r -> {
                if (r.getStatusLine().getStatusCode() >= 300) {
                    throw new ClientProtocolException(
                                    "Unable to " + errorSupplier.get() + ": " + r.getStatusLine() + " " + EntityUtils.toString(r.getEntity()));
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
        log.info("Starting up RemoteHttpService " + System.identityHashCode(this));
        objectMapper = objectMapperDecorator.decorate(new ObjectMapper());
        voidResponseReader = objectMapper.readerFor(VoidResponse.class);

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

            List<Class<? extends IOException>> nonRetriableClasses = getNonRetriableClasses();
            List<Class<? extends IOException>> unavailableRetryClasses = getUnavailableRetryClasses();
            DefaultHttpRequestRetryHandler datawaveRetryHandler = new DatawaveRetryHandler(retryCount(), unavailableRetryCount(), unavailableRetryDelay(),
                            retryCounter(), nonRetriableClasses, unavailableRetryClasses);

            // @formatter:off
            client = HttpClients.custom()
                    .setSSLContext(ctx)
                    .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                    .setDefaultHeaders(defaultHeaders)
                    .setMaxConnTotal(maxConnections())
                    .setMaxConnPerRoute(maxConnections())
                    .setRetryHandler(datawaveRetryHandler)
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
        log.info("Started up RemoteHttpService " + System.identityHashCode(this));
    }

    @PreDestroy
    protected void shutdown() {
        log.info("Shutting down RemoteHttpService " + System.identityHashCode(this));
        executorService.submit(() -> {
            log.info("Executing shutdown RemoteHttpService " + System.identityHashCode(RemoteHttpService.this));
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
            log.info("Shutdown RemoteHttpService " + System.identityHashCode(RemoteHttpService.this));
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

    public URIBuilder buildRedirectURI(String suffix, URI baseURI, boolean useConfiguredBaseURI) throws TextParseException {
        URIBuilder builder;
        if (useConfiguredBaseURI) {
            builder = buildURI();
        } else {
            builder = new URIBuilder(baseURI);
        }
        builder.setPath(serviceURI() + suffix);
        return builder;
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

    protected <T> T executePostMethod(String uriSuffix, Consumer<URIBuilder> uriCustomizer, Consumer<HttpPost> requestCustomizer, IOFunction<T> resultConverter,
                    Supplier<String> errorSupplier) throws URISyntaxException, IOException {
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
                // If we got a void response here, then there was an underlying error. Throw this error up with the
                // void response messages. If it was not a void response, then an IOException will
                // have been thrown... again.
                if (!ListUtils.emptyIfNull(response.getExceptions()).isEmpty()) {
                    throw new DatawaveWebApplicationException(getException(response.getExceptions().get(0)), response);
                } else {
                    throw new DatawaveWebApplicationException(new RuntimeException(response.getMessages().toString()), response);
                }
            }
        }
    }

    public static Exception getException(QueryExceptionType qet) {
        if (qet.getCode() != null) {
            if (qet.getCause() != null) {
                return new QueryException(qet.getMessage(), new RuntimeException(qet.getCause()), qet.getCode());
            } else {
                return new QueryException(qet.getMessage(), qet.getCode());
            }
        } else {
            return new RuntimeException(qet.getMessage());
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
                    throw new RuntimeException(String.valueOf(response.getMessages()), ioe1);
                }
            }
        }
    }

    public String getContent(InputStream content) throws IOException {
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
            return response;
        }
    }

    public <T> T executePostMethodWithRuntimeException(String uriSuffix, Consumer<URIBuilder> uriCustomizer, Consumer<HttpPost> requestCustomizer,
                    IOFunction<T> resultConverter, Supplier<String> errorSupplier) {
        try {
            return executePostMethod(uriSuffix, uriCustomizer, requestCustomizer, resultConverter, errorSupplier);
        } catch (URISyntaxException e) {
            throw new RuntimeException("Invalid URI: " + e.getMessage(), e);
        } catch (IOException e) {
            failureCounter().inc();
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public <T> T executePutMethodWithRuntimeException(String uriSuffix, Consumer<URIBuilder> uriCustomizer, Consumer<HttpPut> requestCustomizer,
                    IOFunction<T> resultConverter, Supplier<String> errorSupplier) {
        try {
            return executePutMethod(uriSuffix, uriCustomizer, requestCustomizer, resultConverter, errorSupplier);
        } catch (URISyntaxException e) {
            throw new RuntimeException("Invalid URI: " + e.getMessage(), e);
        } catch (IOException e) {
            failureCounter().inc();
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public <T> T executeGetMethodWithRuntimeException(String uriSuffix, Consumer<URIBuilder> uriCustomizer, Consumer<HttpGet> requestCustomizer,
                    IOFunction<T> resultConverter, Supplier<String> errorSupplier) {
        try {
            return executeGetMethod(uriSuffix, uriCustomizer, requestCustomizer, resultConverter, errorSupplier);
        } catch (URISyntaxException e) {
            throw new RuntimeException("Invalid URI: " + e.getMessage(), e);
        } catch (IOException e) {
            failureCounter().inc();
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    protected <T> T executeGetMethodWithAuthorizationException(String uriSuffix, Consumer<URIBuilder> uriCustomizer, Consumer<HttpGet> requestCustomizer,
                    IOFunction<T> resultConverter, Supplier<String> errorSupplier) throws AuthorizationException {
        try {
            return executeGetMethod(uriSuffix, uriCustomizer, requestCustomizer, resultConverter, errorSupplier);
        } catch (URISyntaxException e) {
            throw new AuthorizationException("Invalid URI: " + e.getMessage(), e);
        } catch (IOException e) {
            failureCounter().inc();
            throw new AuthorizationException(e.getMessage(), e);
        }
    }

    /**
     * Useful for setting proxied entities header
     *
     * @param callerPrincipal
     *            the caller principal
     * @return proxied entities
     */
    public static String getProxiedEntities(DatawavePrincipal callerPrincipal) {
        return callerPrincipal.getProxiedUsers().stream().map(u -> new StringBuilder().append('<').append(u.getDn().subjectDN()).append('>'))
                        .collect(Collectors.joining());
    }

    /**
     * Useful for setting proxied issuers header
     *
     * @param callerPrincipal
     *            the caller principal
     * @return proxied entities
     */
    public static String getProxiedIssuers(DatawavePrincipal callerPrincipal) {
        return callerPrincipal.getProxiedUsers().stream().map(u -> new StringBuilder().append('<').append(u.getDn().issuerDN()).append('>'))
                        .collect(Collectors.joining());
    }

    protected String serviceHost() {
        return config.getServiceHost();
    }

    protected int servicePort() {
        return config.getServicePort();
    }

    protected String serviceURI() {
        return config.getServiceURI();
    }

    protected boolean useSrvDns() {
        return config.isUseSrvDNS();
    }

    protected List<String> srvDnsServers() {
        return config.getSrvDnsServers();
    }

    protected int srvDnsPort() {
        return config.getSrvDnsPort();
    }

    protected String serviceScheme() {
        return config.getServiceScheme();
    }

    protected int maxConnections() {
        return config.getMaxConnections();
    }

    protected int retryCount() {
        return config.getRetryCount();
    }

    protected int unavailableRetryCount() {
        return config.getUnavailableRetryCount();
    }

    protected int unavailableRetryDelay() {
        return config.getUnavailableRetryDelay();
    }

    protected Counter retryCounter() {
        return config.getRetryCounter();
    }

    protected Counter failureCounter() {
        return config.getFailureCounter();
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

    /**
     * Classes that are instances of IOException that will cause DatawaveRetryHandler to retry with delay. Subclasses of RemoteHttpService should override this
     * method if necessary
     */
    protected List<Class<? extends IOException>> getUnavailableRetryClasses() {
        return Arrays.asList(ConnectException.class);
    }

    /**
     * Classes that are instances of IOException that should not cause a retry. Subclasses of RemoteHttpService should override this method if necessary. The
     * default list of classes in DefaultHttpRequestRetryHandler is: InterruptedIOException.class, UnknownHostException.class, ConnectException.class,
     * SSLException.class));
     */
    protected List<Class<? extends IOException>> getNonRetriableClasses() {
        return Arrays.asList(UnknownHostException.class, SSLException.class);
    }

    private static class DatawaveRetryHandler extends DefaultHttpRequestRetryHandler {
        private static final Logger log = LoggerFactory.getLogger(DatawaveRetryHandler.class);
        private final int unavailableRetryCount;
        private final int unavailableRetryDelay;
        private final Counter retryCounter;
        private List<Class<? extends IOException>> unavailableRetryClasses;

        public DatawaveRetryHandler(int retryCount, int unavailableRetryCount, int unavailableRetryDelay, Counter retryCounter,
                        List<Class<? extends IOException>> nonRetriableClasses, List<Class<? extends IOException>> unavailableRetryClasses) {
            super(retryCount, false, nonRetriableClasses);
            this.unavailableRetryCount = unavailableRetryCount;
            this.unavailableRetryDelay = unavailableRetryDelay;
            this.retryCounter = retryCounter;
            this.unavailableRetryClasses = unavailableRetryClasses;
        }

        @Override
        public boolean retryRequest(IOException exception, int executionCount, HttpContext context) {
            boolean shouldRetry = super.retryRequest(exception, executionCount, context);
            // if any class e is the same as exception or any class e is a superclass of exception then retryWithDelay
            boolean retryWithDelay = unavailableRetryClasses.stream().anyMatch(e -> e.isAssignableFrom(exception.getClass()));
            if (retryWithDelay) {
                shouldRetry = (executionCount <= unavailableRetryCount);
                if (shouldRetry) {
                    try {
                        if (log.isTraceEnabled()) {
                            log.trace("retrying call after exception {}, executionCount {}, sleeping for {}ms", exception.getClass().getName(), executionCount,
                                            unavailableRetryDelay);
                        }
                        Thread.sleep(unavailableRetryDelay);
                    } catch (InterruptedException e) {
                        // Ignore -- we'll just end up retrying a little too fast
                    }
                }
            } else {
                if (log.isTraceEnabled()) {
                    log.trace("retrying call after exception {}, executionCount {}", exception.getClass().getName(), executionCount);
                }
            }
            if (shouldRetry) {
                retryCounter.inc();
            }
            return shouldRetry;
        }
    }

    private static class DatawaveUnavailableRetryStrategy extends DefaultServiceUnavailableRetryStrategy {
        private static final Logger log = LoggerFactory.getLogger(DatawaveUnavailableRetryStrategy.class);
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
            int statusCode = response.getStatusLine().getStatusCode();
            boolean shouldRetry = executionCount <= maxRetries && (statusCode == HttpStatus.SC_SERVICE_UNAVAILABLE || statusCode == HttpStatus.SC_NOT_FOUND);
            if (shouldRetry) {
                retryCounter.inc();
                if (log.isTraceEnabled()) {
                    log.trace("retrying call after statusCode {}, executionCount {}", statusCode, executionCount);
                }
            }
            return shouldRetry;
        }
    }

    protected interface IOFunction<T> {
        T apply(HttpEntity entity) throws IOException;
    }
}
