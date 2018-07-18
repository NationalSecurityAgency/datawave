package datawave.webservice.common.remote;

import com.codahale.metrics.Counter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.spotify.dns.DnsSrvResolver;
import com.spotify.dns.DnsSrvResolvers;
import datawave.security.authorization.JWTTokenHandler;
import datawave.security.util.DnUtils;
import org.apache.http.*;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpRequestBase;
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
import org.xbill.DNS.ExtendedResolver;
import org.xbill.DNS.Lookup;

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
import java.util.function.Supplier;

/**
 * A base class for services that need to use HTTPClient to make remote calls to a microservice.
 */
abstract public class RemoteHttpService {
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
        objectMapper.registerModule(new GuavaModule());
        
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
            
            jwtTokenHandler = new JWTTokenHandler(certs[0], signingKey, 24, TimeUnit.HOURS, objectMapper);
            
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
    
    abstract protected boolean useSrvDns();
    
    abstract protected List<String> srvDnsServers();
    
    abstract protected int srvDnsPort();
    
    abstract protected String serviceScheme();
    
    abstract protected int maxConnections();
    
    abstract protected int retryCount();
    
    abstract protected int unavailableRetryCount();
    
    abstract protected int unavailableRetryDelay();
    
    abstract protected Counter retryCounter();
    
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
