package datawave.microservice.annotation.util.lookup.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;

import datawave.microservice.annotation.util.lookup.config.LookupProperties;

/**
 * Regression test for T11: LookupRequest previously held a single, field-level BasicCookieStore that was reused across every lookupId() invocation for every
 * caller/principal, since LookupRequest is a shared singleton bean. This is a real cross-principal cookie leakage vector: a session cookie set by the
 * downstream DataWave query service in response to one caller's request could be resent on a completely unrelated caller's subsequent lookup.
 * <p>
 * This test drives two real HTTPS round trips (via a local, in-process HttpsServer) through the exact same LookupRequest instance, using the same TLS trust
 * material as the rest of the module's test suite (ssl/host.p12 as the server's identity, ssl/rootCA.p12 as the trusted CA). The first response sets a
 * Set-Cookie header; the test then asserts that the second, independent lookupId() call does NOT present that cookie back to the server - proving that
 * LookupRequest.getHttpContext() creates a fresh, request-scoped cookie store per call rather than retaining state across calls.
 */
class LookupRequestCookieIsolationTest {

    private static final String KEY_STORE_PASSWORD = "LetMeIn";

    private HttpsServer server;
    private final Map<Integer,String> cookieHeaderSeenByRequestNumber = new HashMap<>();
    private final AtomicReference<Integer> requestCounter = new AtomicReference<>(0);

    @BeforeEach
    void startServer() throws Exception {
        SSLContext serverSslContext = SSLContext.getInstance("TLS");
        KeyStore serverKeyStore = KeyStore.getInstance("PKCS12");
        try (InputStream is = getClass().getClassLoader().getResourceAsStream("ssl/host.p12")) {
            serverKeyStore.load(is, KEY_STORE_PASSWORD.toCharArray());
        }
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(serverKeyStore, KEY_STORE_PASSWORD.toCharArray());
        serverSslContext.init(kmf.getKeyManagers(), null, null);

        server = HttpsServer.create(new InetSocketAddress("localhost", 0), 0);
        server.setHttpsConfigurator(new HttpsConfigurator(serverSslContext));
        server.createContext("/DataWave/Annotations/v1/", exchange -> {
            int requestNumber = requestCounter.updateAndGet(n -> n + 1);
            cookieHeaderSeenByRequestNumber.put(requestNumber, exchange.getRequestHeaders().getFirst("Cookie"));

            byte[] body = "{}".getBytes(StandardCharsets.UTF_8);
            if (requestNumber == 1) {
                // Simulate the downstream DataWave query service setting a session cookie on the first caller's response
                exchange.getResponseHeaders().add("Set-Cookie", "JSESSIONID=principal-A-session; Path=/");
            }
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        });
        server.start();
    }

    @AfterEach
    void stopServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void secondLookupDoesNotReceiveCookieSetDuringFirstLookup() throws Exception {
        SSLContext clientSslContext = SSLContext.getInstance("TLS");
        KeyStore trustStore = KeyStore.getInstance("PKCS12");
        try (InputStream is = getClass().getClassLoader().getResourceAsStream("ssl/rootCA.p12")) {
            trustStore.load(is, KEY_STORE_PASSWORD.toCharArray());
        }
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(trustStore);
        clientSslContext.init(null, tmf.getTrustManagers(), null);

        // Mirror HttpClientConfig's production wiring (NoopHostnameVerifier is the intentional, discovery-compatible TLS policy - see
        // HttpClientConfigTlsPolicyTest for a dedicated assertion of that behavior). Only the trust material differs here (test CA vs. production truststore).
        CloseableHttpClient httpClient = HttpClientBuilder.create()
                        .setSSLSocketFactory(new SSLConnectionSocketFactory(clientSslContext, NoopHostnameVerifier.INSTANCE)).build();

        LookupProperties lookupProperties = new LookupProperties();
        lookupProperties.setDatawaveQueryHost("localhost:" + server.getAddress().getPort());

        LookupRequest lookupRequest = new LookupRequest(httpClient, lookupProperties);

        // First lookup: server sets a session cookie in the response
        ParsedResponse first = lookupRequest.lookupId(Lookup.MARKINGS, "1234", Collections.emptyMap(), "params", "test");
        assertEquals(200, first.getCode());
        assertNull(cookieHeaderSeenByRequestNumber.get(1), "the first request should not have sent any cookie");

        // Second lookup on the SAME LookupRequest singleton instance: must not carry the cookie set during the first call
        ParsedResponse second = lookupRequest.lookupId(Lookup.MARKINGS, "5678", Collections.emptyMap(), "params", "test");
        assertEquals(200, second.getCode());
        assertNotNull(cookieHeaderSeenByRequestNumber.containsKey(2));
        assertNull(cookieHeaderSeenByRequestNumber.get(2), "cookie set during a prior lookup must not be retained/resent on a later, independent lookup call");
    }
}
