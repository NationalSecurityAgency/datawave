package datawave.microservice.annotation.util.lookup.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.TrustManagerFactory;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;

/**
 * Regression/documentation test for T11 (Q06, Option A): the annotation service's outbound HttpClient (wired up in {@link HttpClientConfig}) intentionally
 * registers {@link org.apache.http.conn.ssl.NoopHostnameVerifier} for its "https" scheme, so that it can reach a downstream DataWave query host whose
 * certificate's CN/SAN may not match the configured discovery hostname (a "discovery-compatible" TLS policy). This is a deliberate, retained decision - not a
 * defect - but it was previously untested and undocumented in code. This test proves the policy is in effect by presenting a server certificate (ssl/host.p12,
 * CN=test.testcorp.com) while connecting to a mismatched hostname ("localhost"), and asserting the handshake still succeeds because hostname verification is
 * skipped. As a control, it also proves that a client configured with normal (non-Noop) hostname verification would reject the exact same mismatched
 * connection, confirming the mismatch is real and the Noop verifier is doing meaningful work.
 */
class HttpClientConfigTlsPolicyTest {

    private static final String KEY_STORE_PASSWORD = "LetMeIn";

    private HttpsServer server;

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

        // The server's certificate (ssl/host.p12) has CN=test.testcorp.com, which will NOT match the "localhost" hostname used to connect below.
        server = HttpsServer.create(new InetSocketAddress("localhost", 0), 0);
        server.setHttpsConfigurator(new HttpsConfigurator(serverSslContext));
        server.createContext("/", exchange -> {
            byte[] body = "ok".getBytes(StandardCharsets.UTF_8);
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

    private SSLContext trustingClientSslContext() throws Exception {
        SSLContext clientSslContext = SSLContext.getInstance("TLS");
        KeyStore trustStore = KeyStore.getInstance("PKCS12");
        try (InputStream is = getClass().getClassLoader().getResourceAsStream("ssl/rootCA.p12")) {
            trustStore.load(is, KEY_STORE_PASSWORD.toCharArray());
        }
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(trustStore);
        clientSslContext.init(null, tmf.getTrustManagers(), null);
        return clientSslContext;
    }

    @Test
    void productionHttpClientAcceptsMismatchedHostnameByDesign() throws Exception {
        // Exercise the actual production bean-building code, exactly as Spring would.
        CloseableHttpClient httpClient = new HttpClientConfig().httpClient(trustingClientSslContext());

        HttpGet get = new HttpGet("https://localhost:" + server.getAddress().getPort() + "/");
        try (CloseableHttpResponse response = httpClient.execute(get)) {
            assertEquals(200, response.getStatusLine().getStatusCode(),
                            "the production HttpClient must accept a certificate whose CN doesn't match the connected hostname, "
                                            + "per the intentional discovery-compatible TLS policy (Q06, Option A)");
        }
    }

    @Test
    void hostnameMismatchIsRealAndWouldBeRejectedByDefaultVerification() throws Exception {
        // Control: build a client identical to the production one, EXCEPT using default (non-Noop) hostname verification, to prove the mismatch
        // above is genuine and that NoopHostnameVerifier is the reason the production client tolerates it.
        CloseableHttpClient strictHttpClient = HttpClientBuilder.create().setSSLContext(trustingClientSslContext()).build();

        HttpGet get = new HttpGet("https://localhost:" + server.getAddress().getPort() + "/");
        try (CloseableHttpResponse ignored = strictHttpClient.execute(get)) {
            fail("expected hostname verification to reject a certificate whose CN doesn't match the connected hostname");
        } catch (SSLHandshakeException | javax.net.ssl.SSLPeerUnverifiedException expected) {
            // expected: default hostname verification rejects the CN=test.testcorp.com certificate for host "localhost"
        }
    }
}
