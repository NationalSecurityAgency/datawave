package datawave.security.authorization.remote;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.util.DnUtils;
import datawave.user.AuthorizationsListBase;
import datawave.user.DefaultAuthorizationsList;
import datawave.webservice.common.json.DefaultMapperDecorator;
import datawave.webservice.common.remote.TestJSSESecurityDomain;
import datawave.webservice.dictionary.data.DataDictionaryBase;
import datawave.webservice.dictionary.data.DescriptionBase;
import datawave.webservice.dictionary.data.FieldsBase;
import datawave.webservice.metadata.MetadataFieldBase;
import datawave.webservice.query.Query;
import datawave.webservice.query.cachedresults.CacheableQueryRow;
import datawave.webservice.query.result.EdgeQueryResponseBase;
import datawave.webservice.query.result.edge.EdgeBase;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.FacetsBase;
import datawave.webservice.query.result.event.FieldBase;
import datawave.webservice.query.result.event.FieldCardinalityBase;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.response.objects.KeyBase;
import datawave.webservice.result.EventQueryResponseBase;
import datawave.webservice.result.FacetQueryResponseBase;
import datawave.webservice.result.GenericResponse;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.wildfly.security.x500.cert.X509CertificateBuilder;

import javax.security.auth.x500.X500Principal;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.math.BigInteger;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;

public class RemoteUserOperationsImplHttpTest {

    private static final int keysize = 2048;

    private static final String commonName = "cn=www.test.us";
    private static final String alias = "tomcat";
    private static final char[] keyPass = "changeit".toCharArray();

    private X500Principal x500Principal;

    private static final int PORT = 0;

    private HttpServer server;

    private RemoteUserOperationsImpl remote;

    @Before
    public void setup() throws Exception {
        final ObjectMapper objectMapper = new DefaultMapperDecorator().decorate(new ObjectMapper());
        System.setProperty(DnUtils.SUBJECT_DN_PATTERN_PROPERTY, ".*ou=server.*");
        KeyPairGenerator generater = KeyPairGenerator.getInstance("RSA");
        generater.initialize(keysize);
        KeyPair keypair = generater.generateKeyPair();
        PrivateKey privKey = keypair.getPrivate();
        final X509Certificate[] chain = new X509Certificate[1];
        x500Principal = new X500Principal(commonName);
        final ZonedDateTime start = ZonedDateTime.now().minusWeeks(1);
        final ZonedDateTime until = start.plusYears(1);
        X509CertificateBuilder builder = new X509CertificateBuilder().setIssuerDn(x500Principal).setSerialNumber(new BigInteger(10, new SecureRandom()))
                        .setNotValidBefore(start).setNotValidAfter(until).setSubjectDn(x500Principal).setPublicKey(keypair.getPublic())
                        .setSigningKey(keypair.getPrivate()).setSignatureAlgorithmName("SHA256withRSA");
        chain[0] = builder.build();

        server = HttpServer.create(new InetSocketAddress(PORT), 0);
        server.setExecutor(null);
        server.start();

        DefaultAuthorizationsList listEffectiveAuthResponse = new DefaultAuthorizationsList();
        listEffectiveAuthResponse.setUserAuths("testuserDn", "testissuerDn", Arrays.asList("auth1", "auth2"));
        listEffectiveAuthResponse.setAuthMapping(new HashMap<>());

        HttpHandler listEffectiveAuthorizationsHandler = new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                String responseBody = objectMapper.writeValueAsString(listEffectiveAuthResponse);
                exchange.getResponseHeaders().add("Content-Type", MediaType.APPLICATION_JSON);
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, responseBody.length());
                IOUtils.write(responseBody, exchange.getResponseBody(), Charset.forName("UTF-8"));
                exchange.close();
            }
        };

        GenericResponse<String> flushResponse = new GenericResponse<>();
        flushResponse.setResult("test flush result");

        HttpHandler flushHandler = new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                String responseBody = objectMapper.writeValueAsString(flushResponse);
                exchange.getResponseHeaders().add("Content-Type", MediaType.APPLICATION_JSON);
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, responseBody.length());
                IOUtils.write(responseBody, exchange.getResponseBody(), Charset.forName("UTF-8"));
                exchange.close();
            }
        };

        server.createContext("/Security/User/listEffectiveAuthorizations", listEffectiveAuthorizationsHandler);
        server.createContext("/Security/User/flushCachedCredentials", flushHandler);

        // create a remote event query logic that has our own server behind it
        remote = new RemoteUserOperationsImpl();
        remote.setQueryServiceURI("/Security/User/");
        remote.setQueryServiceScheme("http");
        remote.setQueryServiceHost("localhost");
        remote.setQueryServicePort(server.getAddress().getPort());
        remote.setExecutorService(null);
        remote.setObjectMapperDecorator(new DefaultMapperDecorator());
        remote.setResponseObjectFactory(new MockResponseObjectFactory());
        remote.setJsseSecurityDomain(new TestJSSESecurityDomain(alias, privKey, keyPass, chain));
    }

    @After
    public void after() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    public void testRemoteUserOperations() throws Exception {
        DatawavePrincipal principal = new DatawavePrincipal(commonName);

        AuthorizationsListBase auths = remote.listEffectiveAuthorizations(principal);
        assertEquals(2, auths.getAllAuths().size());

        GenericResponse flush = remote.flushCachedCredentials(principal);
        assertEquals("test flush result", flush.getResult());
    }

    public static class MockResponseObjectFactory extends ResponseObjectFactory {

        @Override
        public EventBase getEvent() {
            return null;
        }

        @Override
        public FieldBase getField() {
            return null;
        }

        @Override
        public EventQueryResponseBase getEventQueryResponse() {
            return null;
        }

        @Override
        public CacheableQueryRow getCacheableQueryRow() {
            return null;
        }

        @Override
        public EdgeBase getEdge() {
            return null;
        }

        @Override
        public EdgeQueryResponseBase getEdgeQueryResponse() {
            return null;
        }

        @Override
        public FacetQueryResponseBase getFacetQueryResponse() {
            return null;
        }

        @Override
        public FacetsBase getFacets() {
            return null;
        }

        @Override
        public FieldCardinalityBase getFieldCardinality() {
            return null;
        }

        @Override
        public KeyBase getKey() {
            return null;
        }

        @Override
        public AuthorizationsListBase getAuthorizationsList() {
            return new DefaultAuthorizationsList();
        }

        @Override
        public Query getQueryImpl() {
            return null;
        }

        @Override
        public DataDictionaryBase getDataDictionary() {
            return null;
        }

        @Override
        public FieldsBase getFields() {
            return null;
        }

        @Override
        public DescriptionBase getDescription() {
            return null;
        }

        @Override
        public MetadataFieldBase getMetadataField() {
            return null;
        }
    }

}
