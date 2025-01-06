package datawave.security.authorization.remote;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.io.IOException;
import java.math.BigInteger;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.enterprise.concurrent.ManagedExecutorService;
import javax.security.auth.x500.X500Principal;
import javax.ws.rs.core.MediaType;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.io.IOUtils;
import org.jboss.security.JSSESecurityDomain;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.concurrent.ConcurrentMapCache;
import org.springframework.cache.support.SimpleCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.wildfly.security.x500.cert.X509CertificateBuilder;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import datawave.microservice.query.Query;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.ProxiedUserDetails;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.authorization.UserOperations;
import datawave.security.util.DnUtils;
import datawave.user.AuthorizationsListBase;
import datawave.user.DefaultAuthorizationsList;
import datawave.webservice.common.json.DefaultMapperDecorator;
import datawave.webservice.common.json.ObjectMapperDecorator;
import datawave.webservice.common.remote.TestJSSESecurityDomain;
import datawave.webservice.dictionary.data.DataDictionaryBase;
import datawave.webservice.dictionary.data.DescriptionBase;
import datawave.webservice.dictionary.data.FieldsBase;
import datawave.webservice.metadata.MetadataFieldBase;
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

@RunWith(SpringRunner.class)
@ContextConfiguration
public class RemoteUserOperationsImplHttpTest {

    @EnableCaching
    @Configuration
    static class Config {

        @Bean
        public CacheManager remoteOperationsCacheManager() {
            SimpleCacheManager cacheManager = new SimpleCacheManager();
            List<Cache> caches = new ArrayList<Cache>();
            caches.add(new ConcurrentMapCache("listEffectiveAuthorizations"));
            caches.add(new ConcurrentMapCache("getRemoteUser"));
            cacheManager.setCaches(caches);
            return cacheManager;
        }

        @Bean
        public ObjectMapperDecorator objectMapperDecorator() {
            return new DefaultMapperDecorator();
        }

        @Bean
        public ManagedExecutorService executorService() {
            return Mockito.mock(ManagedExecutorService.class);
        }

        @Bean
        public JSSESecurityDomain jsseSecurityDomain() throws CertificateException, NoSuchAlgorithmException {
            String alias = "tomcat";
            char[] keyPass = "changeit".toCharArray();
            int keysize = 2048;
            String commonName = "cn=www.test.us";

            KeyPairGenerator generater = KeyPairGenerator.getInstance("RSA");
            generater.initialize(keysize);
            KeyPair keypair = generater.generateKeyPair();
            PrivateKey privKey = keypair.getPrivate();
            final X509Certificate[] chain = new X509Certificate[1];
            X500Principal x500Principal = new X500Principal(commonName);
            final ZonedDateTime start = ZonedDateTime.now().minusWeeks(1);
            final ZonedDateTime until = start.plusYears(1);
            X509CertificateBuilder builder = new X509CertificateBuilder().setIssuerDn(x500Principal).setSerialNumber(new BigInteger(10, new SecureRandom()))
                            .setNotValidBefore(start).setNotValidAfter(until).setSubjectDn(x500Principal).setPublicKey(keypair.getPublic())
                            .setSigningKey(keypair.getPrivate()).setSignatureAlgorithmName("SHA256withRSA");
            chain[0] = builder.build();

            return new TestJSSESecurityDomain(alias, privKey, keyPass, chain);
        }

        @Bean
        public HttpServer server() throws IOException {
            HttpServer server = HttpServer.create(new InetSocketAddress(PORT), 0);
            server.setExecutor(null);
            server.start();
            return server;
        }

        @Bean
        public RemoteUserOperationsImpl remote(HttpServer server) {
            // create a remote event query logic that has our own server behind it
            RemoteUserOperationsImpl remote = new RemoteUserOperationsImpl();
            remote.setQueryServiceURI("/Security/User/");
            remote.setQueryServiceScheme("http");
            remote.setQueryServiceHost("localhost");
            remote.setQueryServicePort(server.getAddress().getPort());
            remote.setResponseObjectFactory(new MockResponseObjectFactory());
            return remote;
        }
    }

    private static final SubjectIssuerDNPair userDN = SubjectIssuerDNPair.of("userDn", "issuerDn");
    private static final SubjectIssuerDNPair otherUserDN = SubjectIssuerDNPair.of("otherUserDn", "issuerDn");
    private static Authorizations auths = new Authorizations("auth1", "auth2");

    private static final int PORT = 0;

    private final DatawaveUser user = new DatawaveUser(userDN, DatawaveUser.UserType.USER, Sets.newHashSet(auths.toString().split(",")), null, null, -1L);
    private final DatawavePrincipal principal = new DatawavePrincipal((Collections.singleton(user)));

    private final DatawaveUser otherUser = new DatawaveUser(otherUserDN, DatawaveUser.UserType.USER, Sets.newHashSet(auths.toString().split(",")), null, null,
                    -1L);
    private final DatawavePrincipal otherPrincipal = new DatawavePrincipal((Collections.singleton(otherUser)));

    @Autowired
    private HttpServer server;

    @Autowired
    private UserOperations remote;

    private DefaultAuthorizationsList listEffectiveAuthResponse;

    @Before
    public void setup() throws Exception {
        final ObjectMapper objectMapper = new DefaultMapperDecorator().decorate(new ObjectMapper());
        System.setProperty(DnUtils.SUBJECT_DN_PATTERN_PROPERTY, ".*ou=server.*");

        setListEffectiveAuthResponse(userDN, auths);

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
    }

    @After
    public void after() {
        if (server != null) {
            server.stop(0);
        }
    }

    private void setListEffectiveAuthResponse(SubjectIssuerDNPair userDN, Authorizations auths) {
        listEffectiveAuthResponse = new DefaultAuthorizationsList();
        listEffectiveAuthResponse.setUserAuths(userDN.subjectDN(), userDN.issuerDN(), Arrays.asList(auths.toString().split(",")));
        listEffectiveAuthResponse.addAuths(userDN.subjectDN(), userDN.issuerDN(), Arrays.asList(auths.toString().split(",")));
    }

    @Test
    public void testRemoteUserOperations() throws Exception {

        AuthorizationsListBase returnedAuths = remote.listEffectiveAuthorizations(principal);
        assertEquals(2, returnedAuths.getAllAuths().size());

        GenericResponse flush = remote.flushCachedCredentials(principal);
        assertEquals("test flush result", flush.getResult());

        ProxiedUserDetails returnedUser = remote.getRemoteUser(principal);

        // ensure that we get the cached user details
        ProxiedUserDetails dupeReturnedUser = remote.getRemoteUser(principal);
        assertEquals(returnedUser, dupeReturnedUser);

        // setup the list effective auth response for the other user
        setListEffectiveAuthResponse(otherUserDN, auths);

        // ensure that we get the other user details, not the cached user details
        ProxiedUserDetails newReturnedUser = remote.getRemoteUser(otherPrincipal);
        assertNotEquals(returnedUser, newReturnedUser);
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
