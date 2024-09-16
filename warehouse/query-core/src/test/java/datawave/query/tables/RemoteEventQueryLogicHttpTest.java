package datawave.query.tables;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import javax.ws.rs.core.MediaType;

import org.apache.commons.io.IOUtils;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.result.event.DefaultResponseObjectFactory;
import datawave.microservice.query.QueryImpl;
import datawave.microservice.query.QueryParameters;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.util.DnUtils;
import datawave.webservice.common.json.DefaultMapperDecorator;
import datawave.webservice.common.remote.TestJSSESecurityDomain;
import datawave.webservice.query.remote.RemoteQueryServiceImpl;
import datawave.webservice.query.result.event.DefaultEvent;
import datawave.webservice.query.result.event.DefaultField;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.result.DefaultEventQueryResponse;
import datawave.webservice.result.GenericResponse;
import datawave.webservice.result.VoidResponse;

public class RemoteEventQueryLogicHttpTest {

    private static final int keysize = 2048;

    private static final String commonName = "cn=www.test.us";
    private static final String alias = "tomcat";
    private static final char[] keyPass = "changeit".toCharArray();
    private static final String query = "Grinning\uD83D\uDE00Face";

    private X500Name x500Name;
    RemoteEventQueryLogic logic = new RemoteEventQueryLogic();

    private static final int PORT = 0;

    private HttpServer server;

    volatile int nextCalls = 0;

    private volatile String content = null;

    private void setContent(InputStream content) throws IOException {
        StringBuilder builder = new StringBuilder();
        InputStreamReader reader = new InputStreamReader(content, "UTF8");
        char[] buffer = new char[1024];
        int chars = reader.read(buffer);
        while (chars >= 0) {
            builder.append(buffer, 0, chars);
            chars = reader.read(buffer);
        }
        List<NameValuePair> data = URLEncodedUtils.parse(builder.toString(), Charset.forName("UTF-8"));
        for (NameValuePair pair : data) {
            if (pair.getName().equals(QueryParameters.QUERY_STRING)) {
                this.content = pair.getValue();
                break;
            }
        }
    }

    @Before
    public void setup() throws Exception {
        final ObjectMapper objectMapper = new DefaultMapperDecorator().decorate(new ObjectMapper());
        System.setProperty(DnUtils.SUBJECT_DN_PATTERN_PROPERTY, ".*ou=server.*");
        KeyPairGenerator generater = KeyPairGenerator.getInstance("RSA");
        generater.initialize(keysize);
        KeyPair keypair = generater.generateKeyPair();
        PrivateKey privKey = keypair.getPrivate();
        final X509Certificate[] chain = new X509Certificate[1];
        x500Name = new X500Name(commonName);
        SubjectPublicKeyInfo subPubKeyInfo = SubjectPublicKeyInfo.getInstance(keypair.getPublic().getEncoded());
        final Date start = new Date();
        final Date until = Date.from(LocalDate.now().plus(365, ChronoUnit.DAYS).atStartOfDay().toInstant(ZoneOffset.UTC));
        X509v3CertificateBuilder builder = new X509v3CertificateBuilder(x500Name, new BigInteger(10, new SecureRandom()), // Choose something better for real
                                                                                                                          // use
                        start, until, x500Name, subPubKeyInfo);
        ContentSigner signer = new JcaContentSignerBuilder("SHA256WithRSA").setProvider(new BouncyCastleProvider()).build(keypair.getPrivate());
        final X509CertificateHolder holder = builder.build(signer);

        chain[0] = new JcaX509CertificateConverter().setProvider(new BouncyCastleProvider()).getCertificate(holder);

        server = HttpServer.create(new InetSocketAddress(PORT), 0);
        server.setExecutor(null);
        server.start();

        UUID uuid = UUID.randomUUID();
        GenericResponse<String> createResponse = new GenericResponse<String>();
        createResponse.setResult(uuid.toString());

        HttpHandler createHandler = new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                setContent(exchange.getRequestBody());
                String responseBody = objectMapper.writeValueAsString(createResponse);
                exchange.getResponseHeaders().add("Content-Type", MediaType.APPLICATION_JSON);
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, responseBody.length());
                IOUtils.write(responseBody, exchange.getResponseBody(), Charset.forName("UTF-8"));
                exchange.close();
            }
        };

        DefaultEventQueryResponse response1 = new DefaultEventQueryResponse();
        DefaultEvent event1 = new DefaultEvent();
        event1.setFields(Collections.singletonList(new DefaultField("FOO1", "FOO|BAR", new HashMap(), -1L, "FOOBAR1")));
        response1.setEvents(Collections.singletonList(event1));
        response1.setReturnedEvents(1L);

        DefaultEventQueryResponse response2 = new DefaultEventQueryResponse();
        DefaultEvent event2 = new DefaultEvent();
        event1.setFields(Collections.singletonList(new DefaultField("FOO2", "FOO|BAR", new HashMap(), -1L, "FOOBAR2")));
        response2.setEvents(Collections.singletonList(event1));
        response2.setReturnedEvents(1L);

        DefaultEventQueryResponse response3 = new DefaultEventQueryResponse();
        response3.setReturnedEvents(0L);

        HttpHandler nextHandler = new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                nextCalls++;
                DefaultEventQueryResponse response = (nextCalls == 1 ? response1 : (nextCalls == 2 ? response2 : response3));
                String responseBody = objectMapper.writeValueAsString(response);
                exchange.getResponseHeaders().add("Content-Type", MediaType.APPLICATION_JSON);
                int responseCode = nextCalls > 2 ? HttpURLConnection.HTTP_NO_CONTENT : HttpURLConnection.HTTP_OK;
                exchange.sendResponseHeaders(responseCode, responseBody.length());
                IOUtils.write(responseBody, exchange.getResponseBody(), Charset.forName("UTF-8"));
                exchange.close();
            }
        };

        VoidResponse closeResponse = new VoidResponse();
        closeResponse.addMessage(uuid.toString() + " closed.");

        HttpHandler closeHandler = new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                String responseBody = objectMapper.writeValueAsString(closeResponse);
                exchange.getResponseHeaders().add("Content-Type", MediaType.APPLICATION_JSON);
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, responseBody.length());
                IOUtils.write(responseBody, exchange.getResponseBody(), Charset.forName("UTF-8"));
                exchange.close();
            }
        };

        server.createContext("/DataWave/Query/TestQuery/create", createHandler);
        server.createContext("/DataWave/Query/" + uuid.toString() + "/next", nextHandler);
        server.createContext("/DataWave/Query/" + uuid.toString() + "/close", closeHandler);

        // create a remote event query logic that has our own server behind it
        RemoteQueryServiceImpl remote = new RemoteQueryServiceImpl();
        remote.setQueryServiceURI("/DataWave/Query/");
        remote.setQueryServiceScheme("http");
        remote.setQueryServiceHost("localhost");
        remote.setQueryServicePort(server.getAddress().getPort());
        remote.setExecutorService(null);
        remote.setObjectMapperDecorator(new DefaultMapperDecorator());
        remote.setResponseObjectFactory(new DefaultResponseObjectFactory());
        remote.setJsseSecurityDomain(new TestJSSESecurityDomain(alias, privKey, keyPass, chain));

        logic.setRemoteQueryService(remote);
        logic.setRemoteQueryLogic("TestQuery");
    }

    @After
    public void after() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    public void testRemoteQuery() throws Exception {
        logic.setCurrentUser(new DatawavePrincipal(commonName));
        QueryImpl settings = new QueryImpl();
        settings.setQuery(query);
        GenericQueryConfiguration config = logic.initialize(null, settings, null);
        logic.setupQuery(config);

        Iterator<EventBase> t = logic.iterator();
        List<EventBase> events = new ArrayList();
        while (t.hasNext()) {
            events.add(t.next());
        }
        assertEquals(2, events.size());
        assertNotNull(content);
        assertEquals(query, content);
    }

}
