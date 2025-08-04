package datawave.query.tables;

import static java.lang.Thread.sleep;
import static java.nio.charset.StandardCharsets.UTF_8;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
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
import java.util.concurrent.atomic.AtomicBoolean;

import javax.ws.rs.core.MediaType;

import datawave.webservice.common.remote.RemoteHttpService;
import datawave.webservice.common.remote.RemoteHttpServiceConfiguration;
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
        InputStreamReader reader = new InputStreamReader(content, UTF_8);
        char[] buffer = new char[1024];
        int chars = reader.read(buffer);
        while (chars >= 0) {
            builder.append(buffer, 0, chars);
            chars = reader.read(buffer);
        }
        List<NameValuePair> data = URLEncodedUtils.parse(builder.toString(), UTF_8);
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

    @Test
    public void testDefaultConnectTimeoutHangForever() {
        // override the endpoint to a non-routable ip, so it will block forever
        ((RemoteHttpService) logic.getRemoteQueryService()).setQueryServiceHost("10.255.255.255");

        // track query execution state
        AtomicBoolean setup = new AtomicBoolean(false);
        AtomicBoolean caught = new AtomicBoolean(false);

        // execute this in a thread so it can be interrupted
        Runnable r = () -> {
            logic.setCurrentUser(new DatawavePrincipal(commonName));
            QueryImpl settings = new QueryImpl();
            settings.setQuery(query);
            try {
                GenericQueryConfiguration config = logic.initialize(null, settings, null);
                logic.setupQuery(config);
                setup.set(true);
            } catch (Exception e) {
                caught.set(true);
                throw new RuntimeException(e);
            }

            Iterator<EventBase> t = logic.iterator();
            List<EventBase> events = new ArrayList();
            while (t.hasNext()) {
                events.add(t.next());
            }
            assertEquals(2, events.size());
            assertNotNull(content);
            assertEquals(query, content);
        };
        Thread t = new Thread(r);
        t.start();

        // ensure the thread wasn't interrupted
        boolean interrupted = false;
        try {
            // waiting forever is the default state
            t.join(1000);
        } catch (InterruptedException e) {
            interrupted = true;
        }

        assertFalse(interrupted);
        // this would be TERMINATED if the thread ran
        assertTrue(t.getState().toString(), t.getState() == Thread.State.RUNNABLE);
        assertFalse(setup.get());
        assertFalse(caught.get());
    }

    @Test(expected = RuntimeException.class)
    public void testConnectTimeoutQuery() throws Exception {
        RemoteHttpService remoteHttpService = (RemoteHttpService) logic.getRemoteQueryService();
        RemoteHttpServiceConfiguration remoteConfig = remoteHttpService.getConfig();

        // set a super fast connect timeout
        remoteConfig.setConnectTimeout(1);

        // override the endpoint to a non-routable ip
        ((RemoteHttpService) logic.getRemoteQueryService()).setQueryServiceHost("10.255.255.255");

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

    @Test
    public void testDefaultSocketTimeout() throws InterruptedException {
        AtomicBoolean handlerInterrupt = new AtomicBoolean(false);
        HttpHandler foreverHandler = exchange -> {
            while (true) {
                try {
                    sleep(1000);
                    if (handlerInterrupt.get()) {
                        throw new InterruptedException();
                    }
                } catch (InterruptedException e) {
                    throw new IOException(e);
                }
            }
        };

        // remove the old handler
        server.removeContext("/DataWave/Query/TestQuery/create");
        // attach a new one that hangs for 5s
        server.createContext("/DataWave/Query/TestQuery/create", foreverHandler);

        // track query execution state
        AtomicBoolean setup = new AtomicBoolean(false);
        AtomicBoolean caught = new AtomicBoolean(false);

        // execute this in a thread so it can be interrupted
        Runnable r = () -> {
            logic.setCurrentUser(new DatawavePrincipal(commonName));
            QueryImpl settings = new QueryImpl();
            settings.setQuery(query);
            try {
                GenericQueryConfiguration config = logic.initialize(null, settings, null);
                logic.setupQuery(config);
                setup.set(true);
            } catch (Exception e) {
                caught.set(true);
                return;
            }

            Iterator<EventBase> t = logic.iterator();
            List<EventBase> events = new ArrayList();
            while (t.hasNext()) {
                events.add(t.next());
            }
            assertEquals(2, events.size());
            assertNotNull(content);
            assertEquals(query, content);
        };
        Thread t = new Thread(r);
        t.start();

        // ensure the thread wasn't interrupted
        boolean interrupted = false;
        try {
            // waiting forever is the default state
            t.join(1000);
        } catch (InterruptedException e) {
            interrupted = true;
        }

        // this would be TERMINATED if the thread ran
        assertTrue(t.getState().toString(), t.getState() == Thread.State.RUNNABLE);

        // force an handlerInterrupt on the handler
        handlerInterrupt.set(true);
        assertFalse(interrupted);

        while(t.isAlive()) {
            sleep(200);
        }

        assertFalse(setup.get());
        assertTrue(caught.get());
    }

    @Test
    public void testSocketTimeout() throws InterruptedException {
        RemoteHttpService remoteHttpService = (RemoteHttpService) logic.getRemoteQueryService();
        RemoteHttpServiceConfiguration remoteConfig = remoteHttpService.getConfig();

        // set a super fast socket timeout
        remoteConfig.setSocketTimeout(1);

        AtomicBoolean interrupt = new AtomicBoolean(false);
        HttpHandler foreverHandler = new ForeverHandler(interrupt);

        // remove the old handler
        server.removeContext("/DataWave/Query/TestQuery/create");
        // attach a new one that hangs for 5s
        server.createContext("/DataWave/Query/TestQuery/create", foreverHandler);

        // track query execution state
        QueryRunnable r = new QueryRunnable(logic, content);
        r.run();

        assertFalse(r.isSetup().get());
        // the socket timeout will cause this to throw an exception prior to the interrupt being sent
        assertTrue(r.isCaught().get());

        interrupt.set(true);
    }

    @Test
    public void testDefaultConnectionPoolTimeout() throws InterruptedException {
        RemoteHttpService remoteHttpService = (RemoteHttpService) logic.getRemoteQueryService();
        RemoteHttpServiceConfiguration remoteConfig = remoteHttpService.getConfig();

        // only allow a single max connection
        remoteConfig.setMaxConnections(1);

        // patch in the forever handler which will block until its unlocked
        AtomicBoolean handlerInterrupt = new AtomicBoolean(false);
        HttpHandler foreverHandler = new ForeverHandler(handlerInterrupt);

        // remove the old handler
        server.removeContext("/DataWave/Query/TestQuery/create");
        // attach a new one that hangs for 5s
        server.createContext("/DataWave/Query/TestQuery/create", foreverHandler);

        // create two threads that both access the forever handler
        // execute this in a thread so it can be interrupted
        QueryRunnable r1 = new QueryRunnable(logic, content);
        QueryRunnable r2 = new QueryRunnable(logic, content);

        // start both threads
        Thread t1 = new Thread(r1);
        t1.start();
        Thread t2 = new Thread(r2);
        t2.start();

        sleep(1050);

        // check that neither thread has changed state
        assertFalse(r1.isCaught().get());
        assertFalse(r2.isCaught().get());

        assertFalse(r1.isSetup().get());
        assertFalse(r2.isSetup().get());

        handlerInterrupt.set(true);

        sleep(1050);

        assertTrue(r1.isCaught().get() || r2.isCaught().get());
        assertFalse(r1.isSetup().get());
        assertFalse(r2.isSetup().get());
    }

    @Test
    public void testConnectionPoolTimeout() throws InterruptedException {
        RemoteHttpService remoteHttpService = (RemoteHttpService) logic.getRemoteQueryService();
        RemoteHttpServiceConfiguration remoteConfig = remoteHttpService.getConfig();

        // only allow a single max connection
        remoteConfig.setMaxConnections(1);
        // only wait 1ms for a thread
        remoteConfig.setConnectionPoolTimeout(1);

        // patch in the forever handler which will block until its unlocked
        AtomicBoolean handlerInterrupt = new AtomicBoolean(false);
        HttpHandler foreverHandler = new ForeverHandler(handlerInterrupt);

        // remove the old handler
        server.removeContext("/DataWave/Query/TestQuery/create");
        // attach a new one that hangs for 5s
        server.createContext("/DataWave/Query/TestQuery/create", foreverHandler);

        // create two threads that both access the forever handler
        // execute this in a thread so it can be interrupted
        QueryRunnable r1 = new QueryRunnable(logic, content);
        QueryRunnable r2 = new QueryRunnable(logic, content);

        // start both threads
        Thread t1 = new Thread(r1);
        t1.start();
        Thread t2 = new Thread(r2);
        t2.start();

        sleep(1050);

        // one of these should have state changed, but don't assert until after the interrupt or the test will never stop
        boolean r1Caught = r1.isCaught().get();
        boolean r2Caught = r2.isCaught().get();

        handlerInterrupt.set(true);

        // exactly one should have caught an exception due to the pool timeout, the other should be stuck in the forever handler
        assertTrue(r1Caught || r2Caught);
        assertFalse(r1Caught && r2Caught);

        assertFalse(r1.isSetup().get());
        assertFalse(r2.isSetup().get());
    }

    public static class ForeverHandler implements HttpHandler {
        private AtomicBoolean interrupt = new AtomicBoolean(false);

        public ForeverHandler(AtomicBoolean interrupt) {
            this.interrupt = interrupt;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            while (true) {
                try {
                    sleep(1000);
                    if (interrupt.get()) {
                        throw new InterruptedException();
                    }
                } catch (InterruptedException e) {
                    throw new IOException(e);
                }
            }
        }
    }

    public static class QueryRunnable implements Runnable {
        private RemoteEventQueryLogic logic;
        private String content;

        private AtomicBoolean setup = new AtomicBoolean(false);
        private AtomicBoolean caught = new AtomicBoolean(false);

        public QueryRunnable(RemoteEventQueryLogic logic, String content) {
            this.logic = logic;
            this.content = content;
        }

        @Override
        public void run() {
            logic.setCurrentUser(new DatawavePrincipal(commonName));
            QueryImpl settings = new QueryImpl();
            settings.setQuery(query);
            try {
                GenericQueryConfiguration config = logic.initialize(null, settings, null);
                logic.setupQuery(config);
                setup.set(true);
            } catch (Exception e) {
                caught.set(true);
                return;
            }

            Iterator<EventBase> t = logic.iterator();
            List<EventBase> events = new ArrayList();
            while (t.hasNext()) {
                events.add(t.next());
            }
            assertEquals(2, events.size());
            assertNotNull(content);
            assertEquals(query, content);
        }

        public AtomicBoolean isCaught() {
            return caught;
        }

        public AtomicBoolean isSetup() {
            return setup;
        }
    }
}
