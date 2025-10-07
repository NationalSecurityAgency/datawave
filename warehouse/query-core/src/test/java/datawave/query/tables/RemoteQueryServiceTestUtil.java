package datawave.query.tables;

import static java.nio.charset.StandardCharsets.UTF_8;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.net.HttpURLConnection;
import java.nio.charset.Charset;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
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

import org.apache.commons.collections4.iterators.TransformIterator;
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
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.HashMultimap;
import com.sun.net.httpserver.HttpHandler;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.logic.QueryLogic;
import datawave.core.query.logic.composite.CompositeQueryLogic;
import datawave.core.query.remote.RemoteQueryService;
import datawave.core.query.result.event.DefaultResponseObjectFactory;
import datawave.microservice.query.QueryImpl;
import datawave.microservice.query.QueryParameters;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.util.DnUtils;
import datawave.webservice.common.json.DefaultMapperDecorator;
import datawave.webservice.common.remote.RemoteServiceUtil;
import datawave.webservice.common.remote.TestJSSESecurityDomain;
import datawave.webservice.query.remote.RemoteQueryServiceImpl;
import datawave.webservice.query.result.event.DefaultEvent;
import datawave.webservice.query.result.event.DefaultField;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.result.DefaultEventQueryResponse;
import datawave.webservice.result.GenericResponse;
import datawave.webservice.result.VoidResponse;

public class RemoteQueryServiceTestUtil extends RemoteServiceUtil {
    public static final String COMMON_NAME = "cn=www.test.us";
    public static final String DEFAULT_REMOTE_LOGIC = "TestQuery";

    private static final int keysize = 2048;
    private static final String alias = "tomcat";
    private static final char[] keyPass = "changeit".toCharArray();

    private final UUID uuid;
    private final int totalNext;

    private PrivateKey privateKey;
    private X509Certificate[] chain;

    private String query;
    private String remoteLogic;
    private int nextCount;

    public RemoteQueryServiceTestUtil() {
        this(UUID.randomUUID(), DEFAULT_REMOTE_LOGIC, 2);
    }

    public RemoteQueryServiceTestUtil(UUID uuid, String remoteLogic, int totalNext) {
        this.uuid = uuid;
        this.remoteLogic = remoteLogic;
        this.totalNext = totalNext;
    }

    public void initialize() throws IOException {
        super.initialize();

        final ObjectMapper objectMapper = new DefaultMapperDecorator().decorate(new ObjectMapper());
        System.setProperty(DnUtils.SUBJECT_DN_PATTERN_PROPERTY, ".*ou=server.*");
        KeyPairGenerator generater = null;
        try {
            generater = KeyPairGenerator.getInstance("RSA");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        generater.initialize(keysize);
        KeyPair keypair = generater.generateKeyPair();
        privateKey = keypair.getPrivate();
        chain = new X509Certificate[1];
        X500Name x500Name = new X500Name(COMMON_NAME);
        SubjectPublicKeyInfo subPubKeyInfo = SubjectPublicKeyInfo.getInstance(keypair.getPublic().getEncoded());
        final Date start = new Date();
        final Date until = Date.from(LocalDate.now().plus(365, ChronoUnit.DAYS).atStartOfDay().toInstant(ZoneOffset.UTC));
        X509v3CertificateBuilder builder = new X509v3CertificateBuilder(x500Name, new BigInteger(10, new SecureRandom()), // Choose something better for real
                        // use
                        start, until, x500Name, subPubKeyInfo);
        ContentSigner signer = null;
        try {
            signer = new JcaContentSignerBuilder("SHA256WithRSA").setProvider(new BouncyCastleProvider()).build(keypair.getPrivate());
        } catch (OperatorCreationException e) {
            throw new RuntimeException(e);
        }
        final X509CertificateHolder holder = builder.build(signer);

        try {
            chain[0] = new JcaX509CertificateConverter().setProvider(new BouncyCastleProvider()).getCertificate(holder);
        } catch (CertificateException e) {
            throw new RuntimeException(e);
        }

        GenericResponse<String> createResponse = new GenericResponse<>();
        createResponse.setResult(uuid.toString());

        HttpHandler createHandler = exchange -> {
            setQuery(exchange.getRequestBody());

            String responseBody = objectMapper.writeValueAsString(createResponse);
            exchange.getResponseHeaders().add("Content-Type", MediaType.APPLICATION_JSON);
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, responseBody.length());
            IOUtils.write(responseBody, exchange.getResponseBody(), Charset.forName("UTF-8"));
            exchange.close();
        };

        HttpHandler nextHandler = exchange -> {
            nextCount++;
            int responseCode = HttpURLConnection.HTTP_OK;
            DefaultEventQueryResponse response = new DefaultEventQueryResponse();
            if (nextCount < totalNext) {
                DefaultEvent event = new DefaultEvent();
                event.setFields(Collections.singletonList(new DefaultField("FOO" + nextCount, "FOO|BAR", new HashMap(), -1L, "FOOBAR" + nextCount)));
                response.setEvents(Collections.singletonList(event));
                response.setReturnedEvents(1L);
            } else {
                response.setReturnedEvents(0L);
                responseCode = HttpURLConnection.HTTP_NO_CONTENT;
            }

            String responseBody = objectMapper.writeValueAsString(response);
            exchange.getResponseHeaders().add("Content-Type", MediaType.APPLICATION_JSON);
            exchange.sendResponseHeaders(responseCode, responseBody.length());
            IOUtils.write(responseBody, exchange.getResponseBody(), Charset.forName("UTF-8"));
            exchange.close();
        };

        VoidResponse closeResponse = new VoidResponse();
        closeResponse.addMessage(uuid + " closed.");

        HttpHandler closeHandler = exchange -> {
            String responseBody = objectMapper.writeValueAsString(closeResponse);
            exchange.getResponseHeaders().add("Content-Type", MediaType.APPLICATION_JSON);
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, responseBody.length());
            IOUtils.write(responseBody, exchange.getResponseBody(), Charset.forName("UTF-8"));
            exchange.close();
        };

        addRoute("/DataWave/Query/" + remoteLogic + "/create", createHandler);
        addRoute("/DataWave/Query/" + remoteLogic + "/plan", createHandler);
        addRoute("/DataWave/Query/" + uuid + "/next", nextHandler);
        addRoute("/DataWave/Query/" + uuid + "/close", closeHandler);
    }

    public RemoteQueryService getRemoteService() {
        if (!isInitialized()) {
            throw new IllegalStateException("must call initialize() first");
        }

        RemoteQueryServiceImpl remote = new RemoteQueryServiceImpl();
        remote.setQueryServiceURI("/DataWave/Query/");
        remote.setQueryServiceScheme("http");
        remote.setQueryServiceHost("localhost");
        remote.setQueryServicePort(getPort());
        remote.setExecutorService(null);
        remote.setObjectMapperDecorator(new DefaultMapperDecorator());
        remote.setResponseObjectFactory(new DefaultResponseObjectFactory());
        remote.setJsseSecurityDomain(new TestJSSESecurityDomain(alias, privateKey, keyPass, chain));

        return remote;
    }

    private void setQuery(InputStream content) throws IOException {
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
                this.query = pair.getValue();
                break;
            }
        }
    }

    public String getQuery() {
        return query;
    }

    public static class QueryRunnable implements Runnable {
        private final QueryLogic logic;
        private final int expectedCount;

        private AtomicBoolean setup = new AtomicBoolean(false);
        private AtomicBoolean caught = new AtomicBoolean(false);
        private Exception exception = null;

        private String query;
        private RemoteQueryServiceTestUtil testUtil;

        private boolean planTest = false;
        private String plan;

        public QueryRunnable(QueryLogic logic) {
            this(logic, null);
        }

        public QueryRunnable(QueryLogic logic, RemoteQueryServiceTestUtil testUtil) {
            this(logic, 2, "", testUtil);
        }

        public QueryRunnable(QueryLogic logic, int expectedCount, String query, RemoteQueryServiceTestUtil testUtil) {
            this.logic = logic;
            this.expectedCount = expectedCount;
            this.query = query;
            this.testUtil = testUtil;
        }

        @Override
        public void run() {
            DatawaveUser user = new DatawaveUser(SubjectIssuerDNPair.of(COMMON_NAME), DatawaveUser.UserType.USER, List.of("A"), List.of(),
                            HashMultimap.create(), 1);
            logic.setCurrentUser(new DatawavePrincipal(List.of(user)));

            QueryImpl settings = new QueryImpl();
            settings.setId(UUID.randomUUID());
            settings.setQueryAuthorizations("A,B,C,D,E");
            settings.setPagesize(10);
            settings.setQuery(query);
            try {
                if (planTest) {
                    plan = logic.getPlan(null, settings, null, false, false);
                    return;
                }

                GenericQueryConfiguration config = logic.initialize(null, settings, null);
                logic.setupQuery(config);
                setup.set(true);

                if (logic instanceof CompositeQueryLogic) {
                    TransformIterator itr = logic.getTransformIterator(settings);
                    List<DefaultEvent> events = new ArrayList();
                    while (itr.hasNext()) {
                        DefaultEvent event = (DefaultEvent) itr.next();
                        events.add(event);
                    }
                    assertEquals(expectedCount, events.size());
                } else {
                    Iterator<EventBase> t = logic.iterator();
                    List<EventBase> events = new ArrayList();
                    while (t.hasNext()) {
                        events.add(t.next());
                    }
                    assertEquals(expectedCount, events.size());
                }
            } catch (Exception e) {
                caught.set(true);
                exception = e;
                return;
            }

            if (expectedCount > 0 && testUtil != null) {
                assertNotNull(testUtil.getQuery());
                assertEquals(query, testUtil.getQuery());
            }
        }

        public AtomicBoolean isCaught() {
            return caught;
        }

        public AtomicBoolean isSetup() {
            return setup;
        }

        public Exception getException() {
            return exception;
        }

        public void setPlanTest(boolean planTest) {
            this.planTest = planTest;
        }

        public String getPlan() {
            return this.plan;
        }
    }
}
