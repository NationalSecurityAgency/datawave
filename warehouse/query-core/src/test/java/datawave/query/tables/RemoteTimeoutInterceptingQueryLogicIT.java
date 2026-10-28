package datawave.query.tables;

import static datawave.query.tables.RemoteQueryServiceTestUtil.DEFAULT_REMOTE_LOGIC;
import static datawave.query.tables.RemoteQueryServiceTestUtil.NON_ROUTABLE_HOST;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.bouncycastle.operator.OperatorCreationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Sets;
import com.sun.net.httpserver.HttpHandler;

import datawave.authorization.remote.RemoteAuthorizationException;
import datawave.core.query.logic.QueryLogic;
import datawave.core.query.logic.composite.CompositeLogicException;
import datawave.core.query.logic.composite.CompositeQueryLogic;
import datawave.core.query.remote.RemoteQueryService;
import datawave.core.query.remote.RemoteQueryServiceProperties;
import datawave.core.query.remote.RemoteTimeoutQueryException;
import datawave.core.query.remote.RemoteTimeoutQueryRuntimeException;
import datawave.core.query.result.event.DefaultResponseObjectFactory;
import datawave.marking.MarkingFunctions;
import datawave.query.tables.remote.RemoteQueryLogic;
import datawave.security.authorization.AuthorizationException;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.ProxiedUserDetails;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.authorization.UserOperations;
import datawave.user.AuthorizationsListBase;
import datawave.webservice.common.remote.RemoteHttpService;
import datawave.webservice.result.GenericResponse;

@Tag("IntegrationTest")
public class RemoteTimeoutInterceptingQueryLogicIT {
    private static final String QUERY = "Grinning\uD83D\uDE00Face";

    private RemoteTimeoutInterceptingQueryLogic logic;
    private QueryLogic remoteDelegate;
    private RemoteQueryService remoteQueryService;
    private RemoteQueryServiceTestUtil testUtil;
    private UUID uuid;
    private AtomicBoolean interrupt = new AtomicBoolean(false);

    @BeforeEach
    public void setup() throws CertificateException, NoSuchAlgorithmException, IOException, OperatorCreationException {
        uuid = UUID.randomUUID();
        interrupt.set(false);

        testUtil = new RemoteQueryServiceTestUtil(uuid, DEFAULT_REMOTE_LOGIC, 3);
        testUtil.initialize();

        logic = new RemoteTimeoutInterceptingQueryLogic();

        remoteDelegate = new RemoteEventQueryLogic();
        remoteQueryService = testUtil.getRemoteService();
        ((RemoteEventQueryLogic) remoteDelegate).setRemoteQueryService(remoteQueryService);
        ((RemoteEventQueryLogic) remoteDelegate).setRemoteQueryLogic(DEFAULT_REMOTE_LOGIC);

        logic.setDelegate(remoteDelegate);
    }

    @AfterEach
    public void cleanup() {
        interrupt.set(true);
    }

    @Test
    public void noTimeoutTest() {
        RemoteQueryServiceTestUtil.QueryRunnable r = new RemoteQueryServiceTestUtil.QueryRunnable(logic, 2, "FIELD:value", testUtil);
        r.run();

        assertFalse(r.isCaught().get());
    }

    @Test
    public void unsuppressedConnectTimeoutTest() {
        RemoteQueryService remoteQueryService = ((RemoteEventQueryLogic) remoteDelegate).getRemoteQueryService();
        RemoteQueryServiceProperties remoteQueryServiceProperties = remoteQueryService.getRemoteQueryServiceProperties();

        String uri = remoteQueryService.getRemoteQueryServiceProperties().getQueryServiceUri();
        uri = uri.replace("localhost", NON_ROUTABLE_HOST);
        remoteQueryServiceProperties.setQueryServiceUri(uri);

        // set a super fast connect timeout
        ((RemoteHttpService) remoteQueryService).getConfig().setConnectTimeout(100);

        HttpHandler foreverHandler = new RemoteQueryServiceTestUtil.ForeverHandler(interrupt);
        testUtil.updateRoute("/DataWave/Query/" + DEFAULT_REMOTE_LOGIC + "/create", foreverHandler);

        RemoteQueryServiceTestUtil.QueryRunnable r = new RemoteQueryServiceTestUtil.QueryRunnable(logic);
        r.run();

        assertTrue(r.isCaught().get());
        assertTrue(r.getException() instanceof RemoteTimeoutQueryException);
    }

    @Test
    public void suppressedConnectTimeoutTest() {
        RemoteQueryService remoteQueryService = ((RemoteEventQueryLogic) remoteDelegate).getRemoteQueryService();
        RemoteQueryServiceProperties remoteQueryServiceProperties = remoteQueryService.getRemoteQueryServiceProperties();

        String uri = remoteQueryService.getRemoteQueryServiceProperties().getQueryServiceUri();
        uri = uri.replace("localhost", NON_ROUTABLE_HOST);
        remoteQueryServiceProperties.setQueryServiceUri(uri);

        // set a super fast connect timeout
        ((RemoteHttpService) remoteQueryService).getConfig().setConnectTimeout(100);

        HttpHandler foreverHandler = new RemoteQueryServiceTestUtil.ForeverHandler(interrupt);
        testUtil.updateRoute("/DataWave/Query/" + DEFAULT_REMOTE_LOGIC + "/create", foreverHandler);

        logic.setSuppressTimeout(true);
        RemoteQueryServiceTestUtil.QueryRunnable r = new RemoteQueryServiceTestUtil.QueryRunnable(logic, 0, "FIELD:value", testUtil);
        r.run();

        assertFalse(r.isCaught().get());
    }

    @Test
    public void unsuppressedSocketTimeoutTest() {
        RemoteQueryService remoteQueryService = ((RemoteEventQueryLogic) remoteDelegate).getRemoteQueryService();
        RemoteQueryServiceProperties remoteQueryServiceProperties = remoteQueryService.getRemoteQueryServiceProperties();

        // set a super fast connect timeout
        remoteQueryServiceProperties.setCreateTimeout(100);

        HttpHandler foreverHandler = new RemoteQueryServiceTestUtil.ForeverHandler(interrupt);
        testUtil.updateRoute("/DataWave/Query/" + DEFAULT_REMOTE_LOGIC + "/create", foreverHandler);

        RemoteQueryServiceTestUtil.QueryRunnable r = new RemoteQueryServiceTestUtil.QueryRunnable(logic);
        r.run();

        assertTrue(r.isCaught().get());
        assertTrue(r.getException() instanceof RemoteTimeoutQueryException);
    }

    @Test
    public void suppressedSocketTimeoutTest() {
        RemoteQueryService remoteQueryService = ((RemoteEventQueryLogic) remoteDelegate).getRemoteQueryService();
        RemoteQueryServiceProperties remoteQueryServiceProperties = remoteQueryService.getRemoteQueryServiceProperties();

        // only wait 100ms for the read
        remoteQueryServiceProperties.setCreateTimeout(100);

        HttpHandler foreverHandler = new RemoteQueryServiceTestUtil.ForeverHandler(interrupt);
        testUtil.updateRoute("/DataWave/Query/" + DEFAULT_REMOTE_LOGIC + "/create", foreverHandler);

        logic.setSuppressTimeout(true);
        RemoteQueryServiceTestUtil.QueryRunnable r = new RemoteQueryServiceTestUtil.QueryRunnable(logic, 0, "FIELD:value", testUtil);
        r.run();

        assertFalse(r.isCaught().get());
    }

    @Test
    public void unsuppressedIteratorTimeoutTest() {
        RemoteQueryService remoteQueryService = ((RemoteEventQueryLogic) remoteDelegate).getRemoteQueryService();
        RemoteQueryServiceProperties remoteQueryServiceProperties = remoteQueryService.getRemoteQueryServiceProperties();

        // only wait 100ms for the read
        remoteQueryServiceProperties.setNextTimeout(100);

        HttpHandler foreverHandler = new RemoteQueryServiceTestUtil.ForeverHandler(interrupt);
        testUtil.updateRoute("/DataWave/Query/" + uuid + "/next", foreverHandler);

        RemoteQueryServiceTestUtil.QueryRunnable r = new RemoteQueryServiceTestUtil.QueryRunnable(logic);
        r.run();

        assertTrue(r.isCaught().get());
        assertTrue(r.getException() instanceof RemoteTimeoutQueryRuntimeException);
    }

    @Test
    public void suppressedIteratorTimeoutTest() {
        RemoteQueryService remoteQueryService = ((RemoteEventQueryLogic) remoteDelegate).getRemoteQueryService();
        RemoteQueryServiceProperties remoteQueryServiceProperties = remoteQueryService.getRemoteQueryServiceProperties();

        // only wait 100ms for the read
        remoteQueryServiceProperties.setNextTimeout(100);

        HttpHandler foreverHandler = new RemoteQueryServiceTestUtil.ForeverHandler(interrupt);
        testUtil.updateRoute("/DataWave/Query/" + uuid + "/next", foreverHandler);

        logic.setSuppressTimeout(true);
        RemoteQueryServiceTestUtil.QueryRunnable r = new RemoteQueryServiceTestUtil.QueryRunnable(logic, 0, "FIELD:value", testUtil);
        r.run();

        assertFalse(r.isCaught().get());
    }

    @Test
    public void testCompositeRemoteWithTimeoutOnInitialize() {
        RemoteQueryService remoteQueryService = ((RemoteEventQueryLogic) remoteDelegate).getRemoteQueryService();
        RemoteQueryServiceProperties remoteQueryServiceProperties = remoteQueryService.getRemoteQueryServiceProperties();

        // only wait 10ms for the read
        remoteQueryServiceProperties.setCreateTimeout(10);

        logic.setMarkingFunctions(new MarkingFunctions.Default());
        logic.setResponseObjectFactory(new DefaultResponseObjectFactory());

        HttpHandler foreverHandler = new RemoteQueryServiceTestUtil.ForeverHandler(interrupt);

        testUtil.updateRoute("/DataWave/Query/" + DEFAULT_REMOTE_LOGIC + "/create", foreverHandler);

        Map<String,QueryLogic<?>> logics = new HashMap<>();

        logics.put("RemoteLogic", logic);

        CompositeQueryLogic compositeLogic = new CompositeQueryLogic();
        compositeLogic.setMaxPageSize(1);
        compositeLogic.setMarkingFunctions(new MarkingFunctions.Default());
        compositeLogic.setQueryLogics(logics);
        compositeLogic.setResponseObjectFactory(new DefaultResponseObjectFactory());

        RemoteQueryServiceTestUtil.QueryRunnable r = new RemoteQueryServiceTestUtil.QueryRunnable(compositeLogic);
        r.run();

        assertTrue(r.isCaught().get());
        assertTrue(r.getException() instanceof CompositeLogicException);
        assertTrue(r.getException().getMessage().startsWith("All logics have failed to initialize"));
    }

    @Test
    public void testCompositeRemoteWithTimeoutOnInitializePassThrough() {
        logic.setSuppressTimeout(true);

        RemoteQueryService remoteQueryService = ((RemoteEventQueryLogic) remoteDelegate).getRemoteQueryService();
        RemoteQueryServiceProperties remoteQueryServiceProperties = remoteQueryService.getRemoteQueryServiceProperties();

        // only wait 100ms for the read
        remoteQueryServiceProperties.setCreateTimeout(100);

        logic.setMarkingFunctions(new MarkingFunctions.Default());
        logic.setResponseObjectFactory(new DefaultResponseObjectFactory());

        HttpHandler foreverHandler = new RemoteQueryServiceTestUtil.ForeverHandler(interrupt);

        testUtil.updateRoute("/DataWave/Query/" + DEFAULT_REMOTE_LOGIC + "/create", foreverHandler);

        Map<String,QueryLogic<?>> logics = new HashMap<>();

        logics.put("RemoteLogic", logic);

        CompositeQueryLogic compositeLogic = new CompositeQueryLogic();
        compositeLogic.setMaxPageSize(1);
        compositeLogic.setMarkingFunctions(new MarkingFunctions.Default());
        compositeLogic.setQueryLogics(logics);
        compositeLogic.setResponseObjectFactory(new DefaultResponseObjectFactory());

        RemoteQueryServiceTestUtil.QueryRunnable r = new RemoteQueryServiceTestUtil.QueryRunnable(compositeLogic, 0, QUERY, testUtil);
        r.run();

        if (r.getException() != null) {
            throw new RuntimeException(r.getException());
        }
        assertFalse(r.isCaught().get());
    }

    @Test
    public void testCompositeRemoteWithTimeoutOnResults() {
        RemoteQueryService remoteQueryService = ((RemoteEventQueryLogic) remoteDelegate).getRemoteQueryService();
        RemoteQueryServiceProperties remoteQueryServiceProperties = remoteQueryService.getRemoteQueryServiceProperties();

        // only wait 100ms for the read
        remoteQueryServiceProperties.setNextTimeout(100);

        logic.setMarkingFunctions(new MarkingFunctions.Default());
        logic.setResponseObjectFactory(new DefaultResponseObjectFactory());

        HttpHandler foreverHandler = new RemoteQueryServiceTestUtil.ForeverHandler(interrupt);

        testUtil.updateRoute("/DataWave/Query/" + uuid.toString() + "/next", foreverHandler);

        Map<String,QueryLogic<?>> logics = new HashMap<>();

        logics.put("RemoteLogic", logic);

        CompositeQueryLogic compositeLogic = new CompositeQueryLogic();
        compositeLogic.setMaxPageSize(1);
        compositeLogic.setMarkingFunctions(new MarkingFunctions.Default());
        compositeLogic.setQueryLogics(logics);
        compositeLogic.setResponseObjectFactory(new DefaultResponseObjectFactory());

        RemoteQueryServiceTestUtil.QueryRunnable r = new RemoteQueryServiceTestUtil.QueryRunnable(compositeLogic);
        r.run();

        assertTrue(r.isCaught().get());
        assertTrue(r.getException() instanceof CompositeLogicException);
        assertTrue(r.getException().getMessage().startsWith("Failed to retrieve results"));
    }

    @Test
    public void testCompositeRemoteWithTimeoutOnResultsPassThrough() {
        logic.setSuppressTimeout(true);

        RemoteQueryService remoteQueryService = ((RemoteEventQueryLogic) remoteDelegate).getRemoteQueryService();
        RemoteQueryServiceProperties remoteQueryServiceProperties = remoteQueryService.getRemoteQueryServiceProperties();

        // only wait 100ms for the read
        remoteQueryServiceProperties.setNextTimeout(100);

        logic.setMarkingFunctions(new MarkingFunctions.Default());
        logic.setResponseObjectFactory(new DefaultResponseObjectFactory());

        HttpHandler foreverHandler = new RemoteQueryServiceTestUtil.ForeverHandler(interrupt);

        testUtil.updateRoute("/DataWave/Query/" + uuid.toString() + "/next", foreverHandler);

        Map<String,QueryLogic<?>> logics = new HashMap<>();

        logics.put("RemoteLogic", logic);

        CompositeQueryLogic compositeLogic = new CompositeQueryLogic();
        compositeLogic.setMaxPageSize(1);
        compositeLogic.setMarkingFunctions(new MarkingFunctions.Default());
        compositeLogic.setQueryLogics(logics);
        compositeLogic.setResponseObjectFactory(new DefaultResponseObjectFactory());

        RemoteQueryServiceTestUtil.QueryRunnable r = new RemoteQueryServiceTestUtil.QueryRunnable(compositeLogic, 0, QUERY, testUtil);
        r.run();

        assertFalse(r.isCaught().get());
    }

    @Test
    public void testCompositeGetPlanTimeout() {
        RemoteQueryService remoteQueryService = ((RemoteEventQueryLogic) remoteDelegate).getRemoteQueryService();
        RemoteQueryServiceProperties remoteQueryServiceProperties = remoteQueryService.getRemoteQueryServiceProperties();

        // only wait 100ms for the read
        remoteQueryServiceProperties.setPlanTimeout(100);

        logic.setMarkingFunctions(new MarkingFunctions.Default());
        logic.setResponseObjectFactory(new DefaultResponseObjectFactory());

        HttpHandler foreverHandler = new RemoteQueryServiceTestUtil.ForeverHandler(interrupt);

        testUtil.updateRoute("/DataWave/Query/" + DEFAULT_REMOTE_LOGIC + "/plan", foreverHandler);

        Map<String,QueryLogic<?>> logics = new HashMap<>();

        logics.put("RemoteLogic", logic);

        CompositeQueryLogic compositeLogic = new CompositeQueryLogic();
        compositeLogic.setMaxPageSize(1);
        compositeLogic.setMarkingFunctions(new MarkingFunctions.Default());
        compositeLogic.setQueryLogics(logics);
        compositeLogic.setResponseObjectFactory(new DefaultResponseObjectFactory());

        RemoteQueryServiceTestUtil.QueryRunnable r = new RemoteQueryServiceTestUtil.QueryRunnable(compositeLogic);
        r.setPlanTest(true);
        r.run();

        assertTrue(r.isCaught().get());
        assertTrue(r.getException() instanceof RemoteTimeoutQueryException);
    }

    @Test
    public void testCompositeGetPlanTimeoutPassThrough() {
        logic.setSuppressTimeout(true);

        RemoteQueryService remoteQueryService = ((RemoteEventQueryLogic) remoteDelegate).getRemoteQueryService();
        RemoteQueryServiceProperties remoteQueryServiceProperties = remoteQueryService.getRemoteQueryServiceProperties();

        // only wait 100ms for the read
        remoteQueryServiceProperties.setPlanTimeout(100);

        logic.setMarkingFunctions(new MarkingFunctions.Default());
        logic.setResponseObjectFactory(new DefaultResponseObjectFactory());

        HttpHandler foreverHandler = new RemoteQueryServiceTestUtil.ForeverHandler(interrupt);

        testUtil.updateRoute("/DataWave/Query/" + DEFAULT_REMOTE_LOGIC + "/plan", foreverHandler);

        Map<String,QueryLogic<?>> logics = new HashMap<>();

        logics.put("RemoteLogic", logic);

        CompositeQueryLogic compositeLogic = new CompositeQueryLogic();
        compositeLogic.setMaxPageSize(1);
        compositeLogic.setMarkingFunctions(new MarkingFunctions.Default());
        compositeLogic.setQueryLogics(logics);
        compositeLogic.setResponseObjectFactory(new DefaultResponseObjectFactory());

        RemoteQueryServiceTestUtil.QueryRunnable r = new RemoteQueryServiceTestUtil.QueryRunnable(compositeLogic);
        r.setPlanTest(true);
        r.run();

        assertFalse(r.isCaught().get());
        assertEquals("1: " + RemoteTimeoutInterceptingQueryLogic.REMOTE_TIMEOUT_PLAN, r.getPlan());
    }

    @Test
    public void testListEffectiveAuthorizations() throws AuthorizationException {
        ((RemoteQueryLogic) remoteDelegate).setUserOperations(new AlwaysRemoteAuthorizationExceptionUserOperations());

        assertThrows(RemoteAuthorizationException.class, () -> logic.getUserOperations().listEffectiveAuthorizations(null));
    }

    @Test
    public void testListEffectiveAuthorizationsSuppressed() throws AuthorizationException {
        logic.setSuppressTimeout(true);

        ((RemoteQueryLogic) remoteDelegate).setUserOperations(new AlwaysRemoteAuthorizationExceptionUserOperations());

        SubjectIssuerDNPair dn = SubjectIssuerDNPair.of("userDn", "issuerDn");
        DatawaveUser user = new DatawaveUser(dn, DatawaveUser.UserType.USER, Sets.newHashSet(), null, null, -1L);
        DatawavePrincipal datawavePrincipal = new DatawavePrincipal(Collections.singleton(user));

        logic.getUserOperations().listEffectiveAuthorizations(datawavePrincipal);
    }

    @Test
    public void testFlushCachedCredentials() throws AuthorizationException {
        ((RemoteQueryLogic) remoteDelegate).setUserOperations(new AlwaysRemoteAuthorizationExceptionUserOperations());

        assertThrows(RemoteAuthorizationException.class, () -> logic.getUserOperations().flushCachedCredentials(null));
    }

    @Test
    public void testFlushCachedCredentialsSuppressed() throws AuthorizationException {
        logic.setSuppressTimeout(true);

        ((RemoteQueryLogic) remoteDelegate).setUserOperations(new AlwaysRemoteAuthorizationExceptionUserOperations());

        logic.getUserOperations().flushCachedCredentials(null);
    }

    @Test
    public void testCompositeListEffectAuths() {
        ((RemoteQueryLogic) remoteDelegate).setUserOperations(new AlwaysRemoteAuthorizationExceptionUserOperations());

        logic.setMarkingFunctions(new MarkingFunctions.Default());
        logic.setResponseObjectFactory(new DefaultResponseObjectFactory());

        HttpHandler foreverHandler = new RemoteQueryServiceTestUtil.ForeverHandler(interrupt);

        testUtil.updateRoute("/DataWave/Query/" + DEFAULT_REMOTE_LOGIC + "/plan", foreverHandler);

        Map<String,QueryLogic<?>> logics = new HashMap<>();

        logics.put("RemoteLogic", logic);

        CompositeQueryLogic compositeLogic = new CompositeQueryLogic();
        compositeLogic.setMaxPageSize(1);
        compositeLogic.setMarkingFunctions(new MarkingFunctions.Default());
        compositeLogic.setQueryLogics(logics);
        compositeLogic.setResponseObjectFactory(new DefaultResponseObjectFactory());

        RemoteQueryServiceTestUtil.QueryRunnable r = new RemoteQueryServiceTestUtil.QueryRunnable(compositeLogic);
        r.run();

        assertTrue(r.isCaught().get());
        assertNotNull(r.getException());
        assertTrue(r.getException().getCause() instanceof AuthorizationException);
    }

    @Test
    public void testCompositeListEffectAuthsSuppressed() {
        logic.setSuppressTimeout(true);

        ((RemoteQueryLogic) remoteDelegate).setUserOperations(new AlwaysRemoteAuthorizationExceptionUserOperations());

        logic.setMarkingFunctions(new MarkingFunctions.Default());
        logic.setResponseObjectFactory(new DefaultResponseObjectFactory());

        Map<String,QueryLogic<?>> logics = new HashMap<>();

        logics.put("RemoteLogic", logic);

        CompositeQueryLogic compositeLogic = new CompositeQueryLogic();
        compositeLogic.setMaxPageSize(1);
        compositeLogic.setMarkingFunctions(new MarkingFunctions.Default());
        compositeLogic.setQueryLogics(logics);
        compositeLogic.setResponseObjectFactory(new DefaultResponseObjectFactory());

        RemoteQueryServiceTestUtil.QueryRunnable r = new RemoteQueryServiceTestUtil.QueryRunnable(compositeLogic, 0, QUERY, testUtil);
        r.run();

        assertFalse(r.isCaught().get());
    }

    // RemoteUserOperationsImpl is in web-services/security which is outside the scope of this package, so simulate this
    private static class AlwaysRemoteAuthorizationExceptionUserOperations implements UserOperations {
        @Override
        public AuthorizationsListBase listEffectiveAuthorizations(ProxiedUserDetails callerObject) throws AuthorizationException {
            throw new RemoteAuthorizationException("test listEffectiveAuthorizations", null);
        }

        @Override
        public GenericResponse<String> flushCachedCredentials(ProxiedUserDetails callerObject) throws AuthorizationException {
            throw new RemoteAuthorizationException("test flushCachedCredentials", null);
        }
    }
}
