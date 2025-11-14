package datawave.query.tables;

import static datawave.query.tables.RemoteQueryServiceTestUtil.DEFAULT_REMOTE_LOGIC;
import static datawave.query.tables.RemoteQueryServiceTestUtil.NON_ROUTABLE_HOST;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.bouncycastle.operator.OperatorCreationException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Sets;
import com.sun.net.httpserver.HttpHandler;

import datawave.authorization.remote.RemoteAuthorizationException;
import datawave.common.test.integration.IntegrationTest;
import datawave.core.query.logic.QueryLogic;
import datawave.core.query.logic.composite.CompositeLogicException;
import datawave.core.query.logic.composite.CompositeQueryLogic;
import datawave.core.query.remote.RemoteQueryService;
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
import datawave.webservice.common.remote.RemoteHttpServiceConfiguration;
import datawave.webservice.result.GenericResponse;

@Category(IntegrationTest.class)
public class RemoteTimeoutInterceptingQueryLogicIT {
    private static final String QUERY = "Grinning\uD83D\uDE00Face";

    private RemoteTimeoutInterceptingQueryLogic logic;
    private QueryLogic remoteDelegate;
    private RemoteQueryService remoteQueryService;
    private RemoteQueryServiceTestUtil testUtil;
    private UUID uuid;
    private AtomicBoolean interrupt = new AtomicBoolean(false);

    @Before
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

    @After
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
        RemoteHttpService remoteHttpService = (RemoteHttpService) ((RemoteEventQueryLogic) remoteDelegate).getRemoteQueryService();
        RemoteHttpServiceConfiguration remoteConfig = remoteHttpService.getConfig();
        remoteHttpService.setQueryServiceHost(NON_ROUTABLE_HOST);

        // set a super fast connect timeout
        remoteConfig.setConnectTimeout(100);

        HttpHandler foreverHandler = new RemoteQueryServiceTestUtil.ForeverHandler(interrupt);
        testUtil.updateRoute("/DataWave/Query/" + DEFAULT_REMOTE_LOGIC + "/create", foreverHandler);

        RemoteQueryServiceTestUtil.QueryRunnable r = new RemoteQueryServiceTestUtil.QueryRunnable(logic);
        r.run();

        assertTrue(r.isCaught().get());
        assertTrue(r.getException() instanceof RemoteTimeoutQueryException);
    }

    @Test
    public void suppressedConnectTimeoutTest() {
        RemoteHttpService remoteHttpService = (RemoteHttpService) ((RemoteEventQueryLogic) remoteDelegate).getRemoteQueryService();
        RemoteHttpServiceConfiguration remoteConfig = remoteHttpService.getConfig();
        remoteHttpService.setQueryServiceHost(NON_ROUTABLE_HOST);

        // set a super fast connect timeout
        remoteConfig.setConnectTimeout(100);

        HttpHandler foreverHandler = new RemoteQueryServiceTestUtil.ForeverHandler(interrupt);
        testUtil.updateRoute("/DataWave/Query/" + DEFAULT_REMOTE_LOGIC + "/create", foreverHandler);

        logic.setSuppressTimeout(true);
        RemoteQueryServiceTestUtil.QueryRunnable r = new RemoteQueryServiceTestUtil.QueryRunnable(logic, 0, "FIELD:value", testUtil);
        r.run();

        assertFalse(r.isCaught().get());
    }

    @Test
    public void unsuppressedSocketTimeoutTest() {
        RemoteHttpService remoteHttpService = (RemoteHttpService) ((RemoteEventQueryLogic) remoteDelegate).getRemoteQueryService();
        RemoteHttpServiceConfiguration remoteConfig = remoteHttpService.getConfig();

        // set a super fast connect timeout
        remoteConfig.setSocketTimeout(100);

        HttpHandler foreverHandler = new RemoteQueryServiceTestUtil.ForeverHandler(interrupt);
        testUtil.updateRoute("/DataWave/Query/" + DEFAULT_REMOTE_LOGIC + "/create", foreverHandler);

        RemoteQueryServiceTestUtil.QueryRunnable r = new RemoteQueryServiceTestUtil.QueryRunnable(logic);
        r.run();

        assertTrue(r.isCaught().get());
        assertTrue(r.getException() instanceof RemoteTimeoutQueryException);
    }

    @Test
    public void suppressedSocketTimeoutTest() {
        RemoteHttpService remoteHttpService = (RemoteHttpService) ((RemoteEventQueryLogic) remoteDelegate).getRemoteQueryService();
        RemoteHttpServiceConfiguration remoteConfig = remoteHttpService.getConfig();

        // set a super fast connect timeout
        remoteConfig.setSocketTimeout(100);

        HttpHandler foreverHandler = new RemoteQueryServiceTestUtil.ForeverHandler(interrupt);
        testUtil.updateRoute("/DataWave/Query/" + DEFAULT_REMOTE_LOGIC + "/create", foreverHandler);

        logic.setSuppressTimeout(true);
        RemoteQueryServiceTestUtil.QueryRunnable r = new RemoteQueryServiceTestUtil.QueryRunnable(logic, 0, "FIELD:value", testUtil);
        r.run();

        assertFalse(r.isCaught().get());
    }

    @Test
    public void unsuppressedIteratorTimeoutTest() {
        RemoteHttpService remoteHttpService = (RemoteHttpService) ((RemoteEventQueryLogic) remoteDelegate).getRemoteQueryService();
        RemoteHttpServiceConfiguration remoteConfig = remoteHttpService.getConfig();

        // set a super fast connect timeout
        remoteConfig.setSocketTimeout(100);

        HttpHandler foreverHandler = new RemoteQueryServiceTestUtil.ForeverHandler(interrupt);
        testUtil.updateRoute("/DataWave/Query/" + uuid + "/next", foreverHandler);

        RemoteQueryServiceTestUtil.QueryRunnable r = new RemoteQueryServiceTestUtil.QueryRunnable(logic);
        r.run();

        assertTrue(r.isCaught().get());
        assertTrue(r.getException() instanceof RemoteTimeoutQueryRuntimeException);
    }

    @Test
    public void suppressedIteratorTimeoutTest() {
        RemoteHttpService remoteHttpService = (RemoteHttpService) ((RemoteEventQueryLogic) remoteDelegate).getRemoteQueryService();
        RemoteHttpServiceConfiguration remoteConfig = remoteHttpService.getConfig();

        // set a super fast connect timeout
        remoteConfig.setSocketTimeout(100);

        HttpHandler foreverHandler = new RemoteQueryServiceTestUtil.ForeverHandler(interrupt);
        testUtil.updateRoute("/DataWave/Query/" + uuid + "/next", foreverHandler);

        logic.setSuppressTimeout(true);
        RemoteQueryServiceTestUtil.QueryRunnable r = new RemoteQueryServiceTestUtil.QueryRunnable(logic, 0, "FIELD:value", testUtil);
        r.run();

        assertFalse(r.isCaught().get());
    }

    @Test
    public void testCompositeRemoteWithTimeoutOnInitialize() {
        RemoteHttpService remoteHttpService = (RemoteHttpService) ((RemoteEventQueryLogic) remoteDelegate).getRemoteQueryService();
        RemoteHttpServiceConfiguration remoteConfig = remoteHttpService.getConfig();

        // only wait 100ms for the read
        remoteConfig.setSocketTimeout(10);

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

        RemoteHttpService remoteHttpService = (RemoteHttpService) ((RemoteEventQueryLogic) remoteDelegate).getRemoteQueryService();
        RemoteHttpServiceConfiguration remoteConfig = remoteHttpService.getConfig();

        // only wait 100ms for the read
        remoteConfig.setSocketTimeout(100);

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
        RemoteHttpService remoteHttpService = (RemoteHttpService) ((RemoteEventQueryLogic) remoteDelegate).getRemoteQueryService();
        RemoteHttpServiceConfiguration remoteConfig = remoteHttpService.getConfig();

        // only wait 100ms for the read
        remoteConfig.setSocketTimeout(100);

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

        RemoteHttpService remoteHttpService = (RemoteHttpService) ((RemoteEventQueryLogic) remoteDelegate).getRemoteQueryService();
        RemoteHttpServiceConfiguration remoteConfig = remoteHttpService.getConfig();

        // only wait 100ms for the read
        remoteConfig.setSocketTimeout(100);

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
        RemoteHttpService remoteHttpService = (RemoteHttpService) ((RemoteEventQueryLogic) remoteDelegate).getRemoteQueryService();
        RemoteHttpServiceConfiguration remoteConfig = remoteHttpService.getConfig();

        // only wait 100ms for the read
        remoteConfig.setSocketTimeout(100);

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

        RemoteHttpService remoteHttpService = (RemoteHttpService) ((RemoteEventQueryLogic) remoteDelegate).getRemoteQueryService();
        RemoteHttpServiceConfiguration remoteConfig = remoteHttpService.getConfig();

        // only wait 100ms for the read
        remoteConfig.setSocketTimeout(100);

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

    @Test(expected = RemoteAuthorizationException.class)
    public void testListEffectiveAuthorizations() throws AuthorizationException {
        ((RemoteQueryLogic) remoteDelegate).setUserOperations(new AlwaysRemoteAuthorizationExceptionUserOperations());

        logic.getUserOperations().listEffectiveAuthorizations(null);
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

    @Test(expected = RemoteAuthorizationException.class)
    public void testFlushCachedCredentials() throws AuthorizationException {
        ((RemoteQueryLogic) remoteDelegate).setUserOperations(new AlwaysRemoteAuthorizationExceptionUserOperations());

        logic.getUserOperations().flushCachedCredentials(null);
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
