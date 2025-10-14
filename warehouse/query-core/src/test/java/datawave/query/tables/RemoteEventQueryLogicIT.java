package datawave.query.tables;

import static datawave.query.tables.RemoteQueryServiceTestUtil.DEFAULT_REMOTE_LOGIC;
import static datawave.query.tables.RemoteQueryServiceTestUtil.NON_ROUTABLE_HOST;
import static datawave.webservice.common.remote.RemoteServiceUtil.ForeverHandler;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.sun.net.httpserver.HttpHandler;

import datawave.common.test.integration.IntegrationTest;
import datawave.core.query.remote.RemoteTimeoutQueryException;
import datawave.core.query.remote.RemoteTimeoutQueryRuntimeException;
import datawave.webservice.common.remote.RemoteHttpService;
import datawave.webservice.common.remote.RemoteHttpServiceConfiguration;
import datawave.webservice.query.remote.RemoteQueryServiceImpl;

@Category(IntegrationTest.class)
public class RemoteEventQueryLogicIT {
    private RemoteEventQueryLogic logic = new RemoteEventQueryLogic();
    private RemoteQueryServiceTestUtil testUtil;

    private UUID uuid;
    private AtomicBoolean handlerInterrupt = new AtomicBoolean(false);

    @Before
    public void setup() throws Exception {
        uuid = UUID.randomUUID();
        handlerInterrupt.set(false);
        testUtil = new RemoteQueryServiceTestUtil(uuid, DEFAULT_REMOTE_LOGIC, 3);
        testUtil.initialize();

        // create a remote event query logic that has our own server behind it
        RemoteQueryServiceImpl remote = (RemoteQueryServiceImpl) testUtil.getRemoteService();

        logic.setRemoteQueryService(remote);
        logic.setRemoteQueryLogic(DEFAULT_REMOTE_LOGIC);
    }

    @After
    public void after() {
        handlerInterrupt.set(true);
        testUtil.stop();
    }

    @Test
    public void testRemoteQuery() {
        RemoteQueryServiceTestUtil.QueryRunnable r = new RemoteQueryServiceTestUtil.QueryRunnable(logic, testUtil);
        r.run();
    }

    @Test
    public void testDefaultConnectTimeoutHangForever() {
        // override the endpoint to a non-routable ip, so it will block forever
        ((RemoteHttpService) logic.getRemoteQueryService()).setQueryServiceHost(NON_ROUTABLE_HOST);

        RemoteQueryServiceTestUtil.QueryRunnable r = new RemoteQueryServiceTestUtil.QueryRunnable(logic);
        Thread t = new Thread(r);
        t.start();

        // ensure the thread wasn't interrupted
        boolean done = false;
        while (!done) {
            try {
                // waiting forever is the default state, 5s will do
                t.join(5000);
                done = true;
            } catch (InterruptedException e) {
                // no-op
            }
        }

        // this would be TERMINATED if the thread ran
        assertTrue(t.getState().toString(), t.getState() == Thread.State.RUNNABLE);
        assertFalse(r.isSetup().get());
        assertFalse(r.isCaught().get());
    }

    @Test
    public void testConnectTimeoutQuery() throws Exception {
        RemoteHttpService remoteHttpService = (RemoteHttpService) logic.getRemoteQueryService();
        RemoteHttpServiceConfiguration remoteConfig = remoteHttpService.getConfig();

        // set a super fast connect timeout
        remoteConfig.setConnectTimeout(1);

        // override the endpoint to a non-routable ip
        ((RemoteHttpService) logic.getRemoteQueryService()).setQueryServiceHost(NON_ROUTABLE_HOST);

        RemoteQueryServiceTestUtil.QueryRunnable runnable = new RemoteQueryServiceTestUtil.QueryRunnable(logic);
        runnable.run();

        assertTrue(runnable.isCaught().get());

        assertNotNull(runnable.getException());
        assertTrue(runnable.getException() instanceof RemoteTimeoutQueryException);
    }

    @Test
    public void testDefaultSocketTimeout() throws InterruptedException {
        HttpHandler foreverHandler = new ForeverHandler(handlerInterrupt);

        testUtil.updateRoute("/DataWave/Query/" + DEFAULT_REMOTE_LOGIC + "/create", foreverHandler);

        // execute this in a thread so it can be interrupted
        RemoteQueryServiceTestUtil.QueryRunnable r = new RemoteQueryServiceTestUtil.QueryRunnable(logic);

        Thread t = new Thread(r);
        t.start();

        // ensure the thread wasn't interrupted
        boolean done = false;
        while (!done) {
            try {
                // waiting forever is the default state, 5s will do
                t.join(5000);
                done = true;
            } catch (InterruptedException e) {
                // no-op
            }
        }

        // this would be TERMINATED if the thread ran
        assertTrue(t.getState().toString(), t.getState() == Thread.State.RUNNABLE);
        assertFalse(r.isSetup().get());
        assertFalse(r.isCaught().get());
    }

    @Test
    public void testSocketTimeout() {
        RemoteHttpService remoteHttpService = (RemoteHttpService) logic.getRemoteQueryService();
        RemoteHttpServiceConfiguration remoteConfig = remoteHttpService.getConfig();

        // set a super fast socket timeout
        remoteConfig.setSocketTimeout(1);

        HttpHandler foreverHandler = new ForeverHandler(handlerInterrupt);

        testUtil.updateRoute("/DataWave/Query/" + DEFAULT_REMOTE_LOGIC + "/create", foreverHandler);

        // track query execution state
        RemoteQueryServiceTestUtil.QueryRunnable r = new RemoteQueryServiceTestUtil.QueryRunnable(logic);
        r.run();

        assertFalse(r.isSetup().get());
        // the socket timeout will cause this to throw an exception prior to the interrupt being sent
        assertTrue(r.isCaught().get());
        assertNotNull(r.getException());
        assertTrue(r.getException() instanceof RemoteTimeoutQueryException);
    }

    @Test(timeout = 30000)
    public void testDefaultConnectionPoolTimeout() {
        RemoteHttpService remoteHttpService = (RemoteHttpService) logic.getRemoteQueryService();
        RemoteHttpServiceConfiguration remoteConfig = remoteHttpService.getConfig();

        // only allow a single max connection
        remoteConfig.setMaxConnections(1);

        // patch in the forever handler which will block until its unlocked
        HttpHandler foreverHandler = new ForeverHandler(handlerInterrupt);

        testUtil.updateRoute("/DataWave/Query/" + DEFAULT_REMOTE_LOGIC + "/create", foreverHandler);

        // create two threads that both access the forever handler
        // execute this in a thread so it can be interrupted
        RemoteQueryServiceTestUtil.QueryRunnable r1 = new RemoteQueryServiceTestUtil.QueryRunnable(logic);
        RemoteQueryServiceTestUtil.QueryRunnable r2 = new RemoteQueryServiceTestUtil.QueryRunnable(logic);

        // start both threads
        Thread t1 = new Thread(r1);
        t1.start();
        Thread t2 = new Thread(r2);
        t2.start();

        boolean done = false;
        while (!done) {
            try {
                // wait for one of the threads to get into a WAIT state
                if (t1.getState() == Thread.State.WAITING || t2.getState() == Thread.State.WAITING) {
                    done = true;
                }
                t1.join(100);
            } catch (InterruptedException e) {
                // no-op
            }
        }

        // when WAITING for a connection one of the threads should be stuck here
        boolean t1Waiting = Arrays.stream(t1.getStackTrace()).anyMatch(stackTrace -> stackTrace.getMethodName().equals("getPoolEntryBlocking")
                        && stackTrace.getClassName().equals("org.apache.http.pool.AbstractConnPool"));
        boolean t2Waiting = Arrays.stream(t2.getStackTrace()).anyMatch(stackTrace -> stackTrace.getMethodName().equals("getPoolEntryBlocking")
                        && stackTrace.getClassName().equals("org.apache.http.pool.AbstractConnPool"));

        assertTrue(t1Waiting || t2Waiting);
        assertFalse(r1.isSetup().get());
        assertFalse(r2.isSetup().get());
    }

    @Test
    public void testConnectionPoolTimeout() {
        RemoteHttpService remoteHttpService = (RemoteHttpService) logic.getRemoteQueryService();
        RemoteHttpServiceConfiguration remoteConfig = remoteHttpService.getConfig();

        // only allow a single max connection
        remoteConfig.setMaxConnections(1);
        // only wait 1ms for a thread
        remoteConfig.setConnectionPoolTimeout(1);

        // patch in the forever handler which will block until its unlocked
        HttpHandler foreverHandler = new ForeverHandler(handlerInterrupt);

        testUtil.updateRoute("/DataWave/Query/" + DEFAULT_REMOTE_LOGIC + "/create", foreverHandler);

        // create two threads that both access the forever handler
        // execute this in a thread so it can be interrupted
        RemoteQueryServiceTestUtil.QueryRunnable r1 = new RemoteQueryServiceTestUtil.QueryRunnable(logic);
        RemoteQueryServiceTestUtil.QueryRunnable r2 = new RemoteQueryServiceTestUtil.QueryRunnable(logic);

        // start both threads
        Thread t1 = new Thread(r1);
        t1.start();
        Thread t2 = new Thread(r2);
        t2.start();

        // wait until the exception is caught
        while (!r1.isCaught().get() && !r2.isCaught().get()) {
            try {
                t1.join(100);
            } catch (InterruptedException e) {
                // no-op
            }
        }

        assertFalse(r1.isSetup().get());
        assertFalse(r2.isSetup().get());

        // only one thread had an exception
        assertFalse(r1.isCaught().get() && r2.isCaught().get());
        assertTrue(r1.getException() != null || r2.getException() != null);
        assertTrue(r1.getException() == null || r1.getException() instanceof RemoteTimeoutQueryException);
        assertTrue(r2.getException() == null || r2.getException() instanceof RemoteTimeoutQueryException);
    }

    @Test
    public void testIteratorTimeout() {
        RemoteHttpService remoteHttpService = (RemoteHttpService) logic.getRemoteQueryService();
        RemoteHttpServiceConfiguration remoteConfig = remoteHttpService.getConfig();

        // only wait 100ms for the read
        remoteConfig.setSocketTimeout(100);

        // patch in the forever handler which will block until its unlocked
        HttpHandler foreverHandler = new ForeverHandler(handlerInterrupt);

        testUtil.updateRoute("/DataWave/Query/" + uuid.toString() + "/next", foreverHandler);

        RemoteQueryServiceTestUtil.QueryRunnable r1 = new RemoteQueryServiceTestUtil.QueryRunnable(logic);
        r1.run();
        assertTrue(r1.isCaught().get());
        assertTrue(r1.getException().getMessage(), r1.getException() instanceof RemoteTimeoutQueryRuntimeException);
    }
}
