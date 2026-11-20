package datawave.webservice.query.runner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.ignoreStubs;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.commons.collections4.Transformer;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.query.cache.ResultsPage;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.exception.EmptyObjectException;
import datawave.core.query.iterator.DatawaveTransformIterator;
import datawave.core.query.logic.BaseQueryLogic;
import datawave.core.query.logic.QueryLogic;
import datawave.core.query.logic.QueryLogicTransformer;
import datawave.core.query.logic.ResponseEnricher;
import datawave.core.query.logic.composite.CompositeQueryLogic;
import datawave.core.query.logic.composite.CompositeQueryLogicResults;
import datawave.core.query.logic.composite.CompositeQueryLogicTransformer;
import datawave.core.query.predict.QueryPredictor;
import datawave.microservice.authorization.util.AuthorizationsUtil;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.BaseQueryMetric.Prediction;
import datawave.microservice.querymetric.QueryMetricFactoryImpl;
import datawave.security.authorization.AuthorizationException;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.DatawaveUser.UserType;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.util.DnProperties;
import datawave.webservice.query.logic.TestQueryLogic;
import datawave.webservice.query.logic.composite.CompositeQueryLogicTest;
import datawave.webservice.query.result.event.DefaultEvent;
import datawave.webservice.query.util.QueryUncaughtExceptionHandler;
import datawave.webservice.result.BaseQueryResponse;

public class RunningQueryTest {

    class SampleGenericQueryConfiguration extends GenericQueryConfiguration {
        // GenericQueryConfiguration is abstract. Looks like we are using the GenericShardQueryConfiguration
        // in the datawave-query package. This is not a dependency in maven though, but added from the lib. See
        // the setup() below. Here is an instantiatable class for testing.
        @SuppressWarnings("unused")
        private static final long serialVersionUID = -2981779719095867631L;
    }

    // variables common to all current tests
    private final QueryImpl settings = new QueryImpl();
    private final AccumuloConnectionFactory.Priority connectionPriority = AccumuloConnectionFactory.Priority.NORMAL;
    private String methodAuths = "";
    private SubjectIssuerDNPair userDN = SubjectIssuerDNPair.of("userDn", "issuerDn");
    private final QueryLogic<?> logic = mock(BaseQueryLogic.class);

    @BeforeEach
    public void setup() throws MalformedURLException, IllegalArgumentException, IllegalAccessException {

        System.setProperty(DnProperties.NPE_OU_PROPERTY, "iamnotaperson");
        System.setProperty("dw.metadatahelper.all.auths", "A,B,C,D");

        settings.setQueryLogicName("testQueryLogic");
        settings.setQuery("FOO == BAR");
        settings.setQueryName("test");
        settings.setColumnVisibility("A&B");
        settings.setBeginDate(new Date());
        settings.setEndDate(new Date());
        settings.setQueryAuthorizations("NONE");
        settings.setExpirationDate(new Date());
        settings.setPagesize(10);
        settings.setId(UUID.randomUUID());

        // get the files in conf and the jars in the lib directory and add them
        // to the classpath
        // so we can run this test. See notes in pom.xml under the
        // copy-dependencies plugin
        List<URL> additionalURLs = new LinkedList<>();
        additionalURLs.add(new File("conf/").toURI().toURL());

        URL[] urlsArray = additionalURLs.toArray(new URL[additionalURLs.size()]);
        ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();
        URLClassLoader urlClassloader = new URLClassLoader(urlsArray, currentClassLoader);
        Thread.currentThread().setContextClassLoader(urlClassloader);

    }

    enum WorkerExit {
        EXHAUSTED, TRANSFORM_FAILURE, ITERATOR_FAILURE
    }

    @ParameterizedTest(name = "worker exit after partial page: {0}")
    @EnumSource(WorkerExit.class)
    @Timeout(15)
    @SuppressWarnings("unchecked")
    void differentiatesWorkerFailureFromExhaustionBetweenPages(WorkerExit workerExit) throws Exception {
        CountDownLatch releaseWorker = new CountDownLatch(1);
        IllegalStateException transformFailure = new IllegalStateException("transform failed between pages");
        RuntimeException iteratorFailure = new RuntimeException("underlying iterator failed between pages");
        DefaultEvent intermediate = new DefaultEvent();
        intermediate.setIntermediateResult(true);
        Iterator<DefaultEvent> input = mock(Iterator.class);
        // DatawaveTransformIterator probes the input again after transforming the intermediate event.
        when(input.hasNext()).thenReturn(true, true).thenAnswer(invocation -> {
            releaseWorker.await();
            return workerExit != WorkerExit.EXHAUSTED;
        });
        switch (workerExit) {
            case ITERATOR_FAILURE:
                when(input.next()).thenReturn(intermediate).thenThrow(iteratorFailure);
                break;
            case TRANSFORM_FAILURE:
                when(input.next()).thenReturn(intermediate, new DefaultEvent());
                break;
            case EXHAUSTED:
                when(input.next()).thenReturn(intermediate);
                break;
        }
        Transformer<DefaultEvent,DefaultEvent> transformer = event -> {
            if (event != intermediate) {
                throw transformFailure;
            }
            return event;
        };
        TransformIterator<DefaultEvent,DefaultEvent> iterator = new DatawaveTransformIterator<>(input, transformer);

        QueryLogic<?> workerLogic = mock(QueryLogic.class);
        when(workerLogic.initialize(any(), any(), any())).thenReturn(new SampleGenericQueryConfiguration());
        when(workerLogic.getTransformIterator(settings)).thenReturn(iterator);
        when(workerLogic.isIntermediateEmptyPagesEnabled()).thenReturn(true);
        when(workerLogic.getMaxPageSize()).thenReturn(10);
        when(workerLogic.getMaxWork()).thenReturn(-1L);
        when(workerLogic.getMaxResults()).thenReturn(-1L);
        when(workerLogic.getResultLimit(settings)).thenReturn(-1L);

        DatawaveUser user = new DatawaveUser(userDN, UserType.USER, null, null, null, 0L);
        DatawavePrincipal principal = new DatawavePrincipal(Collections.singletonList(user));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (AccumuloClient client = new InMemoryAccumuloClient("root", new InMemoryInstance("worker failure test"))) {
            RunningQuery query = new RunningQuery(null, client, connectionPriority, workerLogic, settings, methodAuths, principal, null, null,
                            new QueryMetricFactoryImpl());
            query.setExecutor(executor);
            // The intermediate result ends the page; no page deadline or sleep is involved.
            ResultsPage partial = query.next();
            assertEquals(ResultsPage.Status.PARTIAL, partial.getStatus());
            assertTrue(partial.getResults().isEmpty());
            releaseWorker.countDown();
            // A queued task proves the results worker exited before the next request starts.
            executor.submit(() -> {}).get(5, TimeUnit.SECONDS);
            switch (workerExit) {
                case TRANSFORM_FAILURE:
                    assertSame(transformFailure, assertThrows(IllegalStateException.class, query::next));
                    break;
                case ITERATOR_FAILURE:
                    assertSame(iteratorFailure, assertThrows(RuntimeException.class, query::next));
                    break;
                case EXHAUSTED:
                    assertEquals(ResultsPage.Status.NONE, query.next().getStatus());
                    break;
            }
        } finally {
            releaseWorker.countDown();
            executor.shutdownNow();
        }
    }

    enum NextOutcome {
        EXHAUSTED, ITERATOR_FAILURE, TRANSFORM_FAILURE
    }

    @ParameterizedTest(name = "plain TransformIterator after the partial page: {0}")
    @EnumSource(NextOutcome.class)
    @Timeout(15)
    @SuppressWarnings("unchecked")
    void distinguishesNextFailureAfterHasNextFromExhaustion(NextOutcome outcome) throws Exception {
        assertNull(settings.getUncaughtExceptionHandler(), "Exercise RunningQuery's own worker failure handling");
        CountDownLatch releaseWorker = new CountDownLatch(1);
        RuntimeException failure = new RuntimeException("failed after hasNext returned true");
        DefaultEvent intermediate = new DefaultEvent();
        intermediate.setIntermediateResult(true);
        Iterator<DefaultEvent> input = mock(Iterator.class);
        when(input.hasNext()).thenReturn(true).thenAnswer(invocation -> {
            releaseWorker.await();
            return outcome != NextOutcome.EXHAUSTED;
        });
        switch (outcome) {
            case ITERATOR_FAILURE:
                when(input.next()).thenReturn(intermediate).thenThrow(failure);
                break;
            case TRANSFORM_FAILURE:
                when(input.next()).thenReturn(intermediate, new DefaultEvent());
                break;
            case EXHAUSTED:
                when(input.next()).thenReturn(intermediate);
                break;
        }
        // Unlike DatawaveTransformIterator, the plain iterator does not fetch or transform during hasNext().
        TransformIterator<DefaultEvent,DefaultEvent> iterator = new TransformIterator<>(input, event -> {
            if (event != intermediate) {
                throw failure;
            }
            return event;
        });

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (AccumuloClient client = new InMemoryAccumuloClient("root", new InMemoryInstance("worker next failure test"))) {
            RunningQuery query = createAsyncQuery(client, executor, iterator);
            ResultsPage<?> firstPage = query.next();
            assertEquals(ResultsPage.Status.PARTIAL, firstPage.getStatus());
            assertTrue(firstPage.getResults().isEmpty());

            releaseWorker.countDown();
            // The queued task cannot run until the worker either exhausts the input or fails in next().
            executor.submit(() -> {}).get(5, TimeUnit.SECONDS);
            if (outcome == NextOutcome.EXHAUSTED) {
                assertEquals(ResultsPage.Status.NONE, query.next().getStatus());
            } else {
                assertSame(failure, assertThrows(RuntimeException.class, query::next));
            }
        } finally {
            releaseWorker.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    @Timeout(15)
    void reportsCompositeChildFailureAfterHasNextAdvertisesResult() throws Exception {
        assertNull(settings.getUncaughtExceptionHandler());
        CountDownLatch readingSecondResult = new CountDownLatch(1);
        CountDownLatch childFailureReported = new CountDownLatch(1);
        RuntimeException failure = new IllegalStateException("composite child query failed");
        CompositeQueryLogicResults results = new CompositeQueryLogicResults(mock(CompositeQueryLogic.class), 2, 1, TimeUnit.SECONDS);
        DefaultEvent intermediate = new DefaultEvent();
        intermediate.setIntermediateResult(true);
        results.add(intermediate);
        results.add(new DefaultEvent());

        // Use the same iterator and pass-through transformer as the non-checkpointable composite query path.
        TransformIterator<Object,Object> iterator = new TransformIterator<>(results.iterator(), new CompositeQueryLogicTransformer<>(List.of())) {
            private int nextCalls;

            @Override
            public Object next() {
                if (++nextCalls == 2) {
                    // RunningQuery has already observed hasNext() and incremented its counter. Let the child fail before the real next().
                    readingSecondResult.countDown();
                    try {
                        childFailureReported.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("Interrupted waiting for the child failure", e);
                    }
                }
                return super.next();
            }
        };

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (AccumuloClient client = new InMemoryAccumuloClient("root", new InMemoryInstance("composite next failure test"))) {
            RunningQuery query = createAsyncQuery(client, executor, iterator);
            ResultsPage<?> firstPage = query.next();
            assertEquals(ResultsPage.Status.PARTIAL, firstPage.getStatus());
            assertTrue(firstPage.getResults().isEmpty());

            assertTrue(readingSecondResult.await(5, TimeUnit.SECONDS));
            assertEquals(0, results.size(), "Composite hasNext() should have taken the second result from its queue");
            results.uncaughtException(Thread.currentThread(), failure);
            childFailureReported.countDown();
            executor.submit(() -> {}).get(5, TimeUnit.SECONDS);

            assertSame(failure, assertThrows(RuntimeException.class, query::next));
        } finally {
            childFailureReported.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    @Timeout(15)
    @SuppressWarnings("unchecked")
    void reportsNextFailureWhilePageWaitsForTheAdvertisedResult() throws Exception {
        CountDownLatch readingSecondResult = new CountDownLatch(1);
        CountDownLatch waitingForResult = new CountDownLatch(1);
        AtomicBoolean readingSecondPage = new AtomicBoolean();
        RuntimeException failure = new RuntimeException("worker failed while the page was waiting");
        DefaultEvent intermediate = new DefaultEvent();
        intermediate.setIntermediateResult(true);
        Iterator<DefaultEvent> input = mock(Iterator.class);
        when(input.hasNext()).thenReturn(true);
        when(input.next()).thenReturn(intermediate).thenAnswer(invocation -> {
            readingSecondResult.countDown();
            waitingForResult.await();
            throw failure;
        });
        TransformIterator<DefaultEvent,DefaultEvent> iterator = new TransformIterator<>(input, event -> event);
        RunningQuery.RunningQueryTiming timing = new RunningQuery.RunningQueryTimingNoOp() {
            @Override
            public long getPageShortCircuitTimeoutMs() {
                if (readingSecondPage.get()) {
                    // The advertised result keeps hasNext() from waiting. Only getNext() asks for this deadline on page two.
                    waitingForResult.countDown();
                }
                return super.getPageShortCircuitTimeoutMs();
            }
        };

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (AccumuloClient client = new InMemoryAccumuloClient("root", new InMemoryInstance("worker failure during wait"))) {
            RunningQuery query = createAsyncQuery(client, executor, iterator, timing);
            assertEquals(ResultsPage.Status.PARTIAL, query.next().getStatus());
            assertTrue(readingSecondResult.await(5, TimeUnit.SECONDS));
            readingSecondPage.set(true);

            assertSame(failure, assertThrows(RuntimeException.class, query::next));
            assertEquals(0, waitingForResult.getCount());
        } finally {
            waitingForResult.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    @Timeout(15)
    @SuppressWarnings("unchecked")
    void prefersRecordedNextFailureOverWaitTimeout() throws Exception {
        CountDownLatch releaseRead = new CountDownLatch(1);
        CountDownLatch failureRecorded = new CountDownLatch(1);
        CountDownLatch releaseHandler = new CountDownLatch(1);
        AtomicBoolean expiredDeadline = new AtomicBoolean();
        RuntimeException failure = new RuntimeException("worker failed before the wait deadline");
        QueryUncaughtExceptionHandler handler = mock(QueryUncaughtExceptionHandler.class);
        doAnswer(invocation -> {
            // RunningQuery records the failure before calling the handler. Keep the worker in that call so running remains true.
            failureRecorded.countDown();
            releaseHandler.await();
            return null;
        }).when(handler).uncaughtException(any(), any());
        // The mock does not expose the failure to the request thread through getUncaughtException().
        settings.setUncaughtExceptionHandler(handler);

        DefaultEvent intermediate = new DefaultEvent();
        intermediate.setIntermediateResult(true);
        Iterator<DefaultEvent> input = mock(Iterator.class);
        when(input.hasNext()).thenReturn(true);
        when(input.next()).thenReturn(intermediate).thenAnswer(invocation -> {
            releaseRead.await();
            throw failure;
        });
        TransformIterator<DefaultEvent,DefaultEvent> iterator = new TransformIterator<>(input, event -> event);
        RunningQuery.RunningQueryTiming timing = new RunningQuery.RunningQueryTimingNoOp() {
            @Override
            public long getPageShortCircuitTimeoutMs() {
                return expiredDeadline.get() ? 0 : super.getPageShortCircuitTimeoutMs();
            }
        };

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (AccumuloClient client = new InMemoryAccumuloClient("root", new InMemoryInstance("recorded failure at wait deadline"))) {
            RunningQuery query = createAsyncQuery(client, executor, iterator, timing);
            assertEquals(ResultsPage.Status.PARTIAL, query.next().getStatus());
            releaseRead.countDown();
            assertTrue(failureRecorded.await(5, TimeUnit.SECONDS));
            expiredDeadline.set(true);

            assertSame(failure, assertThrows(RuntimeException.class, query::next));
        } finally {
            releaseRead.countDown();
            releaseHandler.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    @Timeout(15)
    @SuppressWarnings("unchecked")
    void returnsQueuedResultBeforeReportingSubsequentNextFailure() throws Exception {
        settings.setPagesize(1);
        CountDownLatch releaseWorker = new CountDownLatch(1);
        RuntimeException failure = new RuntimeException("worker failed after queuing a result");
        DefaultEvent intermediate = new DefaultEvent();
        intermediate.setIntermediateResult(true);
        DefaultEvent queued = new DefaultEvent();
        Iterator<DefaultEvent> input = mock(Iterator.class);
        when(input.hasNext()).thenReturn(true).thenAnswer(invocation -> {
            releaseWorker.await();
            return true;
        });
        when(input.next()).thenReturn(intermediate, queued).thenThrow(failure);
        TransformIterator<DefaultEvent,DefaultEvent> iterator = new TransformIterator<>(input, event -> event);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (AccumuloClient client = new InMemoryAccumuloClient("root", new InMemoryInstance("worker failure with queued result"))) {
            RunningQuery query = createAsyncQuery(client, executor, iterator);
            assertEquals(ResultsPage.Status.PARTIAL, query.next().getStatus());
            releaseWorker.countDown();
            // Wait until the worker has both published the queued result and recorded the failure on the following item.
            executor.submit(() -> {}).get(5, TimeUnit.SECONDS);

            ResultsPage<?> resultPage = query.next();
            assertEquals(1, resultPage.getResults().size());
            assertSame(queued, resultPage.getResults().get(0));
            assertSame(failure, assertThrows(RuntimeException.class, query::next));
        } finally {
            releaseWorker.countDown();
            executor.shutdownNow();
        }
    }

    private RunningQuery createAsyncQuery(AccumuloClient client, ExecutorService executor, TransformIterator<?,?> iterator) throws Exception {
        return createAsyncQuery(client, executor, iterator, null);
    }

    private RunningQuery createAsyncQuery(AccumuloClient client, ExecutorService executor, TransformIterator<?,?> iterator,
                    RunningQuery.RunningQueryTiming timing) throws Exception {
        QueryLogic<?> workerLogic = mock(QueryLogic.class);
        when(workerLogic.initialize(any(), any(), any())).thenReturn(new SampleGenericQueryConfiguration());
        doReturn(iterator).when(workerLogic).getTransformIterator(settings);
        when(workerLogic.isIntermediateEmptyPagesEnabled()).thenReturn(true);
        when(workerLogic.getMaxPageSize()).thenReturn(10);
        when(workerLogic.getMaxWork()).thenReturn(-1L);
        when(workerLogic.getMaxResults()).thenReturn(-1L);
        when(workerLogic.getResultLimit(settings)).thenReturn(-1L);
        DatawaveUser user = new DatawaveUser(userDN, UserType.USER, null, null, null, 0L);
        DatawavePrincipal principal = new DatawavePrincipal(Collections.singletonList(user));
        RunningQuery query = new RunningQuery(null, client, connectionPriority, workerLogic, settings, methodAuths, principal, timing, null,
                        new QueryMetricFactoryImpl());
        query.setExecutor(executor);
        return query;
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testConstructorSetsConnection() throws Exception {
        DatawaveUser user = new DatawaveUser(userDN, UserType.USER, null, null, null, 0L);
        DatawavePrincipal principal = new DatawavePrincipal(Collections.singletonList(user));

        // setup mock connector
        InMemoryInstance instance = new InMemoryInstance("test instance");
        AccumuloClient client = new InMemoryAccumuloClient("root", instance);

        // setup mock logic, handles the setConnection method
        SampleGenericQueryConfiguration config = new SampleGenericQueryConfiguration();
        doReturn(config).when(logic).initialize(any(), any(), any());

        TransformIterator<?,?> iter = new TransformIterator<>();
        doReturn(Boolean.FALSE).when(logic).getCollectQueryMetrics();
        doReturn(iter).when(logic).getTransformIterator(settings);
        lenient().doReturn(true).when(logic).isQueryLimiterEnabled();
        doReturn(false).when(logic).isIntermediateEmptyPagesEnabled();
        doReturn(false).when(logic).isUseSynchronousRunningQuery();
        doReturn(-1L).when(logic).getResultLimit(settings);
        doReturn(-1L).when(logic).getMaxResults();

        doReturn(null).when(logic).getUserOperations();

        RunningQuery query = new RunningQuery(client, connectionPriority, logic, settings, methodAuths, principal, new QueryMetricFactoryImpl());

        verify(logic, times(1)).initialize(any(), any(), any());
        verify(logic, times(1)).getCollectQueryMetrics();
        verify(logic, times(1)).getTransformIterator(settings);
        verify(logic, times(1)).isIntermediateEmptyPagesEnabled();
        verify(logic, times(1)).isUseSynchronousRunningQuery();
        verify(logic, times(1)).getResultLimit(settings);
        verify(logic, times(1)).getMaxResults();
        verify(logic, times(1)).getUserOperations();
        verify(logic, times(1)).setupQuery(config);
        verify(logic, times(1)).preInitialize(settings, AuthorizationsUtil.buildAuthorizations(null));

        // extra tests to verify setConnection worked. Would rather mock and don't really like multiple asserts per test, but there is too much setup
        assertEquals(client, query.getClient());
        assertEquals(iter, query.getTransformIterator());

        if (logic != null && mockingDetails(logic).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(logic));
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testConstructorRetainsAndAppliesPredictor() throws Exception {
        DatawaveUser user = new DatawaveUser(userDN, UserType.USER, null, null, null, 0L);
        DatawavePrincipal principal = new DatawavePrincipal(Collections.singletonList(user));
        InMemoryInstance instance = new InMemoryInstance("predictor test instance");
        try (AccumuloClient client = new InMemoryAccumuloClient("root", instance)) {
            SampleGenericQueryConfiguration config = new SampleGenericQueryConfiguration();
            doReturn(config).when(logic).initialize(any(), any(), any());

            doReturn(false).when(logic).getCollectQueryMetrics();
            doReturn(new TransformIterator<>()).when(logic).getTransformIterator(settings);
            doReturn(false).when(logic).isIntermediateEmptyPagesEnabled();
            doReturn(false).when(logic).isUseSynchronousRunningQuery();
            doReturn(-1L).when(logic).getResultLimit(settings);
            doReturn(-1L).when(logic).getMaxResults();

            doReturn(null).when(logic).getUserOperations();

            QueryPredictor<BaseQueryMetric> predictor = mock(QueryPredictor.class);
            Set<Prediction> predictions = Collections.singleton(new Prediction("runtime", 7));
            doReturn(predictions).when(predictor).predict(any());

            RunningQuery query = new RunningQuery(null, client, connectionPriority, logic, settings, methodAuths, principal,
                            new RunningQuery.RunningQueryTimingNoOp(), predictor, new QueryMetricFactoryImpl());

            assertSame(predictor, query.getPredictor());
            assertEquals(predictions, query.getMetric().getPredictions());
            verify(logic, times(1)).initialize(any(), any(), any());
            verify(logic, times(1)).getCollectQueryMetrics();
            verify(logic, times(1)).getTransformIterator(settings);
            verify(logic, times(1)).isIntermediateEmptyPagesEnabled();
            verify(logic, times(1)).isUseSynchronousRunningQuery();
            verify(logic, times(1)).getResultLimit(settings);
            verify(logic, times(1)).getMaxResults();
            verify(logic, times(1)).getUserOperations();
            verify(predictor, times(1)).predict(any());
            verify(logic, times(1)).setupQuery(config);
            verify(logic, times(1)).preInitialize(settings, AuthorizationsUtil.buildAuthorizations(null));
            verifyNoMoreInteractions(ignoreStubs(predictor));
        }

        if (logic != null && mockingDetails(logic).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(logic));
        }
    }

    @Test
    public void testConstructorWithNullConnector() throws Exception {
        AccumuloClient client = null;
        DatawaveUser user = new DatawaveUser(userDN, UserType.USER, null, null, null, 0L);
        DatawavePrincipal principal = new DatawavePrincipal(Collections.singletonList(user));

        doReturn(false).when(logic).getCollectQueryMetrics();
        doReturn(false).when(logic).isIntermediateEmptyPagesEnabled();
        doReturn(-1L).when(logic).getResultLimit(settings);
        doReturn(-1L).when(logic).getMaxResults();

        doReturn(null).when(logic).getUserOperations();

        RunningQuery query = new RunningQuery(client, connectionPriority, logic, settings, methodAuths, principal, new QueryMetricFactoryImpl());

        assertEquals(client, query.getClient());
    }

    @Test
    public void testConstructorShouldNotMergeAuths() throws Exception {
        // setup
        AccumuloClient client = null;
        methodAuths = "A,B,C";

        // user auths
        String[] auths = new String[2];
        auths[0] = "A";
        auths[1] = "C";

        doReturn(false).when(logic).getCollectQueryMetrics();

        doReturn(null).when(logic).getUserOperations();

        DatawaveUser user = new DatawaveUser(userDN, UserType.USER, Arrays.asList(auths), null, null, 0L);
        DatawavePrincipal principal = new DatawavePrincipal(Collections.singletonList(user));
        assertThrows(AuthorizationException.class,
                        () -> new RunningQuery(client, connectionPriority, logic, settings, methodAuths, principal, new QueryMetricFactoryImpl()));
    }

    @Test
    public void testWithCompositeQueryLogic() throws Exception {
        // setup
        InMemoryInstance instance = new InMemoryInstance("test instance");
        AccumuloClient client = new InMemoryAccumuloClient("root", instance);

        // expected merged auths
        String[] auths = new String[2];
        auths[0] = "A";
        auths[1] = "C";
        Map<String,QueryLogic<?>> logics = new HashMap<>();
        TestQueryLogic<?> logic1 = new TestQueryLogic<>();
        HashSet<String> roles = new HashSet<>();
        roles.add("NONTESTROLE");
        logic1.setTableName("thisTable");
        logic1.setRequiredRoles(roles);
        CompositeQueryLogicTest.TestQueryLogic2 logic2 = new CompositeQueryLogicTest.TestQueryLogic2();
        HashSet<String> roles2 = new HashSet<>();
        roles2.add("NONTESTROLE");
        logic2.setTableName("thatTable");
        logic2.setRequiredRoles(roles2);
        logics.put("TestQueryLogic", logic1);
        logics.put("TestQueryLogic2", logic2);
        CompositeQueryLogic compositeQueryLogic = new CompositeQueryLogic();
        compositeQueryLogic.setQueryLogics(logics);

        DatawaveUser user = new DatawaveUser(userDN, UserType.USER, Arrays.asList(auths), null, null, 0L);
        DatawavePrincipal principal = new DatawavePrincipal(Collections.singletonList(user));

        compositeQueryLogic.setCurrentUser(principal);
        try {
            RunningQuery query = new RunningQuery(client, connectionPriority, compositeQueryLogic, settings, null, principal, new QueryMetricFactoryImpl());
        } catch (NullPointerException npe) {
            fail("NullPointer encountered. This could be caused by configuration being null. Check logic.initialize() ");
        }
    }

    // this shows that updates to the query plan will back propagate to the metric
    @Test
    public void testQueryStringUpdate() throws Exception {
        InMemoryInstance instance = new InMemoryInstance("test instance");
        AccumuloClient client = new InMemoryAccumuloClient("root", instance);

        String[] auths = new String[] {"A", "B"};

        DatawaveUser user = new DatawaveUser(userDN, UserType.USER, Arrays.asList(auths), null, null, 0L);
        DatawavePrincipal principal = new DatawavePrincipal(Collections.singletonList(user));

        RotatingPlanQueryLogic logic = new RotatingPlanQueryLogic(3);
        logic.setMaxPageSize(1);

        settings.setQuery("FOO == 'bar'");
        RunningQuery rq = new RunningQuery(client, connectionPriority, logic, settings, null, principal, new QueryMetricFactoryImpl());
        assertEquals("((setup = 'true') && ((initialize = 'true') && FOO == 'bar'))", rq.getMetric().getPlan());
        ResultsPage page = rq.next();
        // transforms two or three page/results depending on how far the asynchronous results thread has gotten
        List<String> plans = Arrays.asList(new String[] {"((page = '2') && ((page = '1') && ((setup = 'true') && ((initialize = 'true') && FOO == 'bar'))))",
                "((page = '3') && ((page = '2') && ((page = '1') && ((setup = 'true') && ((initialize = 'true') && FOO == 'bar')))))"});
        assertTrue(plans.contains(rq.getMetric().getPlan()));
        assertEquals(1, page.getResults().size());
        page = rq.next();
        // transforms the third page
        assertEquals("((page = '3') && ((page = '2') && ((page = '1') && ((setup = 'true') && ((initialize = 'true') && FOO == 'bar')))))",
                        rq.getMetric().getPlan());
        assertEquals(1, page.getResults().size());
        page = rq.next();
        // transforms nothing, but returns the third page
        assertEquals("((page = '3') && ((page = '2') && ((page = '1') && ((setup = 'true') && ((initialize = 'true') && FOO == 'bar')))))",
                        rq.getMetric().getPlan());
        assertEquals(1, page.getResults().size());
        page = rq.next();
        // neither transforms nor returns anything
        assertEquals("((page = '3') && ((page = '2') && ((page = '1') && ((setup = 'true') && ((initialize = 'true') && FOO == 'bar')))))",
                        rq.getMetric().getPlan());
        assertEquals(0, page.getResults().size());
    }

    @Test
    public void testEmptyOrNullUpdate() throws Exception {
        InMemoryInstance instance = new InMemoryInstance("test instance");
        AccumuloClient client = new InMemoryAccumuloClient("root", instance);

        String[] auths = new String[] {"A", "B"};

        DatawaveUser user = new DatawaveUser(userDN, UserType.USER, Arrays.asList(auths), null, null, 0L);
        DatawavePrincipal principal = new DatawavePrincipal(Collections.singletonList(user));

        // send back no change
        RotatingPlanQueryLogic logic = new RotatingPlanQueryLogic(3, false);
        logic.setMaxPageSize(1);
        settings.setQuery("FOO == 'bar'");
        RunningQuery rq = new RunningQuery(client, connectionPriority, logic, settings, null, principal, new QueryMetricFactoryImpl());
        assertEquals("((setup = 'true') && ((initialize = 'true') && FOO == 'bar'))", rq.getMetric().getPlan());
        rq.next();
        rq.next();
        rq.next();
        rq.next();
        // unchanged
        assertEquals("((setup = 'true') && ((initialize = 'true') && FOO == 'bar'))", rq.getMetric().getPlan());

        // set a null string
        logic = new RotatingPlanQueryLogic(3, false, true, null);
        logic.setMaxPageSize(1);
        settings.setQuery("FOO == 'bar'");
        rq = new RunningQuery(client, connectionPriority, logic, settings, null, principal, new QueryMetricFactoryImpl());
        assertEquals("((setup = 'true') && ((initialize = 'true') && FOO == 'bar'))", rq.getMetric().getPlan());
        rq.next();
        rq.next();
        rq.next();
        rq.next();
        // unchanged
        assertEquals("((setup = 'true') && ((initialize = 'true') && FOO == 'bar'))", rq.getMetric().getPlan());

        // set an empty string
        logic = new RotatingPlanQueryLogic(3, false, true, "");
        logic.setMaxPageSize(1);
        settings.setQuery("FOO == 'bar'");
        rq = new RunningQuery(client, connectionPriority, logic, settings, null, principal, new QueryMetricFactoryImpl());
        assertEquals("((setup = 'true') && ((initialize = 'true') && FOO == 'bar'))", rq.getMetric().getPlan());
        rq.next();
        rq.next();
        rq.next();
        rq.next();
        // unchanged
        assertEquals("((setup = 'true') && ((initialize = 'true') && FOO == 'bar'))", rq.getMetric().getPlan());

        // prove the overwrite works
        logic = new RotatingPlanQueryLogic(3, false, true, "overwrite-works");
        logic.setMaxPageSize(1);
        settings.setQuery("FOO == 'bar'");
        rq = new RunningQuery(client, connectionPriority, logic, settings, null, principal, new QueryMetricFactoryImpl());
        assertEquals("((setup = 'true') && ((initialize = 'true') && FOO == 'bar'))", rq.getMetric().getPlan());
        rq.next();
        rq.next();
        rq.next();
        rq.next();
        // unchanged
        assertEquals("overwrite-works", rq.getMetric().getPlan());
    }

    private static class RotatingPlanQueryLogic extends BaseQueryLogic {
        final private int pages;
        final private boolean updatePages;
        final private boolean usePageOverride;
        final private String pageOverride;

        private int count = 0;

        public RotatingPlanQueryLogic(int pages) {
            this(pages, true);
        }

        public RotatingPlanQueryLogic(int pages, boolean updatePages) {
            this(pages, updatePages, false, null);
        }

        public RotatingPlanQueryLogic(int pages, boolean updatePages, boolean usePageOverride, String pageOverride) {
            this.pages = pages;
            this.updatePages = updatePages;
            this.usePageOverride = usePageOverride;
            this.pageOverride = pageOverride;
        }

        @Override
        public GenericQueryConfiguration initialize(AccumuloClient client, Query settings, Set runtimeQueryAuthorizations) throws Exception {
            getConfig().setQuery(settings);
            getConfig().setQueryString("((initialize = 'true') && " + settings.getQuery() + ")");
            return getConfig();
        }

        @Override
        public void setupQuery(GenericQueryConfiguration configuration) throws Exception {
            String oldPlan = getConfig().getQueryString();
            getConfig().setQueryString("((setup = 'true') && " + oldPlan + ")");
            this.iterator = new Iterator() {
                @Override
                public boolean hasNext() {
                    return count < pages;
                }

                @Override
                public Object next() {
                    count++;
                    return new Object();
                }
            };
        }

        @Override
        public Object clone() {
            return new RotatingPlanQueryLogic(pages, updatePages, usePageOverride, pageOverride);
        }

        @Override
        public AccumuloConnectionFactory.Priority getConnectionPriority() {
            return null;
        }

        @Override
        public QueryLogicTransformer getTransformer(Query settings) {
            return new QueryLogicTransformer() {
                @Override
                public void setResponseEnricher(ResponseEnricher responseTransform) {

                }

                @Override
                public BaseQueryResponse createResponse(ResultsPage resultList) {
                    return null;
                }

                @Override
                public Object transform(Object input) throws EmptyObjectException {
                    if (updatePages) {
                        getConfig().setQueryString("((page = '" + count + "') && " + getConfig().getQueryString() + ")");
                    } else if (usePageOverride) {
                        getConfig().setQueryString(pageOverride);
                    }
                    return new Object();
                }

                @Override
                public void setQueryExecutionForPageStartTime(long queryExecutionForCurrentPageStartTime) {

                }
            };
        }

        @Override
        public Set<String> getOptionalQueryParameters() {
            return Set.of();
        }

        @Override
        public Set<String> getRequiredQueryParameters() {
            return Set.of();
        }

        @Override
        public Set<String> getExampleQueries() {
            return Set.of();
        }
    }
}
