package datawave.webservice.query.runner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.common.connection.AccumuloConnectionFactory.Priority;
import datawave.core.query.cache.ResultsPage;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.logic.QueryLogic;
import datawave.core.query.predict.QueryPredictor;
import datawave.microservice.query.QueryImpl;
import datawave.microservice.querymetric.QueryMetric;
import datawave.microservice.querymetric.QueryMetricFactoryImpl;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.runner.RunningQuery.RunningQueryTiming;
import datawave.webservice.query.util.QueryUncaughtExceptionHandler;

class RunningQueryLifecycleTest {
    private QueryImpl settings;
    private QueryLogic<Object> logic;
    private AccumuloClient client;
    private AccumuloConnectionFactory connections;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() throws Exception {
        settings = new QueryImpl();
        settings.setId(UUID.randomUUID());
        settings.setQuery("FIELD == 'value'");
        settings.setPagesize(10);
        logic = mock(QueryLogic.class);
        client = mock(AccumuloClient.class);
        connections = mock(AccumuloConnectionFactory.class);
        when(logic.initialize(any(), any(), any())).thenReturn(mock(GenericQueryConfiguration.class));
        when(logic.getResultLimit(settings)).thenReturn(100L);
        when(logic.getMaxResults()).thenReturn(100L);
        when(logic.getMaxWork()).thenReturn(-1L);
    }

    private RunningQuery query(TransformIterator<?,?> iterator, RunningQueryTiming timing) throws Exception {
        when(logic.getTransformIterator(settings)).thenReturn(iterator);
        return new RunningQuery(client, Priority.NORMAL, logic, settings, null, null, timing, new QueryMetricFactoryImpl());
    }

    private TransformIterator<String,String> rows(String... values) {
        return new TransformIterator<>(Arrays.asList(values).iterator(), value -> value);
    }

    private RunningQueryTiming earlyReturn() {
        return new RunningQueryTiming() {
            @Override
            public boolean shouldReturnPartialResults(int count, int maximum, long elapsed) {
                return true;
            }

            @Override
            public int getMaxLongRunningTimeoutRetries() {
                return 3;
            }

            @Override
            public long getPageShortCircuitTimeoutMs() {
                return 60000;
            }
        };
    }

    @Test
    void fullPageDoesNotReadAhead() throws Exception {
        settings.setPagesize(1);
        AtomicInteger consumed = new AtomicInteger();
        RunningQuery query = query(new TransformIterator<Object,Object>() {
            @Override
            public boolean hasNext() {
                if (consumed.get() > 0) {
                    throw new IllegalStateException("unnecessary lookahead");
                }
                return true;
            }

            @Override
            public Object next() {
                consumed.incrementAndGet();
                return "one";
            }
        }, null);
        assertEquals(Collections.singletonList("one"), query.next().getResults());
        assertFalse(query.isFinished(), "a full page does not establish query exhaustion");
    }

    @Test
    void unlimitedResultsStillReturnPartialPages() throws Exception {
        when(logic.getResultLimit(settings)).thenReturn(-1L);
        when(logic.getMaxResults()).thenReturn(-1L);
        when(logic.getMaxPageSize()).thenReturn(10);
        RunningQuery query = query(rows("one", "two"), earlyReturn());
        assertEquals(ResultsPage.Status.PARTIAL, query.next().getStatus());
        assertEquals(Collections.singletonList("two"), query.next().getResults());
    }

    @Test
    void nonpositivePageCapUsesRequestedPageSize() throws Exception {
        RunningQuery query = query(rows("one", "two"), earlyReturn());
        assertEquals(ResultsPage.Status.PARTIAL, query.next().getStatus());
    }

    @Test
    void resultOverrideIsTerminalAndComplete() throws Exception {
        settings.setMaxResultsOverride(1);
        settings.setMaxResultsOverridden(true);
        when(logic.getMaxPageSize()).thenReturn(10);
        RunningQuery query = query(rows("one", "two"), null);
        assertEquals(ResultsPage.Status.COMPLETE, query.next().getStatus());
        assertTrue(query.isFinished());
        assertEquals(QueryMetric.Lifecycle.MAXRESULTS, query.getMetric().getLifecycle());
        assertEquals(ResultsPage.Status.NONE, query.next().getStatus());
    }

    @Test
    void exhaustionMarksQueryFinished() throws Exception {
        RunningQuery query = query(rows("one"), null);
        assertEquals(Collections.singletonList("one"), query.next().getResults());
        assertTrue(query.isFinished());
        assertEquals(ResultsPage.Status.NONE, query.next().getStatus());
    }

    @Test
    void resetStartsFreshAfterResultLimitAndClose() throws Exception {
        when(logic.getResultLimit(settings)).thenReturn(1L);
        RunningQuery query = query(rows("old"), null);
        assertEquals(Collections.singletonList("old"), query.next().getResults());
        query.closeConnection(connections);
        when(logic.getTransformIterator(settings)).thenReturn(rows("new"));
        query.setClient(client);
        assertEquals(Collections.singletonList("new"), query.next().getResults());
        assertEquals(1, query.getLastPageNumber());
        assertEquals(QueryMetric.Lifecycle.MAXRESULTS, query.getMetric().getLifecycle());
    }

    @Test
    void resetClearsCancellationAndRecordedFailure() throws Exception {
        RunningQuery query = query(rows("old"), null);
        settings.setUncaughtExceptionHandler(new QueryUncaughtExceptionHandler());
        settings.getUncaughtExceptionHandler().uncaughtException(Thread.currentThread(), new IllegalStateException("old failure"));
        query.cancel();
        query.closeConnection(connections);
        when(logic.getTransformIterator(settings)).thenReturn(rows("new"));
        query.setClient(client);
        assertFalse(query.isCanceled());
        assertEquals(Collections.singletonList("new"), query.next().getResults());
        assertEquals(QueryMetric.Lifecycle.RESULTS, query.getMetric().getLifecycle());
    }

    @Test
    void resetReevaluatesResultLimit() throws Exception {
        when(logic.getResultLimit(settings)).thenReturn(1L);
        RunningQuery query = query(rows("old"), null);
        query.next();
        query.closeConnection(connections);
        when(logic.getResultLimit(settings)).thenReturn(2L);
        when(logic.getTransformIterator(settings)).thenReturn(rows("one", "two", "three"));
        query.setClient(client);
        assertEquals(Arrays.asList("one", "two"), query.next().getResults());
        assertTrue(query.isFinished());
    }

    @Test
    void failedResetCannotReadOldIterator() throws Exception {
        RunningQuery query = query(rows("old"), null);
        query.closeConnection(connections);
        IllegalStateException failure = new IllegalStateException("initialization failed");
        when(logic.initialize(any(), any(), any())).thenThrow(failure);
        assertSame(failure, assertThrows(IllegalStateException.class, () -> query.setClient(client)));
        assertSame(failure, assertThrows(IllegalStateException.class, query::next));
    }

    @Test
    void cancellationIsNotNormalExhaustion() throws Exception {
        RunningQuery query = query(rows("one"), null);
        query.cancel();
        assertThrows(QueryException.class, query::next);
    }

    @Test
    void clientReturnFailureStillClosesLogic() throws Exception {
        RunningQuery query = query(rows("one"), null);
        doThrow(new IllegalStateException("pool unavailable")).when(connections).returnClient(client);
        assertThrows(IllegalStateException.class, () -> query.closeConnection(connections));
        verify(logic).close();
    }

    @Test
    void cleanupPreservesBothFailuresAndStillAttemptsClientReturn() throws Exception {
        RunningQuery query = query(rows("one"), null);
        IllegalStateException closeFailure = new IllegalStateException("scanner close failed");
        IllegalStateException poolFailure = new IllegalStateException("pool unavailable");
        doThrow(closeFailure).when(logic).close();
        doThrow(poolFailure).when(connections).returnClient(client);
        assertSame(closeFailure, assertThrows(IllegalStateException.class, () -> query.closeConnection(connections)));
        assertEquals(Collections.singletonList(poolFailure), Arrays.asList(closeFailure.getSuppressed()));
        assertSame(client, query.getClient(), "retain ownership when returning the client fails");
        verify(connections).returnClient(client);
    }

    @Test
    void predictorOverloadRetainsPredictor() throws Exception {
        QueryPredictor predictor = mock(QueryPredictor.class);
        RunningQuery query = new RunningQuery(null, null, Priority.NORMAL, logic, settings, null, null, null, predictor, new QueryMetricFactoryImpl());
        assertSame(predictor, query.getPredictor());
    }

    @Test
    void explicitAuthorizationsAreParsedBeforeInitialization() throws Exception {
        when(logic.getTransformIterator(settings)).thenReturn(rows("one"));
        RunningQuery query = RunningQuery.createQueryWithAuthorizations(null, client, Priority.NORMAL, logic, settings, "A,B", null, null,
                        new QueryMetricFactoryImpl());
        assertEquals(Collections.singleton(new Authorizations("A", "B")), query.getCalculatedAuths());
        verify(logic).initialize(client, settings, Collections.singleton(new Authorizations("A", "B")));
        assertNotNull(query.getTransformIterator());
    }
}
