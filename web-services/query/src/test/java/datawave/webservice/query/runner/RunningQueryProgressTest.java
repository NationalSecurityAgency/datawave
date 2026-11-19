package datawave.webservice.query.runner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.commons.collections4.Transformer;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import datawave.core.common.connection.AccumuloConnectionFactory.Priority;
import datawave.core.query.cache.ResultsPage;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.logic.QueryLogic;
import datawave.core.query.logic.ScanProgressAware;
import datawave.microservice.query.QueryImpl;
import datawave.microservice.querymetric.QueryMetricFactoryImpl;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;

@Timeout(15)
class RunningQueryProgressTest {
    private interface ProgressTransformer extends Transformer<Object,Object>, ScanProgressAware {}

    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final CountDownLatch stalled = new CountDownLatch(1);
    private final CountDownLatch release = new CountDownLatch(1);
    private final AtomicLong progress = new AtomicLong();

    @AfterEach
    void stopWorker() {
        release.countDown();
        executor.shutdownNow();
    }

    @Test
    void stalledScanReturnsPartialThenResumesWithItsResult() throws Exception {
        RunningQuery query = stalledQuery(true, null);
        ResultsPage partial = query.next();
        assertEquals(ResultsPage.Status.PARTIAL, partial.getStatus());
        assertTrue(partial.getResults().isEmpty());

        release.countDown();
        assertEquals(List.of("event"), query.next().getResults());
        assertEquals(ResultsPage.Status.NONE, query.next().getStatus());
    }

    @Test
    void sameProgressCannotReturnAnotherPartialPage() throws Exception {
        RunningQuery query = stalledQuery(true, null);
        assertEquals(ResultsPage.Status.PARTIAL, query.next().getStatus());
        assertQueryTimeout(query);
    }

    @Test
    void progressAfterTimeoutSnapshotIsAvailableToNextPage() throws Exception {
        RunningQuery query = stalledQuery(true, null, true, true);
        assertEquals(ResultsPage.Status.PARTIAL, query.next().getStatus());
        assertEquals(ResultsPage.Status.PARTIAL, query.next().getStatus());
        assertQueryTimeout(query);
    }

    @Test
    void progressDisabledPreservesSynchronousExecution() throws Exception {
        assertFalse(stalledQuery(true, null, false, false).isUseResultsThread());
        assertTrue(stalledQuery(true, null, true, false).isUseResultsThread());
    }

    @Test
    void stallWithoutProgressStillTimesOut() throws Exception {
        assertQueryTimeout(stalledQuery(false, null));
    }

    @Test
    void workerFailureAfterProgressIsNotReportedAsAnotherPartialPage() throws Exception {
        RuntimeException failure = new IllegalStateException("scan failed");
        RunningQuery query = stalledQuery(true, failure);
        assertEquals(ResultsPage.Status.PARTIAL, query.next().getStatus());
        release.countDown();
        // Wait for the producer to exit before requesting another page.
        executor.submit(() -> {}).get(5, TimeUnit.SECONDS);
        assertSame(failure, assertThrows(IllegalStateException.class, query::next));
    }

    private void assertQueryTimeout(RunningQuery query) {
        QueryException exception = assertThrows(QueryException.class, query::next);
        assertEquals(DatawaveErrorCode.QUERY_TIMEOUT.getErrorCode(), exception.getErrorCode());
    }

    private RunningQuery stalledQuery(boolean madeProgress, RuntimeException failure) throws Exception {
        return stalledQuery(madeProgress, failure, true, false);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private RunningQuery stalledQuery(boolean madeProgress, RuntimeException failure, boolean reportProgress, boolean advanceAfterSnapshot) throws Exception {
        TransformIterator iterator = mock(TransformIterator.class);
        ProgressTransformer transformer = mock(ProgressTransformer.class);
        when(transformer.tracksWebserverProgress()).thenReturn(true);
        when(transformer.getWebserverProgress()).thenAnswer(invocation -> {
            long snapshot = progress.get();
            if (advanceAfterSnapshot && snapshot == 1) {
                progress.incrementAndGet();
            }
            return snapshot;
        });
        when(iterator.getTransformer()).thenReturn(transformer);
        when(iterator.hasNext()).thenAnswer(invocation -> {
            if (madeProgress) {
                progress.incrementAndGet();
            }
            stalled.countDown();
            release.await();
            if (failure != null) {
                throw failure;
            }
            return true;
        }).thenReturn(false);
        when(iterator.next()).thenReturn("event");

        QueryImpl settings = new QueryImpl();
        settings.setId(UUID.randomUUID());
        settings.setPagesize(10);
        QueryLogic logic = mock(QueryLogic.class);
        when(logic.getTransformIterator(settings)).thenReturn(iterator);
        GenericQueryConfiguration configuration = mock(GenericQueryConfiguration.class);
        when(configuration.isReportScanProgress()).thenReturn(reportProgress);
        when(logic.initialize(any(), any(), any())).thenReturn(configuration);
        when(logic.isUseSynchronousRunningQuery()).thenReturn(true);
        when(logic.getMaxPageSize()).thenReturn(10);
        when(logic.getMaxResults()).thenReturn(-1L);
        when(logic.getMaxWork()).thenReturn(-1L);
        when(logic.getResultLimit(settings)).thenReturn(-1L);
        RunningQuery.RunningQueryTiming timing = new RunningQuery.RunningQueryTimingNoOp() {
            @Override
            public long getPageShortCircuitTimeoutMs() {
                try {
                    assertTrue(stalled.await(5, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
                return release.getCount() == 0 ? 5000 : 25;
            }
        };
        DatawaveUser user = new DatawaveUser(SubjectIssuerDNPair.of("userDN", "issuerDN"), DatawaveUser.UserType.USER, null, null, null, 0L);
        DatawavePrincipal principal = new DatawavePrincipal(Collections.singletonList(user));
        RunningQuery query = new RunningQuery(null, mock(AccumuloClient.class), Priority.NORMAL, logic, settings, "", principal, timing, null,
                        new QueryMetricFactoryImpl());
        query.setExecutor(executor);
        return query;
    }
}
