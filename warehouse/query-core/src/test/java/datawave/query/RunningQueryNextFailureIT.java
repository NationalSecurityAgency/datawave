package datawave.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.clientImpl.AccumuloServerException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.commons.collections4.Transformer;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import datawave.core.common.connection.AccumuloConnectionFactory.Priority;
import datawave.core.query.cache.ResultsPage;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.logic.QueryLogic;
import datawave.microservice.query.QueryImpl;
import datawave.microservice.querymetric.QueryMetricFactoryImpl;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.test.iter.RuntimeExceptionIterator;
import datawave.webservice.query.result.event.DefaultEvent;
import datawave.webservice.query.runner.RunningQuery;

class RunningQueryNextFailureIT {
    private static final String TABLE = "next_failure";
    private static final String FIRST_ROW = "intermediate";
    private static final String NEXT_ROW = "next";
    private static final String PASSWORD = "test-password";

    @TempDir
    static Path clusterDir;
    private static MiniAccumuloCluster cluster;
    private static AccumuloClient client;

    enum WorkerExit {
        EXHAUSTED, TRANSFORM_FAILURE, SCANNER_FAILURE
    }

    @BeforeAll
    static void startCluster() throws Exception {
        MiniAccumuloConfig config = new MiniAccumuloConfig(clusterDir.toFile(), PASSWORD);
        config.setNumTservers(1);
        cluster = new MiniAccumuloCluster(config);
        cluster.start();
        client = cluster.createAccumuloClient("root", new PasswordToken(PASSWORD));
        client.tableOperations().create(TABLE);
        try (BatchWriter writer = client.createBatchWriter(TABLE)) {
            for (String row : new String[] {FIRST_ROW, NEXT_ROW}) {
                Mutation mutation = new Mutation(row);
                mutation.put("cf", "cq", "value");
                writer.addMutation(mutation);
            }
        }
    }

    @AfterAll
    static void stopCluster() throws Exception {
        try {
            if (client != null) {
                client.close();
            }
        } finally {
            if (cluster != null) {
                cluster.stop();
            }
        }
    }

    @ParameterizedTest
    @EnumSource(WorkerExit.class)
    @Timeout(30)
    @SuppressWarnings({"rawtypes", "unchecked"})
    void reportsFailureDuringNextAfterAResultWasAdvertised(WorkerExit workerExit) throws Exception {
        CountDownLatch releaseWorker = new CountDownLatch(1);
        AtomicReference<RuntimeException> scannerFailure = new AtomicReference<>();
        IllegalStateException transformFailure = new IllegalStateException("live event transformation failed");
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (Scanner first = client.createScanner(TABLE, Authorizations.EMPTY); Scanner later = client.createScanner(TABLE, Authorizations.EMPTY)) {
            first.setRange(new Range(FIRST_ROW));
            later.setRange(new Range(workerExit == WorkerExit.EXHAUSTED ? "missing" : NEXT_ROW));
            later.setTimeout(10, TimeUnit.SECONDS);
            if (workerExit == WorkerExit.SCANNER_FAILURE) {
                // This iterator runs in the tablet server, not in the test's results worker.
                later.addScanIterator(new IteratorSetting(100, "fail", RuntimeExceptionIterator.class,
                                Map.of(RuntimeExceptionIterator.EXCEPTION_CLASS, IllegalStateException.class.getName(),
                                                RuntimeExceptionIterator.EXCEPTION_MESSAGE, "tablet-server scan failed", RuntimeExceptionIterator.FIRE_ON_SEEK,
                                                "true")));
            }
            Iterator<Map.Entry<Key,Value>> firstRows = first.iterator();
            Iterator<Map.Entry<Key,Value>> laterRows = later.iterator();
            Iterator<Map.Entry<Key,Value>> input = mock(Iterator.class);
            when(input.hasNext()).thenAnswer(call -> firstRows.hasNext()).thenAnswer(call -> {
                releaseWorker.await();
                // For failure cases, advertise an item before asking the second scanner to read it.
                return workerExit != WorkerExit.EXHAUSTED || laterRows.hasNext();
            });
            when(input.next()).thenAnswer(call -> firstRows.next()).thenAnswer(call -> {
                try {
                    return laterRows.next();
                } catch (RuntimeException failure) {
                    scannerFailure.set(failure);
                    throw failure;
                }
            });
            Transformer<Map.Entry<Key,Value>,DefaultEvent> transformer = entry -> {
                if (NEXT_ROW.equals(entry.getKey().getRow().toString())) {
                    throw transformFailure;
                }
                assertEquals(FIRST_ROW, entry.getKey().getRow().toString());
                DefaultEvent intermediate = new DefaultEvent();
                intermediate.setIntermediateResult(true);
                return intermediate;
            };
            // The plain wrapper reads and transforms during next(), after RunningQuery increments its counter.
            TransformIterator<Map.Entry<Key,Value>,DefaultEvent> results = new TransformIterator<>(input, transformer);
            RunningQuery query = createQuery(results, executor);
            ResultsPage<?> partial = query.next();
            assertEquals(ResultsPage.Status.PARTIAL, partial.getStatus());
            assertTrue(partial.getResults().isEmpty());
            releaseWorker.countDown();
            executor.submit(() -> {}).get(15, TimeUnit.SECONDS);
            switch (workerExit) {
                case EXHAUSTED:
                    assertEquals(ResultsPage.Status.NONE, query.next().getStatus());
                    break;
                case TRANSFORM_FAILURE:
                    assertSame(transformFailure, assertThrows(IllegalStateException.class, query::next));
                    break;
                case SCANNER_FAILURE:
                    RuntimeException observed = scannerFailure.get();
                    assertNotNull(observed, "The live scanner must fail before the second page request");
                    // Accumulo reports the injected tablet-server exception through its scanner wrapper.
                    assertInstanceOf(AccumuloServerException.class, observed.getCause(), "Expected a tablet-server error, not a client-side failure");
                    assertSame(observed, assertThrows(RuntimeException.class, query::next));
                    break;
            }
        } finally {
            releaseWorker.countDown();
            executor.shutdownNow();
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static RunningQuery createQuery(TransformIterator<?,?> results, ExecutorService executor) throws Exception {
        QueryImpl settings = new QueryImpl();
        settings.setId(UUID.randomUUID());
        settings.setPagesize(10);
        assertNull(settings.getUncaughtExceptionHandler());
        QueryLogic logic = mock(QueryLogic.class);
        when(logic.initialize(any(), any(), any())).thenReturn(mock(GenericQueryConfiguration.class));
        when(logic.getTransformIterator(settings)).thenReturn(results);
        when(logic.isIntermediateEmptyPagesEnabled()).thenReturn(true);
        when(logic.getMaxPageSize()).thenReturn(10);
        when(logic.getMaxResults()).thenReturn(-1L);
        when(logic.getResultLimit(settings)).thenReturn(-1L);
        when(logic.getMaxWork()).thenReturn(-1L);
        DatawaveUser user = new DatawaveUser(SubjectIssuerDNPair.of("userDN", "issuerDN"), DatawaveUser.UserType.USER, null, null, null, 0L);
        DatawavePrincipal principal = new DatawavePrincipal(Collections.singletonList(user));
        RunningQuery query = new RunningQuery(null, client, Priority.NORMAL, logic, settings, "", principal, null, null, new QueryMetricFactoryImpl());
        query.setExecutor(executor);
        return query;
    }
}
