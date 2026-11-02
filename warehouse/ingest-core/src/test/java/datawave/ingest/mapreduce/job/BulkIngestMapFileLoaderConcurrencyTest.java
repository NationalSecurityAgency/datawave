package datawave.ingest.mapreduce.job;

import static datawave.ingest.mapreduce.job.BulkIngestMapFileLoader.CLEANUP_FILE_MARKER;
import static datawave.ingest.mapreduce.job.BulkIngestMapFileLoader.FAILED_FILE_MARKER;
import static datawave.ingest.mapreduce.job.BulkIngestMapFileLoader.LOADING_FILE_MARKER;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;

class BulkIngestMapFileLoaderConcurrencyTest {
    @TempDir
    Path directory;

    private final BulkIngestMapFileLoader loader = mock(BulkIngestMapFileLoader.class);
    private final CountDownLatch shutdownRequested = new CountDownLatch(1);
    private final CountDownLatch releaseLoader = new CountDownLatch(1);
    private final AtomicReference<Thread> loaderThread = new AtomicReference<>();
    private final List<String> log = Collections.synchronizedList(new ArrayList<>());
    private Thread runner;

    @BeforeEach
    void configureShutdown() {
        doAnswer(invocation -> {
            shutdownRequested.countDown();
            return null;
        }).when(loader).shutdown();
    }

    @AfterEach
    void stopRunner() throws InterruptedException {
        releaseLoader.countDown();
        if (runner != null) {
            runner.interrupt();
            runner.join(TimeUnit.SECONDS.toMillis(10));
            assertFalse(runner.isAlive(), "Test runner did not stop");
        }
    }

    @Test
    void cleanupMarkerDoesNotFinishSuccessfulJob() throws Exception {
        Path jobDir = Files.createDirectory(directory.resolve("job"));
        Path flaggedFile = Files.createFile(directory.resolve("flagged"));
        Path loadedFile = directory.resolve("loaded");
        CountDownLatch cleanupMarked = new CountDownLatch(1);

        FutureTask<Void> result = startLoader(jobDir, true, () -> {
            Files.createFile(jobDir.resolve(CLEANUP_FILE_MARKER));
            log.add("Renamed file:" + jobDir.resolve(LOADING_FILE_MARKER) + " to " + CLEANUP_FILE_MARKER);
            log.add("Marking 1 sequence files from flagged to loaded");
            cleanupMarked.countDown();
            await(releaseLoader);
            Files.move(flaggedFile, loadedFile);
            Files.delete(jobDir.resolve(CLEANUP_FILE_MARKER));
            Files.delete(jobDir);
            await(shutdownRequested);
        });

        await(cleanupMarked);
        assertFalse(shutdownRequested.await(200, TimeUnit.MILLISECONDS), "Loader stopped before source-file moves and directory cleanup");
        releaseLoader.countDown();
        result.get(10, TimeUnit.SECONDS);

        assertTrue(Files.exists(loadedFile));
        assertTrue(Files.notExists(flaggedFile));
        assertTrue(Files.notExists(jobDir));
        assertFalse(loaderThread.get().isAlive());
    }

    @Test
    void failedJobWaitsForLoaderToExit() throws Exception {
        Path jobDir = Files.createDirectory(directory.resolve("job"));
        FutureTask<Void> result = startLoader(jobDir, false, () -> {
            Files.createFile(jobDir.resolve(FAILED_FILE_MARKER));
            await(shutdownRequested);
            await(releaseLoader);
            log.add("Worker finished");
        });

        await(shutdownRequested);
        assertThrows(TimeoutException.class, () -> result.get(200, TimeUnit.MILLISECONDS));
        releaseLoader.countDown();
        result.get(10, TimeUnit.SECONDS);

        assertTrue(log.contains("Worker finished"));
        assertFalse(loaderThread.get().isAlive());
    }

    @Test
    void interruptedWaitStillStopsLoader() throws Exception {
        Path jobDir = Files.createDirectory(directory.resolve("job"));
        CountDownLatch started = new CountDownLatch(1);
        FutureTask<Void> result = startLoader(jobDir, true, () -> {
            started.countDown();
            await(shutdownRequested);
            await(releaseLoader);
        });

        await(started);
        runner.interrupt();
        await(shutdownRequested);
        assertThrows(TimeoutException.class, () -> result.get(200, TimeUnit.MILLISECONDS));
        releaseLoader.countDown();

        ExecutionException failure = assertThrows(ExecutionException.class, () -> result.get(10, TimeUnit.SECONDS));
        assertInstanceOf(InterruptedException.class, failure.getCause());
        assertFalse(loaderThread.get().isAlive());
    }

    @Test
    void workerFailureIsReported() throws Exception {
        Path jobDir = Files.createDirectory(directory.resolve("job"));
        IllegalStateException failure = new IllegalStateException("Loader failed");
        doThrow(failure).when(loader).run();

        ExecutionException thrown = assertThrows(ExecutionException.class, () -> BulkIngestMapFileLoaderTest.runLoader(loader, jobDir, true, log));

        assertSame(failure, thrown.getCause());
        verify(loader).shutdown();
    }

    @Test
    void workerCannotExitBeforeFinishingJob() throws Exception {
        Path jobDir = Files.createDirectory(directory.resolve("job"));

        AssertionError failure = assertThrows(AssertionError.class, () -> BulkIngestMapFileLoaderTest.runLoader(loader, jobDir, true, log));

        assertTrue(failure.getMessage().contains("Loader stopped before finishing"));
        verify(loader).shutdown();
    }

    @Test
    void workerCanExitAfterFinishingJob() throws Exception {
        Path jobDir = Files.createDirectory(directory.resolve("job"));
        doAnswer(invocation -> {
            Files.delete(jobDir);
            return null;
        }).when(loader).run();

        BulkIngestMapFileLoaderTest.runLoader(loader, jobDir, true, log);

        assertTrue(Files.notExists(jobDir));
        verify(loader).shutdown();
    }

    private FutureTask<Void> startLoader(Path jobDir, boolean expectSuccess, Executable work) {
        doAnswer(invocation -> {
            loaderThread.set(Thread.currentThread());
            work.execute();
            return null;
        }).when(loader).run();
        FutureTask<Void> task = new FutureTask<>(() -> {
            BulkIngestMapFileLoaderTest.runLoader(loader, jobDir, expectSuccess, log);
            return null;
        });
        runner = new Thread(task, "loader-test-runner");
        runner.start();
        return task;
    }

    private static void await(CountDownLatch latch) throws InterruptedException {
        assertTrue(latch.await(10, TimeUnit.SECONDS), "Timed out waiting for test coordination");
    }
}
