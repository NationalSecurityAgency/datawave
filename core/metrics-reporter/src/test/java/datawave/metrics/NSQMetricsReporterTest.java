package datawave.metrics;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.http.client.config.RequestConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.sun.net.httpserver.HttpServer;

class NSQMetricsReporterTest {
    private final List<byte[]> requestBodies = new CopyOnWriteArrayList<>();
    private final AtomicBoolean delayNextResponse = new AtomicBoolean();

    private HttpServer server;
    private NSQMetricsReporter reporter;
    private CountDownLatch requestReceived;
    private CountDownLatch releaseResponse;
    private CountDownLatch responseCompleted;

    @BeforeEach
    void setUp() throws IOException {
        requestReceived = new CountDownLatch(1);
        releaseResponse = new CountDownLatch(1);
        responseCompleted = new CountDownLatch(1);
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/mpub", exchange -> {
            try {
                requestBodies.add(exchange.getRequestBody().readAllBytes());
                if (delayNextResponse.compareAndSet(true, false)) {
                    requestReceived.countDown();
                    try {
                        releaseResponse.await(5, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                exchange.sendResponseHeaders(200, -1);
            } finally {
                exchange.close();
                responseCompleted.countDown();
            }
        });
        server.start();
        reporter = new NSQMetricsReporter("127.0.0.1", server.getAddress().getPort(), new MetricRegistry(), "test", MetricFilter.ALL, TimeUnit.SECONDS,
                        TimeUnit.MILLISECONDS);
    }

    @AfterEach
    void tearDown() {
        releaseResponse.countDown();
        if (reporter != null) {
            reporter.stop();
        }
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void requestConfigSetsConfiguredTimeouts() {
        RequestConfig config = NSQMetricsReporter.createRequestConfig(123);

        assertEquals(123, config.getConnectionRequestTimeout());
        assertEquals(123, config.getConnectTimeout());
        assertEquals(123, config.getSocketTimeout());
    }

    @Test
    void responseTimeoutDoesNotPreventLaterReports() throws InterruptedException {
        reporter.stop();
        reporter = new NSQMetricsReporterFactory().forRegistry(new MetricRegistry()).withTimeout(100, TimeUnit.MILLISECONDS).build("127.0.0.1",
                        server.getAddress().getPort());
        delayNextResponse.set(true);
        reporter.connect();
        reporter.reportMetric("first");

        long start = System.nanoTime();
        reporter.flush();
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        assertTrue(requestReceived.await(1, TimeUnit.SECONDS));
        assertTrue(elapsedMillis < TimeUnit.SECONDS.toMillis(2));

        releaseResponse.countDown();
        assertTrue(responseCompleted.await(2, TimeUnit.SECONDS));

        reporter.connect();
        reporter.reportMetric("second");
        reporter.flush();

        assertEquals(2, requestBodies.size());
        assertArrayEquals("second".getBytes(US_ASCII), requestBodies.get(1));
    }

    @Test
    void emptyBatchDoesNotSendRequest() {
        reporter.connect();

        reporter.flush();

        assertTrue(requestBodies.isEmpty());
    }

    @Test
    void oversizedMetricSendsOneNonEmptyRequest() {
        String metric = "x".repeat((7 * 1024) + 1);
        reporter.connect();

        reporter.reportMetric(metric);
        reporter.flush();

        assertEquals(1, requestBodies.size());
        assertArrayEquals(metric.getBytes(US_ASCII), requestBodies.get(0));
    }

    @Test
    void batchRolloverSendsEachMetricOnce() {
        String firstMetric = "a".repeat(4 * 1024);
        String secondMetric = "b".repeat(4 * 1024);
        reporter.connect();

        reporter.reportMetric(firstMetric);
        reporter.reportMetric(secondMetric);
        reporter.flush();

        assertEquals(2, requestBodies.size());
        assertArrayEquals(firstMetric.getBytes(US_ASCII), requestBodies.get(0));
        assertArrayEquals(secondMetric.getBytes(US_ASCII), requestBodies.get(1));
    }
}
