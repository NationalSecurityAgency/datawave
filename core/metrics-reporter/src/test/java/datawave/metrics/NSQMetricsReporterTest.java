package datawave.metrics;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.sun.net.httpserver.HttpServer;

class NSQMetricsReporterTest {
    private final List<byte[]> requestBodies = new CopyOnWriteArrayList<>();

    private HttpServer server;
    private NSQMetricsReporter reporter;

    @BeforeEach
    void setUp() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/mpub", exchange -> {
            requestBodies.add(exchange.getRequestBody().readAllBytes());
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
        });
        server.start();
        reporter = new NSQMetricsReporter("127.0.0.1", server.getAddress().getPort(), new MetricRegistry(), "test", MetricFilter.ALL, TimeUnit.SECONDS,
                        TimeUnit.MILLISECONDS);
    }

    @AfterEach
    void tearDown() {
        if (reporter != null) {
            reporter.stop();
        }
        if (server != null) {
            server.stop(0);
        }
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
