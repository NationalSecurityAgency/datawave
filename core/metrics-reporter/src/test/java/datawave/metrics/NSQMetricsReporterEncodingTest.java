package datawave.metrics;

import static java.nio.charset.StandardCharsets.UTF_8;
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

class NSQMetricsReporterEncodingTest {
    private HttpServer server;
    private NSQMetricsReporter reporter;
    private final List<byte[]> requestBodies = new CopyOnWriteArrayList<>();

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
        reporter.connect();
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
    void sendsExactUtf8Bytes() {
        String metric = "put m\u00e9tric 1 2 tag=caf\u00e9\n";

        reporter.reportMetric(metric);
        reporter.flush();

        assertEquals(1, requestBodies.size());
        assertArrayEquals(metric.getBytes(UTF_8), requestBodies.get(0));
    }

    @Test
    void batchesByEncodedByteLength() {
        String first = "put first 1 " + "\u00e9".repeat(2000) + "\n";
        String second = "put second 1 " + "\u00e9".repeat(2000) + "\n";
        assertTrue(first.length() + second.length() < 7 * 1024);
        assertTrue(first.getBytes(UTF_8).length + second.getBytes(UTF_8).length > 7 * 1024);

        reporter.reportMetric(first);
        reporter.reportMetric(second);
        reporter.flush();

        assertEquals(2, requestBodies.size());
        assertTrue(requestBodies.get(0).length > 0);
        assertTrue(requestBodies.get(1).length > 0);
        assertArrayEquals(first.getBytes(UTF_8), requestBodies.get(0));
        assertArrayEquals(second.getBytes(UTF_8), requestBodies.get(1));
    }
}
