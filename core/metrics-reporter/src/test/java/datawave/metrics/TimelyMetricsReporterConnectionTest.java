package datawave.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.junit.jupiter.api.Test;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;

class TimelyMetricsReporterConnectionTest {

    @Test
    void reconnectClosesOldSocket() {
        TrackingSocket oldSocket = new TrackingSocket(false);
        TrackingSocket newSocket = new TrackingSocket(false);
        TimelyMetricsReporter reporter = newReporter(socketSupplier(oldSocket, newSocket));

        assertTrue(reporter.connect());
        oldSocket.connected = false;
        reporter.connectTime = 0L;
        reporter.backoff = 0L;
        assertTrue(reporter.connect());

        assertTrue(oldSocket.closed);
        assertTrue(newSocket.connected);
    }

    @Test
    void failedWriterSetupClosesPartialSocket() {
        TrackingSocket socket = new TrackingSocket(true);
        TimelyMetricsReporter reporter = newReporter(() -> socket);

        assertFalse(reporter.connect());

        assertTrue(socket.closed);
    }

    @Test
    void stopIsIdempotent() {
        TrackingSocket socket = new TrackingSocket(false);
        TimelyMetricsReporter reporter = newReporter(() -> socket);
        assertTrue(reporter.connect());

        reporter.stop();
        reporter.stop();

        assertEquals(1, socket.closeCount);
    }

    @Test
    void timeoutClosesSocketAndStartsBackoff() {
        TrackingSocket socket = new TrackingSocket(false);
        socket.connectFailure = new SocketTimeoutException("timed out");
        AtomicInteger socketRequests = new AtomicInteger();
        TimelyMetricsReporter reporter = new TimelyMetricsReporter("localhost", 4242, new MetricRegistry(), "test", MetricFilter.ALL, TimeUnit.SECONDS,
                        TimeUnit.MILLISECONDS, 123, () -> {
                            socketRequests.incrementAndGet();
                            return socket;
                        });

        assertFalse(reporter.connect());
        assertEquals(123, socket.connectTimeout);
        assertTrue(socket.closed);
        assertTrue(reporter.connectTime >= socket.connectFailureTime);

        assertFalse(reporter.connect());
        assertEquals(1, socketRequests.get());
    }

    @Test
    void factoryConfiguresConnectTimeout() {
        TimelyMetricsReporter reporter = new TimelyMetricsReporterFactory().forRegistry(new MetricRegistry())
                        .withConnectTimeout(123, TimeUnit.MILLISECONDS).build("localhost", 4242);

        assertEquals(123, reporter.connectTimeoutMillis);
        assertThrows(IllegalArgumentException.class,
                        () -> new TimelyMetricsReporterFactory().forRegistry(new MetricRegistry()).withConnectTimeout(0, TimeUnit.MILLISECONDS));
    }

    private static TimelyMetricsReporter newReporter(Supplier<Socket> socketSupplier) {
        return new TimelyMetricsReporter("localhost", 4242, new MetricRegistry(), "test", MetricFilter.ALL, TimeUnit.SECONDS, TimeUnit.MILLISECONDS,
                        socketSupplier);
    }

    private static Supplier<Socket> socketSupplier(Socket... sockets) {
        Queue<Socket> queue = new ArrayDeque<>(Arrays.asList(sockets));
        return queue::remove;
    }

    private static class TrackingSocket extends Socket {
        private final boolean failOutputStream;
        private boolean connected;
        private boolean closed;
        private int closeCount;
        private int connectTimeout;
        private long connectFailureTime;
        private IOException connectFailure;

        private TrackingSocket(boolean failOutputStream) {
            this.failOutputStream = failOutputStream;
        }

        @Override
        public void connect(SocketAddress endpoint, int timeout) throws IOException {
            connectTimeout = timeout;
            if (connectFailure != null) {
                try {
                    Thread.sleep(20);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("interrupted", e);
                }
                connectFailureTime = System.currentTimeMillis();
                throw connectFailure;
            }
            connected = true;
        }

        @Override
        public boolean isConnected() {
            return connected;
        }

        @Override
        public OutputStream getOutputStream() throws IOException {
            if (failOutputStream) {
                throw new IOException("writer setup failed");
            }
            return new ByteArrayOutputStream();
        }

        @Override
        public synchronized void close() {
            closed = true;
            closeCount++;
        }
    }
}
