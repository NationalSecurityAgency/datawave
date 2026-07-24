package datawave.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
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

        private TrackingSocket(boolean failOutputStream) {
            this.failOutputStream = failOutputStream;
        }

        @Override
        public void connect(SocketAddress endpoint) {
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
