package datawave.util.timely;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

public class TcpClientConnectionTest {

    @Test
    public void reconnectClosesOldSocket() throws Exception {
        TrackingSocket oldSocket = new TrackingSocket(false);
        TrackingSocket newSocket = new TrackingSocket(false);
        TcpClient client = new TcpClient("localhost", 4242, socketSupplier(oldSocket, newSocket));

        client.open();
        oldSocket.connected = false;
        setReconnectDelay(client, 0L);
        client.open();

        assertTrue(oldSocket.closed);
        assertTrue(newSocket.connected);
    }

    @Test
    public void failedWriterSetupClosesPartialSocket() {
        TrackingSocket socket = new TrackingSocket(true);
        TcpClient client = new TcpClient("localhost", 4242, () -> socket);

        assertThrows(IOException.class, client::open);

        assertTrue(socket.closed);
    }

    @Test
    public void closeIsIdempotent() throws IOException {
        TrackingSocket socket = new TrackingSocket(false);
        TcpClient client = new TcpClient("localhost", 4242, () -> socket);
        client.open();

        client.close();
        client.close();

        assertEquals(1, socket.closeCount);
    }

    @Test
    public void timeoutClosesSocketAndStartsBackoff() {
        TrackingSocket socket = new TrackingSocket(false);
        socket.connectFailure = new SocketTimeoutException("timed out");
        AtomicInteger socketRequests = new AtomicInteger();
        TcpClient client = new TcpClient("localhost", 4242, 123, () -> {
            socketRequests.incrementAndGet();
            return socket;
        });

        assertThrows(IOException.class, client::open);
        assertEquals(123, socket.connectTimeout);
        assertTrue(socket.closed);
        assertTrue(getConnectTime(client) >= socket.connectFailureTime);

        assertThrows(IOException.class, client::open);
        assertEquals(1, socketRequests.get());
    }

    @Test
    public void flushFailureIsReportedAndClosesConnection() throws IOException {
        TrackingSocket socket = new TrackingSocket(false);
        socket.outputStream = new FailingFlushOutputStream();
        TcpClient client = new TcpClient("localhost", 4242, () -> socket);
        client.open();
        client.write("put metric 1 1\n");
        assertFalse(socket.closed);

        UncheckedIOException exception = assertThrows(UncheckedIOException.class, client::flush);

        assertTrue(exception.getMessage().contains("localhost:4242"));
        assertTrue(socket.closed);
        assertEquals(1, socket.closeCount);
    }

    @Test
    public void writeFailureIsReportedBeforeReconnect() throws IOException {
        TrackingSocket failedSocket = new TrackingSocket(false);
        failedSocket.outputStream = new FailingWriteOutputStream();
        TrackingSocket retrySocket = new TrackingSocket(false);
        TcpClient client = new TcpClient("localhost", 4242, socketSupplier(failedSocket, retrySocket));
        client.open();

        assertThrows(IOException.class, () -> client.write("x".repeat(16_384)));
        assertTrue(failedSocket.closed);
        assertFalse(retrySocket.connected);

        client.write("put metric 1 1\n");
        client.flush();
        assertTrue(retrySocket.connected);
    }

    private static java.util.function.Supplier<Socket> socketSupplier(Socket... sockets) {
        Queue<Socket> queue = new ArrayDeque<>(Arrays.asList(sockets));
        return queue::remove;
    }

    private static void setReconnectDelay(TcpClient client, long delay) throws Exception {
        Field connectTime = TcpClient.class.getDeclaredField("connectTime");
        Field backoff = TcpClient.class.getDeclaredField("backoff");
        connectTime.setAccessible(true);
        backoff.setAccessible(true);
        connectTime.setLong(client, 0L);
        backoff.setLong(client, delay);
    }

    private static long getConnectTime(TcpClient client) {
        try {
            Field connectTime = TcpClient.class.getDeclaredField("connectTime");
            connectTime.setAccessible(true);
            return connectTime.getLong(client);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private static class TrackingSocket extends Socket {
        private final boolean failOutputStream;
        private boolean connected;
        private boolean closed;
        private int closeCount;
        private int connectTimeout;
        private long connectFailureTime;
        private IOException connectFailure;
        private OutputStream outputStream = new ByteArrayOutputStream();

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
            return outputStream;
        }

        @Override
        public synchronized void close() {
            closed = true;
            closeCount++;
        }
    }

    private static class FailingFlushOutputStream extends OutputStream {
        private int flushCount;

        @Override
        public void write(int value) {}

        @Override
        public void flush() throws IOException {
            if (++flushCount > 1) {
                throw new IOException("flush failed");
            }
        }
    }

    private static class FailingWriteOutputStream extends OutputStream {
        @Override
        public void write(int value) throws IOException {
            throw new IOException("write failed");
        }
    }
}
