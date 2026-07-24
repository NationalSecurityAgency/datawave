package datawave.microservice.querymetric;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Queue;

import org.junit.jupiter.api.Test;

class TimelyTcpClientTest {

    @Test
    void writesStayBufferedUntilExplicitFlush() throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        TrackingSocket socket = new TrackingSocket(output);
        TimelyTcpClient client = new TimelyTcpClient("localhost", 4242, 1000, () -> socket, () -> 0L);

        client.open();
        client.write("put métric 1 1\n");
        client.write("put second 1 2\n");

        assertEquals(0, output.size());
        client.flush();
        assertArrayEquals("put métric 1 1\nput second 1 2\n".getBytes(UTF_8), output.toByteArray());
    }

    @Test
    void flushFailureInvalidatesConnectionBeforeReconnect() throws Exception {
        TrackingSocket failedSocket = new TrackingSocket(new FailingFlushOutputStream());
        TrackingSocket retrySocket = new TrackingSocket(new ByteArrayOutputStream());
        TimelyTcpClient client = new TimelyTcpClient("localhost", 4242, 1000, socketSupplier(failedSocket, retrySocket), () -> 0L);

        client.open();
        client.write("put first 1 1\n");
        assertFalse(failedSocket.closed);

        IOException failure = assertThrows(IOException.class, client::flush);
        assertTrue(failure.getMessage().contains("localhost:4242"));
        assertTrue(failedSocket.closed);

        client.write("put second 1 2\n");
        client.flush();
        assertTrue(retrySocket.connected);
    }

    private static java.util.function.Supplier<Socket> socketSupplier(Socket... sockets) {
        Queue<Socket> queue = new ArrayDeque<>(Arrays.asList(sockets));
        return queue::remove;
    }

    private static class TrackingSocket extends Socket {
        private final OutputStream outputStream;
        private boolean connected;
        private boolean closed;

        private TrackingSocket(OutputStream outputStream) {
            this.outputStream = outputStream;
        }

        @Override
        public void connect(SocketAddress endpoint, int timeout) {
            connected = true;
        }

        @Override
        public boolean isConnected() {
            return connected;
        }

        @Override
        public boolean isClosed() {
            return closed;
        }

        @Override
        public OutputStream getOutputStream() {
            return outputStream;
        }

        @Override
        public synchronized void close() {
            closed = true;
        }
    }

    private static class FailingFlushOutputStream extends OutputStream {
        @Override
        public void write(int value) {}

        @Override
        public void flush() throws IOException {
            throw new IOException("flush failed");
        }
    }
}
