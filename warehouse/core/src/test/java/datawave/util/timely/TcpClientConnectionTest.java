package datawave.util.timely;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Queue;

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
