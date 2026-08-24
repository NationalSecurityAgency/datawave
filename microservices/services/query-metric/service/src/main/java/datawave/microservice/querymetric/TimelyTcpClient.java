package datawave.microservice.querymetric;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Query-metric service TCP transport used until the minimum immutable datawave-core release contains the same failure and timeout handling.
 */
class TimelyTcpClient implements AutoCloseable {
    static final int DEFAULT_CONNECT_TIMEOUT_MILLIS = (int) TimeUnit.SECONDS.toMillis(10);
    private static final long INITIAL_BACKOFF_MILLIS = TimeUnit.SECONDS.toMillis(2);
    private static final long MAX_BACKOFF_MILLIS = TimeUnit.MINUTES.toMillis(2);

    private final String host;
    private final int port;
    private final int connectTimeoutMillis;
    private final Supplier<Socket> socketSupplier;
    private final LongSupplier clock;
    private Socket socket;
    private BufferedWriter writer;
    private long nextConnectTime;
    private long backoffMillis = INITIAL_BACKOFF_MILLIS;

    TimelyTcpClient(String host, int port) {
        this(host, port, DEFAULT_CONNECT_TIMEOUT_MILLIS, Socket::new, System::currentTimeMillis);
    }

    TimelyTcpClient(String host, int port, int connectTimeoutMillis) {
        this(host, port, connectTimeoutMillis, Socket::new, System::currentTimeMillis);
    }

    TimelyTcpClient(String host, int port, int connectTimeoutMillis, Supplier<Socket> socketSupplier, LongSupplier clock) {
        if (host == null || host.trim().isEmpty()) {
            throw new IllegalArgumentException("host must not be blank");
        }
        if (port < 1 || port > 65_535) {
            throw new IllegalArgumentException("port must be between 1 and 65535");
        }
        if (connectTimeoutMillis <= 0) {
            throw new IllegalArgumentException("connectTimeoutMillis must be greater than zero");
        }
        this.host = host;
        this.port = port;
        this.connectTimeoutMillis = connectTimeoutMillis;
        this.socketSupplier = socketSupplier;
        this.clock = clock;
    }

    synchronized void open() throws IOException {
        connect();
    }

    synchronized void write(String metric) throws IOException {
        connect();
        try {
            writer.write(metric);
        } catch (IOException e) {
            discardConnection();
            throw new IOException("Error writing metric to Timely at " + host + ":" + port, e);
        }
    }

    synchronized void flush() throws IOException {
        if (writer == null) {
            throw new IOException("Not connected to Timely at " + host + ":" + port);
        }
        try {
            writer.flush();
        } catch (IOException e) {
            discardConnection();
            throw new IOException("Error flushing metrics to Timely at " + host + ":" + port, e);
        }
    }

    private void connect() throws IOException {
        if (socket != null && (!socket.isConnected() || socket.isClosed() || writer == null)) {
            discardConnection();
        }
        if (socket != null) {
            return;
        }

        long now = clock.getAsLong();
        if (now < nextConnectTime) {
            throw new IOException("Waiting to reconnect to Timely at " + host + ":" + port);
        }

        Socket candidate = socketSupplier.get();
        try {
            candidate.connect(new InetSocketAddress(host, port), connectTimeoutMillis);
            BufferedWriter candidateWriter = new BufferedWriter(new OutputStreamWriter(candidate.getOutputStream(), UTF_8));
            socket = candidate;
            writer = candidateWriter;
            nextConnectTime = 0;
            backoffMillis = INITIAL_BACKOFF_MILLIS;
        } catch (IOException e) {
            nextConnectTime = clock.getAsLong() + backoffMillis;
            backoffMillis = Math.min(backoffMillis * 2, MAX_BACKOFF_MILLIS);
            closeSocket(candidate);
            throw new IOException("Error connecting to Timely at " + host + ":" + port, e);
        }
    }

    private void discardConnection() {
        BufferedWriter discardedWriter = writer;
        Socket discardedSocket = socket;
        writer = null;
        socket = null;
        closeSocket(discardedSocket);
        if (discardedWriter != null) {
            try {
                discardedWriter.close();
            } catch (IOException ignored) {
                // The connection is already unusable.
            }
        }
    }

    private static void closeSocket(Socket socket) {
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException ignored) {
                // The connection is already unusable.
            }
        }
    }

    @Override
    public synchronized void close() throws IOException {
        BufferedWriter closingWriter = writer;
        Socket closingSocket = socket;
        writer = null;
        socket = null;

        IOException failure = null;
        if (closingWriter != null) {
            try {
                closingWriter.close();
            } catch (IOException e) {
                failure = e;
            }
        }
        if (closingSocket != null) {
            try {
                closingSocket.close();
            } catch (IOException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }
}
