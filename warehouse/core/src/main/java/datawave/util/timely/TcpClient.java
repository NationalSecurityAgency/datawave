package datawave.util.timely;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TcpClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(TcpClient.class);
    static final int DEFAULT_CONNECT_TIMEOUT_MILLIS = (int) TimeUnit.SECONDS.toMillis(10);

    private final String host;
    private final int port;
    private final int connectTimeoutMillis;
    private final Supplier<Socket> socketSupplier;
    private Socket sock = null;
    private PrintWriter out = null;
    private long connectTime = 0L;
    private long backoff = 2000;

    public TcpClient(String hostname, int port) {
        this(hostname, port, DEFAULT_CONNECT_TIMEOUT_MILLIS);
    }

    public TcpClient(String hostname, int port, int connectTimeoutMillis) {
        this(hostname, port, connectTimeoutMillis, Socket::new);
    }

    TcpClient(String hostname, int port, Supplier<Socket> socketSupplier) {
        this(hostname, port, DEFAULT_CONNECT_TIMEOUT_MILLIS, socketSupplier);
    }

    TcpClient(String hostname, int port, int connectTimeoutMillis, Supplier<Socket> socketSupplier) {
        if (connectTimeoutMillis <= 0) {
            throw new IllegalArgumentException("connectTimeoutMillis must be greater than zero");
        }
        this.host = hostname;
        this.port = port;
        this.connectTimeoutMillis = connectTimeoutMillis;
        this.socketSupplier = socketSupplier;
    }

    /**
     * Opens a TCP connection to the specified host and port
     *
     * @throws IOException
     *             if an error occurs
     */
    public void open() throws IOException {
        if (connect() != 0) {
            throw new IOException();
        }
    }

    /**
     * Write a metric to Timely
     *
     * @param metric
     *            newline terminated string representation of Timely metric
     * @throws IOException
     *             an error occurs
     */
    public synchronized void write(String metric) throws IOException {
        if (connect() != 0) {
            throw new IOException();
        }
        out.write(metric);
        if (out.checkError()) {
            closeConnection();
            throw new IOException("Error writing metric to Timely at " + host + ":" + port);
        }
    }

    public synchronized void flush() {
        if (null != out && out.checkError()) {
            closeConnection();
            throw new UncheckedIOException("Error flushing metrics to Timely at " + host + ":" + port, new IOException("PrintWriter reported an error"));
        }
    }

    /**
     * Closes the tcp connection to Timely
     *
     * @throws IOException
     *             if an error occurs
     */
    @Override
    public synchronized void close() throws IOException {
        LOG.info("Shutting down connection to Timely at {}:{}", host, port);
        closeConnection();
    }

    private synchronized int connect() {
        if (null != sock && (!sock.isConnected() || null == out)) {
            closeConnection();
        }
        if (null == sock) {
            if (System.currentTimeMillis() > (connectTime + backoff)) {
                Socket newSocket = socketSupplier.get();
                try {
                    newSocket.connect(new InetSocketAddress(host, port), connectTimeoutMillis);
                    PrintWriter newWriter = new PrintWriter(newSocket.getOutputStream(), false);
                    sock = newSocket;
                    out = newWriter;
                    backoff = 2000;
                    LOG.info("Connected to Timely at {}:{}", host, port);
                } catch (IOException e) {
                    connectTime = System.currentTimeMillis();
                    LOG.error("Error connecting to Timely at {}:" + host + ":" + port + ". Error: " + e.getMessage());
                    backoff = backoff * 2;
                    closeSocket(newSocket);
                    LOG.warn("Will retry connection in {} ms.", backoff);
                    return -1;
                }
            } else {
                LOG.warn("Not writing to Timely, waiting to reconnect");
                return -1;
            }
        }
        return 0;
    }

    private void closeConnection() {
        PrintWriter writer = out;
        Socket socket = sock;
        out = null;
        sock = null;
        if (null != writer) {
            writer.close();
        }
        closeSocket(socket);
    }

    private void closeSocket(Socket socket) {
        if (null != socket) {
            try {
                socket.close();
            } catch (IOException e) {
                LOG.error("Error closing connection to Timely at {}:{}. Error: {}", host, port, e.getMessage());
            }
        }
    }

}
