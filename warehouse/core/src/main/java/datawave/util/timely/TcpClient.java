package datawave.util.timely;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TcpClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(TcpClient.class);

    private final String host;
    private final int port;
    private final Supplier<Socket> socketSupplier;
    private Socket sock = null;
    private PrintWriter out = null;
    private long connectTime = 0L;
    private long backoff = 2000;

    public TcpClient(String hostname, int port) {
        this(hostname, port, Socket::new);
    }

    TcpClient(String hostname, int port, Supplier<Socket> socketSupplier) {
        this.host = hostname;
        this.port = port;
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
    }

    public synchronized void flush() {
        if (null != out) {
            out.flush();
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
        if (null != sock && (!sock.isConnected() || null == out || out.checkError())) {
            closeConnection();
        }
        if (null == sock) {
            if (System.currentTimeMillis() > (connectTime + backoff)) {
                Socket newSocket = socketSupplier.get();
                try {
                    connectTime = System.currentTimeMillis();
                    newSocket.connect(new InetSocketAddress(host, port));
                    PrintWriter newWriter = new PrintWriter(newSocket.getOutputStream(), false);
                    sock = newSocket;
                    out = newWriter;
                    backoff = 2000;
                    LOG.info("Connected to Timely at {}:{}", host, port);
                } catch (IOException e) {
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
