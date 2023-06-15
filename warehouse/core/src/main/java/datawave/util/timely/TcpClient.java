package datawave.util.timely;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TcpClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(TcpClient.class);

    private final String host;
    private final int port;
    private Socket sock = null;
    private PrintWriter out = null;
    private long connectTime = 0L;
    private long backoff = 2000;

    public TcpClient(String hostname, int port) {
        this.host = hostname;
        this.port = port;
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
    public void close() throws IOException {
        LOG.info("Shutting down connection to Timely at {}:{}", host, port);
        if (null != sock) {
            try {
                if (null != out) {
                    out.close();
                }
                sock.close();
            } catch (IOException e) {
                LOG.error("Error closing connection to Timely at " + host + ":" + port + ". Error: " + e.getMessage());
            } finally {
                sock = null;
            }
        }
    }

    private synchronized int connect() {
        if (null == sock || !sock.isConnected() || out.checkError()) {
            if (System.currentTimeMillis() > (connectTime + backoff)) {
                try {
                    connectTime = System.currentTimeMillis();
                    sock = new Socket(host, port);
                    out = new PrintWriter(sock.getOutputStream(), false);
                    backoff = 2000;
                    LOG.info("Connected to Timely at {}:{}", host, port);
                } catch (IOException e) {
                    LOG.error("Error connecting to Timely at {}:" + host + ":" + port + ". Error: " + e.getMessage());
                    backoff = backoff * 2;
                    sock = null;
                    out = null;
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

}
