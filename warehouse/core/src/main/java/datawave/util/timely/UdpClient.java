package datawave.util.timely;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

public class UdpClient implements AutoCloseable {

    public static final int MAX_DATAGRAM_PAYLOAD_SIZE = 65_507;

    private final String hostname;
    private final int port;
    private final HostResolver resolver;
    private DatagramPacket packet;
    private DatagramSocket sock;

    public UdpClient(String hostname, int port) {
        this(hostname, port, InetAddress::getByName);
    }

    UdpClient(String hostname, int port, HostResolver resolver) {
        InetSocketAddress address = InetSocketAddress.createUnresolved(hostname, port);
        this.hostname = address.getHostString();
        this.port = address.getPort();
        this.resolver = resolver;
    }

    public synchronized void open() throws IOException {
        if (null == sock) {
            InetAddress address;
            try {
                address = resolver.resolve(hostname);
            } catch (UnknownHostException e) {
                throw new IOException("Unable to resolve Timely UDP host " + hostname, e);
            }
            DatagramPacket newPacket = new DatagramPacket(new byte[0], 0, address, port);
            DatagramSocket newSocket = new DatagramSocket();
            this.packet = newPacket;
            this.sock = newSocket;
        }
    }

    public synchronized void write(String metric) throws IOException {
        if (null == this.sock) {
            throw new IllegalStateException("Must call open first");
        }
        byte[] metricBytes = metric.getBytes(UTF_8);
        if (metricBytes.length > MAX_DATAGRAM_PAYLOAD_SIZE) {
            throw new DatagramTooLargeException(metricBytes.length);
        }
        this.packet.setData(metricBytes);
        this.sock.send(packet);
    }

    public void flush() throws IOException {}

    public synchronized void close() throws IOException {
        try {
            if (null != this.sock) {
                this.sock.close();
            }
        } finally {
            this.sock = null;
            this.packet = null;
        }
    }

    @FunctionalInterface
    interface HostResolver {
        InetAddress resolve(String hostname) throws UnknownHostException;
    }

    public static final class DatagramTooLargeException extends IOException {
        private static final long serialVersionUID = 1L;

        private DatagramTooLargeException(int payloadSize) {
            super("Timely UDP metric is " + payloadSize + " bytes; maximum is " + MAX_DATAGRAM_PAYLOAD_SIZE);
        }
    }
}
