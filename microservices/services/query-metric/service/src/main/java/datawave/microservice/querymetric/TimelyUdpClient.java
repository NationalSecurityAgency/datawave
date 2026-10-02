package datawave.microservice.querymetric;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Query-metric service UDP transport used until the minimum immutable datawave-core release retries hostname resolution.
 */
class TimelyUdpClient implements AutoCloseable {
    static final int MAX_DATAGRAM_PAYLOAD_SIZE = 65_507;

    private final String host;
    private final int port;
    private final HostResolver resolver;
    private DatagramPacket packet;
    private DatagramSocket socket;

    TimelyUdpClient(String host, int port) {
        this(host, port, InetAddress::getByName);
    }

    TimelyUdpClient(String host, int port, HostResolver resolver) {
        if (host == null || host.trim().isEmpty()) {
            throw new IllegalArgumentException("host must not be blank");
        }
        if (port < 1 || port > 65_535) {
            throw new IllegalArgumentException("port must be between 1 and 65535");
        }
        this.host = host;
        this.port = port;
        this.resolver = resolver;
    }

    synchronized void open() throws IOException {
        if (socket != null) {
            return;
        }

        InetAddress address;
        try {
            address = resolver.resolve(host);
        } catch (UnknownHostException e) {
            throw new IOException("Unable to resolve Timely UDP host " + host, e);
        }

        DatagramSocket candidate = new DatagramSocket();
        packet = new DatagramPacket(new byte[0], 0, address, port);
        socket = candidate;
    }

    synchronized void write(String metric) throws IOException {
        if (socket == null) {
            throw new IllegalStateException("Must call open first");
        }
        byte[] bytes = metric.getBytes(UTF_8);
        if (bytes.length > MAX_DATAGRAM_PAYLOAD_SIZE) {
            throw new IOException("Timely UDP metric is " + bytes.length + " bytes; maximum is " + MAX_DATAGRAM_PAYLOAD_SIZE);
        }
        packet.setData(bytes);
        socket.send(packet);
    }

    @Override
    public synchronized void close() {
        if (socket != null) {
            socket.close();
            socket = null;
            packet = null;
        }
    }

    @FunctionalInterface
    interface HostResolver {
        InetAddress resolve(String host) throws UnknownHostException;
    }
}
