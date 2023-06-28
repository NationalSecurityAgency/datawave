package datawave.util.timely;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;

public class UdpClient implements AutoCloseable {

    private final InetSocketAddress address;
    private final DatagramPacket packet;
    private DatagramSocket sock;

    public UdpClient(String hostname, int port) {
        this.address = new InetSocketAddress(hostname, port);
        this.packet = new DatagramPacket("".getBytes(UTF_8), 0, 0, address.getAddress(), port);
    }

    public void open() throws IOException {
        if (null == sock) {
            this.sock = new DatagramSocket();
        }
    }

    public void write(String metric) throws IOException {
        if (null == this.sock) {
            throw new IllegalStateException("Must call open first");
        }
        this.packet.setData(metric.getBytes(UTF_8));
        this.sock.send(packet);
    }

    public void flush() throws IOException {}

    public void close() throws IOException {
        try {
            if (null != this.sock) {
                this.sock.close();
            }
        } finally {
            this.sock = null;
        }
    }

}
