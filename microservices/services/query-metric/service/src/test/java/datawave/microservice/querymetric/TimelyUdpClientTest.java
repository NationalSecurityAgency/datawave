package datawave.microservice.querymetric;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

class TimelyUdpClientTest {

    @Test
    void failedResolutionCanBeRetried() throws Exception {
        AtomicInteger resolutionAttempts = new AtomicInteger();
        try (DatagramSocket receiver = new DatagramSocket(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
                        TimelyUdpClient client = new TimelyUdpClient("timely.test", receiver.getLocalPort(), host -> {
                            if (resolutionAttempts.getAndIncrement() == 0) {
                                throw new UnknownHostException(host);
                            }
                            return InetAddress.getLoopbackAddress();
                        })) {
            IOException failure = assertThrows(IOException.class, client::open);
            assertTrue(failure.getMessage().contains("timely.test"));

            client.open();
            client.open();
            String metric = "put métric 1 2\n";
            client.write(metric);

            byte[] buffer = new byte[256];
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
            receiver.setSoTimeout(1000);
            receiver.receive(packet);

            assertEquals(2, resolutionAttempts.get());
            assertArrayEquals(metric.getBytes(UTF_8), Arrays.copyOf(packet.getData(), packet.getLength()));
        }
    }

    @Test
    void rejectsInvalidPortsAndOversizedUtf8Datagrams() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> new TimelyUdpClient("localhost", 0));
        try (DatagramSocket receiver = new DatagramSocket(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
                        TimelyUdpClient client = new TimelyUdpClient("localhost", receiver.getLocalPort())) {
            client.open();
            String oversized = "é".repeat((TimelyUdpClient.MAX_DATAGRAM_PAYLOAD_SIZE / 2) + 1);
            assertThrows(IOException.class, () -> client.write(oversized));
        }
    }
}
