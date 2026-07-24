package datawave.util.timely;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

public class UdpClientTest {

    @Test
    public void failedResolutionCanBeRetried() throws Exception {
        AtomicInteger resolutionAttempts = new AtomicInteger();
        try (DatagramSocket receiver = new DatagramSocket(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
                        UdpClient client = new UdpClient("timely.test", receiver.getLocalPort(), hostname -> {
                            if (resolutionAttempts.getAndIncrement() == 0) {
                                throw new UnknownHostException(hostname);
                            }
                            return InetAddress.getLoopbackAddress();
                        })) {
            IOException failure = assertThrows(IOException.class, client::open);
            assertTrue(failure.getMessage().contains("timely.test"));

            client.open();
            client.open();
            String metric = "put m\u00e9tric 1 2\n";
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
    public void constructorStillRejectsInvalidPorts() {
        assertThrows(IllegalArgumentException.class, () -> new UdpClient("timely.test", -1));
        assertThrows(IllegalArgumentException.class, () -> new UdpClient("timely.test", 65536));
    }
}
