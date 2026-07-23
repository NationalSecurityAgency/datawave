package datawave.util.timely;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertArrayEquals;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;

import org.junit.Test;

public class TcpClientTest {

    @Test
    public void testCreateWriter_usesUtf8() {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrintWriter writer = TcpClient.createWriter(output);
        String metric = "put caf\u00e9 1 2 tag=\u20ac\n";

        writer.write(metric);
        writer.flush();

        assertArrayEquals(metric.getBytes(UTF_8), output.toByteArray());
    }
}
