package datawave.util.timely;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;

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

    @Test
    public void testOpen_capsReconnectBackoff() throws Exception {
        TcpClient client = new TcpClient("127.0.0.1", 0);
        Field backoff = TcpClient.class.getDeclaredField("backoff");
        backoff.setAccessible(true);
        long maxBackoff = TimeUnit.SECONDS.toMillis(120);
        backoff.setLong(client, maxBackoff);

        assertThrows(IOException.class, client::open);

        assertEquals(maxBackoff, backoff.getLong(client));
    }
}
