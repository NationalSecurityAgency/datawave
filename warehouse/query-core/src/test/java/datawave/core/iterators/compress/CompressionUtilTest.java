package datawave.core.iterators.compress;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompressionUtilTest {

    private static final Logger log = LoggerFactory.getLogger(CompressionUtilTest.class);

    @Test
    public void testSmallRoundTrip() {
        String original = "The quick brown fox jumped over the lazy dog";
        compress(original);
    }

    @Test
    public void testLargeRoundTrip() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            sb.append(RandomStringUtils.random(1));
            sb.append(RandomStringUtils.randomNumeric(6));
            sb.append(RandomStringUtils.random(1));
            sb.append(RandomStringUtils.randomAlphanumeric(4));
        }

        String original = sb.toString();
        compress(original);
    }

    private void compress(String input) {
        compressGZIP(input);
        compressZSTD(input);
    }

    private void compressGZIP(String input) {
        byte[] data = input.getBytes();
        byte[] compressed = CompressionUtil.compressGZIP(data);
        byte[] uncompressed = CompressionUtil.decompressGZIP(compressed);
        log.info("gzip orig: {} compressed: {}", data.length, compressed.length);
        assertEquals(input, new String(uncompressed));
    }

    private void compressZSTD(String input) {
        byte[] data = input.getBytes();
        byte[] compressed = CompressionUtil.compressZSTD(data);
        byte[] uncompressed = CompressionUtil.decompressZSTD(compressed);
        log.info("zstd orig: {} compressed: {}", data.length, compressed.length);
        assertEquals(input, new String(uncompressed));
    }

}
