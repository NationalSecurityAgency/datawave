package datawave.util.compression;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link GZipCompression}.
 * Notes:
 * - We do NOT assert compressed bytes equality because GZIP headers (e.g., timestamps)
 *   can make outputs differ across runs/environments.
 * - We focus on round-trip integrity and expected failure modes on bad inputs.
 */
class GZipCompressionTest {

    private GZipCompression gzip;

    @BeforeEach
    void setUp() {
        gzip = new GZipCompression();
    }

    @Test
    void roundTrip_utf8_simpleAndUnicode() throws Exception {
        String original = "Hello, café — Καλημέρα — こんにちは — 👋🌍";
        String base64 = gzip.compress(original, StandardCharsets.UTF_8);
        String restored = gzip.decompress(base64, StandardCharsets.UTF_8);
        assertEquals(original, restored, "UTF-8 round trip should restore original string");
    }

    @Test
    void compressibleData_resultsInReasonableSize() throws Exception {
        // Highly repetitive -> compresses very well even after Base64 inflation.
        String original = "A".repeat(10_000);
        String base64 = gzip.compress(original, StandardCharsets.UTF_8);

        // Compare Base64 encoded compressed length vs original byte length
        int originalBytes = original.getBytes(StandardCharsets.UTF_8).length;
        assertTrue(base64.length() < originalBytes,
                "Base64(compressed) should be smaller than original bytes for highly compressible data");
        // Still verify round-trip
        assertEquals(original, gzip.decompress(base64, StandardCharsets.UTF_8));
    }

    @Test
    void emptyString_roundTrip_ok() throws Exception {
        String original = "";
        String base64 = gzip.compress(original, StandardCharsets.UTF_8);
        String restored = gzip.decompress(base64, StandardCharsets.UTF_8);
        assertEquals(original, restored);
    }

    @Test
    void iso88591_roundTrip_ok() throws Exception {
        // é exists in ISO-8859-1. verify round-trip with a non-UTF charset.
        Charset latin1 = StandardCharsets.ISO_8859_1;
        String original = "café naïve façade résumé";
        String base64 = gzip.compress(original, latin1);
        String restored = gzip.decompress(base64, latin1);
        assertEquals(original, restored, "ISO-8859-1 round trip should restore original string");
    }

    @Test
    void invalidBase64_throwsOnDecompress() {
        // Not valid Base64. decode will yield garbage/empty, GZIPInputStream should fail.
        String notBase64 = "this is not base64!!!";
        assertThrows(Exception.class, () -> gzip.decompress(notBase64, StandardCharsets.UTF_8),
                "Decompression should fail on invalid Base64");
    }

    @Test
    void validBase64ButNotGzip_throwsOnDecompress() {
        // Base64 of plain text "hello" (not a GZIP stream).
        String base64OfPlain = java.util.Base64.getEncoder().encodeToString("hello".getBytes(StandardCharsets.UTF_8));
        assertThrows(Exception.class, () -> gzip.decompress(base64OfPlain, StandardCharsets.UTF_8),
                "Decompression should fail when bytes are not GZIP-compressed");
    }

    @Test
    void largeData_roundTrip_ok() throws Exception {
        // Pseudo-random string to simulate less-compressible data; validates stability and correctness.
        byte[] bytes = new byte[1 << 18]; // 256 KiB
        new Random(12345).nextBytes(bytes);
        String original = new String(bytes, StandardCharsets.ISO_8859_1); // 1:1 mapping for bytes 0-255

        String base64 = gzip.compress(original, StandardCharsets.ISO_8859_1);
        String restored = gzip.decompress(base64, StandardCharsets.ISO_8859_1);
        assertEquals(original, restored, "Large data should round-trip correctly");
    }

    @Test
    void nullData_throwsNullPointer_inCompress() {
        assertThrows(NullPointerException.class, () -> gzip.compress(null, StandardCharsets.UTF_8),
                "Compressing null data should throw NPE");
    }

    @Test
    void nullCharset_throwsNullPointer_inCompress() {
        assertThrows(NullPointerException.class, () -> gzip.compress("data", null),
                "Compressing with null charset should throw NPE");
    }

    @Test
    void nullBase64_throwsNullPointer_inDecompress() {
        assertThrows(NullPointerException.class, () -> gzip.decompress(null, StandardCharsets.UTF_8),
                "Decompressing null Base64 should throw NPE");
    }

}
