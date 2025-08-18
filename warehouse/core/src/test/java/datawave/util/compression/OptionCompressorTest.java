package datawave.util.compression;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class OptionCompressorTest {

    private OptionCompressor compressor;

    @BeforeEach
    void setUp() {
        compressor = new OptionCompressor();
    }

    // Helper wrappers for readability
    private String compress(String s, OptionCompressor.CompressionMethod m, Charset cs) throws Exception {
        return compressor.compress(s, m, cs);
    }
    private String decompress(String s, OptionCompressor.CompressionMethod m, Charset cs) throws Exception {
        return compressor.decompress(s, m, cs);
    }

    //  Round-trip: UTF-8 content across all real compression methods
    @ParameterizedTest
    @EnumSource(value = OptionCompressor.CompressionMethod.class, names = {"GZIP", "BZIP2", "SEVEN_ZIP"})
    void roundTrip_utf8_allAlgos(OptionCompressor.CompressionMethod method) throws Exception {
        String original = "Hello, café — Καλημέρα — こんにちは — 👋🌍";
        String base64 = compress(original, method, StandardCharsets.UTF_8);
        String restored = decompress(base64, method, StandardCharsets.UTF_8);
        assertEquals(original, restored, method + " UTF-8 round trip should restore original string");
    }

    //  Highly compressible data should shrink after Base64 (still smaller than original bytes)
    @ParameterizedTest
    @EnumSource(value = OptionCompressor.CompressionMethod.class, names = {"GZIP", "BZIP2", "SEVEN_ZIP"})
    void compressibleData_resultsInReasonableSize_allAlgos(OptionCompressor.CompressionMethod method) throws Exception {
        String original = "A".repeat(10_000);
        String base64 = compress(original, method, StandardCharsets.UTF_8);

        int originalBytes = original.getBytes(StandardCharsets.UTF_8).length;
        assertTrue(base64.length() < originalBytes,
                () -> method + " Base64(compressed) should be smaller than original bytes for highly compressible data");

        assertEquals(original, decompress(base64, method, StandardCharsets.UTF_8));
    }

    //  Large-ish random data round-trip (ISO-8859-1 for 1:1 byte<->char)
    @ParameterizedTest
    @EnumSource(value = OptionCompressor.CompressionMethod.class, names = {"GZIP", "BZIP2", "SEVEN_ZIP"})
    void largeData_roundTrip_allAlgos(OptionCompressor.CompressionMethod method) throws Exception {
        byte[] bytes = new byte[1 << 18]; // 256 KiB
        new Random(12345).nextBytes(bytes);
        String original = new String(bytes, StandardCharsets.ISO_8859_1);

        String base64 = compress(original, method, StandardCharsets.ISO_8859_1);
        String restored = decompress(base64, method, StandardCharsets.ISO_8859_1);
        assertEquals(original, restored, method + " large data should round-trip correctly");
    }

    //  Bad inputs: invalid Base64 should fail on real decompressors
    @ParameterizedTest
    @EnumSource(value = OptionCompressor.CompressionMethod.class, names = {"GZIP", "BZIP2", "SEVEN_ZIP"})
    void invalidBase64_throwsOnDecompress_allAlgos(OptionCompressor.CompressionMethod method) {
        String notBase64 = "this is not base64!!!";
        assertThrows(Exception.class, () -> decompress(notBase64, method, StandardCharsets.UTF_8),
                method + " decompression should fail on invalid Base64");
    }

    //  Bad inputs: valid Base64 but not compressed bytes should fail
    @ParameterizedTest
    @EnumSource(value = OptionCompressor.CompressionMethod.class, names = {"GZIP", "BZIP2", "SEVEN_ZIP"})
    void validBase64ButNotCompressed_throwsOnDecompress_allAlgos(OptionCompressor.CompressionMethod method) {
        String base64OfPlain = java.util.Base64.getEncoder()
                .encodeToString("hello".getBytes(StandardCharsets.UTF_8));
        assertThrows(Exception.class, () -> decompress(base64OfPlain, method, StandardCharsets.UTF_8),
                method + " decompression should fail when bytes are not " + method + "-compressed");
    }

    //  NONE: identity behavior (no Base64, no compression)
    @Test
    void noneMethod_identityAndNoBase64Assumptions() throws Exception {
        String original = "Plain text 👀 stays as-is.";
        String compressed = compress(original, OptionCompressor.CompressionMethod.NONE, StandardCharsets.UTF_8);
        assertEquals(original, compressed, "NONE compress should be identity");

        String restored = decompress(original, OptionCompressor.CompressionMethod.NONE, StandardCharsets.UTF_8);
        assertEquals(original, restored, "NONE decompress should be identity");
    }
}
