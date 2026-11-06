package datawave.util.compression;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Random;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class CodecTest {

    @Test
    void algorithmsMapToTheirSingletonCodecs() {
        assertSame(GZIPCodec.instance(), CodecAlgorithm.GZIP.getCodec());
        assertSame(BZIP2Codec.instance(), CodecAlgorithm.BZIP2.getCodec());
        assertSame(SevenZIPCodec.instance(), CodecAlgorithm.SEVEN_ZIP.getCodec());
    }

    @ParameterizedTest
    @EnumSource(CodecAlgorithm.class)
    void codecReportsItsAlgorithm(CodecAlgorithm algorithm) {
        assertSame(algorithm, algorithm.getCodec().getAlgorithm());
    }

    @ParameterizedTest
    @EnumSource(CodecAlgorithm.class)
    void roundTripEmptyString(CodecAlgorithm algorithm) {
        Codec codec = algorithm.getCodec();
        String compressed = codec.compress("", StandardCharsets.UTF_8);
        assertNotEquals("", compressed, algorithm + " should still emit a compressed stream for empty input");
        assertEquals("", codec.decompress(compressed, StandardCharsets.UTF_8));
    }

    @ParameterizedTest
    @EnumSource(CodecAlgorithm.class)
    void roundTripNonAsciiUtf8(CodecAlgorithm algorithm) {
        Codec codec = algorithm.getCodec();
        String original = "Hello, café — Καλημέρα — こんにちは — 👋🌍";
        String compressed = codec.compress(original, StandardCharsets.UTF_8);
        assertEquals(original, codec.decompress(compressed, StandardCharsets.UTF_8));
    }

    @ParameterizedTest
    @EnumSource(CodecAlgorithm.class)
    void outputIsBase64(CodecAlgorithm algorithm) {
        String compressed = algorithm.getCodec().compress("some data to compress", StandardCharsets.UTF_8);
        // throws IllegalArgumentException if not valid Base64
        byte[] decoded = Base64.getDecoder().decode(compressed);
        assertTrue(decoded.length > 0);
    }

    @ParameterizedTest
    @EnumSource(CodecAlgorithm.class)
    void compressibleDataShrinks(CodecAlgorithm algorithm) {
        Codec codec = algorithm.getCodec();
        String original = "A".repeat(10_000);
        String compressed = codec.compress(original, StandardCharsets.UTF_8);
        assertTrue(compressed.length() < original.length(), algorithm + " Base64(compressed) should be smaller than highly compressible input");
        assertEquals(original, codec.decompress(compressed, StandardCharsets.UTF_8));
    }

    @ParameterizedTest
    @EnumSource(CodecAlgorithm.class)
    void roundTripLargeRandomInput(CodecAlgorithm algorithm) {
        Codec codec = algorithm.getCodec();
        byte[] bytes = new byte[1 << 18]; // 256 KiB
        new Random(12345).nextBytes(bytes);
        // ISO-8859-1 maps bytes to chars 1:1, so arbitrary bytes survive the round trip
        String original = new String(bytes, StandardCharsets.ISO_8859_1);
        String compressed = codec.compress(original, StandardCharsets.ISO_8859_1);
        assertEquals(original, codec.decompress(compressed, StandardCharsets.ISO_8859_1));
    }

    @ParameterizedTest
    @EnumSource(CodecAlgorithm.class)
    void roundTripLargeUtf8Input(CodecAlgorithm algorithm) {
        Codec codec = algorithm.getCodec();
        StringBuilder sb = new StringBuilder();
        Random random = new Random(42);
        String alphabet = "abcXYZ019 éüñ漢字👋";
        int[] codePoints = alphabet.codePoints().toArray();
        for (int i = 0; i < 200_000; i++) {
            sb.appendCodePoint(codePoints[random.nextInt(codePoints.length)]);
        }
        String original = sb.toString();
        String compressed = codec.compress(original, StandardCharsets.UTF_8);
        assertEquals(original, codec.decompress(compressed, StandardCharsets.UTF_8));
    }

    @ParameterizedTest
    @EnumSource(CodecAlgorithm.class)
    void invalidBase64FailsToDecompress(CodecAlgorithm algorithm) {
        assertThrows(UncheckedIOException.class, () -> algorithm.getCodec().decompress("this is not base64!!!", StandardCharsets.UTF_8));
    }

    @ParameterizedTest
    @EnumSource(CodecAlgorithm.class)
    void base64OfUncompressedDataFailsToDecompress(CodecAlgorithm algorithm) {
        String base64OfPlain = Base64.getEncoder().encodeToString("hello".getBytes(StandardCharsets.UTF_8));
        assertThrows(UncheckedIOException.class, () -> algorithm.getCodec().decompress(base64OfPlain, StandardCharsets.UTF_8));
    }

    @Test
    void codecsAreNotInterchangeable() {
        String gzipped = GZIPCodec.instance().compress("hello world", StandardCharsets.UTF_8);
        assertThrows(UncheckedIOException.class, () -> BZIP2Codec.instance().decompress(gzipped, StandardCharsets.UTF_8));
    }
}
