package datawave.query.table.parser;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Random;
import java.util.zip.GZIPOutputStream;

import org.junit.jupiter.api.Test;

public class ContentKeyValueFactoryTest {

    @Test
    public void decodedContentIsBoundedByCharactersRatherThanBytes() {
        byte[] content = "a\u00e9\u4e2d\ud83d\ude00z".getBytes(StandardCharsets.UTF_8);
        assertEquals("a\u00e9\u4e2d", ContentKeyValueFactory.decodeContent(content, 3));
        assertEquals("a\u00e9\u4e2d", ContentKeyValueFactory.decodeContent(content, 4));
        assertEquals("a\u00e9\u4e2d\ud83d\ude00", ContentKeyValueFactory.decodeContent(content, 5));
        assertEquals("a\u00e9\u4e2d\ud83d\ude00z", ContentKeyValueFactory.decodeContent(content, 100));
    }

    @Test
    public void handlesMultibyteCharactersAcrossReadBuffers() throws IOException {
        String prefix = "\u4e2d".repeat(8191);
        byte[] content = (prefix + "\ud83d\ude00z").getBytes(StandardCharsets.UTF_8);
        assertEquals(prefix, ContentKeyValueFactory.decodeContent(content, 8192));
        assertEquals(prefix + "\ud83d\ude00", ContentKeyValueFactory.decodeContent(content, 8193));
        assertEquals(prefix + "\ud83d\ude00", ContentKeyValueFactory.decodeAndDecompressContent(Base64.getEncoder().encode(gzip(content)), 8193));
    }

    @Test
    public void doesNotReturnHalfASupplementaryCharacter() throws IOException {
        byte[] content = "\ud83d\ude00tail".getBytes(StandardCharsets.UTF_8);
        assertEquals("", ContentKeyValueFactory.decodeContent(content, 1));
        assertEquals("", ContentKeyValueFactory.decodeAndDecompressContent(Base64.getEncoder().encode(gzip(content)), 1));
    }

    @Test
    public void supportsBase64GzipMimeBase64AndRawGzip() throws IOException {
        byte[] content = "a\u00e9\u4e2d\ud83d\ude00tail".getBytes(StandardCharsets.UTF_8);
        byte[] compressed = gzip(content);
        assertEquals("a\u00e9\u4e2d", ContentKeyValueFactory.decodeAndDecompressContent(Base64.getEncoder().encode(compressed), 3));
        assertEquals("a\u00e9\u4e2d",
                        ContentKeyValueFactory.decodeAndDecompressContent(Base64.getMimeEncoder(8, new byte[] {'\r', '\n'}).encode(compressed), 3));
        assertEquals("a\u00e9\u4e2d", ContentKeyValueFactory.decodeAndDecompressContent(compressed, 3));
    }

    @Test
    public void recognizesBase64GzipHeaderAcrossMimeWhitespace() throws IOException {
        byte[] content = "a\u00e9\u4e2d\ud83d\ude00tail".getBytes(StandardCharsets.UTF_8);
        byte[] encoded = Base64.getMimeEncoder(2, new byte[] {'\r', '\n'}).encode(gzip(content));
        byte[] withLeadingWhitespace = (" \r\n" + new String(encoded, StandardCharsets.US_ASCII)).getBytes(StandardCharsets.US_ASCII);
        assertEquals("a\u00e9\u4e2d", ContentKeyValueFactory.decodeAndDecompressContent(withLeadingWhitespace, 3));
    }

    @Test
    public void readsGzipPrefixWithoutValidatingADistantBase64Error() throws IOException {
        // Keep the compressed suffix beyond stream read-ahead so the prefix is readable despite a later encoding error.
        byte[] content = new byte[64 * 1024];
        new Random(0).nextBytes(content);
        Arrays.fill(content, 0, 1500, (byte) 'x');
        byte[] encoded = Base64.getEncoder().encode(gzip(content));
        encoded[encoded.length - 16] = '=';
        // Full-content retrieval still validates all Base64; the bounded API deliberately accepts a readable GZIP prefix.
        assertArrayEquals(encoded, ContentKeyValueFactory.decodeAndDecompressContent(encoded));
        assertEquals("x".repeat(1500), ContentKeyValueFactory.decodeAndDecompressContent(encoded, 1500));
    }

    @Test
    public void readsGzipContentWithoutValidatingBase64AlphabetAfterPadding() throws IOException {
        byte[] encoded = Base64.getEncoder().encode(gzip("prefix".getBytes(StandardCharsets.UTF_8)));
        byte[] withInvalidSuffix = Arrays.copyOf(encoded, encoded.length + 1);
        withInvalidSuffix[encoded.length] = 'A';
        assertArrayEquals(withInvalidSuffix, ContentKeyValueFactory.decodeAndDecompressContent(withInvalidSuffix));
        assertEquals("prefix", ContentKeyValueFactory.decodeAndDecompressContent(withInvalidSuffix, 1500));
    }

    @Test
    public void retainsRawFallbackWhenBase64ErrorIsEncounteredDuringGzipReading() throws IOException {
        byte[] encoded = Base64.getEncoder().encode(gzip("prefix content".getBytes(StandardCharsets.UTF_8)));
        // The first decoded bytes contain a valid GZIP header, but padding at the start of the next quartet is invalid.
        encoded[16] = '=';
        assertEquals(new String(encoded, StandardCharsets.US_ASCII).substring(0, 15),
                        ContentKeyValueFactory.decodeAndDecompressContent(encoded, 15));
    }

    @Test
    public void retainsRawFallbackWhenDecodedGzipHeaderIsInvalid() throws IOException {
        byte[] compressed = gzip("prefix content".getBytes(StandardCharsets.UTF_8));
        compressed[2] = 0; // GZIP requires the DEFLATE compression method.
        byte[] encoded = Base64.getEncoder().encode(compressed);
        assertEquals(new String(encoded, StandardCharsets.US_ASCII).substring(0, 15),
                        ContentKeyValueFactory.decodeAndDecompressContent(encoded, 15));
    }

    @Test
    public void supportsBase64WithoutGzip() {
        byte[] content = "short content".getBytes(StandardCharsets.UTF_8);
        assertEquals("short", ContentKeyValueFactory.decodeAndDecompressContent(Base64.getEncoder().encode(content), 5));
        assertEquals("short content", ContentKeyValueFactory.decodeAndDecompressContent(Base64.getEncoder().encode(content), 100));
    }

    @Test
    public void retainsTheUnencodedFallback() {
        // One Base64 alphabet character is invalid, so both APIs fall back to the original bytes.
        byte[] content = {'A'};
        assertEquals("A", ContentKeyValueFactory.decodeAndDecompressContent(content, 10));
        assertArrayEquals(content, ContentKeyValueFactory.decodeAndDecompressContent(content));
    }

    @Test
    public void retainsRawContentWhenInvalidBase64EndingIsBeyondThePrefix() {
        String raw = "hello ".repeat(4000) + "A";
        byte[] content = raw.getBytes(StandardCharsets.UTF_8);
        assertArrayEquals(content, ContentKeyValueFactory.decodeAndDecompressContent(content));
        assertEquals(raw.substring(0, 1500), ContentKeyValueFactory.decodeAndDecompressContent(content, 1500));
    }

    @Test
    public void retainsRawContentWhenBase64AlphabetFollowsPadding() {
        String raw = "YWJjZA== hello";
        byte[] content = raw.getBytes(StandardCharsets.UTF_8);
        assertArrayEquals(content, ContentKeyValueFactory.decodeAndDecompressContent(content));
        assertEquals(raw, ContentKeyValueFactory.decodeAndDecompressContent(content, 1500));
        assertEquals("YWJj", ContentKeyValueFactory.decodeAndDecompressContent(content, 4));
    }

    @Test
    public void mimeBase64StructureMatchesLegacyFallbackSelection() {
        String[] candidates = {"", "A", "AA", "AAA", "AAAA", "YQ==", "YWI=", "YWJj", "YWJjZA", "YWJjZA==", "AA=", "A===", "AAAA=", "AA= =", "AAA==", "AA====",
                " \r\nYQ== \n!", "YQ==\u00ff", "YQ== A", "YWJjZA== hello", "!", "=\n", "+/8=", "Y!Q==", "YQ=\n="};
        for (String candidate : candidates) {
            byte[] content = candidate.getBytes(StandardCharsets.UTF_8);
            String expected = new String(ContentKeyValueFactory.decodeAndDecompressContent(content), StandardCharsets.UTF_8);
            assertEquals(expected, ContentKeyValueFactory.decodeAndDecompressContent(content, 1500), candidate);
        }
    }

    @Test
    public void handlesEmptyAndShortDecodedContent() throws IOException {
        assertEquals("", ContentKeyValueFactory.decodeContent(new byte[0], 1500));
        assertEquals("a", ContentKeyValueFactory.decodeContent(new byte[] {'a'}, 1500));
        assertEquals("", ContentKeyValueFactory.decodeAndDecompressContent(new byte[0], 1500));
        assertEquals("", ContentKeyValueFactory.decodeAndDecompressContent(Base64.getEncoder().encode(gzip(new byte[0])), 1500));
    }

    @Test
    public void handlesEmptyAndShortContentInTheLegacyByteApi() {
        assertArrayEquals(new byte[0], ContentKeyValueFactory.decodeAndDecompressContent(new byte[0]));
        assertArrayEquals(new byte[] {'a'}, ContentKeyValueFactory.decodeAndDecompressContent(new byte[] {'a'}));
    }

    @Test
    public void zeroLimitReturnsEmptyWithoutDecoding() {
        byte[] corruptGzip = {(byte) 0x1f, (byte) 0x8b};
        assertEquals("", ContentKeyValueFactory.decodeAndDecompressContent(corruptGzip, 0));
        assertEquals("", ContentKeyValueFactory.decodeContent(corruptGzip, 0));
    }

    @Test
    public void rejectsNegativeLimits() {
        assertThrows(IllegalArgumentException.class, () -> ContentKeyValueFactory.decodeAndDecompressContent(new byte[0], -1));
        assertThrows(IllegalArgumentException.class, () -> ContentKeyValueFactory.decodeContent(new byte[0], -1));
    }

    @Test
    public void replacesMalformedUtf8LikeTheStringConstructor() throws IOException {
        byte[] content = {'a', (byte) 0xc3, 'b', (byte) 0xff, (byte) 0xe2};
        String expected = new String(content, StandardCharsets.UTF_8);
        assertEquals(expected, ContentKeyValueFactory.decodeContent(content, 100));
        assertEquals(expected.substring(0, 2), ContentKeyValueFactory.decodeContent(content, 2));
        assertEquals(expected.substring(0, 2), ContentKeyValueFactory.decodeAndDecompressContent(Base64.getEncoder().encode(gzip(content)), 2));
    }

    @Test
    public void stopsBeforeACorruptGzipTrailerBeyondThePrefix() throws IOException {
        byte[] content = new byte[64 * 1024];
        Arrays.fill(content, (byte) 'x');
        byte[] compressed = gzip(content);
        compressed[compressed.length - 8] ^= 1;
        assertEquals("x".repeat(1500), ContentKeyValueFactory.decodeAndDecompressContent(Base64.getEncoder().encode(compressed), 1500));
        assertEquals("x".repeat(1500), ContentKeyValueFactory.decodeAndDecompressContent(compressed, 1500));
    }

    @Test
    public void readsPrefixOfADocumentLargerThanTheTestHeap() throws IOException {
        // Generate 512 MiB of expanded content using an 8 KiB block. Neither fixture creation nor the bounded API needs a full document allocation.
        byte[] block = new byte[8192];
        Arrays.fill(block, (byte) 'x');
        ByteArrayOutputStream encoded = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(Base64.getEncoder().wrap(encoded))) {
            for (int i = 0; i < 65536; i++) {
                gzip.write(block);
            }
        }
        assertEquals("x".repeat(1500), ContentKeyValueFactory.decodeAndDecompressContent(encoded.toByteArray(), 1500));
    }

    private static byte[] gzip(byte[] content) throws IOException {
        ByteArrayOutputStream compressed = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(compressed)) {
            gzip.write(content);
        }
        return compressed.toByteArray();
    }
}
