package datawave.util.compression;

import org.apache.commons.codec.binary.Base64;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * A concrete implementation of {@link CompressionMethod} that uses GZIP for compression
 * and Base64 for safe string encoding/decoding.
 *
 * <p>Process overview:</p>
 * <ul>
 *     <li><b>Compression</b>: String → Bytes (charset) → GZIP → Base64 String</li>
 *     <li><b>Decompression</b>: Base64 String → Bytes → GZIP → String (charset)</li>
 * </ul>
 *
 * <p>Base64 ensures that the compressed binary data can be represented as plain text.</p>
 */
public class GZipCompression extends CompressionMethod {

    /**
     * Compresses a string into a Base64-encoded GZIP stream.
     *
     * @param data    The input string to be compressed.
     * @param charset The charset for encoding the string to bytes.
     * @return A Base64 string containing the GZIP-compressed data.
     * @throws IOException If compression or encoding fails.
     */
    @Override
    public String compress(final String data, final Charset charset) throws IOException {
        final byte[] input = data.getBytes(charset);

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             GZIPOutputStream gzip = new GZIPOutputStream(baos)) {

            gzip.write(input);
            gzip.close(); // must close to flush all compressed data into baos

            return Base64.encodeBase64String(baos.toByteArray());
        }
    }

    /**
     * Decompresses a Base64-encoded GZIP string back into its original form.
     *
     * @param base64  The Base64-encoded string containing GZIP-compressed data.
     * @param charset The charset used to convert decompressed bytes into a string.
     * @return The original, decompressed string.
     * @throws IOException If decoding or decompression fails.
     */
    @Override
    public String decompress(final String base64, final Charset charset) throws IOException {
        final byte[] compressed = Base64.decodeBase64(base64);

        try (ByteArrayInputStream bais = new ByteArrayInputStream(compressed);
             GZIPInputStream gzip = new GZIPInputStream(bais);
             ByteArrayOutputStream baos = new ByteArrayOutputStream()) {

            byte[] buf = new byte[4096];
            int n;
            while ((n = gzip.read(buf)) != -1) {
                baos.write(buf, 0, n);
            }

            return baos.toString(charset);
        }
    }
}
