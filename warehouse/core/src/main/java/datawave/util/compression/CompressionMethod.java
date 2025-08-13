package datawave.util.compression;

import org.apache.commons.codec.binary.Base64;

import java.io.IOException;
import java.nio.charset.Charset;

/**
 * Provides a base contract for compression and decompression methods.
 *
 * <p>General process:</p>
 * <ol>
 *     <li><b>Compression</b>: Original Data → Compress with Algorithm (e.g., GZIP) → Encode in Base64 → Return as String</li>
 *     <li><b>Decompression</b>: Base64 String → Decode from Base64 → Decompress with Algorithm (e.g., GZIP) → Return Original Data</li>
 * </ol>
 *
 * <p>Base64 encoding ensures that compressed data can be safely stored or transferred as text,
 * regardless of binary content.</p>
 */
public abstract class CompressionMethod {

    /**
     * Compresses the given string into a Base64-encoded representation.
     *
     * <p>Order of operations:</p>
     * <ol>
     *     <li>Take the original data string.</li>
     *     <li>Compress the data using the specified compression algorithm (e.g., GZIP).</li>
     *     <li>Encode the compressed byte array into Base64.</li>
     *     <li>Return the Base64 string, encoded using the specified character set.</li>
     * </ol>
     *
     * @param data    The uncompressed string to be processed.
     * @param charset The charset used for converting between strings and byte arrays.
     * @return A Base64-encoded string containing the compressed data.
     * @throws IOException If compression or encoding fails.
     */
    public abstract String compress(final String data, final Charset charset) throws IOException;

    /**
     * Decompresses a Base64-encoded, compressed string back to its original form.
     *
     * <p>This is the reverse of {@link #compress(String, Charset)}:</p>
     * <ol>
     *     <li>Take the Base64-encoded string.</li>
     *     <li>Decode the Base64 string into the original compressed byte array.</li>
     *     <li>Decompress the byte array using the appropriate algorithm (e.g., GZIP).</li>
     *     <li>Convert the decompressed byte array back into a string using the specified charset.</li>
     * </ol>
     *
     * @param base64  A Base64-encoded string containing compressed data.
     * @param charset The charset used for decoding the final string.
     * @return The original uncompressed string.
     * @throws IOException If decoding or decompression fails.
     */
    public abstract String decompress(final String base64, final Charset charset) throws IOException;
}
