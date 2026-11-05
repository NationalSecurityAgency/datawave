package datawave.util.compression;

import java.nio.charset.Charset;

/**
 * A compression codec that converts a string to compressed, Base64-encoded text and back.
 * <p>
 * Compression: {@code data} → bytes via {@code charset} → compress → Base64-encode.<br>
 * Decompression: Base64-decode → decompress → string via {@code charset}.
 * <p>
 * Base64 encoding ensures that the compressed data can be safely stored or transferred as text, e.g. as an iterator option. Implementations are stateless
 * singletons; see {@link CodecAlgorithm} for the available codecs and {@link CodecUtils} for static convenience methods.
 */
public interface Codec {

    /**
     * Compresses the given string.
     *
     * @param data
     *            the string to compress
     * @param charset
     *            the charset used to convert {@code data} to bytes
     * @return the Base64 encoding of the compressed bytes
     * @throws java.io.UncheckedIOException
     *             if compression fails
     */
    String compress(String data, Charset charset);

    /**
     * Decompresses a string previously produced by {@link #compress(String, Charset)} with the same codec.
     *
     * @param data
     *            the Base64 encoding of the compressed bytes
     * @param charset
     *            the charset used to convert the decompressed bytes back to a string
     * @return the original string
     * @throws java.io.UncheckedIOException
     *             if the data is not valid compressed data for this codec
     */
    String decompress(String data, Charset charset);

    /**
     * @return the {@link CodecAlgorithm} this codec implements
     */
    CodecAlgorithm getAlgorithm();
}
