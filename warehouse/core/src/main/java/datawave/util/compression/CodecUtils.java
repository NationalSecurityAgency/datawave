package datawave.util.compression;

import java.nio.charset.Charset;

/**
 * Static convenience methods for compressing and decompressing strings with a given {@link CodecAlgorithm}.
 */
public final class CodecUtils {

    /**
     * Compresses {@code data} with the given algorithm. See {@link Codec#compress(String, Charset)}.
     */
    public static String compress(String data, Charset charset, CodecAlgorithm algorithm) {
        return algorithm.getCodec().compress(data, charset);
    }

    /**
     * Decompresses {@code data} with the given algorithm. See {@link Codec#decompress(String, Charset)}.
     */
    public static String decompress(String data, Charset charset, CodecAlgorithm algorithm) {
        return algorithm.getCodec().decompress(data, charset);
    }

    private CodecUtils() {
        throw new UnsupportedOperationException();
    }
}
