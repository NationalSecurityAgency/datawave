package datawave.util.compression;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.compress.compressors.lzma.LZMACompressorInputStream;
import org.apache.commons.compress.compressors.lzma.LZMACompressorOutputStream;

/**
 * A {@link Codec} using LZMA compression, the algorithm used by 7-Zip (Apache Commons Compress, backed by XZ for Java).
 * <p>
 * Note: this produces a raw LZMA stream, not a .7z archive container.
 */
public final class SevenZIPCodec implements Codec {

    private static final SevenZIPCodec instance = new SevenZIPCodec();

    public static SevenZIPCodec instance() {
        return instance;
    }

    private SevenZIPCodec() {}

    @Override
    public String compress(String data, Charset charset) {
        final byte[] input = data.getBytes(charset);

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(input.length)) {
            try (LZMACompressorOutputStream out = new LZMACompressorOutputStream(baos)) {
                out.write(input);
            }
            return Base64.encodeBase64String(baos.toByteArray());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public String decompress(String data, Charset charset) {
        final byte[] compressed = Base64.decodeBase64(data);

        try (ByteArrayInputStream bais = new ByteArrayInputStream(compressed)) {
            try (LZMACompressorInputStream in = new LZMACompressorInputStream(bais)) {
                try (ByteArrayOutputStream baos = new ByteArrayOutputStream(compressed.length)) {
                    byte[] buf = new byte[4096];
                    int n;
                    while ((n = in.read(buf)) != -1) {
                        baos.write(buf, 0, n);
                    }
                    return baos.toString(charset);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public CodecAlgorithm getAlgorithm() {
        return CodecAlgorithm.SEVEN_ZIP;
    }
}
