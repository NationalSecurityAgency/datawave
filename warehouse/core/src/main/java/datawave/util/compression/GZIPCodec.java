package datawave.util.compression;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.codec.binary.Base64;

/**
 * A {@link Codec} using GZIP compression ({@link java.util.zip.GZIPOutputStream}).
 */
public final class GZIPCodec implements Codec {

    private static final GZIPCodec instance = new GZIPCodec();

    public static GZIPCodec instance() {
        return instance;
    }

    private GZIPCodec() {}

    @Override
    public String compress(String data, Charset charset) {
        final byte[] input = data.getBytes(charset);

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(input.length)) {
            try (GZIPOutputStream out = new GZIPOutputStream(baos)) {
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
            try (GZIPInputStream in = new GZIPInputStream(bais)) {
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
        return CodecAlgorithm.GZIP;
    }
}
