package datawave.util.compression;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;

/**
 * A {@link Codec} using BZIP2 compression (Apache Commons Compress).
 */
public final class BZIP2Codec implements Codec {

    private static final BZIP2Codec instance = new BZIP2Codec();

    public static BZIP2Codec instance() {
        return instance;
    }

    private BZIP2Codec() {}

    @Override
    public String compress(String data, Charset charset) {
        final byte[] input = data.getBytes(charset);

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(input.length)) {
            try (BZip2CompressorOutputStream out = new BZip2CompressorOutputStream(baos)) {
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
            try (BZip2CompressorInputStream in = new BZip2CompressorInputStream(bais)) {
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
        return CodecAlgorithm.BZIP2;
    }
}
