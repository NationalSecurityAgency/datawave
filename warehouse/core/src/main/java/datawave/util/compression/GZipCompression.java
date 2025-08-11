package datawave.util.compression;

import org.apache.commons.codec.binary.Base64;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class GZipCompression extends CompressionMethod{

    @Override
    public String compress(final String data, final Charset charset) throws IOException {
        final byte[] input = data.getBytes(charset);

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             GZIPOutputStream gzip = new GZIPOutputStream(baos)) {

            gzip.write(input);
            // closing gzip finishes the stream and flushes to baos
            gzip.close();

            // todo: add codec classes if modularity is needed
            return Base64.encodeBase64String(baos.toByteArray());
        }
    }

    @Override
    public String decompress(final String base64, final Charset charset) throws IOException {

        // always assumes the codec is base64
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


