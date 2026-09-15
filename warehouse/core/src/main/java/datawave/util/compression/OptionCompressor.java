package datawave.util.compression;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.compress.compressors.lzma.LZMACompressorInputStream;
import org.apache.commons.compress.compressors.lzma.LZMACompressorOutputStream;

//     Bzip2, Gz, Lz4, Lzo, NoCompression, Snappy, ZStandard

/**
 * Provides a base contract for compression and decompression methods.
 *
 * <p>
 * General process:
 * </p>
 * <ol>
 * <li><b>Compression</b>: Original Data → Compress with Algorithm (selected via {@link CompressionMethod}) → Encode in Base64 (except {@code NONE}) → Return as
 * String</li>
 * <li><b>Decompression</b>: Input String (Base64 if compressed; plain text if {@code NONE}) → Decode Base64 (if applicable) → Decompress with Algorithm →
 * Return Original Data</li>
 * </ol>
 *
 * <p>
 * Base64 encoding ensures that compressed data can be safely stored or transferred as text. For {@link CompressionMethod#NONE} the data is returned as-is and
 * is not Base64-encoded.
 * </p>
 */
public class OptionCompressor {

    public enum CompressionMethod {
        NONE, GZIP, BZIP2, SEVEN_ZIP
    }

    /**
     * Compresses the given string using the specified {@link CompressionMethod}.
     *
     * <p>
     * Order of operations (by method):
     * </p>
     * <ul>
     * <li>{@code NONE}: Return {@code data} unchanged; no Base64 applied.</li>
     * <li>{@code GZIP}: Convert {@code data} to bytes with {@code charset} → GZIP-compress → Base64-encode → return String.</li>
     * <li>{@code BZIP2}: Convert {@code data} to bytes with {@code charset} → BZIP2-compress → Base64-encode → return String.</li>
     * <li>{@code SEVEN_ZIP}: Convert {@code data} to bytes with {@code charset} → LZMA-compress (7z algorithm stream) → Base64-encode → return String.</li>
     * </ul>
     */
    public String compress(final String data, final CompressionMethod method, final Charset charset) throws IOException {
        switch (method) {
            case NONE:
                return data;
            case GZIP:
                return compressGZIP(data, charset);
            case BZIP2:
                return compressBZIP2(data, charset);
            case SEVEN_ZIP:
                return compress7ZIP(data, charset);
            default:
                throw new IllegalArgumentException("unrecognized compression option: " + method);
        }
    }

    /**
     * GZIP implementation reference: 1) String → bytes via charset 2) Write to GZIPOutputStream (must close to finalize) 3) Base64-encode compressed bytes
     */
    private String compressGZIP(final String data, final Charset charset) {
        final byte[] input = data.getBytes(charset);

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(input.length)) {
            try (GZIPOutputStream gzip = new GZIPOutputStream(baos)) {
                gzip.write(input);
            }
            return Base64.encodeBase64String(baos.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * BZIP2 implementation (mirrors GZIP flow): 1) String → bytes via charset 2) Write to BZip2CompressorOutputStream (close to finalize) 3) Base64-encode
     * compressed bytes
     */
    private String compressBZIP2(final String data, final Charset charset) {
        final byte[] input = data.getBytes(charset);

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(input.length)) {
            try (BZip2CompressorOutputStream bzos = new BZip2CompressorOutputStream(baos)) {
                bzos.write(input);
            }
            return Base64.encodeBase64String(baos.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * "7-Zip" via LZMA stream implementation: 1) String → bytes via charset 2) Write to LZMACompressorOutputStream (close to finalize) 3) Base64-encode
     * compressed bytes
     *
     * Note: This uses the LZMA compressor stream from Apache Commons Compress, which is the algorithm used by 7-Zip; it is not a .7z archive container.
     */
    private String compress7ZIP(final String data, final Charset charset) {
        final byte[] input = data.getBytes(charset);

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(input.length)) {
            try (LZMACompressorOutputStream lzma = new LZMACompressorOutputStream(baos)) {
                lzma.write(input);
            }
            return Base64.encodeBase64String(baos.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * GZIP implementation reference for decompression: 1) Base64-decode to compressed bytes 2) Read via GZIPInputStream into buffer 3) Collect into
     * ByteArrayOutputStream → String via charset
     */
    private String decompressGZIP(final String dataBase64, final Charset charset) {
        final byte[] compressed = Base64.decodeBase64(dataBase64);

        try (ByteArrayInputStream bais = new ByteArrayInputStream(compressed)) {
            try (GZIPInputStream gzip = new GZIPInputStream(bais)) {
                try (ByteArrayOutputStream baos = new ByteArrayOutputStream(compressed.length)) {
                    byte[] buf = new byte[4096];
                    int n;
                    while ((n = gzip.read(buf)) != -1) {
                        baos.write(buf, 0, n);
                    }
                    // NOTE: Keeping the same pattern as provided code.
                    return baos.toString(charset);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * BZIP2 decompression (mirrors GZIP flow): 1) Base64-decode to compressed bytes 2) Read via BZip2CompressorInputStream into buffer 3) Collect into
     * ByteArrayOutputStream → String via charset
     */
    private String decompressBZIP2(final String dataBase64, final Charset charset) {
        final byte[] compressed = Base64.decodeBase64(dataBase64);

        try (ByteArrayInputStream bais = new ByteArrayInputStream(compressed)) {
            try (BZip2CompressorInputStream bzis = new BZip2CompressorInputStream(bais)) {
                try (ByteArrayOutputStream baos = new ByteArrayOutputStream(compressed.length)) {
                    byte[] buf = new byte[4096];
                    int n;
                    while ((n = bzis.read(buf)) != -1) {
                        baos.write(buf, 0, n);
                    }
                    return baos.toString(charset);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * "7-Zip" via LZMA stream decompression (mirrors GZIP/BZIP2 flow): 1) Base64-decode to compressed bytes 2) Read via LZMACompressorInputStream into buffer
     * 3) Collect into ByteArrayOutputStream → String via charset
     *
     * Note: This expects LZMA stream data (not a .7z archive container).
     */
    private String decompress7ZIP(final String dataBase64, final Charset charset) {
        final byte[] compressed = Base64.decodeBase64(dataBase64);

        try (ByteArrayInputStream bais = new ByteArrayInputStream(compressed)) {
            try (LZMACompressorInputStream lzma = new LZMACompressorInputStream(bais)) {
                try (ByteArrayOutputStream baos = new ByteArrayOutputStream(compressed.length)) {
                    byte[] buf = new byte[4096];
                    int n;
                    while ((n = lzma.read(buf)) != -1) {
                        baos.write(buf, 0, n);
                    }
                    return baos.toString(charset);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Decompresses a string using the specified {@link CompressionMethod}.
     *
     * <p>
     * Order of operations (by method):
     * </p>
     * <ul>
     * <li>{@code NONE}: Return input unchanged (input is expected to be the original plain text, not Base64).</li>
     * <li>{@code GZIP}: Base64-decode → GZIP-decompress → decode to String using {@code charset}.</li>
     * <li>{@code BZIP2}: Base64-decode → BZIP2-decompress → decode to String using {@code charset}.</li>
     * <li>{@code SEVEN_ZIP}: Base64-decode → LZMA-decompress → decode to String using {@code charset}.</li>
     * </ul>
     *
     * <p>
     * This is the reverse of {@link #compress(String, CompressionMethod, Charset)}. For all non-{@code NONE} methods, the input should be the Base64 text
     * returned by the corresponding {@code compress(...)} call.
     * </p>
     */
    public String decompress(final String input, final CompressionMethod method, final Charset charset) throws IOException {
        switch (method) {
            case NONE:
                return input;
            case GZIP:
                return decompressGZIP(input, charset);
            case BZIP2:
                return decompressBZIP2(input, charset);
            case SEVEN_ZIP:
                return decompress7ZIP(input, charset);
            default:
                throw new IllegalArgumentException("unrecognized decompression option: " + method);
        }
    }
}
