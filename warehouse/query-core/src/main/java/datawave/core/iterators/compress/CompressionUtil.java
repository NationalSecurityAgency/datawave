package datawave.core.iterators.compress;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.compress.compressors.zstandard.ZstdCompressorInputStream;
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorOutputStream;

/**
 * A utility class for compressing and decompressing byte arrays.
 * <p>
 * Supports GZIP and ZSTD algorithms.
 */
@ThreadSafe
public class CompressionUtil {

    private CompressionUtil() {
        // enforce static access
    }

    /**
     * Compress data using the GZIP algorithm
     *
     * @param data
     *            the data
     * @return compressed data
     */
    public static byte[] compressGZIP(byte[] data) {
        try (var baos = new ByteArrayOutputStream()) {
            try (var gzip = new GZIPOutputStream(baos)) {
                gzip.write(data);
                gzip.flush();
            }
            baos.flush();
            baos.close();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to compress data", e);
        }
    }

    /**
     * Decompress data using the GZIP algorithm
     *
     * @param data
     *            the data
     * @return the uncompressed data
     */
    public static byte[] decompressGZIP(byte[] data) {
        try (var bais = new ByteArrayInputStream(data)) {
            try (var gzip = new GZIPInputStream(bais)) {
                return gzip.readAllBytes();
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to decompress data", e);
        }
    }

    /**
     * Compress the data using the ZSTD algorithm
     *
     * @param data
     *            the data
     * @return the compressed data
     */
    public static byte[] compressZSTD(byte[] data) {
        try (var baos = new ByteArrayOutputStream()) {
            try (var zos = new ZstdCompressorOutputStream(baos)) {
                zos.write(data);
            }
            baos.flush();
            baos.close();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Decompress the data using the ZSTD algorithm
     *
     * @param compressedData
     *            the data
     * @return the compressed data
     */
    public static byte[] decompressZSTD(byte[] compressedData) {
        try (var bais = new ByteArrayInputStream(compressedData)) {
            try (var zis = new ZstdCompressorInputStream(bais)) {
                return zis.readAllBytes();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
