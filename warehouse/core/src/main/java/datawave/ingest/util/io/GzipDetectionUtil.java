package datawave.ingest.util.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.util.zip.GZIPInputStream;

/**
 * Determine if an input stream is gzip compressed or not.
 */
public class GzipDetectionUtil {

    /**
     * Determines whether or not the given File is GZIP compressed.
     *
     * @param file
     *            the file to check for GZIP Compression.
     * @return whether or not the given File is GZIP compressed.
     * @throws IOException
     */
    public static boolean isCompressed(final File file) throws IOException {
        final FileInputStream fis = new FileInputStream(file);
        boolean compressed = false;

        try {
            final byte[] header = new byte[2];

            if (fis.read(header) == 2) {
                final int magic = ((int) header[0] & 0xff | ((header[1] << 8) & 0xff00));
                compressed = (magic == GZIPInputStream.GZIP_MAGIC);
            }
        } finally {
            fis.close();
        }

        return compressed;
    }

    /**
     * Given an input stream, test it to see if it's GZIP compressed, if so return a GZIPInputStream, otherwise return it as normal.
     *
     * @param inputStream
     *            the {@link InputStream} to check.
     * @return an {@link InputStream} to read from.
     */
    public static InputStream decompressTream(InputStream inputStream) throws IOException {
        PushbackInputStream pushBack = new PushbackInputStream(inputStream, 2); // need to check first two bytes
        byte[] header = new byte[2];
        pushBack.read(header);
        pushBack.unread(header); // put it back on the stream
        int magic = ((int) header[0] & 0xff | ((header[1] << 8) & 0xff00)); // some magic
        if (GZIPInputStream.GZIP_MAGIC == magic) {
            return new GZIPInputStream(pushBack);
        } else {
            return pushBack;
        }
    }
}
