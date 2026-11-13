package datawave.query.table.parser;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PushbackInputStream;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.zip.GZIPInputStream;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import datawave.marking.MarkingFunctions;
import datawave.query.Constants;
import datawave.query.table.parser.EventKeyValueFactory.EventKeyValue;

public class ContentKeyValueFactory {

    private static final Logger log = Logger.getLogger(ContentKeyValueFactory.class);

    public static ContentKeyValue parse(Key key, Value value, Authorizations auths, MarkingFunctions<?> markingFunctions) throws MarkingFunctions.Exception {

        if (null == key)
            throw new IllegalArgumentException("Cannot pass null key to ContentKeyValueFactory");
        if (null == value)
            throw new IllegalArgumentException("Cannot pass null value to ContentKeyValueFactory");

        ContentKeyValue c = new ContentKeyValue();

        c.setShardId(key.getRow().toString());

        String[] field = key.getColumnQualifier().toString().split(Constants.NULL_BYTE_STRING);
        if (field.length > 0)
            c.setDatatype(field[0]);
        if (field.length > 1)
            c.setUid(field[1]);
        if (field.length > 2)
            c.setViewName(field[2]);

        if (value.get().length > 0) {

            /*
             * We are storing 'documents' in this column gzip'd and base64 encoded. Base64.decode detects and handles compression.
             */
            byte[] contents = value.get();
            contents = decodeAndDecompressContent(contents);
            c.setContents(contents);
        }

        EventKeyValueFactory.parseColumnVisibility(c, key, auths, markingFunctions);

        return c;
    }

    /**
     * Fully decode and decompress content into a byte array. Callers that only need a string prefix should use {@link #decodeAndDecompressContent(byte[], int)}
     * to avoid allocating the entire expanded document.
     *
     * @param contents
     *            document-column value bytes
     * @return the full decoded content
     */
    public static byte[] decodeAndDecompressContent(byte[] contents) {
        try {
            contents = decompress(Base64.getMimeDecoder().decode(contents));
        } catch (IOException e) {
            log.error("Error decompressing Base64 encoded GZIPInputStream", e);
        } catch (Exception e) {
            // Thrown when data is not Base64 encoded. Try GZIP
            try {
                contents = decompress(contents);
            } catch (IOException ioe) {
                log.error("Error decompressing GZIPInputStream", e);
            }
        }
        return contents;
    }

    /**
     * Decode document-column content to a bounded UTF-8 string. Base64 decoding and GZIP decompression are streamed so that the entire expanded document is
     * never materialized. Raw GZIP and the legacy unencoded-content fallback are also supported.
     * <p>
     * Content bytes are decoded as UTF-8. The limit applies to the resulting Java string's length, which counts UTF-16 code units regardless of the input byte
     * encoding.
     * <p>
     * NOTE: Raw GZIP and Base64-decoded GZIP headers select streaming prefix decoding without validating the entire stored value. Base64 errors and GZIP
     * checksums beyond the consumed prefix are not necessarily detected. When decoded bytes have no GZIP header, the entire Base64 structure is checked before
     * choosing between Base64 decoding and the legacy raw-content fallback, without allocating decoded content. Malformed UTF-8 is replaced, as with the UTF-8
     * String constructor. Encountered GZIP errors are logged before falling back to the original bytes.
     *
     * @param contents
     *            document-column value bytes
     * @param maxChars
     *            maximum resulting {@link String#length()}; must be nonnegative
     * @return a prefix with length at most maxChars, without splitting a surrogate pair
     */
    public static String decodeAndDecompressContent(byte[] contents, int maxChars) {
        validateMaxChars(maxChars);
        if (maxChars == 0 || contents.length == 0) {
            return "";
        }

        if (!isCompressed(contents)) {
            try (PushbackInputStream decoded = new PushbackInputStream(Base64.getMimeDecoder().wrap(new ByteArrayInputStream(contents)), 2)) {
                if (isCompressed(decoded)) {
                    return readGzipContent(decoded, maxChars);
                }
                // Only ambiguous, non-GZIP content needs a full Base64 structure check.
                if (isValidMimeBase64(contents)) {
                    return readContent(decoded, maxChars);
                }
            } catch (IOException e) {
                // Preserve the legacy raw-content fallback. GZIP failures are logged by readGzipContent.
            }
        }

        try (InputStream raw = new ByteArrayInputStream(contents)) {
            return isCompressed(contents) ? readGzipContent(raw, maxChars) : readContent(raw, maxChars);
        } catch (IOException e) {
            return decodeContent(contents, maxChars);
        }
    }

    /**
     * Convert already decoded content to a bounded UTF-8 string. Use {@link #decodeAndDecompressContent(byte[], int)} directly for document-column values to
     * also bound Base64 decoding and GZIP decompression.
     * <p>
     * The input bytes are UTF-8; maxChars limits the resulting Java string's length.
     *
     * @param contents
     *            decoded UTF-8 bytes
     * @param maxChars
     *            maximum resulting {@link String#length()}; must be nonnegative
     * @return a prefix with length at most maxChars, without splitting a surrogate pair
     */
    public static String decodeContent(byte[] contents, int maxChars) {
        validateMaxChars(maxChars);
        if (maxChars == 0 || contents.length == 0) {
            return "";
        }
        try {
            return readContent(new ByteArrayInputStream(contents), maxChars);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Match the structural rules of Base64.getMimeDecoder().decode(byte[]) using constant memory. MIME ignores bytes outside the Base64 alphabet, except that
     * padding must follow a two- or three-symbol final group.
     */
    private static boolean isValidMimeBase64(byte[] contents) {
        int symbolsInQuartet = 0;
        boolean paddingStarted = false;
        for (int i = 0; i < contents.length; i++) {
            int value = contents[i] & 0xff;
            if (paddingStarted) {
                // The MIME array decoder ignores non-alphabet bytes after padding, including additional '=' characters.
                if (isBase64Alphabet(value)) {
                    return false;
                }
            } else if (value == '=') {
                if (symbolsInQuartet == 2) {
                    // A two-symbol final group requires two adjacent padding characters, even in MIME mode.
                    if (++i == contents.length || contents[i] != '=') {
                        return false;
                    }
                } else if (symbolsInQuartet != 3) {
                    return false;
                }
                paddingStarted = true;
            } else if (isBase64Alphabet(value)) {
                symbolsInQuartet = (symbolsInQuartet + 1) & 3;
            }
        }
        // Unpadded two- and three-symbol endings are legal; a single dangling symbol is not.
        return paddingStarted || symbolsInQuartet != 1;
    }

    private static boolean isBase64Alphabet(int value) {
        return (value >= 'A' && value <= 'Z') || (value >= 'a' && value <= 'z') || (value >= '0' && value <= '9') || value == '+' || value == '/';
    }

    private static void validateMaxChars(int maxChars) {
        if (maxChars < 0) {
            throw new IllegalArgumentException("maxChars must be nonnegative");
        }
    }

    private static String readContent(InputStream contents, int maxChars) throws IOException {
        try (Reader reader = new InputStreamReader(contents, StandardCharsets.UTF_8)) {
            char[] buffer = new char[Math.min(maxChars, 8192)];
            StringBuilder result = new StringBuilder(buffer.length);
            int count;
            while (result.length() < maxChars && (count = reader.read(buffer, 0, Math.min(buffer.length, maxChars - result.length()))) != -1) {
                result.append(buffer, 0, count);
            }
            // A supplementary character occupies two Java char values even when its input bytes are UTF-8. Omit it if only the first fits.
            if (result.length() > 0 && Character.isHighSurrogate(result.charAt(result.length() - 1))) {
                result.setLength(result.length() - 1);
            }
            return result.toString();
        }
    }

    private static String readGzipContent(InputStream contents, int maxChars) throws IOException {
        try (InputStream gzip = new GZIPInputStream(contents)) {
            return readContent(gzip, maxChars);
        } catch (IOException e) {
            log.error("Error reading GZIP document content prefix", e);
            throw e;
        }
    }

    private static boolean isCompressed(PushbackInputStream input) throws IOException {
        int first = input.read();
        int second = input.read();
        if (second != -1) {
            input.unread(second);
        }
        if (first != -1) {
            input.unread(first);
        }
        return first == (GZIPInputStream.GZIP_MAGIC & 0xff) && second == (GZIPInputStream.GZIP_MAGIC >> 8);
    }

    private static boolean isCompressed(byte[] compressed) {
        return compressed.length >= 2 && (compressed[0] == (byte) (GZIPInputStream.GZIP_MAGIC)) && (compressed[1] == (byte) (GZIPInputStream.GZIP_MAGIC >> 8));
    }

    private static byte[] decompress(byte[] compressed) throws IOException {
        byte[] decompressed = compressed;
        if (isCompressed(compressed)) {
            try (GZIPInputStream gzis = new GZIPInputStream(new ByteArrayInputStream(compressed))) {
                decompressed = IOUtils.toByteArray(gzis);
            }
        }
        return decompressed;
    }

    public static class ContentKeyValue extends EventKeyValue {

        protected String viewName = null;
        protected byte[] contents = null;

        public String getViewName() {
            return viewName;
        }

        public byte[] getContents() {
            return contents;
        }

        protected void setViewName(String viewName) {
            this.viewName = viewName;
        }

        protected void setContents(byte[] contents) {
            this.contents = contents;
        }
    }

}
