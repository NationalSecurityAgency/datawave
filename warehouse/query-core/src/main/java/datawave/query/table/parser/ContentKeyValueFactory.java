package datawave.query.table.parser;

import java.io.ByteArrayInputStream;
import java.io.IOException;
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
import datawave.util.StringUtils;

public class ContentKeyValueFactory {

    private static final Logger log = Logger.getLogger(ContentKeyValueFactory.class);

    public static ContentKeyValue parse(Key key, Value value, Authorizations auths, MarkingFunctions markingFunctions) throws MarkingFunctions.Exception {

        if (null == key)
            throw new IllegalArgumentException("Cannot pass null key to ContentKeyValueFactory");
        if (null == value)
            throw new IllegalArgumentException("Cannot pass null value to ContentKeyValueFactory");

        ContentKeyValue c = new ContentKeyValue();

        c.setShardId(key.getRow().toString());

        String[] field = StringUtils.split(key.getColumnQualifier().toString(), Constants.NULL_BYTE_STRING);
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

    private static boolean isCompressed(byte[] compressed) {
        return (compressed[0] == (byte) (GZIPInputStream.GZIP_MAGIC)) && (compressed[1] == (byte) (GZIPInputStream.GZIP_MAGIC >> 8));
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
