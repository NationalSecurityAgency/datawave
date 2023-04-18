package datawave.query.table.parser;

import datawave.marking.MarkingFunctions;
import datawave.query.Constants;
import datawave.query.table.parser.EventKeyValueFactory.EventKeyValue;
import datawave.util.StringUtils;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.zip.GZIPInputStream;

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
            
            try {
                c.setContents(decode(new String(value.get())));
            } catch (Exception e) {
                log.error("Base64/GZip decode failed!", e);
                // Thrown when data is not Base64 encoded. Try GZIP
                if (isGzip(value.get())) {
                    try {
                        c.setContents(gunzip(value.get()));
                    } catch (IOException ioe) {
                        log.error("GZip decode failed!", ioe);
                        // Not GZIP, now what?
                        c.setContents(value.get());
                    }
                } else {
                    log.warn("Failed all attempts to decode value. Setting contents ");
                    c.setContents(value.get());
                }
            }
        }
        
        EventKeyValueFactory.parseColumnVisibility(c, key, auths, markingFunctions);
        
        return c;
    }
    
    private static boolean isGzip(byte[] bytes) {
        if (bytes == null || bytes.length < 4) {
            return false;
        }
        int head = ((int) bytes[0] & 0xff) | ((bytes[1] << 8) & 0xff00);
        return java.util.zip.GZIPInputStream.GZIP_MAGIC == head;
    }
    
    private static byte[] gunzip(byte[] gzip) throws IOException {
        byte[] buffer = new byte[4096];
        ByteArrayInputStream bais = new ByteArrayInputStream(gzip);
        try (GZIPInputStream gzis = new GZIPInputStream(bais); ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            int length;
            while ((length = gzis.read(buffer)) >= 0) {
                baos.write(buffer, 0, length);
            }
            return baos.toByteArray();
        }
    }
    
    /**
     * Decodes the given 'document', which should be gzip'd and base64 encoded
     */
    private static byte[] decode(String doc) throws Exception {
        byte[] bytes = Base64.getMimeDecoder().decode(doc);
        if (isGzip(bytes)) {
            bytes = gunzip(bytes);
        }
        return bytes;
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
