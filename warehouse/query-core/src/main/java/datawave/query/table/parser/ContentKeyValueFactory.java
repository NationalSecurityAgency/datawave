package datawave.query.table.parser;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

import datawave.marking.MarkingFunctions;
import datawave.query.Constants;
import datawave.query.table.parser.EventKeyValueFactory.EventKeyValue;
import datawave.util.StringUtils;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;
import org.infinispan.commons.util.Base64;

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
            try {
                c.setContents(Base64.decode(new String(value.get())));
            } catch (IllegalStateException e) {
                // Thrown when data is not Base64 encoded. Try GZIP
                ByteArrayInputStream bais = new ByteArrayInputStream(value.get());
                GZIPInputStream gzip = null;
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                try {
                    gzip = new GZIPInputStream(bais);
                    byte[] buf = new byte[4096];
                    int length = 0;
                    while ((length = gzip.read(buf)) >= 0) {
                        baos.write(buf, 0, length);
                    }
                    c.setContents(baos.toByteArray());
                } catch (IOException ioe) {
                    // Not GZIP, now what?
                    c.setContents(value.get());
                } finally {
                    if (null != gzip) {
                        try {
                            gzip.close();
                        } catch (IOException e1) {
                            log.error("Error closing GZIPInputStream", e1);
                        }
                    }
                }
            }
        }

        EventKeyValueFactory.parseColumnVisibility(c, key, auths, markingFunctions);

        return c;
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
