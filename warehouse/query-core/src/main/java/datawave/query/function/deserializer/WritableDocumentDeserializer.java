package datawave.query.function.deserializer;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;

import datawave.query.attributes.Document;

/**
 * Convert a Value to a Document through Writable#readFields(). Ordering of Attributes is <b>not</b> guaranteed across serialization.
 *
 *
 *
 */
public class WritableDocumentDeserializer extends DocumentDeserializer implements Serializable {

    private static final long serialVersionUID = 1L;

    @Override
    public Document deserialize(InputStream data) {
        DataInputStream dis = new DataInputStream(data);
        Document d = new Document();

        try {
            d.readFields(dis);
        } catch (IOException e) {
            throw new RuntimeException("Could not convert Document through write().", e);
        }

        return d;
    }

}
