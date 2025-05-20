package datawave.query.function.serializer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import datawave.query.attributes.Document;

/**
 * Convert a Document to a Value through Writable#write(). Ordering of Attributes is <b>not</b> guaranteed across serialization.
 *
 *
 *
 */
public class WritableDocumentSerializer extends DocumentSerializer {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream(4096);

    public WritableDocumentSerializer(boolean reducedResponse) {
        super(reducedResponse, false);
    }

    @Override
    public byte[] serialize(Document doc) {
        baos.reset();

        DataOutputStream dos = new DataOutputStream(baos);

        try {
            doc.write(dos);
        } catch (IOException e) {
            throw new RuntimeException("Could not convert Document through write().", e);
        }

        return baos.toByteArray();
    }

}
