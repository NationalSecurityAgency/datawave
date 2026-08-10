package datawave.query.function.deserializer;

import java.io.InputStream;
import java.io.Serializable;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;

import datawave.query.attributes.Document;

/**
 * Transform Kryo-serialized bytes back into a Document. Ordering of Attributes is <b>not</b> guaranteed across serialization.
 */
public class KryoDocumentDeserializer extends DocumentDeserializer implements Serializable {
    private static final long serialVersionUID = -657326925013465794L;

    final transient Kryo kryo = new Kryo();

    public KryoDocumentDeserializer() {
        // empty constructor
    }

    @Override
    public Document deserialize(InputStream data) {
        Document document;
        try (var input = new Input(data)) {
            document = new Document();
            document.read(kryo, input);
        }
        return document;
    }

}
