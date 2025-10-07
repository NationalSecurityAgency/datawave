package datawave.query.function.serializer;

import java.io.ByteArrayOutputStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;

import datawave.query.attributes.Document;

/**
 * Transform the Document into a Kryo-serialized version. Ordering of Attributes is <b>not</b> guaranteed across serialization.
 */
public class KryoDocumentSerializer extends DocumentSerializer {

    private final Kryo kryo = new Kryo();
    private final ByteArrayOutputStream baos = new ByteArrayOutputStream(4096);

    public KryoDocumentSerializer() {
        this(false, false);
    }

    public KryoDocumentSerializer(boolean reducedResponse) {
        this(reducedResponse, false);
    }

    public KryoDocumentSerializer(boolean reducedResponse, boolean compress) {
        super(reducedResponse, compress);
    }

    @Override
    public byte[] serialize(Document doc) {
        baos.reset();
        try (var output = new Output(baos)) {
            doc.write(kryo, output);
        }
        return baos.toByteArray();
    }

}
