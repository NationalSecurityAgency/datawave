package datawave.query.function.serializer;

import java.io.ByteArrayOutputStream;

import datawave.query.attributes.Attribute;
import datawave.query.attributes.Document;
import datawave.query.function.KryoCVAwareSerializableSerializer;

import org.apache.log4j.Logger;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;

/**
 * Transform the Document into a Kryo-serialized version. Ordering of Attributes is <b>not</b> guaranteed across serialization.
 *
 *
 *
 */
public class KryoDocumentSerializer extends DocumentSerializer {
    private static final Logger log = Logger.getLogger(KryoDocumentSerializer.class);
    final Kryo kryo = new Kryo();
    final ByteArrayOutputStream baos = new ByteArrayOutputStream(4096);

    public KryoDocumentSerializer() {
        this(false, false);
    }

    public KryoDocumentSerializer(boolean reducedResponse) {
        this(reducedResponse, false);
    }

    public KryoDocumentSerializer(boolean reducedResponse, boolean compress) {
        super(reducedResponse, compress);
        kryo.addDefaultSerializer(Attribute.class, new KryoCVAwareSerializableSerializer(reducedResponse));
    }

    @Override
    public byte[] serialize(Document doc) {
        baos.reset();

        Output output = new Output(baos);

        kryo.writeObject(output, doc);

        output.close();

        return baos.toByteArray();
    }

}
