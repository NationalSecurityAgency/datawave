package datawave.query.function.deserializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Document;
import datawave.query.function.KryoCVAwareSerializableSerializer;

import java.io.InputStream;
import java.io.Serializable;

/**
 * Transform Kryo-serialized bytes back into a Document. Ordering of Attributes is <b>not</b> guaranteed across serialization.
 *
 * 
 *
 */
public class KryoDocumentDeserializer extends DocumentDeserializer implements Serializable {
    private static final long serialVersionUID = 1L;
    
    final transient Kryo kryo = new Kryo();
    
    public KryoDocumentDeserializer() {
        kryo.register(Document.class);
        kryo.addDefaultSerializer(Attribute.class, new KryoCVAwareSerializableSerializer(true));
    }
    
    @Override
    public Document deserialize(InputStream data) {
        Input input = new Input(data);
        Document document = kryo.readObject(input, Document.class);
        
        if (null == document) {
            throw new RuntimeException("Deserialized null Document");
        }
        
        input.close();
        
        return document;
    }
    
}
