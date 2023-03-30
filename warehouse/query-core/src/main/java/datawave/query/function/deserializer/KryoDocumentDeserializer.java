package datawave.query.function.deserializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import datawave.query.attributes.Document;
import datawave.query.function.util.KryoObjectPool;
import datawave.query.function.util.KryoReference;

import java.io.IOException;
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
    
    final static KryoObjectPool kryoPool = new KryoObjectPool();
    
    final transient Input input = new Input(4096);
    
    @Override
    public Document deserialize(InputStream data) {
        try (KryoReference ref = kryoPool.acquireObject()) {
            input.setInputStream(data);
            Kryo kryo = ref.getKryo();
            Document document = kryo.readObject(input, Document.class);
            
            if (null == document) {
                throw new RuntimeException("Deserialized null Document");
            }
            try {
                data.close();
            } catch (IOException e) {
                // no code
            }
            
            return document;
        }
    }
    
}
