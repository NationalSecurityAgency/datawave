package datawave.query.function.deserializer;

import java.io.InputStream;
import java.io.Serializable;

import datawave.query.attributes.Attribute;
import datawave.query.attributes.Document;
import datawave.query.function.KryoCVAwareSerializableSerializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import org.apache.log4j.Logger;

/**
 * Transform Kryo-serialized bytes back into a Document. Ordering of Attributes is <b>not</b> guaranteed across serialization.
 *
 * 
 *
 */
public class KryoDocumentDeserializer extends DocumentDeserializer implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger log = Logger.getLogger(KryoDocumentDeserializer.class);
    final transient Kryo kryo = new Kryo();
    
    public KryoDocumentDeserializer() {
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
        log.warn("deserialized to "+document.toString());
        return document;
    }
    
}
