package nsa.datawave.query.rewrite.function.deserializer;

import java.io.InputStream;
import java.util.Map.Entry;

import nsa.datawave.query.rewrite.DocumentSerialization;
import nsa.datawave.query.rewrite.attributes.Document;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import com.google.common.base.Function;
import com.google.common.collect.Maps;

public abstract class DocumentDeserializer implements Function<Entry<Key,Value>,Entry<Key,Document>> {
    
    @Override
    public Entry<Key,Document> apply(Entry<Key,Value> from) {
        InputStream is = DocumentSerialization.consumeHeader(from.getValue().get());
        
        Document document = deserialize(is);
        
        return Maps.immutableEntry(from.getKey(), document);
    }
    
    public abstract Document deserialize(InputStream data);
    
}
