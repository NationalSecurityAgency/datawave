package datawave.query.tables.serialization;

import com.google.gson.JsonObject;
import datawave.query.attributes.Document;
import datawave.query.function.deserializer.DocumentJsonDeserializer;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.dataImpl.thrift.TKey;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

public class JsonDocument implements SerializedDocumentIfc{

    private final JsonObject doc;
    private final TKey key ;
    private final long size;
    private DocumentJsonDeserializer deser = new DocumentJsonDeserializer();

    public JsonDocument(JsonObject doc, TKey key, long size){
        this.doc=doc;
        this.key=key;
        this.size = size;
    }
    @Override
    public Key computeKey() {
        return new Key(key);
    }

    @Override
    public <T> T getAs(Class<?> as){
        if (as == Document.class){
        return (T)deser.deserialize(new ByteArrayInputStream(doc.toString().getBytes(StandardCharsets.UTF_8)));
        }

        return (T)(doc);

    }

    @Override
    public int compareTo(SerializedDocumentIfc other) {
        if (other instanceof JsonDocument){
            return this.key.compareTo(((JsonDocument)other).key);
        }
        return -1;
    }

    @Override
    public long size() {
        return size;
    }

}
