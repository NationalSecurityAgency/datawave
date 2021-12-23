package datawave.query.tables.serialization;

import com.google.gson.JsonObject;
import datawave.query.attributes.Document;
import datawave.query.function.deserializer.DocumentJsonDeserializer;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.dataImpl.thrift.TKey;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class RawJsonDocument implements SerializedDocumentIfc{

    private final String doc;
    private final TKey key ;
    private final long size;
    private final byte [] identifier;
    private DocumentJsonDeserializer deser = new DocumentJsonDeserializer();

    public RawJsonDocument(String doc, TKey key,byte [] identifier, long size){
        Objects.requireNonNull(doc);
        Objects.requireNonNull(key);
        this.doc=doc;
        this.key=key;
        this.size = size;
        this.identifier = identifier;
    }
    @Override
    public Key computeKey() {
        return new Key(key);
    }

    @Override
    public <T> T get(){
        return (T)(doc);

    }

    @Override
    public Document getAsDocument() {
        return deser.deserialize(new ByteArrayInputStream(doc.getBytes(StandardCharsets.UTF_8)));
    }

    @Override
    public byte[] getIdentifier() {
        return identifier;
    }

    @Override
    public int compareTo(SerializedDocumentIfc other) {
        if (other instanceof RawJsonDocument){
            return this.key.compareTo(((RawJsonDocument)other).key);
        }
        return -1;
    }

    @Override
    public long size() {
        return size;
    }

}
