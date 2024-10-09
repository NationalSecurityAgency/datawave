package datawave.query.util.sortedmap.rfile;

import java.io.ByteArrayInputStream;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.collections.keyvalue.UnmodifiableMapEntry;

import datawave.query.attributes.Attribute;
import datawave.query.attributes.Document;
import datawave.query.function.deserializer.DocumentDeserializer;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.function.serializer.DocumentSerializer;
import datawave.query.function.serializer.KryoDocumentSerializer;

public class KeyValueByteDocumentTransforms {

    public static byte[] keyToByte(Key key) {
        if (key == null) {
            return null;
        }
        return key.getRow().getBytes();
    }

    public static Key byteToKey(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        return new Key(bytes);
    }

    public static Value documentToValue(Document doc) {
        if (doc == null) {
            return null;
        }
        DocumentSerializer serializer = new KryoDocumentSerializer(false, true);
        byte[] document = serializer.serialize(doc);
        return new Value(document);
    }

    public static Document valueToDocument(Value value) {
        if (value == null) {
            return null;
        }
        DocumentDeserializer deserializer = new KryoDocumentDeserializer();
        Document document = deserializer.deserialize(new ByteArrayInputStream(value.get()));
        return document;
    }

    public static Map.Entry<byte[],Document> keyValueToByteDocument(Map.Entry<Key,Value> keyValue) {
        if (keyValue == null) {
            return null;
        }
        return new UnmodifiableMapEntry(keyToByte(keyValue.getKey()), valueToDocument(keyValue.getValue()));
    }

    public static Map.Entry<Key,Value> byteDocumentToKeyValue(Map.Entry<byte[],Document> byteKey) {
        if (byteKey == null) {
            return null;
        }
        return new UnmodifiableMapEntry(byteToKey(byteKey.getKey()), documentToValue(byteKey.getValue()));
    }
}
