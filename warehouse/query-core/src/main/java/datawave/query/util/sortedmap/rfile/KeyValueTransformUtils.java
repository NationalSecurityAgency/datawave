package datawave.query.util.sortedmap.rfile;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.ValueFormatException;
import org.apache.commons.collections.keyvalue.UnmodifiableMapEntry;
import org.apache.hadoop.io.WritableUtils;

import datawave.query.attributes.Document;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.function.serializer.KryoDocumentSerializer;

public class KeyValueTransformUtils {

    private static final KryoDocumentSerializer serializer = new KryoDocumentSerializer(false, true);
    private static final KryoDocumentDeserializer deserializer = new KryoDocumentDeserializer();

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
        byte[] document;
        synchronized (serializer) {
            document = serializer.serialize(doc);
        }
        return new Value(document);
    }

    public static Document valueToDocument(Value value) {
        if (value == null) {
            return null;
        }
        synchronized (deserializer) {
            return deserializer.deserialize(new ByteArrayInputStream(value.get()));
        }
    }

    public static Value intToValue(Integer integer) {
        if (integer == null) {
            return null;
        }
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        try {
            WritableUtils.writeVInt(dos, integer);
        } catch (IOException e) {
            throw new NumberFormatException(e.getMessage());
        }
        return new Value(baos.toByteArray());
    }

    public static Integer valueToInt(Value value) {
        byte[] bytes = value.get();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes, 0, bytes.length));
        try {
            return WritableUtils.readVInt(dis);
        } catch (IOException e) {
            throw new ValueFormatException(e);
        }
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

    public static Map.Entry<Key,Document> keyValueToKeyDocument(Map.Entry<Key,Value> keyValue) {
        if (keyValue == null) {
            return null;
        }
        return new UnmodifiableMapEntry(keyValue.getKey(), valueToDocument(keyValue.getValue()));
    }

    public static Map.Entry<Key,Value> KeyDocumentToKeyValue(Map.Entry<Key,Document> byteKey) {
        if (byteKey == null) {
            return null;
        }
        return new UnmodifiableMapEntry(byteKey.getKey(), documentToValue(byteKey.getValue()));
    }

    public static Map.Entry<byte[],Integer> keyValueToByteInteger(Map.Entry<Key,Value> entry) {
        if (entry == null) {
            return null;
        }
        return new UnmodifiableMapEntry(keyToByte(entry.getKey()), valueToInt(entry.getValue()));
    }

    public static Map.Entry<Key,Value> byteIntegerTokeyValue(Map.Entry<byte[],Integer> entry) {
        if (entry == null) {
            return null;
        }
        return new UnmodifiableMapEntry(byteToKey(entry.getKey()), intToValue(entry.getValue()));
    }

    private KeyValueTransformUtils() {
        throw new UnsupportedOperationException();
    }
}
