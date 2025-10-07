package datawave.query.function;

import java.nio.ByteBuffer;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;

import com.google.common.base.Function;

import datawave.query.attributes.Content;
import datawave.query.attributes.Document;

/**
 * A mutating function that extracts the data type from the prefix of the column family and adds it to the associated Document as an attribute.
 *
 */
public class DataTypeAsField implements Function<Entry<Key,Document>,Entry<Key,Document>> {
    private String key;

    // buffers for extracting the data type
    private final Text cf = new Text();
    private final Text cv = new Text();

    public DataTypeAsField(String key) {
        this.key = key;
    }

    @Override
    public Entry<Key,Document> apply(Entry<Key,Document> from) {
        Content dataType = extractDataType(from.getKey(), from.getValue().isToKeep());
        from.getValue().put(key, dataType, false);
        return from;
    }

    private Content extractDataType(Key k, boolean toKeep) {
        k.getColumnFamily(cf);
        ByteBuffer b = ByteBuffer.wrap(cf.getBytes(), 0, cf.getLength());
        while (b.get() != 0)
            ;
        cf.set(cf.getBytes(), 0, b.position() - 1);
        return new Content(cf.toString(), k, toKeep);
    }
}
