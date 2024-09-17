package datawave.query.util.sortedset;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.lang.builder.HashCodeBuilder;

import datawave.util.ByteUtil;

/**
 * A KeyValue that is serializable. Well, this is not actually a KeyValue as that class does not have a default constructor and hence cannot be serializable.
 *
 *
 *
 */
public class KeyValueSerializable implements Map.Entry<Key,Value>, Serializable, Comparable<KeyValueSerializable> {
    private static final long serialVersionUID = 7247815774125171270L;

    public KeyValueSerializable(Key key, byte[] value) {
        this.key = key;
        this.value = value;
    }

    public KeyValueSerializable(Key key, ByteBuffer value) {
        this.key = key;
        this.value = ByteUtil.toBytes(value);
    }

    public Key key;
    public byte[] value;

    @Override
    public Key getKey() {
        return key;
    }

    @Override
    public Value getValue() {
        return new Value(value);
    }

    @Override
    public Value setValue(Value value) {
        throw new UnsupportedOperationException();
    }

    public String toString() {
        return key + " " + new String(value);
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        key.write(out);
        new Value(value, false).write(out);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        this.key = new Key();
        key.readFields(in);
        Value val = new Value();
        val.readFields(in);
        this.value = val.get();
    }

    @Override
    public int compareTo(KeyValueSerializable o) {
        int comparison = this.key.compareTo(o.key);
        if (comparison == 0) {
            comparison = new ByteArrayComparator().compare(this.value, o.value);
        }
        return comparison;
    }

    @Override
    public int hashCode() {
        HashCodeBuilder builder = new HashCodeBuilder();
        builder.append(key).append(value);
        return builder.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof KeyValueSerializable) {
            KeyValueSerializable o = (KeyValueSerializable) obj;
            return key.equals(o.key) && (new ByteArrayComparator().compare(value, o.value) == 0);
        }
        return false;
    }

}
