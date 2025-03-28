package datawave.query.util.count;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Wrapper around a HashMap that supports Kryo serialization
 */
public class CountMap implements KryoSerializable {

    private final Map<String,Long> counts;

    public CountMap() {
        counts = new HashMap<>();
    }

    public Long put(String key, Long value) {
        return counts.put(key, value);
    }

    public void putAll(CountMap other) {
        counts.putAll(other.counts);
    }

    public Set<String> keySet() {
        return counts.keySet();
    }

    public Long get(String key) {
        return counts.get(key);
    }

    public Set<Entry<String,Long>> entrySet() {
        return counts.entrySet();
    }

    public boolean isEmpty() {
        return counts.isEmpty();
    }

    public boolean containsKey(String key) {
        return counts.containsKey(key);
    }

    @Override
    public void read(Kryo kryo, Input input) {
        counts.clear();
        int size = input.readInt(true);
        for (int i = 0; i < size; i++) {
            String key = input.readString();
            Long value = input.readLong(true);
            put(key, value);
        }
    }

    @Override
    public void write(Kryo kryo, Output output) {
        output.writeInt(keySet().size(), true);
        for (Entry<String,Long> entry : entrySet()) {
            output.writeString(entry.getKey());
            output.writeLong(entry.getValue(), true);
        }
    }

    public Map<String,Long> getCounts() {
        return counts;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof CountMap) {
            CountMap other = (CountMap) o;
            return this.counts.equals(other.counts);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return counts.hashCode();
    }

}
