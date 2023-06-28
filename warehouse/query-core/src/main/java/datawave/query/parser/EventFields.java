package datawave.query.parser;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.security.ColumnVisibility;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;
import com.google.common.collect.SetMultimap;

import datawave.query.parser.EventFields.FieldValue;

/**
 * Object used to hold the fields in an event. This is a multimap because fields can be repeated.
 */
public class EventFields implements SetMultimap<String,FieldValue>, KryoSerializable {
    private Multimap<String,FieldValue> map = null;

    public static class FieldValue {
        ColumnVisibility visibility;
        byte[] value;
        String context;
        Boolean hit;

        public FieldValue(ColumnVisibility visibility, byte[] value) {
            this(visibility, value, null);
        }

        public FieldValue(ColumnVisibility visibility, byte[] value, String context) {
            super();
            this.visibility = visibility;
            this.value = value;
            this.context = context;
        }

        public ColumnVisibility getVisibility() {
            return visibility;
        }

        public byte[] getValue() {
            return value;
        }

        public void setVisibility(ColumnVisibility visibility) {
            this.visibility = visibility;
        }

        public void setValue(byte[] value) {
            this.value = value;
        }

        public String getContext() {
            return context;
        }

        public void setContext(String context) {
            this.context = context;
        }

        public Boolean isHit() {
            return hit;
        }

        public void setHit(Boolean hit) {
            this.hit = hit;
        }

        public int size() {
            byte[] exp = visibility.getExpression();
            return (exp == null || exp.length == 0 ? 0 : visibility.flatten().length) + value.length
                            + (context == null ? 0 : context.length() + (hit == null ? 0 : 1));
        }

        @Override
        public String toString() {
            StringBuilder buf = new StringBuilder();
            if (null != visibility) {
                byte[] expr = visibility.getExpression();
                buf.append(" visibility: ").append(new String(expr == null || expr.length == 0 ? new byte[0] : visibility.flatten()));
            }
            if (null != value)
                buf.append(" value size: ").append(value.length);
            if (null != value)
                buf.append(" value: ").append(new String(value));
            if (null != context)
                buf.append(" context: ").append(context);
            if (null != hit)
                buf.append(" hit: ").append(hit);
            return buf.toString();
        }

    }

    public EventFields() {
        map = HashMultimap.create();
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return map.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return map.containsValue(value);
    }

    @Override
    public boolean containsEntry(Object key, Object value) {
        return map.containsEntry(key, value);
    }

    @Override
    public boolean put(String key, FieldValue value) {
        return map.put(key, value);
    }

    @Override
    public boolean remove(Object key, Object value) {
        return map.remove(key, value);
    }

    @Override
    public boolean putAll(String key, Iterable<? extends FieldValue> values) {
        return map.putAll(key, values);
    }

    @Override
    public boolean putAll(Multimap<? extends String,? extends FieldValue> multimap) {
        return map.putAll(multimap);
    }

    @Override
    public void clear() {
        map = HashMultimap.create();
    }

    @Override
    public Set<String> keySet() {
        return map.keySet();
    }

    @Override
    public Multiset<String> keys() {
        return map.keys();
    }

    @Override
    public Collection<FieldValue> values() {
        return map.values();
    }

    @Override
    public Set<FieldValue> get(String key) {
        return (Set<FieldValue>) map.get(key);
    }

    @Override
    public Set<FieldValue> removeAll(Object key) {
        return (Set<FieldValue>) map.removeAll(key);
    }

    @Override
    public Set<FieldValue> replaceValues(String key, Iterable<? extends FieldValue> values) {
        return (Set<FieldValue>) map.replaceValues(key, values);
    }

    @Override
    public Set<Entry<String,FieldValue>> entries() {
        return (Set<Entry<String,FieldValue>>) map.entries();
    }

    @Override
    public Map<String,Collection<FieldValue>> asMap() {
        return map.asMap();
    }

    public int getByteSize() {
        int count = 0;
        for (Entry<String,FieldValue> e : map.entries()) {
            count += e.getKey().getBytes().length + e.getValue().size();
        }
        return count;
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        for (Entry<String,FieldValue> entry : map.entries()) {
            buf.append("\tkey: ").append(entry.getKey()).append(" -> ").append(entry.getValue()).append("\n");
        }
        return buf.toString();
    }

    @Override
    public void read(Kryo kryo, Input input) {
        // Read in the number of map entries
        int entries = input.readInt(true);

        for (int i = 0; i < entries; i++) {
            // Read in the key
            String key = input.readString();

            // Read in the fields in the value
            int numBytes = input.readInt(true);
            ColumnVisibility vis = new ColumnVisibility(input.readBytes(numBytes));

            numBytes = input.readInt(true);
            byte[] value = input.readBytes(numBytes);

            FieldValue fv = new FieldValue(vis, value);

            boolean hasContext = input.readBoolean();
            if (hasContext) {
                fv.setContext(input.readString());
            }

            boolean hasIsHit = input.readBoolean();
            if (hasIsHit) {
                fv.setHit(input.readBoolean());
            }

            map.put(key, fv);
        }

    }

    @Override
    public void write(Kryo kryo, Output output) {
        // Write out the number of entries;
        output.writeInt(map.size(), true);

        for (Entry<String,FieldValue> entry : map.entries()) {
            // Write the key
            output.writeString(entry.getKey());

            // Write the fields in the value
            byte[] vis = entry.getValue().getVisibility().getExpression();

            if (vis == null) {
                vis = new byte[0];
            }

            output.writeInt(vis.length, true);
            output.write(vis);

            output.writeInt(entry.getValue().getValue().length, true);
            output.write(entry.getValue().getValue());

            if (null != entry.getValue().getContext()) {
                output.writeBoolean(true);
                output.writeString(entry.getValue().getContext());
            } else {
                output.writeBoolean(false);
            }

            if (null != entry.getValue().isHit()) {
                output.writeBoolean(true);
                output.writeBoolean(entry.getValue().isHit());
            } else {
                output.writeBoolean(false);
            }
        }
    }

}
