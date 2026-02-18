package datawave.query.index.day;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Wrapper around a map of jexl node strings to bitsets
 */
public class BitSetIndexEntry implements Comparable<BitSetIndexEntry>, KryoSerializable {

    private static final Logger log = LoggerFactory.getLogger(BitSetIndexEntry.class);

    private String yearOrDay;
    private final Map<String,BitSet> entries;

    public BitSetIndexEntry() {
        // no-arg constructor required for Kryo serialization
        entries = new HashMap<>();
    }

    public BitSetIndexEntry(String yearOrDay, Map<String,BitSet> entries) {
        this.yearOrDay = yearOrDay;
        this.entries = entries;
    }

    public String getYearOrDay() {
        return yearOrDay;
    }

    public Map<String,BitSet> getEntries() {
        return entries;
    }

    @Override
    public void write(Kryo kryo, Output output) {
        output.writeString(yearOrDay);
        output.writeInt(entries.keySet().size());
        entries.keySet().forEach(key -> {
            output.writeString(key);
            BitSet bitSet = entries.get(key);

            log.trace("writing bitset: {}", bitSet);
            byte[] bytes = bitSet.toByteArray();
            output.writeInt(bytes.length);
            output.writeBytes(bytes);
        });
    }

    @Override
    public void read(Kryo kryo, Input input) {
        entries.clear();
        yearOrDay = input.readString();
        int size = input.readInt();
        for (int i = 0; i < size; i++) {
            String key = input.readString();
            int len = input.readInt();
            BitSet bits = BitSet.valueOf(input.readBytes(len));

            log.trace("reading bitset: {}", bits);
            entries.put(key, bits);
        }
    }

    @Override
    public int compareTo(BitSetIndexEntry o) {
        int result = yearOrDay.compareTo(o.yearOrDay);
        if (result != 0) {
            return result;
        }

        // comparing the entry keys and values would be too expensive
        // default comparison to -1
        return -1;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof BitSetIndexEntry) {
            BitSetIndexEntry other = (BitSetIndexEntry) o;
            //  @formatter:off
            return new EqualsBuilder()
                            .append(yearOrDay, other.yearOrDay)
                            .append(entries, other.entries)
                            .isEquals();
            //  @formatter:on
        }
        return false;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(yearOrDay).append(entries).build();
    }
}
