package datawave.query.attributes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang3.builder.CompareToBuilder;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Holds timing information for query iterator next, source, seek, and yield counts.
 */
public class TimingMetadata implements WritableComparable<TimingMetadata>, KryoSerializable, Comparable<TimingMetadata>, Serializable {
    private static final long serialVersionUID = -1;
    private static final String NEXT_COUNT = "NEXT_COUNT";
    private static final String SOURCE_COUNT = "SOURCE_COUNT";
    private static final String SEEK_COUNT = "SEEK_COUNT";
    private static final String YIELD_COUNT = "YIELD_COUNT";
    private static final String STAGE_TIMERS = "STAGE_TIMERS";
    private static final String HOST = "HOST";

    private String host;
    private TreeMap<String,Long> metadata = new TreeMap<>();
    private TreeMap<String,Long> stageTimers = new TreeMap<>();

    public long get(String name) {
        if (metadata.containsKey(name)) {
            return metadata.get(name).longValue();
        }
        return 0;
    }

    public void put(String name, long value) {
        metadata.put(name, new Long(value));
    }

    public long getNextCount() {
        return get(NEXT_COUNT);
    }

    public void setNextCount(long nextCount) {
        put(NEXT_COUNT, nextCount);
    }

    public long getSourceCount() {
        return get(SOURCE_COUNT);
    }

    public void setSourceCount(long sourceCount) {
        put(SOURCE_COUNT, sourceCount);
    }

    public long getSeekCount() {
        return get(SEEK_COUNT);
    }

    public void setSeekCount(long seekCount) {
        put(SEEK_COUNT, seekCount);
    }

    public long getYieldCount() {
        return get(YIELD_COUNT);
    }

    public void setYieldCount(long yieldCount) {
        put(YIELD_COUNT, yieldCount);
    }

    public void addStageTimer(String stageName, long elapsed) {
        stageTimers.put(stageName, new Long(elapsed));
    }

    public Map<String,Long> getStageTimers() {
        return Collections.unmodifiableMap(stageTimers);
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof TimingMetadata) {
            return (compareTo((TimingMetadata) o) == 0);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return metadata.hashCode() + stageTimers.hashCode() + host.hashCode();
    }

    @Override
    public int compareTo(TimingMetadata o) {
        return new CompareToBuilder().append(metadata, o.metadata).append(stageTimers, o.stageTimers).append(host, o.host).toComparison();
    }

    @Override
    public void write(Kryo kryo, Output output) {
        write(metadata, kryo, output);
        write(stageTimers, kryo, output);
        output.writeString(host == null ? "" : host);
    }

    public void write(Map<String,Long> metadata, Kryo kryo, Output output) {
        output.writeInt(metadata.size(), true);

        for (Map.Entry<String,Long> entry : metadata.entrySet()) {
            // Write out the field name
            // writeAscii fails to be read correctly if the value has only one character
            // need to use writeString here
            output.writeString(entry.getKey());
            output.writeLong(entry.getValue());
        }
    }

    @Override
    public void read(Kryo kryo, Input input) {
        metadata = readMap(kryo, input);
        stageTimers = readMap(kryo, input);
        host = input.readString();
        if (host.isEmpty()) {
            host = null;
        }
    }

    private TreeMap<String,Long> readMap(Kryo kryo, Input input) {
        int numAttrs = input.readInt(true);
        TreeMap<String,Long> map = new TreeMap<>();

        for (int i = 0; i < numAttrs; i++) {
            // Get the fieldName
            String fieldName = input.readString();
            long fieldValue = input.readLong();
            map.put(fieldName, fieldValue);
        }
        return map;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        write(metadata, out);
        write(stageTimers, out);
        WritableUtils.writeString(out, host == null ? "" : host);
    }

    private void write(Map<String,Long> metadata, DataOutput out) throws IOException {
        // Write out the number of metadata entries we're going to store
        WritableUtils.writeVInt(out, this.metadata.size());

        for (Map.Entry<String,Long> entry : this.metadata.entrySet()) {
            // Write out the field name
            WritableUtils.writeString(out, entry.getKey());

            // Write out the value
            WritableUtils.writeVLong(out, entry.getValue());
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        metadata = readMap(in);
        stageTimers = readMap(in);
        host = WritableUtils.readString(in);
        if (host.isEmpty()) {
            host = null;
        }
    }

    private TreeMap<String,Long> readMap(DataInput in) throws IOException {
        int numAttrs = WritableUtils.readVInt(in);
        TreeMap<String,Long> map = new TreeMap<>();

        for (int i = 0; i < numAttrs; i++) {
            // Get the fieldName
            String fieldName = WritableUtils.readString(in);
            long fieldValue = WritableUtils.readVLong(in);
            map.put(fieldName, fieldValue);
        }
        return map;
    }

}
