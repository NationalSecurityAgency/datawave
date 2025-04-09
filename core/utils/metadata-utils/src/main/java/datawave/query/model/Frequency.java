package datawave.query.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Represents a frequency count.
 */
public class Frequency implements WritableComparable<Frequency> {
    
    // The value.
    private long value;
    
    public Frequency() {}
    
    public Frequency(long value) {
        this.value = value;
    }
    
    /**
     * Return the value of this {@link Frequency}.
     * 
     * @return the frequency
     */
    public long getValue() {
        return value;
    }
    
    /**
     * Increment the value of this {@link Frequency} by the given addend.
     * 
     * @param addend
     *            the addend to add
     */
    public void increment(long addend) {
        this.value += addend;
    }
    
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeVLong(dataOutput, value);
    }
    
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        value = WritableUtils.readVLong(dataInput);
    }
    
    @Override
    public int compareTo(Frequency o) {
        return Long.compare(this.value, o.value);
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Frequency frequency = (Frequency) o;
        return value == frequency.value;
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(value);
    }
    
    @Override
    public String toString() {
        return Long.toString(value);
    }
}
