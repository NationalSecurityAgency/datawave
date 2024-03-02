package datawave.query.attributes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.log4j.Logger;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import datawave.query.Constants;
import datawave.query.jexl.DatawaveJexlContext;

public abstract class Attribute<T extends Comparable<T>> implements WritableComparable<T>, KryoSerializable {

    private static final Logger log = Logger.getLogger(Attribute.class);

    /**
     * The metadata for this attribute. Really only the column visibility and timestamp are preserved in this metadata when serializing and deserializing.
     * However more information (e.g. the document key) can be maintained in this field for use locally.
     */
    protected AttributeMetadata metadata = new AttributeMetadata();
    protected boolean toKeep = true; // a flag denoting whether this attribute is to be kept in the returned results (transient or not)
    protected boolean fromIndex = true; // Assume attributes are from the index unless specified otherwise.

    public Attribute() {}

    public Attribute(boolean toKeep) {
        this.toKeep = toKeep;
    }

    public Attribute(Key key, boolean toKeep) {
        this.toKeep = toKeep;
        metadata.setMetadata(key);
    }

    public boolean isMetadataSet() {
        return metadata.isMetadataSet();
    }

    public Key getMetadata() {
        return metadata.getMetadata();
    }

    public void setMetadata(Key key) {
        metadata.setMetadata(key);
    }

    public void setMetadata(ColumnVisibility vis, long ts) {
        metadata.setMetadata(vis, ts);
    }

    public ColumnVisibility getColumnVisibility() {
        return metadata.getColumnVisibility();
    }

    public void setColumnVisibility(ColumnVisibility vis) {
        metadata.setColumnVisibility(vis);
    }

    public long getTimestamp() {
        return metadata.getTimestamp();
    }

    public void setTimestamp(long ts) {
        metadata.setTimestamp(ts);
    }

    /**
     * Unset the metadata. This should only be set here or from extended classes.
     */
    protected void clearMetadata() {
        metadata.setMetadata(null);
    }

    protected void writeMetadata(DataOutput out, Boolean reducedResponse) throws IOException {
        out.writeBoolean(isMetadataSet());
        if (isMetadataSet()) {
            byte[] cvBytes = getColumnVisibility().getExpression();

            WritableUtils.writeVInt(out, cvBytes.length);

            out.write(cvBytes);
            out.writeLong(getTimestamp());
        }
    }

    protected void writeMetadata(Kryo kryo, Output output, Boolean reducedResponse) {
        output.writeBoolean(isMetadataSet());
        if (isMetadataSet()) {
            byte[] cvBytes = getColumnVisibility().getExpression();
            output.writeInt(cvBytes.length, true);
            output.writeBytes(cvBytes);
            output.writeLong(getTimestamp());
        }
    }

    protected void readMetadata(DataInput in) throws IOException {
        if (in.readBoolean()) {
            int cvBytesLength = WritableUtils.readVInt(in);

            byte[] cvBytes = new byte[cvBytesLength];

            in.readFully(cvBytes);

            this.setMetadata(new ColumnVisibility(cvBytes), in.readLong());
        } else {
            this.clearMetadata();
        }
    }

    protected void readMetadata(Kryo kryo, Input input) {
        if (input.readBoolean()) {
            int size = input.readInt(true);

            this.setMetadata(new ColumnVisibility(input.readBytes(size)), input.readLong());
        } else {
            this.clearMetadata();
        }
    }

    protected int compareMetadata(Attribute<T> other) {
        if (this.isMetadataSet() != other.isMetadataSet()) {
            if (this.isMetadataSet()) {
                return 1;
            } else {
                return -1;
            }
        } else if (this.isMetadataSet()) {
            return this.metadata.compareTo(other.metadata);
        } else {
            return 0;
        }
    }

    public void setFromIndex(boolean fromIndex) {
        this.fromIndex = fromIndex;
    }

    public boolean isFromIndex() {
        return fromIndex;
    }

    public int hashCode() {
        HashCodeBuilder hcb = new HashCodeBuilder(145, 11);
        hcb.append(this.isMetadataSet());
        if (isMetadataSet()) {
            hcb.append(this.getMetadata());
        }
        return hcb.toHashCode();
    }

    @Override
    public String toString() {
        if (isMetadataSet()) {
            return getData() + ":" + getMetadata();
        } else {
            return String.valueOf(getData());
        }
    }

    public int size() {
        return 1;
    }

    private long getMetadataSizeInBytes() {
        return metadata.getSizeInBytes();
    }

    public long sizeInBytes() {
        // return the approximate overhead of this class
        long size = 16;
        // 8 for the object overhead
        // 4 for the key reference
        // 1 for the keep boolean
        // all rounded up to the nearest multiple of 8 to make 16 out of 13 bytes
        size += getMetadataSizeInBytes();
        return size;
    }

    // for use by subclasses to estimate size
    protected long sizeInBytes(long extra) {
        return roundUp(extra + 13) + getMetadataSizeInBytes();
        // 13 is the base size in bytes (see sizeInBytes(), unrounded and without metadata)
    }

    // a helper method to return the size of a string
    protected static long sizeInBytes(String value) {
        if (value == null) {
            return 0;
        } else {
            return 16 + roundUp(12 + (value.length() * 2));
            // 16 for int, array ref, and object overhead
            // 12 for array overhead
        }
    }

    protected static long roundUp(long size) {
        long extra = size % 8;
        if (extra > 0) {
            size = size + 8 - extra;
        }
        return size;
    }

    public abstract T copy();

    public boolean isToKeep() {
        return toKeep;
    }

    public void setToKeep(boolean toKeep) {
        this.toKeep = toKeep;
    }

    /**
     * Reduce the attribute to those to keep
     *
     * @return the attribute
     */
    public Attribute<?> reduceToKeep() {
        // noop for most attributes. Only override for attributes representing sets of other attributes
        if (this.toKeep) {
            return this;
        } else {
            return null;
        }
    }

    public abstract void write(DataOutput output, boolean reducedResponse) throws IOException;

    public abstract void write(Kryo kryo, Output output, Boolean reducedResponse);

    public abstract Object getData();

    public abstract Collection<ValueTuple> visit(Collection<String> fieldnames, DatawaveJexlContext context);

    // TODO Add the ability to prune attributes and return a sub-Document given Authorizations

}
