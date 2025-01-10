package datawave.query.attributes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.io.Text;
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
    private static final Text EMPTY_TEXT = new Text();

    /**
     * The metadata for this attribute. Really only the column visibility and timestamp are preserved in this metadata when serializing and deserializing.
     * However more information (e.g. the document key) can be maintained in this field for use locally.
     */
    protected Key metadata = null;
    protected boolean toKeep = true; // a flag denoting whether this attribute is to be kept in the returned results (transient or not)
    protected boolean fromIndex = true; // Assume attributes are from the index unless specified otherwise.

    public Attribute() {}

    public Attribute(Key metadata, boolean toKeep) {
        this.toKeep = toKeep;
        setMetadata(metadata);
    }

    public boolean isMetadataSet() {
        return (metadata != null);
    }

    public ColumnVisibility getColumnVisibility() {
        if (isMetadataSet()) {
            return metadata.getColumnVisibilityParsed();
        }
        return Constants.EMPTY_VISIBILITY;
    }

    public void setColumnVisibility(ColumnVisibility columnVisibility) {
        if (isMetadataSet()) {
            metadata = new Key(metadata.getRow(), metadata.getColumnFamily(), metadata.getColumnQualifier(), columnVisibility, metadata.getTimestamp());
        } else {
            metadata = new Key(EMPTY_TEXT, EMPTY_TEXT, EMPTY_TEXT, columnVisibility, -1);
        }
    }

    public long getTimestamp() {
        if (isMetadataSet()) {
            return metadata.getTimestamp();
        }
        return -1;
    }

    public void setTimestamp(long ts) {
        if (isMetadataSet()) {
            metadata = new Key(metadata.getRow(), metadata.getColumnFamily(), metadata.getColumnQualifier(), metadata.getColumnVisibility(), ts);
        } else {
            metadata = new Key(EMPTY_TEXT, EMPTY_TEXT, EMPTY_TEXT, Constants.EMPTY_VISIBILITY, ts);
        }
    }

    /*
     *
     * Set the metadata. This should only be set here or from extended classes.
     */
    protected void setMetadata(ColumnVisibility vis, long ts) {
        if (isMetadataSet()) {
            metadata = new Key(metadata.getRow(), metadata.getColumnFamily(), metadata.getColumnQualifier(), vis, ts);
        } else {
            metadata = new Key(EMPTY_TEXT, EMPTY_TEXT, EMPTY_TEXT, vis, ts);
        }
    }

    private static final ByteSequence EMPTY_BYTE_SEQUENCE = new ArrayByteSequence(new byte[0]);

    /*
     * Given a key, set the metadata. Expected input keys can be an event key, an fi key, or a tf key. Expected metadata is row=shardid, cf = type\0uid; cq =
     * empty; cv, ts left as is.
     */
    protected void setMetadata(Key key) {
        if (key == null) {
            this.metadata = null;
            return;
        }

        // convert the key to the form shard type\0uid cv, ts. Possible inputs are an event key, a fi key, or a tf key
        final ByteSequence row = key.getRowData();
        final ByteSequence cf = key.getColumnFamilyData();
        final ByteSequence cv = key.getColumnVisibilityData();
        if (isFieldIndex(cf)) {
            // CQ is 'value\0datatype\0uid
            // iterate backwards to avoid problems from values with nulls in them
            final ByteSequence cq = key.getColumnQualifierData();
            int nullOffset = 0;
            int count = 0;
            for (int i = cq.length() - 1; i >= 0; i--) {
                if (cq.byteAt(i) == '\0' && ++count == 2) {
                    nullOffset = i;
                    break;
                }
            }
            this.metadata = new Key(row.getBackingArray(), row.offset(), row.length(), cq.getBackingArray(), nullOffset + 1, cq.length() - (nullOffset + 1),
                            EMPTY_BYTE_SEQUENCE.getBackingArray(), EMPTY_BYTE_SEQUENCE.offset(), EMPTY_BYTE_SEQUENCE.length(), cv.getBackingArray(),
                            cv.offset(), cv.length(), key.getTimestamp());
        } else if (isTermFrequency(cf)) {
            // find the second null byte in the cq and take everything before that (cq = DataType\0UID\0Normalized Field Value\0Field Name)
            final ByteSequence cq = key.getColumnQualifierData();
            int nullOffset = 0;
            int count = 0;
            for (int i = 0; i < cq.length(); i++) {
                if (cq.byteAt(i) == '\0' && ++count == 2) {
                    nullOffset = i;
                    break;
                }
            }
            this.metadata = new Key(row.getBackingArray(), row.offset(), row.length(), cq.getBackingArray(), cq.offset(), nullOffset,
                            EMPTY_BYTE_SEQUENCE.getBackingArray(), EMPTY_BYTE_SEQUENCE.offset(), EMPTY_BYTE_SEQUENCE.length(), cv.getBackingArray(),
                            cv.offset(), cv.length(), key.getTimestamp());
        } else {
            this.metadata = new Key(row.getBackingArray(), row.offset(), row.length(), cf.getBackingArray(), cf.offset(), cf.length(),
                            EMPTY_BYTE_SEQUENCE.getBackingArray(), EMPTY_BYTE_SEQUENCE.offset(), EMPTY_BYTE_SEQUENCE.length(), cv.getBackingArray(),
                            cv.offset(), cv.length(), key.getTimestamp());
        }
    }

    protected boolean isFieldIndex(ByteSequence cf) {
        return (cf.length() >= 3 && cf.byteAt(0) == 'f' && cf.byteAt(1) == 'i' && cf.byteAt(2) == '\0');
    }

    protected boolean isTermFrequency(ByteSequence cf) {
        return (cf.length() == 2 && cf.byteAt(0) == 't' && cf.byteAt(1) == 'f');
    }

    public Key getMetadata() {
        return metadata;
    }

    /**
     * Unset the metadata. This should only be set here or from extended classes.
     */
    protected void clearMetadata() {
        metadata = null;
    }

    protected void writeMetadata(DataOutput out) throws IOException {
        out.writeBoolean(isMetadataSet());
        if (isMetadataSet()) {
            byte[] cvBytes = getColumnVisibility().getExpression();

            WritableUtils.writeVInt(out, cvBytes.length);

            out.write(cvBytes);
            out.writeLong(getTimestamp());
        }
    }

    protected void writeMetadata(Kryo kryo, Output output) {
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

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Attribute)) {
            return false;
        }
        Attribute other = (Attribute) o;
        EqualsBuilder equals = new EqualsBuilder().append(this.isMetadataSet(), other.isMetadataSet());
        if (this.isMetadataSet()) {
            equals.append(this.getMetadata(), other.getMetadata());
        }
        return equals.isEquals();
    }

    @Override
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
        long size = 0;
        if (isMetadataSet()) {
            size += roundUp(33);
            // 33 is object overhead, 4 array refs, 1 long and 1 boolean
            size += roundUp(metadata.getRowData().length() + 12);
            size += roundUp(metadata.getColumnFamilyData().length() + 12);
            size += roundUp(metadata.getColumnQualifierData().length() + 12);
            size += roundUp(metadata.getColumnVisibilityData().length() + 12);
            // 12 is array overhead
        }
        return size;
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

    public abstract void write(DataOutput output) throws IOException;

    public abstract void write(Kryo kryo, Output output);

    public abstract Object getData();

    public abstract Collection<ValueTuple> visit(Collection<String> fieldnames, DatawaveJexlContext context);

    // TODO Add the ability to prune attributes and return a sub-Document given Authorizations

}
