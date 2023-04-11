package datawave.query.attributes;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.hadoop.io.Text;

import datawave.query.Constants;

public class AttributeMetadata implements Comparable<AttributeMetadata>, Serializable {
    private static final long serialVersionUID = -1;
    private static final Text EMPTY_TEXT = new Text();
    private static final ByteSequence EMPTY_BYTE_SEQUENCE = new ArrayByteSequence(new byte[0]);

    private transient Key metadata;

    public Key getMetadata() {
        return metadata;
    }

    public boolean isMetadataSet() {
        return metadata != null;
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
    public void setMetadata(ColumnVisibility vis, long ts) {
        if (isMetadataSet()) {
            metadata = new Key(metadata.getRow(), metadata.getColumnFamily(), metadata.getColumnQualifier(), vis, ts);
        } else {
            metadata = new Key(EMPTY_TEXT, EMPTY_TEXT, EMPTY_TEXT, vis, ts);
        }
    }

    /*
     * Given a key, set the metadata. Expected input keys can be an event key, an fi key, or a tf key. Expected metadata is row=shardid, cf = type\0uid; cq =
     * empty; cv, ts left as is.
     */
    public void setMetadata(Key key) {
        if (key == null) {
            this.metadata = null;
        } else {
            // convert the key to the form shard type\0uid cv, ts. Possible inputs are an event key, a fi key, or a tf key
            final ByteSequence row = key.getRowData(), cf = key.getColumnFamilyData(), cv = key.getColumnVisibilityData();
            if (isFieldIndex(cf)) {
                // find the first null byte in the cq and take everything after that (cq = Normalized Field Value\0Data Type\0UID)
                final ByteSequence cq = key.getColumnQualifierData();
                int nullOffset = 0;
                for (int i = 0; i < cq.length(); i++) {
                    if (cq.byteAt(i) == '\0') {
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
                for (int i = 0; i < cf.length(); i++) {
                    if (cf.byteAt(i) == '\0') {
                        count++;
                        if (count == 2) {
                            nullOffset = i;
                            break;
                        }
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
    }

    protected boolean isFieldIndex(ByteSequence cf) {
        return (cf.length() >= 3 && cf.byteAt(0) == 'f' && cf.byteAt(1) == 'i' && cf.byteAt(2) == '\0');
    }

    protected boolean isTermFrequency(ByteSequence cf) {
        return (cf.length() == 2 && cf.byteAt(0) == 't' && cf.byteAt(1) == 'f');
    }

    private void writeObject(ObjectOutputStream stream) throws IOException {
        stream.writeBoolean(isMetadataSet());
        if (isMetadataSet()) {
            writeText(metadata.getRow(), stream);
            writeText(metadata.getColumnFamily(), stream);
            writeText(metadata.getColumnQualifier(), stream);
            writeText(metadata.getColumnVisibility(), stream);
            stream.writeLong(metadata.getTimestamp());
            stream.writeBoolean(metadata.isDeleted());
        }
    }

    private void writeText(Text text, ObjectOutputStream stream) throws IOException {
        stream.write(text.getLength());
        stream.write(text.getBytes(), 0, text.getLength());
    }

    private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
        boolean hasMetadata = stream.readBoolean();
        if (hasMetadata) {
            Text row = readText(stream);
            Text cf = readText(stream);
            Text cq = readText(stream);
            Text cv = readText(stream);
            long ts = stream.readLong();
            boolean deleted = stream.readBoolean();
            metadata = new Key(row, cf, cq, cv, ts);
            metadata.setDeleted(deleted);
        }
    }

    private Text readText(ObjectInputStream stream) throws IOException {
        int len = stream.readInt();
        byte[] bytes = new byte[len];
        int read = 0;
        while (read < len) {
            read += stream.read(bytes, read, len - read);
        }
        return new Text(bytes);
    }

    @Override
    public int compareTo(AttributeMetadata o) {
        return metadata.compareTo(o.getMetadata());
    }

    public long getSizeInBytes() {
        // 4 for the key reference
        long size = 4;
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

    protected static long roundUp(long size) {
        long extra = size % 8;
        if (extra > 0) {
            size = size + 8 - extra;
        }
        return size;
    }

    @Override
    public int hashCode() {
        return metadata.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof AttributeMetadata) {
            return metadata.equals(((AttributeMetadata) obj).metadata);
        }
        return false;
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        AttributeMetadata clone = new AttributeMetadata();
        clone.setMetadata(metadata);
        return clone;
    }

    @Override
    public String toString() {
        return String.valueOf(metadata);
    }
}
