package datawave.query.attributes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;

import org.apache.accumulo.core.data.Key;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import datawave.data.type.LcNoDiacriticsType;
import datawave.query.Constants;
import datawave.query.collections.FunctionalSet;
import datawave.query.jexl.DatawaveJexlContext;

/**
 *
 */
public class Cardinality extends Attribute<Cardinality> {

    protected FieldValueCardinality content;

    private static final LcNoDiacriticsType normalizer = new LcNoDiacriticsType();

    protected Cardinality() {
        super(null, true);
        content = new FieldValueCardinality();
    }

    public Cardinality(FieldValueCardinality content, Key docKey, boolean toKeep) {
        super(docKey, toKeep);
        this.content = content;
        normalize();
    }

    protected void normalize() {
        content.setCeiling(normalizer.normalize(content.getCeilingValue()));
        content.setFloor(normalizer.normalize(content.getFloorValue()));
    }

    @Override
    public long sizeInBytes() {
        return super.sizeInBytes(4) + content.sizeInBytes();
        // 4 for content reference
    }

    public FieldValueCardinality getContent() {
        return this.content;
    }

    @Override
    public Object getData() {
        return getContent();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        writeMetadata(out);
        WritableUtils.writeString(out, content.fieldName);
        WritableUtils.writeString(out, content.lower);
        WritableUtils.writeString(out, content.upper);
        WritableUtils.writeCompressedByteArray(out, content.estimate.getBytes());
        WritableUtils.writeVInt(out, toKeep ? 1 : 0);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        readMetadata(in);
        content = new FieldValueCardinality();
        content.fieldName = WritableUtils.readString(in);
        content.lower = WritableUtils.readString(in);
        content.upper = WritableUtils.readString(in);
        byte[] cardArray = WritableUtils.readCompressedByteArray(in);
        content.estimate = HyperLogLogPlus.Builder.build(cardArray);
        this.toKeep = WritableUtils.readVInt(in) != 0;
    }

    @Override
    public int compareTo(Cardinality other) {

        int cmp = content.compareTo(other.content);

        if (cmp == 0) {
            cmp = compareMetadata(other);
        }
        return cmp;
    }

    @Override
    public boolean equals(Object o) {
        if (null == o) {
            return false;
        }

        if (o instanceof Cardinality) {
            return 0 == this.compareTo((Cardinality) o);
        }

        return false;
    }

    @Override
    protected int compareMetadata(Attribute<Cardinality> other) {
        if (this.isMetadataSet() != other.isMetadataSet()) {
            if (this.isMetadataSet()) {
                return 1;
            } else {
                return -1;
            }
        } else if (this.isMetadataSet()) {
            byte[] cvBytes = this.getColumnVisibility().getExpression();
            if (null == cvBytes) {
                cvBytes = Constants.EMPTY_BYTES;
            }

            byte[] otherCVBytes = other.getColumnVisibility().getExpression();
            if (null == otherCVBytes) {
                otherCVBytes = Constants.EMPTY_BYTES;
            }

            int result = WritableComparator.compareBytes(cvBytes, 0, cvBytes.length, otherCVBytes, 0, otherCVBytes.length);

            if (result == 0) {
                result = new Long(this.getTimestamp()).compareTo(other.getTimestamp());
            }

            return result;
        } else {
            return 0;
        }
    }

    @Override
    public int hashCode() {
        HashCodeBuilder hcb = new HashCodeBuilder(2099, 2129);
        hcb.appendSuper(content.hashCode()).append(this.getMetadata().getColumnVisibility()).append(this.getMetadata().getTimestamp());
        return hcb.toHashCode();
    }

    @Override
    public void write(Kryo kryo, Output output) {
        super.writeMetadata(kryo, output);
        output.writeString(this.content.fieldName);
        output.writeString(this.content.lower);
        output.writeString(this.content.upper);
        byte[] cardArray;
        try {
            cardArray = this.content.estimate.getBytes();
            output.writeInt(cardArray.length);
            output.write(cardArray);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        output.writeBoolean(this.toKeep);
    }

    @Override
    public void read(Kryo kryo, Input input) {
        super.readMetadata(kryo, input);
        content = new FieldValueCardinality();
        this.content.fieldName = input.readString();
        this.content.lower = input.readString();
        this.content.upper = input.readString();
        int size = input.readInt();
        byte[] cardArray = new byte[size];
        input.read(cardArray);
        try {
            this.content.estimate = HyperLogLogPlus.Builder.build(cardArray);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.toKeep = input.readBoolean();
    }

    /*
     * (non-Javadoc)
     *
     * @see Attribute#deepCopy()
     */
    @Override
    public Cardinality copy() {
        return new Cardinality(this.getContent(), this.getMetadata(), this.isToKeep());
    }

    @Override
    public Collection<ValueTuple> visit(Collection<String> fieldNames, DatawaveJexlContext context) {
        return FunctionalSet.singleton(new ValueTuple(fieldNames, this.content, this.content, this));
    }

}
