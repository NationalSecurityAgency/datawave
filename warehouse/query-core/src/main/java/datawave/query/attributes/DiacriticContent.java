package datawave.query.attributes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;

import org.apache.accumulo.core.data.Key;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.io.WritableUtils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import datawave.data.normalizer.LcNormalizer;
import datawave.query.collections.FunctionalSet;
import datawave.query.jexl.DatawaveJexlContext;

public class DiacriticContent extends Attribute<DiacriticContent> implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final LcNormalizer normalizer = new LcNormalizer();

    private String content;

    protected DiacriticContent() {
        super(null, true);
    }

    public DiacriticContent(String content, Key docKey, boolean toKeep) {
        super(docKey, toKeep);
        this.content = content;
    }

    @Override
    public long sizeInBytes() {
        return sizeInBytes(content) + super.sizeInBytes(4);
        // 4 for string reference
    }

    public String getContent() {
        return this.content;
    }

    @Override
    public Object getData() {
        return getContent();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        writeMetadata(out);
        WritableUtils.writeString(out, content);
        WritableUtils.writeVInt(out, toKeep ? 1 : 0);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        readMetadata(in);
        this.content = WritableUtils.readString(in);
        this.toKeep = WritableUtils.readVInt(in) != 0;
    }

    @Override
    public int compareTo(DiacriticContent other) {
        int cmp = content.compareTo(other.getContent());

        if (0 == cmp) {
            // Compare the ColumnVisibility as well
            return this.compareMetadata(other);
        }

        return cmp;
    }

    @Override
    public boolean equals(Object o) {
        if (null == o) {
            return false;
        }

        if (o instanceof DiacriticContent) {
            return 0 == this.compareTo((DiacriticContent) o);
        }

        return false;
    }

    @Override
    public int hashCode() {
        HashCodeBuilder hcb = new HashCodeBuilder(4639, 2129);
        hcb.append(content).append(super.hashCode());

        return hcb.toHashCode();
    }

    @Override
    public Collection<ValueTuple> visit(Collection<String> fieldNames, DatawaveJexlContext context) {
        return FunctionalSet.singleton(new ValueTuple(fieldNames, this.content, normalizer.normalize(this.content), this));
    }

    @Override
    public void write(Kryo kryo, Output output) {
        writeMetadata(kryo, output);
        output.writeString(this.content);
        output.writeBoolean(this.toKeep);
    }

    @Override
    public void read(Kryo kryo, Input input) {
        readMetadata(kryo, input);
        this.content = input.readString();
        this.toKeep = input.readBoolean();
    }

    /*
     * (non-Javadoc)
     *
     * @see Attribute#deepCopy()
     */
    @Override
    public DiacriticContent copy() {
        return new DiacriticContent(this.getContent(), this.getMetadata(), this.isToKeep());
    }

}
