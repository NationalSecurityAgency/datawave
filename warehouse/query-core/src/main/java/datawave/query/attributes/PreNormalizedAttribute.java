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

import datawave.query.collections.FunctionalSet;
import datawave.query.jexl.DatawaveJexlContext;

/**
 * An abstract Attribute that does no normalization
 *
 */
public class PreNormalizedAttribute extends Attribute<PreNormalizedAttribute> implements Serializable {
    private static final long serialVersionUID = 1L;
    protected String value;

    public String getValue() {
        return value;
    }

    protected PreNormalizedAttribute() {
        super(null, true);
    }

    public PreNormalizedAttribute(String value, Key docKey, boolean toKeep) {
        super(docKey, toKeep);
        this.value = value;
    }

    @Override
    public long sizeInBytes() {
        return sizeInBytes(value) + super.sizeInBytes(4);
        // 4 for string reference
    }

    @Override
    public boolean equals(Object o) {
        if (null == o) {
            return false;
        }

        if (o instanceof PreNormalizedAttribute) {
            return 0 == this.compareTo((PreNormalizedAttribute) o);
        }

        return false;
    }

    @Override
    public int hashCode() {
        HashCodeBuilder hcb = new HashCodeBuilder(2141, 2137);
        hcb.append(super.hashCode()).append(this.getData());

        return hcb.toHashCode();
    }

    @Override
    public int compareTo(PreNormalizedAttribute o) {
        int cmp = this.value.compareTo(o.value);
        if (0 == cmp) {
            cmp = super.compareMetadata(o);
        }

        return cmp;
    }

    @Override
    public void write(Kryo kryo, Output output) {
        super.writeMetadata(kryo, output);
        output.writeString(this.value);
        output.writeBoolean(this.toKeep);
    }

    @Override
    public void read(Kryo kryo, Input input) {
        super.readMetadata(kryo, input);
        this.value = input.readString();
        this.toKeep = input.readBoolean();
    }

    @Override
    public void write(DataOutput output) throws IOException {
        super.writeMetadata(output);
        WritableUtils.writeString(output, this.value);
        WritableUtils.writeVInt(output, toKeep ? 1 : 0);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        super.readMetadata(input);
        this.value = WritableUtils.readString(input);
        this.toKeep = WritableUtils.readVInt(input) != 0;
    }

    @Override
    public Object getData() {
        return this.value;
    }

    @Override
    public Collection<ValueTuple> visit(Collection<String> fieldNames, DatawaveJexlContext context) {
        return FunctionalSet.singleton(new ValueTuple(fieldNames, this.value, this.value, this));
    }

    @Override
    public PreNormalizedAttribute copy() {
        return new PreNormalizedAttribute(this.getValue(), this.getMetadata(), this.isToKeep());
    }

}
