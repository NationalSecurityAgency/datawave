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

public class IpAddress extends Attribute<IpAddress> implements Serializable {
    private static final long serialVersionUID = 1L;

    private datawave.data.type.util.IpAddress value;
    private String normalizedValue;

    protected IpAddress() {
        super(null, true);
    }

    public IpAddress(String ipAddress, Key docKey, boolean toKeep) {
        super(docKey, toKeep);
        setValue(ipAddress);
        setNormalizedValue(ipAddress);
        validate();
    }

    @Override
    public long sizeInBytes() {
        return sizeInBytes(value.toString()) + sizeInBytes(normalizedValue) + super.sizeInBytes(8);
        // 8 for string references
    }

    protected void validate() {
        if (this.value == null || this.normalizedValue == null) {
            throw new IllegalArgumentException("Unable to create ip value " + this.value + ", " + this.normalizedValue);
        }
    }

    @Override
    public Object getData() {
        return this.value;
    }

    private void setValue(String value) {
        this.value = datawave.data.type.util.IpAddress.parse(value);
    }

    private void setNormalizedValue(String value) {
        this.normalizedValue = datawave.data.type.util.IpAddress.parse(value).toZeroPaddedString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        writeMetadata(out);
        WritableUtils.writeString(out, this.value.toString());
        WritableUtils.writeVInt(out, toKeep ? 1 : 0);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        readMetadata(in);
        String ipString = WritableUtils.readString(in);
        setValue(ipString);
        setNormalizedValue(ipString);
        this.toKeep = WritableUtils.readVInt(in) != 0;
        validate();
    }

    @Override
    public int compareTo(IpAddress o) {
        int cmp = value.compareTo(o.value);

        if (0 == cmp) {
            // Compare the ColumnVisibility as well
            return this.compareMetadata(o);
        }

        return cmp;
    }

    @Override
    public boolean equals(Object o) {
        if (null == o) {
            return false;
        }

        if (o instanceof IpAddress) {
            return 0 == this.compareTo((IpAddress) o);
        }

        return false;
    }

    @Override
    public int hashCode() {
        HashCodeBuilder hcb = new HashCodeBuilder(163, 157);
        hcb.append(super.hashCode()).append(this.value);

        return hcb.toHashCode();
    }

    @Override
    public Collection<ValueTuple> visit(Collection<String> fieldNames, DatawaveJexlContext context) {
        validate();
        return FunctionalSet.singleton(new ValueTuple(fieldNames, this.value, normalizedValue, this));
    }

    @Override
    public void write(Kryo kryo, Output output) {
        writeMetadata(kryo, output);
        output.writeString(this.value.toString());
        output.writeBoolean(this.toKeep);
    }

    @Override
    public void read(Kryo kryo, Input input) {
        readMetadata(kryo, input);
        String ipAddressString = input.readString();
        setValue(ipAddressString);
        setNormalizedValue(ipAddressString);
        this.toKeep = input.readBoolean();
        validate();
    }

    /*
     * (non-Javadoc)
     *
     * @see Attribute#deepCopy()
     */
    @Override
    public IpAddress copy() {
        return new IpAddress(this.value.toString(), this.getMetadata(), this.isToKeep());
    }

}
