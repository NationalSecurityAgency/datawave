package datawave.query.attributes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Collection;

import org.apache.accumulo.core.data.Key;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.io.WritableUtils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import datawave.data.normalizer.NumberNormalizer;
import datawave.data.type.util.NumericalEncoder;
import datawave.query.collections.FunctionalSet;
import datawave.query.jexl.DatawaveJexlContext;

public class Numeric extends Attribute<Numeric> implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final NumberNormalizer normalizer = new NumberNormalizer();

    private Number value;
    private String normalizedValue;

    protected Numeric() {
        super(null, true);
    }

    public Numeric(String value, Key docKey, boolean toKeep) {
        super(docKey, toKeep);
        setValue(value);
        setNormalizedValue(value);
        validate();
    }

    public Numeric(Number value, Key docKey, boolean toKeep) {
        super(docKey, toKeep);
        setValue(value);
        setNormalizedValue(value);
        validate();
    }

    @Override
    public long sizeInBytes() {
        return 20 + sizeInBytes(normalizedValue) + super.sizeInBytes(8);
        // 20 is for a basic int Number
        // 8 for string references
    }

    /**
     * Validates the backing value for this attribute
     */
    protected void validate() {
        if (value == null) {
            throw new IllegalArgumentException("Numeric value was not set");
        }
    }

    @Override
    public Object getData() {
        return value;
    }

    private Number parseToNumber(String value) {
        // some documents will contain numeric data that is already encoded
        // in those cases it is faster to check for encoding up front instead
        // of finding out the hard way via exception.
        try {
            if (NumericalEncoder.isPossiblyEncoded(value)) {
                BigDecimal bigDecimal = NumericalEncoder.decode(value);
                return bigDecimal.doubleValue();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        Number number;
        try {
            number = NumberUtils.createNumber(value);
        } catch (Exception ex) {
            // alternatively, throw a shallow exception to avoid expensive calls to 'fill in stacktrace'
            BigDecimal bigDecimal = NumericalEncoder.decode(value);
            number = bigDecimal.doubleValue();
        }
        return number;
    }

    private void setValue(String value) {
        try {
            this.value = parseToNumber(value);
        } catch (Exception ex) {
            BigDecimal bigDecimal = NumericalEncoder.decode(value);
            this.value = bigDecimal.doubleValue();
        }
    }

    private void setValue(Number value) {
        this.value = value;
    }

    private void setNormalizedValue(String value) {
        try {
            this.normalizedValue = normalizer.normalize(parseToNumber(value).toString());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Numeric value was not set");
        }
    }

    private void setNormalizedValue(Number value) {
        try {
            this.normalizedValue = normalizer.normalize(value.toString());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Numeric value was not set");
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        writeMetadata(out);
        WritableUtils.writeString(out, normalizedValue);
        WritableUtils.writeVInt(out, toKeep ? 1 : 0);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        readMetadata(in);
        String stringValue = WritableUtils.readString(in);
        setValue(stringValue);
        setNormalizedValue(stringValue);
        this.toKeep = WritableUtils.readVInt(in) != 0;
        validate();
    }

    @Override
    public boolean equals(Object o) {
        if (null == o) {
            return false;
        }

        if (o instanceof Numeric) {
            return 0 == this.compareTo((Numeric) o);
        }

        return false;
    }

    @Override
    public int hashCode() {
        HashCodeBuilder hcb = new HashCodeBuilder(113, 127);
        hcb.append(super.hashCode()).append(value);

        return hcb.toHashCode();
    }

    @Override
    public int compareTo(Numeric o) {
        long left = this.value.longValue();
        long right = o.value.longValue();
        if (left != right) {
            return left < right ? -1 : 1;
        }
        int cmp = Double.compare(this.value.doubleValue(), o.value.doubleValue());
        if (cmp == 0) {
            cmp = compareMetadata(o);
        }
        return cmp;
    }

    @Override
    public Collection<ValueTuple> visit(Collection<String> fieldNames, DatawaveJexlContext context) {
        validate();
        return FunctionalSet.singleton(new ValueTuple(fieldNames, this.value, normalizedValue, this));
    }

    @Override
    public void write(Kryo kryo, Output output) {
        writeMetadata(kryo, output);
        output.writeString(this.normalizedValue);
        output.writeBoolean(this.toKeep);
    }

    @Override
    public void read(Kryo kryo, Input input) {
        readMetadata(kryo, input);
        String stringValue = input.readString();
        setValue(stringValue);
        setNormalizedValue(stringValue);
        this.toKeep = input.readBoolean();
        validate();
    }

    /*
     * (non-Javadoc)
     *
     * @see Attribute#deepCopy()
     */
    @Override
    public Numeric copy() {
        return new Numeric((Number) this.getData(), this.getMetadata(), this.isToKeep());
    }

}
