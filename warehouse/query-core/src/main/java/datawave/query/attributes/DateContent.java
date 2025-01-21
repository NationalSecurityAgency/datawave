package datawave.query.attributes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;

import org.apache.accumulo.core.data.Key;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.io.WritableUtils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import datawave.data.normalizer.DateNormalizer;
import datawave.query.collections.FunctionalSet;
import datawave.query.jexl.DatawaveJexlContext;
import datawave.webservice.query.data.ObjectSizeOf;

public class DateContent extends Attribute<DateContent> implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final DateNormalizer normalizer = new DateNormalizer();

    private Calendar value;
    private String normalizedValue;

    protected DateContent() {
        super(null, true);
    }

    public DateContent(String dateContent, Key docKey, boolean toKeep) {
        super(docKey, toKeep);
        setValue(dateContent);
        setNormalizedValue(dateContent);
        validate();
    }

    public DateContent(Date dateContent, Key docKey, boolean toKeep) {
        super(docKey, toKeep);
        setValue(dateContent);
        setNormalizedValue(dateContent);
        validate();
    }

    @Override
    public long sizeInBytes() {
        return super.sizeInBytes(8) + sizeInBytes(normalizedValue) + ObjectSizeOf.Sizer.getObjectSize(value);
        // 8 for 2 object references
    }

    protected void validate() {
        if (value == null || normalizedValue == null) {
            throw new IllegalArgumentException("Date values are null");
        }
    }

    private void setValue(String value) {
        try {
            Calendar cal = Calendar.getInstance();
            cal.setTime(normalizer.denormalize(value));
            this.value = cal;
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Cannot parse the date value " + this.value, e);
        }
    }

    private Calendar toCalendar(Date date) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        return cal;
    }

    private void setValue(Date value) {
        this.value = toCalendar(value);
    }

    private void setNormalizedValue(String value) {
        try {
            this.normalizedValue = normalizer.normalize(value);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Cannot parse the date value " + this.value, e);
        }
    }

    private void setNormalizedValue(Date value) {
        try {
            this.normalizedValue = normalizer.normalize(value.toString());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Cannot parse the date value " + this.value, e);
        }
    }

    @Override
    public Object getData() {
        return this.value;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        writeMetadata(out);
        WritableUtils.writeString(out, normalizer.parseToString(this.value.getTime()));
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        readMetadata(in);
        String readString = WritableUtils.readString(in);
        setValue(readString);
        setNormalizedValue(readString);
        validate();
    }

    @Override
    public int compareTo(DateContent o) {
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

        if (o instanceof DateContent) {
            return 0 == this.compareTo((DateContent) o);
        }

        return false;
    }

    @Override
    public int hashCode() {
        HashCodeBuilder hcb = new HashCodeBuilder(2099, 2129);
        hcb.append(value).append(super.hashCode());

        return hcb.toHashCode();
    }

    @Override
    public Collection<ValueTuple> visit(Collection<String> fieldNames, DatawaveJexlContext context) {
        return FunctionalSet.singleton(new ValueTuple(fieldNames, this.value, normalizedValue, this));
    }

    @Override
    public void write(Kryo kryo, Output output) {
        writeMetadata(kryo, output);
        output.writeString(normalizer.parseToString(this.value.getTime()));
    }

    @Override
    public void read(Kryo kryo, Input input) {
        readMetadata(kryo, input);
        String stringValue = input.readString();
        setValue(stringValue);
        setNormalizedValue(stringValue);
        validate();
    }

    /*
     * (non-Javadoc)
     *
     * @see Attribute#deepCopy()
     */
    @Override
    public DateContent copy() {
        return new DateContent(this.value.getTime(), this.getMetadata(), this.isToKeep());
    }
}
