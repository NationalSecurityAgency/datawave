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

import datawave.data.normalizer.GeoLatNormalizer;
import datawave.query.collections.FunctionalSet;
import datawave.query.jexl.DatawaveJexlContext;

public class Latitude extends Attribute<Latitude> implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final GeoLatNormalizer normalizer = new GeoLatNormalizer();

    private String latitude;

    protected Latitude() {
        super(null, true);
    }

    public Latitude(String latitude, Key docKey, boolean toKeep) {
        super(docKey, toKeep);

        this.latitude = latitude;
        validate();
    }

    @Override
    public long sizeInBytes() {
        return sizeInBytes(latitude) + super.sizeInBytes(4);
        // 4 for string reference
    }

    protected void validate() {
        try {
            normalizer.normalize(this.latitude);
        } catch (IllegalArgumentException ne) {
            throw new IllegalArgumentException("Cannot parse the latitude value " + this.latitude, ne);
        }
    }

    public String getLatitude() {
        return this.latitude;
    }

    @Override
    public Object getData() {
        return getLatitude();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        writeMetadata(out);
        WritableUtils.writeString(out, this.latitude);
        WritableUtils.writeVInt(out, toKeep ? 1 : 0);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        readMetadata(in);
        this.latitude = WritableUtils.readString(in);
        this.toKeep = WritableUtils.readVInt(in) != 0;
        validate();
    }

    @Override
    public int compareTo(Latitude o) {
        String s1 = this.latitude;
        String s2 = o.getLatitude();
        try {
            s1 = normalizer.normalize(s1);
        } catch (IllegalArgumentException ne) {
            throw new IllegalArgumentException("Cannot parse the latitude value " + s1, ne);
        }
        try {
            s2 = normalizer.normalize(s2);
        } catch (IllegalArgumentException ne) {
            throw new IllegalArgumentException("Cannot parse the latitude value " + s2, ne);
        }

        int cmp = s1.compareTo(s2);
        if (0 == cmp) {
            return compareMetadata(o);
        }

        return cmp;
    }

    @Override
    public boolean equals(Object o) {
        if (null == o) {
            return false;
        }

        if (o instanceof Latitude) {
            return 0 == this.compareTo((Latitude) o);
        }

        return false;
    }

    @Override
    public int hashCode() {
        HashCodeBuilder hcb = new HashCodeBuilder(151, 149);
        hcb.append(super.hashCode()).append(this.getLatitude());

        return hcb.toHashCode();
    }

    @Override
    public Collection<ValueTuple> visit(Collection<String> fieldNames, DatawaveJexlContext context) {
        try {
            return FunctionalSet.singleton(new ValueTuple(fieldNames, this.latitude, normalizer.normalize(this.latitude), this));
        } catch (IllegalArgumentException ne) {
            throw new IllegalArgumentException("Cannot parse the latitude value " + this.latitude, ne);
        }
    }

    @Override
    public void write(Kryo kryo, Output output) {
        writeMetadata(kryo, output);
        output.writeString(this.latitude);
        output.writeBoolean(this.toKeep);
    }

    @Override
    public void read(Kryo kryo, Input input) {
        readMetadata(kryo, input);
        this.latitude = input.readString();
        this.toKeep = input.readBoolean();
        validate();
    }

    /*
     * (non-Javadoc)
     *
     * @see Attribute#deepCopy()
     */
    @Override
    public Latitude copy() {
        return new Latitude(this.getLatitude(), this.getMetadata(), this.isToKeep());
    }

}
