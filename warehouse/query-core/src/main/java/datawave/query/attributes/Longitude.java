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

import datawave.data.normalizer.GeoLonNormalizer;
import datawave.query.collections.FunctionalSet;
import datawave.query.jexl.DatawaveJexlContext;

public class Longitude extends Attribute<Longitude> implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final GeoLonNormalizer normalizer = new GeoLonNormalizer();

    private String longitude;

    protected Longitude() {
        super(null, true);
    }

    public Longitude(String longitude, Key docKey, boolean toKeep) {
        super(docKey, toKeep);

        this.longitude = longitude;
        validate();
    }

    @Override
    public long sizeInBytes() {
        return sizeInBytes(longitude) + super.sizeInBytes(4);
        // 4 for string reference
    }

    protected void validate() {
        try {
            normalizer.normalize(this.longitude);
        } catch (IllegalArgumentException ne) {
            throw new IllegalArgumentException("Cannot parse the longitude value " + this.longitude, ne);
        }
    }

    public String getLongitude() {
        return this.longitude;
    }

    @Override
    public Object getData() {
        return getLongitude();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        writeMetadata(out);
        WritableUtils.writeString(out, this.longitude);
        WritableUtils.writeVInt(out, toKeep ? 1 : 0);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        readMetadata(in);
        this.longitude = WritableUtils.readString(in);
        this.toKeep = WritableUtils.readVInt(in) != 0;
        validate();
    }

    @Override
    public int compareTo(Longitude o) {
        String s1 = this.longitude;
        String s2 = o.getLongitude();
        try {
            s1 = normalizer.normalize(s1);
        } catch (IllegalArgumentException ne) {
            throw new IllegalArgumentException("Cannot parse the longitude value " + s1, ne);
        }
        try {
            s2 = normalizer.normalize(s2);
        } catch (IllegalArgumentException ne) {
            throw new IllegalArgumentException("Cannot parse the longitude value " + s2, ne);
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

        if (o instanceof Longitude) {
            return 0 == this.compareTo((Longitude) o);
        }

        return false;
    }

    @Override
    public int hashCode() {
        HashCodeBuilder hcb = new HashCodeBuilder(139, 149);
        hcb.append(super.hashCode()).append(this.getLongitude());

        return hcb.toHashCode();
    }

    @Override
    public Collection<ValueTuple> visit(Collection<String> fieldNames, DatawaveJexlContext context) {
        try {
            return FunctionalSet.singleton(new ValueTuple(fieldNames, this.longitude, normalizer.normalize(this.longitude), this));
        } catch (IllegalArgumentException ne) {
            throw new IllegalArgumentException("Cannot parse the longitude value " + this.longitude, ne);
        }
    }

    @Override
    public void write(Kryo kryo, Output output) {
        writeMetadata(kryo, output);
        output.writeString(this.longitude);
        output.writeBoolean(this.toKeep);
    }

    @Override
    public void read(Kryo kryo, Input input) {
        readMetadata(kryo, input);
        this.longitude = input.readString();
        this.toKeep = input.readBoolean();
        validate();
    }

    /*
     * (non-Javadoc)
     *
     * @see Attribute#deepCopy()
     */
    @Override
    public Longitude copy() {
        return new Longitude(this.getLongitude(), this.getMetadata(), this.isToKeep());
    }

}
