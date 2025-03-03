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

import datawave.data.normalizer.GeoNormalizer;
import datawave.query.collections.FunctionalSet;
import datawave.query.jexl.DatawaveJexlContext;

public class GeoPoint extends Attribute<GeoPoint> implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final GeoNormalizer normalizer = new GeoNormalizer();

    private String point;

    protected GeoPoint() {
        super(null, true);
    }

    public GeoPoint(String point, Key docKey, boolean toKeep) {
        super(docKey, toKeep);
        this.point = point;
        validate();
    }

    @Override
    public long sizeInBytes() {
        return sizeInBytes(point) + super.sizeInBytes(4);
        // 4 for string reference
    }

    protected void validate() {
        try {
            normalizer.normalize(this.point);
        } catch (IllegalArgumentException ne) {
            throw new IllegalArgumentException("Cannot parse the geo value " + this.point, ne);
        }
    }

    public String getGeoPoint() {
        return this.point;
    }

    @Override
    public Object getData() {
        return getGeoPoint();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        writeMetadata(out);
        WritableUtils.writeString(out, this.point);
        WritableUtils.writeVInt(out, toKeep ? 1 : 0);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        readMetadata(in);
        this.point = WritableUtils.readString(in);
        this.toKeep = WritableUtils.readVInt(in) != 0;
        validate();
    }

    @Override
    public int compareTo(GeoPoint o) {
        String s1 = this.getGeoPoint();
        String s2 = o.getGeoPoint();

        try {
            s1 = normalizer.normalize(s1);
        } catch (IllegalArgumentException ne) {
            throw new IllegalArgumentException("Cannot parse the geo value " + s1, ne);
        }
        try {
            s2 = normalizer.normalize(s2);
        } catch (IllegalArgumentException ne) {
            throw new IllegalArgumentException("Cannot parse the geo value " + s2, ne);
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

        if (o instanceof GeoPoint) {
            return 0 == this.compareTo((GeoPoint) o);
        }

        return false;
    }

    @Override
    public int hashCode() {
        HashCodeBuilder hcb = new HashCodeBuilder(163, 157);
        hcb.append(super.hashCode()).append(this.getGeoPoint());

        return hcb.toHashCode();
    }

    @Override
    public Collection<ValueTuple> visit(Collection<String> fieldNames, DatawaveJexlContext context) {
        try {
            return FunctionalSet.singleton(new ValueTuple(fieldNames, this.point, normalizer.normalize(this.point), this));
        } catch (IllegalArgumentException ne) {
            throw new IllegalArgumentException("Cannot parse the geo value " + this.point, ne);
        }
    }

    @Override
    public void write(Kryo kryo, Output output) {
        writeMetadata(kryo, output);
        output.writeString(this.point);
        output.writeBoolean(this.toKeep);
    }

    @Override
    public void read(Kryo kryo, Input input) {
        readMetadata(kryo, input);
        this.point = input.readString();
        this.toKeep = input.readBoolean();
        validate();
    }

    /*
     * (non-Javadoc)
     *
     * @see Attribute#deepCopy()
     */
    @Override
    public GeoPoint copy() {
        return new GeoPoint(this.getGeoPoint(), this.getMetadata(), this.isToKeep());
    }

}
