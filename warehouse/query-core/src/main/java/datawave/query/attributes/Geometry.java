package datawave.query.attributes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;

import org.apache.accumulo.core.data.Key;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.io.WritableUtils;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import datawave.data.normalizer.AbstractGeometryNormalizer;
import datawave.data.normalizer.Normalizer;
import datawave.query.collections.FunctionalSet;
import datawave.query.jexl.DatawaveJexlContext;
import datawave.webservice.query.data.ObjectSizeOf;

public class Geometry extends Attribute<Geometry> implements Serializable {
    private static final long serialVersionUID = 1L;

    private org.locationtech.jts.geom.Geometry geometry;

    protected Geometry() {
        super(null, true);
    }

    public Geometry(String geoString, Key docKey, boolean toKeep) {
        super(docKey, toKeep);
        setGeometryFromGeoString(geoString);
        validate();
    }

    public Geometry(org.locationtech.jts.geom.Geometry geometry, Key docKey, boolean toKeep) {
        super(docKey, toKeep);
        this.geometry = geometry;
        validate();
    }

    @Override
    public long sizeInBytes() {
        return ObjectSizeOf.Sizer.getObjectSize(geometry) + super.sizeInBytes(4);
        // 4 for geometry reference
    }

    private byte[] write() {
        if (geometry != null) {
            return new WKBWriter().write(geometry);
        }
        return new byte[] {};
    }

    protected void validate() {
        if (geometry == null) {
            throw new IllegalArgumentException("Cannot parse the geometry value ");
        }
    }

    public void setGeometryFromGeoString(String geoString) {
        geometry = AbstractGeometryNormalizer.parseGeometry(geoString);
    }

    @Override
    public Object getData() {
        return geometry;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        writeMetadata(out);
        WritableUtils.writeCompressedByteArray(out, write());
        WritableUtils.writeVInt(out, toKeep ? 1 : 0);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        readMetadata(in);

        byte[] wellKnownBinary = WritableUtils.readCompressedByteArray(in);
        try {
            geometry = new WKBReader().read(wellKnownBinary);
        } catch (ParseException e) {
            throw new IllegalArgumentException("Cannot parse the geometry", e);
        }
        this.toKeep = WritableUtils.readVInt(in) != 0;
        validate();
    }

    @Override
    public int compareTo(Geometry other) {
        int cmp;
        if (geometry == null) {
            if (other == null || other.geometry == null) {
                cmp = 0;
            } else {
                cmp = -1;
            }
        } else {
            cmp = geometry.compareTo(other.geometry);
        }
        if (0 == cmp) {
            return compareMetadata(other);
        }

        return cmp;
    }

    @Override
    public boolean equals(Object o) {
        if (null == o) {
            return false;
        }

        if (o instanceof Geometry) {
            return 0 == this.compareTo((Geometry) o);
        }

        return false;
    }

    @Override
    public int hashCode() {
        HashCodeBuilder hcb = new HashCodeBuilder(163, 157);
        hcb.append(super.hashCode()).append(geometry);

        return hcb.toHashCode();
    }

    @Override
    public Collection<ValueTuple> visit(Collection<String> fieldNames, DatawaveJexlContext context) {
        if (geometry == null) {
            return Collections.emptySet();
        }
        try {
            return FunctionalSet.singleton(new ValueTuple(fieldNames, getData(),
                            Normalizer.GEOMETRY_NORMALIZER.normalizeDelegateType(new datawave.data.type.util.Geometry(geometry)), this));
        } catch (IllegalArgumentException ne) {
            throw new IllegalArgumentException("Cannot normalize the geometry");
        }
    }

    @Override
    public void write(Kryo kryo, Output output) {
        writeMetadata(kryo, output);
        output.writeBoolean(this.toKeep);
        byte[] wellKnownBinary = write();
        output.writeInt(wellKnownBinary.length);
        output.writeBytes(wellKnownBinary);
    }

    @Override
    public void read(Kryo kryo, Input input) {
        readMetadata(kryo, input);
        this.toKeep = input.readBoolean();
        int wkbLength = input.readInt();
        byte[] wellKnownBinary = new byte[wkbLength];
        input.readBytes(wellKnownBinary);
        try {
            geometry = new WKBReader().read(wellKnownBinary);
            validate();
        } catch (ParseException e) {
            throw new IllegalArgumentException("Cannot parse the geometry", e);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.query.attributes.Attribute#deepCopy()
     */
    @Override
    public Geometry copy() {
        return new Geometry(geometry, this.getMetadata(), this.isToKeep());
    }

}
