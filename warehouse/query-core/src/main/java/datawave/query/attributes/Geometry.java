package datawave.query.attributes;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;
import datawave.data.normalizer.AbstractGeometryNormalizer;
import datawave.data.normalizer.GeometryNormalizer;
import datawave.data.normalizer.Normalizer;
import datawave.query.jexl.DatawaveJexlContext;
import datawave.query.collections.FunctionalSet;
import datawave.webservice.query.data.ObjectSizeOf;
import org.apache.accumulo.core.data.Key;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;

public class Geometry extends Attribute<Geometry> implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private com.vividsolutions.jts.geom.Geometry geometry;
    
    protected Geometry() {
        super(null, true);
    }
    
    public Geometry(String geoString, Key docKey, boolean toKeep) {
        super(docKey, toKeep);
        setGeometryFromGeoString(geoString);
        validate();
    }
    
    public Geometry(com.vividsolutions.jts.geom.Geometry geometry, Key docKey, boolean toKeep) {
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
        write(out, false);
    }
    
    @Override
    public void write(DataOutput out, boolean reducedResponse) throws IOException {
        writeMetadata(out, reducedResponse);
        
        WritableUtils.writeCompressedByteArray(out, write());
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
        validate();
    }
    
    @Override
    public int compareTo(Geometry other) {
        int cmp;
        if (geometry == null) {
            if (other == null || other.geometry == null) {
                cmp = 0;
            }
            cmp = -1;
        }
        cmp = geometry.compareTo(other.geometry);
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
            return FunctionalSet.singleton(new ValueTuple(fieldNames, getData(), Normalizer.GEOMETRY_NORMALIZER
                            .normalizeDelegateType(new datawave.data.type.util.Geometry(geometry)), this));
        } catch (IllegalArgumentException ne) {
            throw new IllegalArgumentException("Cannot normalize the geometry");
        }
    }
    
    @Override
    public void write(Kryo kryo, Output output) {
        write(kryo, output, false);
    }
    
    @Override
    public void write(Kryo kryo, Output output, Boolean reducedResponse) {
        writeMetadata(kryo, output, reducedResponse);
        byte[] wellKnownBinary = write();
        output.write(wellKnownBinary.length);
        output.write(wellKnownBinary);
    }
    
    @Override
    public void read(Kryo kryo, Input input) {
        readMetadata(kryo, input);
        int wkbLength = input.read();
        byte[] wellKnownBinary = new byte[wkbLength];
        input.read(wellKnownBinary);
        try {
            geometry = new WKBReader().read(wellKnownBinary);
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
