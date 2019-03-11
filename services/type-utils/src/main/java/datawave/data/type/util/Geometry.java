package datawave.data.type.util;

import java.io.Serializable;

public class Geometry implements Comparable<Geometry>, Serializable {
    private static final long serialVersionUID = -6817113902935360827L;
    private final com.vividsolutions.jts.geom.Geometry jtsGeom;
    
    public Geometry(com.vividsolutions.jts.geom.Geometry jtsGeom) {
        this.jtsGeom = jtsGeom;
    }
    
    public com.vividsolutions.jts.geom.Geometry getJTSGeometry() {
        return jtsGeom;
    }
    
    @Override
    public int compareTo(Geometry o) {
        return jtsGeom.compareTo(o.jtsGeom);
    }
    
    public String toString() {
        return this.jtsGeom.toText();
    }
    
}
