package datawave.data.type.util;

import java.io.Serializable;

/**
 * This class operates as a delegate for any JTS Geometry instance.
 */
public class Geometry extends AbstractGeometry<org.locationtech.jts.geom.Geometry> implements Comparable<Geometry>, Serializable {
    public Geometry(org.locationtech.jts.geom.Geometry jtsGeom) {
        super(jtsGeom);
    }
    
    @Override
    public int compareTo(Geometry o) {
        return jtsGeom.compareTo(o.jtsGeom);
    }
}
