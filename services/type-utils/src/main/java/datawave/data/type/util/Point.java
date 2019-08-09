package datawave.data.type.util;

import java.io.Serializable;

/**
 * This class operates as a delegate for JTS Point instances.
 */
public class Point extends AbstractGeometry<com.vividsolutions.jts.geom.Point> implements Comparable<Point>, Serializable {
    public Point(com.vividsolutions.jts.geom.Point jtsGeom) {
        super(jtsGeom);
    }
    
    @Override
    public int compareTo(Point o) {
        return jtsGeom.compareTo(o.jtsGeom);
    }
}
