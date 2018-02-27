package datawave.data.parser;

import com.vividsolutions.jts.geom.Geometry;

public abstract class GeometryParser implements Comparable<GeometryParser> {
    
    public static final int DEFAULT_PRIORITY = 0;
    
    public abstract Geometry parseGeometry(String geoString);
    
    // Used for sorting
    // Smaller numbers have higher priority
    protected abstract int getPriority();
    
    @Override
    public int compareTo(GeometryParser other) {
        return this.getPriority() - other.getPriority();
    }
}
