package datawave.data.parser;

import org.locationtech.jts.geom.Geometry;

public abstract class GeometryParser implements Comparable<GeometryParser> {
    
    public static final int DEFAULT_PRIORITY = 0;
    
    public abstract Geometry parseGeometry(String geoString);
    
    // Used for sorting
    // Smaller numbers have higher priority
    protected abstract int getPriority();
    
    @Override
    public int compareTo(GeometryParser other) {
        int compare = this.getPriority() - other.getPriority();
        if (compare == 0)
            compare = this.getClass().getName().compareTo(other.getClass().getName());
        return compare;
    }
}
