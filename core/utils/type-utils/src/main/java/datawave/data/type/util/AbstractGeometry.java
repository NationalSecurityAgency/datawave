package datawave.data.type.util;

import java.io.Serializable;
import java.util.Objects;

/**
 * The base GeoWave geometry delegate object, which wraps the underlying JTS geometry
 *
 * @param <T>
 *            The underlying JTS Geometry
 */
public abstract class AbstractGeometry<T extends org.locationtech.jts.geom.Geometry> implements Serializable {
    protected final T jtsGeom;
    
    public AbstractGeometry(T jtsGeom) {
        this.jtsGeom = jtsGeom;
    }
    
    public T getJTSGeometry() {
        return jtsGeom;
    }
    
    @Override
    public String toString() {
        return jtsGeom.toText();
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(jtsGeom);
    }
    
    @Override
    public boolean equals(Object o) {
        if (o instanceof Geometry) {
            Geometry other = (Geometry) o;
            return Objects.equals(jtsGeom, other.jtsGeom);
        }
        return false;
    }
}
