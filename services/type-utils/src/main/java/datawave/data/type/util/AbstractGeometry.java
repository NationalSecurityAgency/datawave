package datawave.data.type.util;

import java.io.Serializable;

/**
 * The base GeoWave geometry delegate object, which wraps the underlying JTS geometry
 *
 * @param <T>
 *            The underlying JTS Geometry
 */
public abstract class AbstractGeometry<T extends com.vividsolutions.jts.geom.Geometry> implements Serializable {
    protected final T jtsGeom;
    
    public AbstractGeometry(T jtsGeom) {
        this.jtsGeom = jtsGeom;
    }
    
    public T getJTSGeometry() {
        return jtsGeom;
    }
    
    public String toString() {
        return jtsGeom.toText();
    }
}
