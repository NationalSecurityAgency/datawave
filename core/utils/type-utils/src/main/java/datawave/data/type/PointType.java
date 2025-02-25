package datawave.data.type;

import datawave.data.normalizer.Normalizer;
import datawave.data.type.util.Point;

/**
 * Provides support for point geometry types. Other geometry types are not compatible with this type.
 */
public class PointType extends AbstractGeometryType<Point> {
    
    public PointType() {
        super(Normalizer.POINT_NORMALIZER);
    }
}
