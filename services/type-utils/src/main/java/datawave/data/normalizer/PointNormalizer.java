package datawave.data.normalizer;

import datawave.data.type.util.Point;
import mil.nga.giat.geowave.core.geotime.index.dimension.LatitudeDefinition;
import mil.nga.giat.geowave.core.geotime.index.dimension.LongitudeDefinition;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.SFCFactory;
import mil.nga.giat.geowave.core.index.sfc.tiered.TieredSFCIndexFactory;

/**
 * A normalizer that, given a parseable geometry string representing a point geometry will perform GeoWave indexing with a single-tier spatial geowave index
 * configuration
 *
 */
public class PointNormalizer extends AbstractGeometryNormalizer<Point,com.vividsolutions.jts.geom.Point> {
    private static final long serialVersionUID = 171360806347433135L;
    
    // @formatter:off
    public static NumericIndexStrategy indexStrategy = TieredSFCIndexFactory.createSingleTierStrategy(
            new NumericDimensionDefinition[]{
                    new LongitudeDefinition(),
                    new LatitudeDefinition(
                            true)
                    // just use the same range for latitude to make square sfc values in
                    // decimal degrees (EPSG:4326)
            },
            new int[]{
                    LONGITUDE_BITS,
                    LATITUDE_BITS
            },
            SFCFactory.SFCType.HILBERT);
    // @formatter:on
    
    protected NumericIndexStrategy getIndexStrategy() {
        return PointNormalizer.indexStrategy;
    }
    
    protected Point createDatawaveGeometry(com.vividsolutions.jts.geom.Point geometry) {
        return new Point(geometry);
    }
}
