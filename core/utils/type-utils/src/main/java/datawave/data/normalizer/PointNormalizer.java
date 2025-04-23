package datawave.data.normalizer;

import org.locationtech.geowave.core.geotime.index.dimension.LatitudeDefinition;
import org.locationtech.geowave.core.geotime.index.dimension.LongitudeDefinition;
import org.locationtech.geowave.core.index.NumericIndexStrategy;
import org.locationtech.geowave.core.index.dimension.NumericDimensionDefinition;
import org.locationtech.geowave.core.index.sfc.SFCFactory;
import org.locationtech.geowave.core.index.sfc.tiered.TieredSFCIndexFactory;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.index.CustomNameIndex;

import datawave.data.type.util.Point;

/**
 * A normalizer that, given a parseable geometry string representing a point geometry will perform GeoWave indexing with a single-tier spatial geowave index
 * configuration
 */
public class PointNormalizer extends AbstractGeometryNormalizer<Point,org.locationtech.jts.geom.Point> {
    private static final long serialVersionUID = 171360806347433135L;
    
    // NOTE: If we change the index strategy, then we will need to update the validHash method appropriately.
    // @formatter:off
    public static final ThreadLocal<NumericIndexStrategy> indexStrategy = ThreadLocal.withInitial(PointNormalizer::createIndexStrategy);
    // @formatter:on
    
    protected static NumericIndexStrategy createIndexStrategy() {
        // @formatter:off
        return TieredSFCIndexFactory.createSingleTierStrategy(
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
    }
    
    public static final ThreadLocal<Index> index = ThreadLocal.withInitial(() -> new CustomNameIndex(indexStrategy.get(), null, "pointIndex"));
    
    public NumericIndexStrategy getIndexStrategy() {
        // NOTE: If we change the index strategy, then we will need to update the validHash method appropriately.
        return PointNormalizer.indexStrategy.get();
    }
    
    public static NumericIndexStrategy getPointIndexStrategy() {
        return PointNormalizer.indexStrategy.get();
    }
    
    public Index getIndex() {
        return index.get();
    }
    
    public static Index getPointIndex() {
        return index.get();
    }
    
    protected Point createDatawaveGeometry(org.locationtech.jts.geom.Point geometry) {
        return new Point(geometry);
    }
    
    @Override
    public boolean validTier(short tier) {
        return tier == 0x1f;
    }
    
}
