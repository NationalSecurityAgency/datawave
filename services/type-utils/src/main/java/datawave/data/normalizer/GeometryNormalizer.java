package datawave.data.normalizer;

import com.google.common.collect.Lists;
import datawave.data.type.util.Geometry;
import mil.nga.giat.geowave.core.geotime.index.dimension.LatitudeDefinition;
import mil.nga.giat.geowave.core.geotime.index.dimension.LongitudeDefinition;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.SFCFactory;
import mil.nga.giat.geowave.core.index.sfc.tiered.TieredSFCIndexFactory;

import java.util.List;

/**
 * A normalizer that, given a parseable geometry string representing an arbitrary geometry, will perform GeoWave indexing with a multi-tiered spatial geowave
 * index configuration
 *
 */
public class GeometryNormalizer extends AbstractGeometryNormalizer<Geometry,com.vividsolutions.jts.geom.Geometry> implements OneToManyNormalizer<Geometry> {
    private static final long serialVersionUID = 171360806347433135L;
    
    // @formatter:off
    public static NumericIndexStrategy indexStrategy = TieredSFCIndexFactory.createFullIncrementalTieredStrategy(
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
        return GeometryNormalizer.indexStrategy;
    }
    
    @Override
    public List<String> normalizeToMany(String geoString) throws IllegalArgumentException {
        return normalizeDelegateTypeToMany(createDatawaveGeometry(parseGeometry(geoString)));
    }
    
    @Override
    public List<String> normalizeDelegateTypeToMany(Geometry geometry) {
        List<String> list = Lists.newArrayList();
        for (ByteArrayId one : getIndicesFromGeometry(geometry)) {
            list.add(getEncodedStringFromIndexBytes(one));
        }
        return list;
    }
    
    protected datawave.data.type.util.Geometry createDatawaveGeometry(com.vividsolutions.jts.geom.Geometry geometry) {
        return new datawave.data.type.util.Geometry(geometry);
    }
}
