package datawave.data.normalizer;

import java.util.List;

import org.apache.accumulo.core.util.Pair;
import org.locationtech.geowave.core.geotime.index.dimension.LatitudeDefinition;
import org.locationtech.geowave.core.geotime.index.dimension.LongitudeDefinition;
import org.locationtech.geowave.core.index.NumericIndexStrategy;
import org.locationtech.geowave.core.index.dimension.NumericDimensionDefinition;
import org.locationtech.geowave.core.index.sfc.SFCFactory;
import org.locationtech.geowave.core.index.sfc.tiered.TieredSFCIndexFactory;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.index.CustomNameIndex;

import com.google.common.collect.Lists;

import datawave.data.type.Type;
import datawave.data.type.util.Geometry;

/**
 * A normalizer that, given a parseable geometry string representing an arbitrary geometry, will perform GeoWave indexing with a multi-tiered spatial geowave
 * index configuration
 */
public class GeometryNormalizer extends AbstractGeometryNormalizer<Geometry,org.locationtech.jts.geom.Geometry> implements OneToManyNormalizer<Geometry> {
    private static final long serialVersionUID = 171360806347433135L;
    
    // NOTE: If we change the index strategy, then we will need to update the validHash method appropriately.
    // @formatter:off
    public static final ThreadLocal<NumericIndexStrategy> indexStrategy = ThreadLocal.withInitial(GeometryNormalizer::createIndexStrategy);
    // @formatter:on
    
    public static final ThreadLocal<Index> index = ThreadLocal.withInitial(() -> new CustomNameIndex(indexStrategy.get(), null, "geometryIndex"));
    
    protected static NumericIndexStrategy createIndexStrategy() {
        // @formatter:off
        return TieredSFCIndexFactory.createFullIncrementalTieredStrategy(
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
    
    public NumericIndexStrategy getIndexStrategy() {
        // NOTE: If we change the index strategy, then we will need to update the validHash method appropriately.
        return GeometryNormalizer.indexStrategy.get();
    }
    
    public static NumericIndexStrategy getGeometryIndexStrategy() {
        return GeometryNormalizer.indexStrategy.get();
    }
    
    public Index getIndex() {
        return index.get();
    }
    
    public static Index getGeometryIndex() {
        return index.get();
    }
    
    @Override
    public List<Pair<String,Type.Category>> normalizeToMany(String geoString) throws IllegalArgumentException {
        List<Pair<String,Type.Category>> list = Lists.newArrayList();
        if (validHash(geoString)) {
            for (String s : Lists.newArrayList(geoString)) {
                list.add(new Pair<>(s, Type.Category.GEOHASH));
            }
        }
        return normalizeDelegateTypeToMany(createDatawaveGeometry(parseGeometry(geoString)));
    }
    
    @Override
    public List<Pair<String,Type.Category>> normalizeDelegateTypeToMany(Geometry geometry) {
        List<Pair<String,Type.Category>> list = Lists.newArrayList();
        for (byte[] one : getIndicesFromGeometry(geometry)) {
            list.add(new Pair<>(getEncodedStringFromIndexBytes(one), Type.Category.GEOHASH));
        }
        return list;
    }
    
    protected datawave.data.type.util.Geometry createDatawaveGeometry(org.locationtech.jts.geom.Geometry geometry) {
        return new datawave.data.type.util.Geometry(geometry);
    }
    
}
