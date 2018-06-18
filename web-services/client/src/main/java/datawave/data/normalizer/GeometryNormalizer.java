package datawave.data.normalizer;

import com.google.common.collect.Lists;
import com.vividsolutions.jts.geom.Geometry;
import datawave.data.parser.GeometryParser;
import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.geotime.index.dimension.LatitudeDefinition;
import mil.nga.giat.geowave.core.geotime.index.dimension.LongitudeDefinition;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.SFCFactory;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.index.sfc.tiered.TieredSFCIndexFactory;
import org.apache.commons.codec.binary.Hex;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.ServiceLoader;
import java.util.TreeSet;

/**
 * A normalizer that, given a parseable geometry string representing an arbitrary geometry, will perform GeoWave indexing with a defauls spatial geowave index
 * configuration
 *
 */
public class GeometryNormalizer implements Normalizer<datawave.data.type.util.Geometry>, OneToManyNormalizer<datawave.data.type.util.Geometry> {
    private static final long serialVersionUID = 171360806347433135L;
    
    private static final int LONGITUDE_BITS = 31;
    private static final int LATITUDE_BITS = 31;
    
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
    
    private static TreeSet<GeometryParser> geoParsers = new TreeSet();
    
    static {
        ServiceLoader<GeometryParser> geoParserLoader = ServiceLoader.load(GeometryParser.class, GeometryNormalizer.class.getClassLoader());
        for (GeometryParser geoParser : geoParserLoader)
            geoParsers.add(geoParser);
    }
    
    /**
     * Expects to receive a parseable geometry string. The default geometry parser accepts Open Geospatial Consortium compliant Well-Known test strings An
     * example for points is of the form:
     *
     * POINT ([number][space][number])
     */
    @Override
    public String normalize(String geoString) throws IllegalArgumentException {
        return normalizeDelegateType(new datawave.data.type.util.Geometry(parseGeometry(geoString)));
    }
    
    @Override
    public List<String> normalizeToMany(String geoString) throws IllegalArgumentException {
        return normalizeDelegateTypeToMany(new datawave.data.type.util.Geometry(parseGeometry(geoString)));
    }
    
    /**
     * We cannot support regex against numbers
     */
    @Override
    public String normalizeRegex(String fieldRegex) throws IllegalArgumentException {
        throw new IllegalArgumentException("Cannot normalize a regex against a geometry field");
    }
    
    @Override
    public List<String> normalizeDelegateTypeToMany(datawave.data.type.util.Geometry geometry) {
        List<String> list = Lists.newArrayList();
        for (ByteArrayId one : getIndicesFromGeometry((geometry.getJTSGeometry()))) {
            list.add(getEncodedStringFromIndexBytes(one));
        }
        return list;
    }
    
    public String normalizeDelegateType(datawave.data.type.util.Geometry geometry) {
        return getEncodedStringFromIndexBytes(getSingleIndexFromGeometry((geometry.getJTSGeometry())));
    }
    
    public static List<String> getEncodedStringsFromGeometry(Geometry geometry) {
        List<ByteArrayId> indices = getIndicesFromGeometry(geometry);
        List<String> retVal = new ArrayList<String>(indices.size());
        for (ByteArrayId index : indices) {
            retVal.add(getEncodedStringFromIndexBytes(index));
        }
        return retVal;
    }
    
    public static String getEncodedStringFromIndexBytes(ByteArrayId index) {
        return Hex.encodeHexString(index.getBytes());
    }
    
    public static Geometry parseGeometry(String geoString) throws IllegalArgumentException {
        for (GeometryParser geoParser : geoParsers) {
            Geometry geom = geoParser.parseGeometry(geoString);
            if (geom != null)
                return geom;
        }
        throw new IllegalArgumentException("Cannot parse geometry from string [" + geoString + "]");
    }
    
    private static ByteArrayId getSingleIndexFromGeometry(Geometry geometry) {
        final List<ByteArrayId> insertionIds = new ArrayList<ByteArrayId>();
        for (MultiDimensionalNumericData range : GeometryUtils.basicConstraintsFromGeometry(geometry).getIndexConstraints(indexStrategy)) {
            insertionIds.addAll(indexStrategy.getInsertionIds(range, 1));
        }
        if (insertionIds.size() == 1) {
            return insertionIds.get(0);
        }
        // this should never occur
        throw new IllegalArgumentException("Cannot normalize input geometry, no resulting indices");
    }
    
    private static List<ByteArrayId> getIndicesFromGeometry(Geometry geometry) {
        final List<ByteArrayId> insertionIds = new ArrayList<ByteArrayId>();
        for (MultiDimensionalNumericData range : GeometryUtils.basicConstraintsFromGeometry(geometry).getIndexConstraints(indexStrategy)) {
            insertionIds.addAll(indexStrategy.getInsertionIds(range));
        }
        return insertionIds;
    }
    
    @Override
    public datawave.data.type.util.Geometry denormalize(String geoString) {
        // this is assuming the input string is not actually normalized
        // (which oddly is the case with other normalizers)
        return new datawave.data.type.util.Geometry(parseGeometry(geoString));
    }
    
    @Override
    public Collection<String> expand(String geoString) {
        List<ByteArrayId> indices = getIndicesFromGeometry(parseGeometry(geoString));
        List<String> retVal = new ArrayList<String>(indices.size());
        for (ByteArrayId index : indices) {
            retVal.add(getEncodedStringFromIndexBytes(index));
        }
        return retVal;
    }
}
