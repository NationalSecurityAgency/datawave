package datawave.data.normalizer;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.ServiceLoader;
import java.util.TreeSet;

import org.apache.commons.codec.binary.Hex;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.index.NumericIndexStrategy;
import org.locationtech.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.jts.geom.Geometry;

import datawave.data.parser.GeometryParser;

/**
 * A normalizer that, given a parseable geometry string representing an arbitrary geometry, will perform GeoWave indexing with a spatial geowave index
 * configuration
 *
 */
public abstract class AbstractGeometryNormalizer<T extends datawave.data.type.util.AbstractGeometry,G extends Geometry>
                implements Normalizer<T>, DiscreteIndexNormalizer<T> {
    private static final long serialVersionUID = 171360806347433135L;
    
    protected static final int LONGITUDE_BITS = 31;
    protected static final int LATITUDE_BITS = 31;
    
    private static TreeSet<GeometryParser> geoParsers = new TreeSet<>();
    
    static {
        ServiceLoader<GeometryParser> geoParserLoader = ServiceLoader.load(GeometryParser.class, GeometryNormalizer.class.getClassLoader());
        for (GeometryParser geoParser : geoParserLoader)
            geoParsers.add(geoParser);
    }
    
    // NOTE: If we change the index strategy, then we will need to update the validHash method appropriately.
    abstract public NumericIndexStrategy getIndexStrategy();
    
    abstract public Index getIndex();
    
    abstract protected T createDatawaveGeometry(G geometry);
    
    /**
     * Expects to receive a parseable geometry string. The default geometry parser accepts Open Geospatial Consortium compliant Well-Known test strings An
     * example for points is of the form:
     *
     * POINT ([number][space][number])
     */
    @Override
    public String normalize(String geoString) throws IllegalArgumentException {
        if (validHash(geoString)) {
            return geoString;
        }
        return normalizeDelegateType(createDatawaveGeometry((G) parseGeometry(geoString)));
    }
    
    @Override
    public T denormalize(String geoString) {
        // this is assuming the input string is not actually normalized
        // (which oddly is the case with other normalizers)
        return createDatawaveGeometry((G) parseGeometry(geoString));
    }
    
    /**
     * We cannot support regex against geometry fields
     */
    @Override
    public String normalizeRegex(String fieldRegex) throws IllegalArgumentException {
        throw new IllegalArgumentException("Cannot normalize a regex against a geometry field");
    }
    
    @Override
    public boolean normalizedRegexIsLossy(String in) {
        throw new IllegalArgumentException("Cannot normalize a regex against a geometry field");
    }
    
    public String normalizeDelegateType(T geometry) {
        return getEncodedStringFromIndexBytes(getSingleIndexFromGeometry(geometry));
    }
    
    public static String getEncodedStringFromIndexBytes(byte[] index) {
        return Hex.encodeHexString(index);
    }
    
    public static Geometry parseGeometry(String geoString) throws IllegalArgumentException {
        for (GeometryParser geoParser : geoParsers) {
            Geometry geom = geoParser.parseGeometry(geoString);
            if (geom != null)
                return geom;
        }
        throw new IllegalArgumentException("Cannot parse geometry from string [" + geoString + "]");
    }
    
    private byte[] getSingleIndexFromGeometry(T geometry) {
        NumericIndexStrategy indexStrategy = getIndexStrategy();
        final List<byte[]> insertionIds = new ArrayList<>();
        for (MultiDimensionalNumericData range : GeometryUtils.basicConstraintsFromGeometry(geometry.getJTSGeometry()).getIndexConstraints(getIndex())) {
            insertionIds.addAll(getIndexStrategy().getInsertionIds(range, 1).getCompositeInsertionIds());
        }
        if (insertionIds.size() == 1) {
            return insertionIds.get(0);
        }
        // this should never occur
        throw new IllegalArgumentException("Cannot normalize input geometry, no resulting indices");
    }
    
    protected List<byte[]> getIndicesFromGeometry(T geometry) {
        NumericIndexStrategy indexStrategy = getIndexStrategy();
        final List<byte[]> insertionIds = new ArrayList<>();
        for (MultiDimensionalNumericData range : GeometryUtils.basicConstraintsFromGeometry(geometry.getJTSGeometry()).getIndexConstraints(getIndex())) {
            insertionIds.addAll(getIndexStrategy().getInsertionIds(range).getCompositeInsertionIds());
        }
        return insertionIds;
    }
    
    @Override
    public Collection<String> expand(String geoString) {
        List<byte[]> indices = getIndicesFromGeometry(createDatawaveGeometry((G) parseGeometry(geoString)));
        List<String> retVal = new ArrayList<>(indices.size());
        for (byte[] index : indices) {
            retVal.add(getEncodedStringFromIndexBytes(index));
        }
        return retVal;
    }
    
    @Override
    public String incrementIndex(String index) {
        String nextIndex = adjustHexRange(index, true);
        return (nextIndex.length() != index.length()) ? index : nextIndex;
    }
    
    @Override
    public String decrementIndex(String index) {
        String prevIndex = adjustHexRange(index, false);
        return (prevIndex.length() != index.length()) ? index : prevIndex;
    }
    
    @Override
    public List<String> discretizeRange(String beginIndex, String endIndex) {
        List<String> discreteIndices = new ArrayList<>();
        if (beginIndex.compareTo(endIndex) <= 0) {
            if (beginIndex.length() == endIndex.length()) {
                for (String nextIndex = beginIndex; nextIndex.compareTo(endIndex) <= 0; nextIndex = incrementIndex(nextIndex))
                    discreteIndices.add(nextIndex);
            } else {
                discreteIndices.add(beginIndex);
                discreteIndices.add(endIndex);
            }
        }
        return discreteIndices;
    }
    
    @Override
    public boolean producesFixedLengthRanges() {
        return true;
    }
    
    private String adjustHexRange(String hexValue, boolean increment) {
        int length = hexValue.length();
        String format = "%0" + hexValue.length() + "x";
        if (length < 8) {
            return adjustHexRangeInteger(hexValue, format, increment);
        } else if (length < 16) {
            return adjustHexRangeLong(hexValue, format, increment);
        } else {
            return adjustHexRangeBigInteger(hexValue, format, increment);
        }
    }
    
    private String adjustHexRangeInteger(String hexValue, String format, boolean increment) {
        return String.format(format, Integer.parseInt(hexValue, 16) + ((increment) ? 1 : -1));
    }
    
    private String adjustHexRangeLong(String hexValue, String format, boolean increment) {
        return String.format(format, Long.parseLong(hexValue, 16) + ((increment) ? 1L : -1L));
    }
    
    private String adjustHexRangeBigInteger(String hexValue, String format, boolean increment) {
        if (increment)
            return String.format(format, new BigInteger(hexValue, 16).add(BigInteger.ONE));
        else
            return String.format(format, new BigInteger(hexValue, 16).subtract(BigInteger.ONE));
    }
    
    /**
     * This is used to determine if we have a valid geo hash (tier + position). NOTE: If we change the index strategy, then we will need to update this method
     * appropriately.
     * 
     * @param value
     * @return true if valid
     */
    public boolean validHash(String value) {
        try {
            short tier = getTier(value);
            if (validTier(tier) && validLength(tier, value)) {
                return validPosition(tier, getPosition(value));
            }
        } catch (NumberFormatException e) {
            // not a valid hex string in the first place
        }
        return false;
    }
    
    public short getTier(String value) {
        return Short.parseShort(value.substring(0, 2), 16);
    }
    
    public long getPosition(String value) {
        if (value.length() == 2) {
            return 0;
        }
        return Long.parseLong(value.substring(2), 16);
    }
    
    public boolean validTier(short tier) {
        return tier >= 0 && tier <= 0x1f;
    }
    
    public boolean validLength(short tier, String value) {
        // determine the length of the position in hex characters
        // ceil(tier/4) will get the number of bytes
        int bytes = (tier >> 2) + ((tier & 0x3) == 0 ? 0 : 1);
        
        // multiply by 2 to get the number of hex digits
        int posLen = 2 * bytes;
        // length is the tier length plus the position length
        return value.length() == (2 + posLen);
    }
    
    public boolean validPosition(short tier, long value) {
        // The maximum value must be less than pow(2, tier*2)
        long max = 1L << (tier * 2);
        return value >= 0 && value < max;
    }
    
}
