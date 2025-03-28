package datawave.data.normalizer;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Collection;
import java.util.Date;

import datawave.data.type.util.Geometry;
import datawave.data.type.util.IpAddress;
import datawave.data.type.util.Point;

public interface Normalizer<T> extends Serializable {
    
    Normalizer<IpAddress> IP_ADDRESS_NORMALIZER = new IpAddressNormalizer();
    Normalizer<String> MAC_ADDRESS_NORMALIZER = new MacAddressNormalizer();
    Normalizer<String> LC_NO_DIACRITICS_NORMALIZER = new LcNoDiacriticsNormalizer();
    Normalizer<Date> DATE_NORMALIZER = new DateNormalizer();
    Normalizer<String> RAW_DATE_NORMALIZER = new RawDateNormalizer();
    Normalizer<Geometry> GEOMETRY_NORMALIZER = new GeometryNormalizer();
    Normalizer<String> GEO_LAT_NORMALIZER = new GeoLatNormalizer();
    Normalizer<String> GEO_LON_NORMALIZER = new GeoLonNormalizer();
    Normalizer<String> GEO_NORMALIZER = new GeoNormalizer();
    Normalizer<String> HEX_STRING_NORMALIZER = new HexStringNormalizer();
    Normalizer<String> LC_NORMALIZER = new LcNormalizer();
    Normalizer<String> NETWORK_NORMALIZER = new NetworkNormalizer();
    Normalizer<BigDecimal> NUMBER_NORMALIZER = new NumberNormalizer();
    Normalizer<Point> POINT_NORMALIZER = new PointNormalizer();
    Normalizer<String> TRIM_LEADING_ZEROS_NORMALIZER = new TrimLeadingZerosNormalizer();
    Normalizer<String> NOOP_NORMALIZER = new NoOpNormalizer();
    
    String normalize(String in);
    
    String normalizeDelegateType(T delegateIn);
    
    T denormalize(String in);
    
    String normalizeRegex(String in);
    
    boolean normalizedRegexIsLossy(String in);
    
    Collection<String> expand(String in);
}
