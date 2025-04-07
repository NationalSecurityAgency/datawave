package datawave.data.normalizer;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Iterables;

import datawave.data.type.util.NumericalEncoder;

/**
 * A normalizer that, given a string of the format, [number][non-number character][number], will split the string at the non-numeric and interlace the left and
 * right hand operands. Operands are normalized to contain
 *
 */
public class GeoNormalizer extends AbstractNormalizer<String> {
    private static final long serialVersionUID = -1212607537051869786L;
    
    private static final Logger log = LoggerFactory.getLogger(GeoNormalizer.class);
    
    /*
     * The z-order value that this normalize produces has no regard for precision. The prefix is always six digits and the suffix is always 16 digits
     */
    private static final Pattern normalizedZRef = Pattern.compile("\\d{6}\\.\\.\\d+");
    
    private static final LoadingCache<String,Boolean> isNormalizedCache = CacheBuilder.newBuilder().concurrencyLevel(32).maximumSize(10 * 1024)
                    .build(new CacheLoader<String,Boolean>() {
                        
                        @Override
                        public Boolean load(String key) {
                            boolean m = normalizedZRef.matcher(key).matches();
                            if (log.isTraceEnabled())
                                log.trace(key + " is " + (m ? "" : "not") + " a z-ref.");
                            return m;
                        }
                        
                    });
    
    public static boolean isNormalized(String s) {
        try {
            return isNormalizedCache.get(s);
        } catch (ExecutionException e) {
            
        }
        return false;
    }
    
    public static final String separator = "|";
    
    /**
     * Expects to receive a concatenated string. The string should be of the form:
     *
     * [number][non-numeric nor decimal dot][number]
     *
     * @throws IllegalArgumentException
     *             , if unable to parse the numbers on either side of the delimiter
     */
    @Override
    public String normalize(String fieldValue) throws IllegalArgumentException {
        String normalized = fieldValue;
        if (!isNormalized(fieldValue)) {
            int split = findSplit(fieldValue);
            if (split > 0) {
                try {
                    normalized = combineLatLon(fieldValue.substring(0, split), fieldValue.substring(split + 1));
                } catch (Exception e) {
                    throw new IllegalArgumentException("Failed to normalize value as a Geo: " + fieldValue);
                }
            } else {
                throw new IllegalArgumentException("Failed to normalize value as a Geo: " + fieldValue);
            }
        }
        return normalized;
    }
    
    /**
     * Expects to receive a concatenated string. The string should be of the form:
     *
     * [number][non-numeric nor decimal dot][number]
     *
     * @throws IllegalArgumentException
     *             , if unable to parse the numbers on either side of the delimiter
     */
    public double[] parseLatLon(String fieldValue) throws IllegalArgumentException {
        int split = findSplit(fieldValue);
        if (split > 0) {
            try {
                return new double[] {parseLatOrLon(fieldValue.substring(0, split)), parseLatOrLon(fieldValue.substring(split + 1, fieldValue.length()))};
            } catch (Exception e) {
                throw new IllegalArgumentException("Failed to normalize value as a Geo: " + fieldValue);
            }
        } else {
            throw new IllegalArgumentException("Failed to normalize value as a Geo: " + fieldValue);
        }
    }
    
    /**
     * We cannot support regex against numbers
     */
    @Override
    public String normalizeRegex(String fieldRegex) throws IllegalArgumentException {
        throw new IllegalArgumentException("Cannot normalize a regex against a numeric field");
    }
    
    @Override
    public String normalizeDelegateType(String delegateIn) {
        return normalize(delegateIn);
    }
    
    @Override
    public String denormalize(String in) {
        return in;
    }
    
    public String combineLatLon(String lat, String lon) throws OutOfRangeException, ParseException {
        return combineLatLon(parseLatOrLon(lat), parseLatOrLon(lon));
    }
    
    private static final List<Character> DMS_DESIGNATORS = Arrays.asList('n', 's', 'e', 'w');
    
    public static double parseLatOrLon(String value) throws ParseException {
        value = value.trim();
        
        // If we were given a zero-length value, catch this ahead of time so we
        // can throw a ParseException instead of an IndexOutOfBoundsException
        if (value.isEmpty()) {
            throw new ParseException("Could not normalize empty value as latitute or longitude");
        }
        
        // value may have been encoded
        try {
            if (NumericalEncoder.isPossiblyEncoded(value)) {
                value = NumericalEncoder.decode(value).toPlainString();
            }
        } catch (Exception nfe) {
            // ok, assume not normalized
        }
        
        char end = Character.toLowerCase(value.charAt(value.length() - 1));
        if (DMS_DESIGNATORS.contains(end)) {
            try {
                return convertDMStoDD(value);
            } catch (NormalizationException ne) {
                throw new ParseException("Unable to convert DMS to DD format", ne);
            }
        } else {
            try {
                return parseDouble(value);
            } catch (Exception nfe) {
                throw new ParseException("Unable to parse lat or lon " + value, nfe);
            }
        }
    }
    
    /**
     * Convert a Degrees / Minutes / Seconds latitude or longitude into decimal degrees.
     *
     * @param val
     * @return
     */
    public static double convertDMStoDD(String val) throws NormalizationException {
        try {
            boolean negate = false;
            double degrees = 0.0d;
            double minutes = 0.0d;
            double seconds = 0.0d;
            
            val = val.trim();
            char end = Character.toLowerCase(val.charAt(val.length() - 1));
            if (end == 'n' || end == 'e') {
                val = val.substring(0, val.length() - 1).trim();
            } else if (end == 's' || end == 'w') {
                val = val.substring(0, val.length() - 1).trim();
                negate = true;
            }
            
            // see if it is already split up
            if (val.indexOf(':') >= 0) {
                String[] parts = Iterables.toArray(Splitter.on(':').split(val), String.class);
                degrees = Double.parseDouble(parts[0]);
                if (parts.length > 1) {
                    minutes = Double.parseDouble(parts[1]);
                    if (parts.length > 2) {
                        seconds = Double.parseDouble(parts[2]);
                        if (parts.length > 3) {
                            throw new NormalizationException("Do not know how to convert lat or lon value: " + val);
                        }
                    }
                }
            } else {
                int point = val.indexOf('.');
                if (point < 0)
                    point = val.length();
                // if more than 3 digits, then we have minutes
                if (point > 3) {
                    // if more than 5 digits, then we have seconds
                    if (point > 5) {
                        seconds = Double.parseDouble(val.substring(point - 2));
                        minutes = Double.parseDouble(val.substring(point - 4, point - 2));
                        degrees = Double.parseDouble(val.substring(0, point - 4));
                    } else {
                        minutes = Double.parseDouble(val.substring(point - 2));
                        degrees = Double.parseDouble(val.substring(0, point - 2));
                    }
                } else {
                    degrees = Double.parseDouble(val);
                }
            }
            
            double dd = degrees + (minutes / 60.0d) + (seconds / 3600.0d);
            if (negate) {
                dd = (0.0d - dd);
            }
            
            return dd;
        } catch (Exception nfe) {
            throw new NormalizationException("Failed to convert numeric value part of a lat or lon " + val, nfe);
        }
    }
    
    public static double parseDouble(String val) throws ParseException {
        double value = 0.0d;
        try {
            value = Double.parseDouble(val);
        } catch (Exception e) {
            if (NumericalEncoder.isPossiblyEncoded(val)) {
                try {
                    value = NumericalEncoder.decode(val).doubleValue();
                } catch (Exception e2) {
                    // Don't log, since it's expected that we'll sometimes use this normalizer and pass bad values
                    // when we need to run an unknown type of term through all normalizers.s
                    throw new ParseException("Failed to convert " + val + " into a double value", e2);
                }
            } else {
                throw new ParseException("Unknown double format: " + val);
            }
        }
        return value;
    }
    
    public String combineLatLon(double lat, double lon) throws OutOfRangeException {
        return GeoPoint.getZRefStr(new GeoPoint(lat, lon));
    }
    
    /**
     * Finds the first non numeric and non '.' character and returns its position.
     *
     * @param s
     * @return
     */
    public int findSplit(String s) {
        if (separator != null) {
            int i = s.indexOf(separator);
            if (i > 0) {
                return i;
            }
        }
        // search from the center for a non lat or lon character
        for (int i = 0; i < s.length(); ++i) {
            int side = (i % 2 == 0 ? -1 : 1);
            int dist = (i + 1) / 2;
            int index = (s.length() / 2) + (dist * side);
            if (index >= s.length())
                break;
            char c = s.charAt(index);
            if ((c > '9' || c < '0') && (c != '.' && c != '-' && c != '+') && (c != 'n' && c != 'N' && c != 's' && c != 'S')
                            && (c != 'e' && c != 'E' && c != 'w' && c != 'W')) {
                return index;
            }
        }
        return -1;
    }
    
    public static class GeoPoint {
        private double latitude, longitude;
        
        /**
         * Creates a GeoPoint with a custom fraction precision.
         *
         * @param latitude
         * @param longitude
         */
        public GeoPoint(double latitude, double longitude) throws OutOfRangeException {
            this.latitude = latitude;
            this.longitude = longitude;
            validate();
        }
        
        /**
         * Creates a GeoPoint with a custom fraction precision.
         *
         * @param latitude
         * @param longitude
         * @throws ParseException
         */
        public GeoPoint(String latitude, String longitude) throws OutOfRangeException, ParseException {
            this.latitude = GeoNormalizer.parseDouble(latitude);
            this.longitude = GeoNormalizer.parseDouble(longitude);
            validate();
        }
        
        /**
         * A validation routine that check the latitude and longitude ranges
         *
         * @throws IllegalArgumentException
         *             if an out of range is detected
         */
        public void validate() throws OutOfRangeException {
            if (this.latitude < -90.0 || this.latitude > 90.0) {
                throw new OutOfRangeException("Latitude is outside of valid range [-90, 90]: " + this.latitude + ", " + this.longitude);
            }
            if (this.longitude < -180.0 || this.longitude > 180.0) {
                throw new OutOfRangeException("Longitude is outside of valid range [-180, 180]: " + this.latitude + ", " + this.longitude);
            }
        }
        
        /**
         * Returns an interlaced representation of the latitude and longitude. The latitude's normal range of -90:90 is shifted to 0:180 (+90) and the
         * logitude's normal range of -180:180 has been shifted to 0:360.
         * <p>
         * For example:
         * <p>
         * {@code [45, -150] => [135, 30] => 103350..0000000000000000}
         *
         * @return
         */
        public static Text getZRef(GeoPoint p) {
            double latShift = p.latitude + 90.0;
            double lonShift = p.longitude + 180.0;
            
            NumberFormat formatter = NumberFormat.getInstance();
            formatter.setMaximumIntegerDigits(3);
            formatter.setMinimumIntegerDigits(3);
            formatter.setMaximumFractionDigits(5);
            formatter.setMinimumFractionDigits(5);
            
            String latS = formatter.format(latShift);
            String lonS = formatter.format(lonShift);
            
            byte[] buf = new byte[latS.length() * 2];
            for (int i = 0; i < latS.length(); ++i) {
                buf[2 * i] = (byte) latS.charAt(i);
                buf[2 * i + 1] = (byte) lonS.charAt(i);
            }
            
            return new Text(buf);
        }
        
        /**
         * Returns an interlaced representation of the latitude and longitude. The latitude's normal range of -90:90 is shifted to 0:180 (+90) and the
         * logitude's normal range of -180:180 has been shifted to 0:360.
         * <p>
         * For example:
         * <p>
         * {@code [45, -150] => [135, 30] => 103350..0000000000000000}
         *
         * @return
         */
        public static String getZRefStr(GeoPoint p) {
            double latShift = p.latitude + 90.0;
            double lonShift = p.longitude + 180.0;
            
            NumberFormat formatter = NumberFormat.getInstance();
            formatter.setMaximumIntegerDigits(3);
            formatter.setMinimumIntegerDigits(3);
            formatter.setMaximumFractionDigits(5);
            formatter.setMinimumFractionDigits(5);
            
            String latS = formatter.format(latShift);
            String lonS = formatter.format(lonShift);
            StringBuilder sb = new StringBuilder(latS.length() * 2);
            
            for (int i = 0; i < latS.length(); ++i) {
                sb.append(latS.charAt(i));
                sb.append(lonS.charAt(i));
            }
            
            return sb.toString();
        }
        
        /**
         * Factory method for decoding a zReference from a Text object.
         *
         * @param zref
         * @return
         */
        public static GeoPoint decodeZRef(Text zref) throws OutOfRangeException, ParseException {
            StringBuilder latB = new StringBuilder();
            StringBuilder lonB = new StringBuilder();
            
            ByteBuffer data = ByteBuffer.wrap(zref.getBytes(), 0, zref.getLength());
            boolean isLat = true;
            while (data.hasRemaining()) {
                if (isLat) {
                    latB.append((char) data.get());
                } else {
                    lonB.append((char) data.get());
                }
                isLat = !isLat;
            }
            
            double lat = GeoNormalizer.parseDouble(latB.toString());
            double lon = GeoNormalizer.parseDouble(lonB.toString());
            
            return new GeoPoint(lat - 90.0, lon - 180.0);
        }
        
        /**
         * Factory method for decoding a zReference from a Text object.
         *
         * @param zref
         * @return
         * @throws ParseException
         */
        public static GeoPoint decodeZRef(String zref) throws OutOfRangeException, ParseException {
            StringBuilder latB = new StringBuilder();
            StringBuilder lonB = new StringBuilder();
            
            CharBuffer data = CharBuffer.wrap(zref);
            boolean isLat = true;
            while (data.hasRemaining()) {
                if (isLat) {
                    latB.append(data.get());
                } else {
                    lonB.append(data.get());
                }
                isLat = !isLat;
            }
            
            double lat = GeoNormalizer.parseDouble(latB.toString());
            double lon = GeoNormalizer.parseDouble(lonB.toString());
            
            return new GeoPoint(lat - 90.0, lon - 180.0);
        }
        
        /**
         * Given a bounding box described by the lower left corner (boundMind) and the upper right corner (boundMax), this method tests whether or not the point
         * is within that box.
         *
         * @param boundMin
         * @param boundMax
         * @return
         */
        public boolean within(GeoPoint boundMin, GeoPoint boundMax) {
            return getLatitude() >= boundMin.getLatitude() && getLatitude() <= boundMax.getLatitude() && getLongitude() >= boundMin.getLongitude()
                            && getLongitude() <= boundMax.getLongitude();
        }
        
        public double getLatitude() {
            return latitude;
        }
        
        public double getLongitude() {
            return longitude;
        }
        
        @Override
        public String toString() {
            return "(" + getLongitude() + ", " + getLatitude() + ")";
        }
        
        @Override
        public boolean equals(Object o) {
            if (o == null) {
                return false;
            } else if (o instanceof GeoPoint) {
                GeoPoint ogp = (GeoPoint) o;
                return latitude == ogp.latitude && longitude == ogp.longitude;
            } else {
                return super.equals(o);
            }
        }
        
        @Override
        public int hashCode() {
            return Objects.hash(getLatitude(), getLongitude());
        }
    }
    
    public static class OutOfRangeException extends Exception {
        
        public OutOfRangeException() {
            super();
        }
        
        public OutOfRangeException(String message, Throwable cause) {
            super(message, cause);
        }
        
        public OutOfRangeException(String message) {
            super(message);
        }
        
        public OutOfRangeException(Throwable cause) {
            super(cause);
        }
        
    }
    
    public static class ParseException extends Exception {
        
        public ParseException() {
            super();
        }
        
        public ParseException(String message, Throwable cause) {
            super(message, cause);
        }
        
        public ParseException(String message) {
            super(message);
        }
        
        public ParseException(Throwable cause) {
            super(cause);
        }
        
    }
    
}
