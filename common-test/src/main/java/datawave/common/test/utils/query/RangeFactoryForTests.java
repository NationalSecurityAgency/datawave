package datawave.common.test.utils.query;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;

/**
 * Utility class that generates ranges for unit tests.
 */
public class RangeFactoryForTests {
    
    public static final String NULL_BYTE_STRING = "\u0000";
    public static final String MAX_UNICODE_STRING = new String(Character.toChars(Character.MAX_CODE_POINT));
    
    public static Range makeTestRange(String r, String c) {
        Key s = new Key(r, c), e = s.followingKey(PartialKey.ROW_COLFAM);
        return new Range(s, true, e, false);
    }
    
    public static Range makeTldTestRange(String r, String c) {
        Key s = new Key(r, c), e = new Key(r, c + MAX_UNICODE_STRING);
        return new Range(s, true, e, false);
    }
    
    public static Range makeShardedRange(String r) {
        Key s = new Key(r), e = new Key(r + NULL_BYTE_STRING);
        return new Range(s, true, e, false);
    }
    
    public static Range makeDayRange(String r) {
        Key s = new Key(r + "_0"), e = new Key(r + MAX_UNICODE_STRING);
        return new Range(s, true, e, false);
    }
    
}
