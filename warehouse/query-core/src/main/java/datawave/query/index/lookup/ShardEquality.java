package datawave.query.index.lookup;

/**
 * Convenience class that encapsulates some of the basic equality operators plus a few extra.
 *
 * Supports equality, greater than, less than operators and also a 'matches within' given the rules of days and shards.
 *
 * Some definitions - day ranges are like 20001224 - shard ranges are like 20190314_15
 *
 * Shards are sorted lexicographically. Shard _19 sorts before shard _2.
 */
public class ShardEquality {
    
    // Does A match B?
    public static boolean matches(String a, String b) {
        if (a.length() == b.length()) {
            return a.equals(b);
        } else {
            return matchesWithin(a, b) || matchesWithin(b, a);
        }
    }
    
    public static boolean matchesExactly(String a, String b) {
        return a.equals(b);
    }
    
    // Does A match within B?
    public static boolean matchesWithin(String a, String b) {
        if (isDay(a) && daysMatch(a, b)) {
            return true;
        } else {
            return false;
        }
    }
    
    // Is A before B?
    public static boolean lessThan(String a, String b) {
        return a.compareTo(b) < 0;
    }
    
    // Is A before or equal to B?
    public static boolean lessThanOrEqual(String a, String b) {
        return a.compareTo(b) <= 0;
    }
    
    // Is A after B?
    public static boolean greaterThan(String a, String b) {
        return a.compareTo(b) > 0;
    }
    
    // Is A after or equal to B?
    public static boolean greaterThanOrEqual(String a, String b) {
        return a.compareTo(b) >= 0;
    }
    
    // Days are yyyymmdd, shards are yyyymmdd_shardnum.
    public static boolean isDay(String shard) {
        return shard.lastIndexOf('_') == -1;
    }
    
    /**
     * Given the YYYYMMDD_shardNum construct it is faster to determine shard identity with a search that starts from the end of the string
     * 
     * @param shard
     *            a shard
     * @return true if the shard is not a day
     */
    public static boolean isShard(String shard) {
        return shard.lastIndexOf('_') != -1;
    }
    
    /**
     * A faster method of determining "a.startsWith(b)" given the YYYYMMDD structure of a shard.
     *
     * @param a
     *            shard A that is a day
     * @param b
     *            shard B that could be a day or shard
     * @return true if shard B starts with shard A
     */
    public static boolean daysMatch(String a, String b) {
        int bIndex = b.lastIndexOf('_');
        // Check for case where shard B is a day, thus returning -1 for an underscore index
        if (bIndex == -1) {
            bIndex = b.length();
        }
        
        // Check for case where A is a day range for a shorter year string than B
        // i.e., 867 vs. 1776
        if (bIndex > a.length()) {
            return false;
        }
        
        for (int ii = bIndex - 1; ii >= 0; ii--) {
            if (a.charAt(ii) != b.charAt(ii)) {
                return false;
            }
        }
        return true;
    }
}
