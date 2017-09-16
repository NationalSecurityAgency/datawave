package datawave.query.jexl;

import java.util.regex.Pattern;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * 
 */
public class JexlPatternCache {
    
    private static Cache<String,Pattern> PATTERN_CACHE = CacheBuilder.newBuilder().maximumSize(10000l).initialCapacity(100).concurrencyLevel(10).build();
    
    /**
     * Returns a cached Pattern
     * 
     * @param regex
     * @return
     */
    public static Pattern getPattern(String regex) {
        Pattern pattern = PATTERN_CACHE.getIfPresent(regex);
        if (null != pattern) {
            return pattern;
        }
        
        pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
        PATTERN_CACHE.put(regex, pattern);
        
        return pattern;
    }
}
