package datawave.query.jexl.lookups;

import com.google.common.collect.Sets;

import java.util.Set;

public abstract class BaseRegexIndexLookup extends IndexLookup {
    public static final Set<String> DISALLOWED_PATTERNS = Sets.newHashSet(".*", ".*?");
    
    public static boolean isAcceptedPattern(String pattern) {
        return !DISALLOWED_PATTERNS.contains(pattern);
    }
}
