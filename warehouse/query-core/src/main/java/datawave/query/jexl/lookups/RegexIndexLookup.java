package datawave.query.jexl.lookups;

import java.util.Set;

import com.google.common.collect.Sets;

public abstract class RegexIndexLookup extends IndexLookup {
    public static final Set<String> DISALLOWED_PATTERNS = Sets.newHashSet(".*", ".*?");
    
    public static boolean isAcceptedPattern(String pattern) {
        return !DISALLOWED_PATTERNS.contains(pattern);
    }
}
