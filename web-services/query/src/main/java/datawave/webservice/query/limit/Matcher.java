package datawave.webservice.query.limit;

/**
 * A matcher is intended to determine if it matches against a string.
 */
public interface Matcher {
    
    enum Type {
        // Do not reorder these. The ordinal value is important for use when sorting query limits.
        EXACT,
        PARTIAL,
        ALL
    }
    
    Type getType();
    
    boolean matches(String fieldValue);
}
