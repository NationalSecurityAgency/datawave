package nsa.datawave.query.rewrite.predicate;

import org.apache.accumulo.core.data.Key;

public interface LimitedFilter {
    /**
     * Based on the current document context is the field from this Key limited
     * 
     * @param key
     *            the Key to test
     * @return true if the Key is limited, false otherwise
     */
    boolean isLimited(Key key);
    
    /**
     * Generate the limit field key given the Key to limit
     * 
     * @param toLimit
     *            the Key to limit
     * @return the Key reflecting the applied limit for a field
     */
    Key applyLimit(Key toLimit);
}
