package datawave.query.predicate;

import datawave.query.util.TypeMetadata;
import org.apache.accumulo.core.data.Key;
import org.apache.commons.jexl2.parser.ASTJexlScript;

/**
 * This filter will filter event data keys by only those fields that are required in the specified query.
 */
public class ParentEventDataFilter extends EventDataQueryExpressionFilter {
    
    /**
     * Initialize the query field filter with all of the fields required to evaluation this query
     * 
     * @param script
     */
    public ParentEventDataFilter(ASTJexlScript script, TypeMetadata metadata) {
        super(script, metadata);
    }
    
    public ParentEventDataFilter(ParentEventDataFilter other) {
        super(other);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see datawave.query.function.Filter#keep(org.apache.accumulo.core.data.Key)
     */
    @Override
    public boolean keep(Key k) {
        // do not keep any of these fields because we will be re-fetching the parent document anyway
        return false;
    }
    
    @Override
    public EventDataQueryFilter clone() {
        return new ParentEventDataFilter(this);
    }
}
