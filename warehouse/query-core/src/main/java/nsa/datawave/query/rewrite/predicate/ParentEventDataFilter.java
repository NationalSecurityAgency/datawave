package nsa.datawave.query.rewrite.predicate;

import nsa.datawave.query.util.TypeMetadata;
import org.apache.accumulo.core.data.Key;
import org.apache.commons.jexl2.parser.ASTJexlScript;

/**
 * This filter will filter event data keys by only those fields that are required in the specified query.
 */
public class ParentEventDataFilter extends ConfigurableEventDataQueryFilter {
    
    /**
     * Initialize the query field filter with all of the fields required to evaluation this query
     * 
     * @param script
     */
    public ParentEventDataFilter(ASTJexlScript script, TypeMetadata metadata, boolean expressionFilterEnabled) {
        super(script, metadata, expressionFilterEnabled);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see nsa.datawave.query.rewrite.function.Filter#keep(org.apache.accumulo.core.data.Key)
     */
    @Override
    public boolean keep(Key k) {
        // do not keep any of these fields because we will be re-fetching the parent document anyway
        return false;
    }
}
