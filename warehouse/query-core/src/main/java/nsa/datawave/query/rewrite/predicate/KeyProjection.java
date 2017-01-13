package nsa.datawave.query.rewrite.predicate;

import java.util.Map.Entry;

import nsa.datawave.query.data.parsers.DatawaveKey;
import nsa.datawave.query.rewrite.jexl.JexlASTHelper;

import org.apache.accumulo.core.data.Key;
import org.apache.log4j.Logger;

/**
 * 
 */
public class KeyProjection extends KeyStringProjection {
    
    private static final Logger log = Logger.getLogger(KeyProjection.class);
    
    public KeyProjection() {
        super();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.google.common.base.Predicate#apply(java.lang.Object)
     */
    @Override
    public boolean apply(Entry<Key,String> input) {
        DatawaveKey parser = new DatawaveKey(input.getKey());
        final String fieldName = JexlASTHelper.removeGroupingContext(parser.getFieldName());
        
        return projection.apply(fieldName);
    }
    
}
