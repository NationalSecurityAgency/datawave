package datawave.query.predicate;

import java.util.Map.Entry;
import java.util.Set;

import com.google.common.base.Predicate;
import datawave.query.data.parsers.DatawaveKey;
import datawave.query.jexl.JexlASTHelper;

import org.apache.accumulo.core.data.Key;
import org.apache.log4j.Logger;

/**
 * 
 */
public class KeyProjection implements Predicate<Entry<Key,String>> {
    
    protected Projection projection;
    
    public KeyProjection() {
        projection = new Projection();
    }
    
    public KeyProjection(KeyProjection other) {
        projection = other.getProjection();
    }
    
    public void initializeWhitelist(Set<String> whiteListFields) {
        projection.setWhitelist(whiteListFields);
    }
    
    public void initializeBlacklist(Set<String> blackListFields) {
        projection.setBlacklist(blackListFields);
    }
    
    public Projection getProjection() {
        return projection;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.google.common.base.Predicate#apply(java.lang.Object)
     */
    @Override
    public boolean apply(Entry<Key,String> input) {
        final DatawaveKey parser = new DatawaveKey(input.getKey());
        final String fieldName = JexlASTHelper.removeGroupingContext(parser.getFieldName());
        
        return projection.apply(fieldName);
    }
    
}
