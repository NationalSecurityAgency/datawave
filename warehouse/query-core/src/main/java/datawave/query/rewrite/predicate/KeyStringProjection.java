package datawave.query.rewrite.predicate;

import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.data.Key;

import com.google.common.base.Predicate;

/**
 * 
 */
public class KeyStringProjection implements Predicate<Entry<Key,String>> {
    
    protected Projection projection;
    
    public KeyStringProjection() {
        projection = new Projection();
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
        return projection.apply(input.getValue());
    }
    
}
