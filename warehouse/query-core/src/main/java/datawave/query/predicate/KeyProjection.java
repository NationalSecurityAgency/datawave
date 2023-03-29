package datawave.query.predicate;

import java.util.Map.Entry;
import java.util.Set;

import datawave.query.data.parsers.DatawaveKey;
import datawave.query.jexl.JexlASTHelper;

import org.apache.accumulo.core.data.Key;

/**
 * Wraps a {@link Projection} so it can be applied to a {@link Key}.
 * <p>
 * The use of a {@link DatawaveKey} means this projection is applicable to any type of key (field index, event, tf, etc).
 */
public class KeyProjection implements PeekingPredicate<Entry<Key,String>> {
    
    protected Projection projection;
    
    public KeyProjection(Set<String> projections, Projection.ProjectionType projectionType) {
        projection = new Projection(projections, projectionType);
    }
    
    public KeyProjection(KeyProjection other) {
        projection = other.getProjection();
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
    
    @Override
    public boolean peek(Entry<Key,String> input) {
        // no difference so just redirect
        return apply(input);
    }
    
}
