package nsa.datawave.query.rewrite.predicate;

import com.google.common.base.Predicate;
import nsa.datawave.query.rewrite.attributes.Document;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;

import java.util.Map;

/**
 * This filter will filter event data keys by only those fields that are required in the specified query.
 */
public interface EventDataQueryFilter extends Predicate<Map.Entry<Key,String>>, Filter, SeekingFilter, TransformingFilter {
    
    /**
     * This method can be used to change the document context fo the keep(Key k) method.
     *
     * @param document
     */
    void setDocumentKey(Key document);
    
    /*
     * (non-Javadoc)
     * 
     * @see nsa.datawave.query.rewrite.predicate.Filter#keep(org.apache.accumulo.core.data.Key)
     */
    @Override
    boolean keep(Key k);
    
    /**
     * Define the start key given the from condition.
     *
     * @param from
     * @return
     */
    Key getStartKey(Key from);
    
    /**
     * Define the end key given the from condition.
     *
     * @param from
     * @return
     */
    Key getStopKey(Key from);
    
    /**
     * Get the key range that covers the complete document specified by the input key range
     *
     * @param from
     * @return
     */
    public Range getKeyRange(Map.Entry<Key,Document> from);
}
