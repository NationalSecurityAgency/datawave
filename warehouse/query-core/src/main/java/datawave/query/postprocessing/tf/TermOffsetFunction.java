package datawave.query.postprocessing.tf;

import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Document;
import datawave.query.attributes.DocumentKey;
import datawave.query.util.Tuple2;
import datawave.query.util.Tuple3;
import datawave.query.util.Tuples;
import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class TermOffsetFunction implements com.google.common.base.Function<Tuple2<Key,Document>,Tuple3<Key,Document,Map<String,Object>>> {
    
    // Runs the iterator to collect term frequency offsets
    private TermOffsetPopulator tfPopulator;
    
    // Responsible for reducing the term frequency search space
    private TermFrequencyHitFunction hitFunction;
    
    private TermFrequencyConfig tfConfig;
    
    public TermOffsetFunction(TermFrequencyConfig tfConfig, TermOffsetPopulator tfPopulator) {
        this.tfConfig = tfConfig;
        this.tfPopulator = tfPopulator;
        this.hitFunction = new TermFrequencyHitFunction(tfConfig, tfPopulator.getTermFrequencyFieldValues());
    }
    
    @Override
    public Tuple3<Key,Document,Map<String,Object>> apply(Tuple2<Key,Document> from) {
        Document merged = from.second();
        Attribute<?> docKeyAttr = merged.get(Document.DOCKEY_FIELD_NAME);
        
        // gather the set of doc keys
        Set<Key> docKeys = new HashSet<>();
        if (docKeyAttr == null) {
            docKeys.add(from.first());
        } else if (docKeyAttr instanceof DocumentKey) {
            docKeys.add(((DocumentKey) docKeyAttr).getDocKey());
        } else if (docKeyAttr instanceof Attributes) {
            for (Attribute<?> docKey : ((Attributes) docKeyAttr).getAttributes()) {
                if (docKey instanceof DocumentKey) {
                    docKeys.add(((DocumentKey) docKey).getDocKey());
                } else {
                    throw new IllegalStateException("Unexpected sub-Attribute type for " + Document.DOCKEY_FIELD_NAME + ": " + docKey.getClass());
                }
            }
        } else {
            throw new IllegalStateException("Unexpected Attribute type for " + Document.DOCKEY_FIELD_NAME + ": " + docKeys.getClass());
        }
        
        // The search space is the intersection of document field-values and the query's term frequency function's field-values
        TreeSet<Text> searchSpace = hitFunction.apply(from.first(), from.second());
        
        // Collect the term frequency offsets defined by the search space
        Map<String,Object> map = new HashMap<>();
        map.putAll(tfPopulator.getContextMap(from.first(), searchSpace));
        merged.putAll(tfPopulator.document(), false);
        return Tuples.tuple(from.first(), merged, map);
    }
}
