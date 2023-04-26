package datawave.query.postprocessing.tf;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Multimap;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Document;
import datawave.query.attributes.DocumentKey;
import datawave.query.util.Tuple2;
import datawave.query.util.Tuple3;
import datawave.query.util.Tuples;

import org.apache.accumulo.core.data.Key;
import org.apache.log4j.Logger;

public class TermOffsetFunction implements com.google.common.base.Function<Tuple2<Key,Document>,Tuple3<Key,Document,Map<String,Object>>> {
    private static final Logger log = Logger.getLogger(TermOffsetFunction.class);
    private TermOffsetPopulator tfPopulator;
    private Set<String> tfIndexOnlyFields;
    private int aggregationThreshold;
    private long aggregationStart;
    
    public TermOffsetFunction(TermOffsetPopulator tfPopulator, Set<String> tfIndexOnlyFields) {
        this.tfPopulator = tfPopulator;
        this.tfIndexOnlyFields = tfIndexOnlyFields;
    }
    
    @Override
    public Tuple3<Key,Document,Map<String,Object>> apply(Tuple2<Key,Document> from) {
        Document merged = from.second();
        Map<String,Object> map = new HashMap<>();
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
        
        Set<String> fields = getFieldsToRemove(from.second(), tfPopulator.getTermFrequencyFieldValues());
        
        logStart();
        map.putAll(tfPopulator.getContextMap(from.first(), docKeys, fields));
        logStop(docKeys.iterator().next());

        merged.putAll(tfPopulator.document(), false);
        return Tuples.tuple(from.first(), merged, map);
    }
    
    private Set<String> getFieldsToRemove(Document doc, Multimap<String,String> tfFVs) {
        Set<String> fieldsToRemove = new HashSet<>();
        Set<String> docFields = doc.getDictionary().keySet();
        Set<String> tfFields = tfFVs.keySet();
        for (String tfField : tfFields) {
            // Can only prune a field if it is not index only
            if (!docFields.contains(tfField) && !tfIndexOnlyFields.contains(tfField)) {
                // mark this field for removal prior to building the TermFrequencyIterator
                fieldsToRemove.add(tfField);
            }
        }
        return fieldsToRemove;
    }

    private void logStart(){
        aggregationStart = System.currentTimeMillis();
    }

    private void logStop(Key k){
        if (aggregationThreshold == -1) {
            return;
        }

        long elapsed = System.currentTimeMillis() - aggregationStart;
        if (elapsed > aggregationThreshold) {
            log.warn("time to aggregate offsets " + k.getRow() + " " + k.getColumnFamily().toString().replace("\0", "0x00") + " was " + elapsed);
        }
    }

    public void setAggregationThreshold(int aggregationThreshold){
        this.aggregationThreshold = aggregationThreshold;
    }
}
