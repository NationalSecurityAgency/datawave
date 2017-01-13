package nsa.datawave.query.rewrite.postprocessing.tf;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import nsa.datawave.query.rewrite.attributes.Attribute;
import nsa.datawave.query.rewrite.attributes.Attributes;
import nsa.datawave.query.rewrite.attributes.Document;
import nsa.datawave.query.rewrite.attributes.DocumentKey;
import nsa.datawave.query.util.Tuple2;
import nsa.datawave.query.util.Tuple3;
import nsa.datawave.query.util.Tuples;

import org.apache.accumulo.core.data.Key;

public class TermOffsetFunction implements com.google.common.base.Function<Tuple2<Key,Document>,Tuple3<Key,Document,Map<String,Object>>> {
    private TermOffsetPopulator tfPopulator;
    
    public TermOffsetFunction(TermOffsetPopulator tfPopulator) {
        this.tfPopulator = tfPopulator;
    }
    
    @Override
    public Tuple3<Key,Document,Map<String,Object>> apply(Tuple2<Key,Document> from) {
        Document merged = from.second();
        Map<String,Object> map = new HashMap<String,Object>();
        Attribute<?> docKeyAttr = merged.get(Document.DOCKEY_FIELD_NAME);
        
        // gather the set of doc keys
        Set<Key> docKeys = new HashSet<Key>();
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
        
        map.putAll(tfPopulator.getContextMap(from.first(), docKeys));
        merged.putAll(tfPopulator.document(), false);
        return Tuples.tuple(from.first(), merged, map);
    }
}
