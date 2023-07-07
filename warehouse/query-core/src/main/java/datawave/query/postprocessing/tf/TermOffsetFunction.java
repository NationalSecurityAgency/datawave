package datawave.query.postprocessing.tf;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.log4j.Logger;

import com.google.common.collect.Multimap;

import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Document;
import datawave.query.attributes.DocumentKey;
import datawave.query.util.Tuple2;
import datawave.query.util.Tuple3;
import datawave.query.util.Tuples;

public class TermOffsetFunction implements com.google.common.base.Function<Tuple2<Key,Document>,Tuple3<Key,Document,Map<String,Object>>> {
    private static final Logger log = Logger.getLogger(TermOffsetFunction.class);
    private TermOffsetPopulator tfPopulator;
    private Set<String> tfIndexOnlyFields;
    private int aggregationThreshold;
    private long aggregationStart;
    private DocumentKeysFunction docKeyFunction;

    public TermOffsetFunction(TermOffsetPopulator tfPopulator, Set<String> tfIndexOnlyFields) {
        this(tfPopulator, tfIndexOnlyFields, null);
    }

    public TermOffsetFunction(TermOffsetPopulator tfPopulator, Set<String> tfIndexOnlyFields, DocumentKeysFunction docKeyFunction) {
        this.tfPopulator = tfPopulator;
        this.tfIndexOnlyFields = tfIndexOnlyFields;
        this.docKeyFunction = docKeyFunction;
    }

    @Override
    public Tuple3<Key,Document,Map<String,Object>> apply(Tuple2<Key,Document> from) {

        Set<Key> docKeys = getDocumentKeys(from);
        Set<String> fields = getFieldsToRemove(from.second(), tfPopulator.getTermFrequencyFieldValues());

        logStart();
        Map<String,Object> map = new HashMap<>(tfPopulator.getContextMap(from.first(), docKeys, fields));
        logStop(docKeys.iterator().next());

        Document merged = from.second();
        merged.putAll(tfPopulator.document(), false);
        return Tuples.tuple(from.first(), merged, map);
    }

    private Set<Key> getDocumentKeys(Tuple2<Key,Document> from) {
        Attribute<?> docKeyAttr = from.second().get(Document.DOCKEY_FIELD_NAME);
        Set<Key> docKeys = new TreeSet<>((left, right) -> left.compareTo(right, PartialKey.ROW_COLFAM));
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

        if (docKeyFunction != null) {
            docKeys = docKeyFunction.getDocKeys(from.second(), docKeys);
        }

        return docKeys;
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

    private void logStart() {
        aggregationStart = System.currentTimeMillis();
    }

    private void logStop(Key k) {
        if (aggregationThreshold == -1) {
            return;
        }

        long elapsed = System.currentTimeMillis() - aggregationStart;
        if (elapsed > aggregationThreshold) {
            log.warn("time to aggregate offsets " + k.getRow() + " " + k.getColumnFamily().toString().replace("\0", "0x00") + " was " + elapsed);
        }
    }

    public void setAggregationThreshold(int aggregationThreshold) {
        this.aggregationThreshold = aggregationThreshold;
    }
}
