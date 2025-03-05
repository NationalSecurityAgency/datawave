package datawave.query.function;

import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;

import org.apache.accumulo.core.data.Key;
import org.apache.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.collect.Maps;
import com.google.common.collect.TreeMultimap;

import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Cardinality;
import datawave.query.attributes.Document;

/**
 *
 */
public class MinimumEstimation implements Function<Entry<Key,Document>,Entry<Key,Document>> {

    private static final Logger log = Logger.getLogger(MinimumEstimation.class);

    protected int minimumCount;

    public MinimumEstimation(int minimumCount) {
        this.minimumCount = minimumCount;
    }

    /*
     * (non-Javadoc)
     *
     * @see com.google.common.base.Function#apply(java.lang.Object)
     */

    @Override
    public Entry<Key,Document> apply(Entry<Key,Document> input) {

        Document currentDoc = new Document();
        TreeMultimap<String,Attribute<? extends Comparable<?>>> newDocumentAttributes = TreeMultimap.create();
        Key topKey = null;

        if (topKey == null)
            topKey = input.getKey();
        currentDoc = input.getValue();

        Map<?,?> currentAttr = currentDoc.getDictionary();

        for (Entry<?,?> attrE : currentAttr.entrySet()) {
            Entry<String,Attribute<?>> attr = (Entry<String,Attribute<?>>) attrE;

            if (!attr.getKey().equals(Document.DOCKEY_FIELD_NAME)) {
                if (attr.getValue() instanceof Attributes) {
                    Attributes attrs = (Attributes) attr.getValue();

                    NavigableSet<Attribute<? extends Comparable<?>>> attributes = newDocumentAttributes.get(attr.getKey());

                    for (Attribute<?> myAttribute : attrs.getAttributes()) {

                        if (log.isTraceEnabled())
                            log.trace("Attributes for " + attr.getKey() + " " + attributes.iterator().hasNext());

                        if (myAttribute instanceof Cardinality) {

                            Cardinality card = (Cardinality) myAttribute;

                            if (card.getContent().getEstimate().cardinality() >= minimumCount) {
                                newDocumentAttributes.put(attr.getKey(), myAttribute);
                            }

                        } else
                            throw new RuntimeException("Have " + myAttribute.getClass());
                    }

                } else {
                    Cardinality card = (Cardinality) attr.getValue();
                    if (card.getContent().getEstimate().cardinality() >= minimumCount) {
                        newDocumentAttributes.put(attr.getKey(), card);
                    }

                }
            }
        }

        currentDoc = new Document();
        if (log.isTraceEnabled())
            log.trace("entries" + newDocumentAttributes.entries());
        currentDoc.putAll(newDocumentAttributes.entries().iterator(), false);
        if (log.isTraceEnabled())
            log.trace("currentDoc" + currentDoc);
        return Maps.immutableEntry(topKey, currentDoc);
    }
}
