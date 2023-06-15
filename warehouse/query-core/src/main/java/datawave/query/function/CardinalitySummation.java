package datawave.query.function;

import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;

import datawave.query.attributes.Cardinality;
import datawave.query.data.parsers.DatawaveKey;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Document;

import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.google.common.base.Function;
import com.google.common.collect.Maps;
import com.google.common.collect.TreeMultimap;

/**
 *
 */
public class CardinalitySummation implements Function<Entry<Key,Document>,Entry<Key,Document>> {

    private static final Logger log = Logger.getLogger(CardinalitySummation.class);

    private static final Text MAX_UNICODE = new Text(new String(Character.toChars(Character.MAX_CODE_POINT)));

    protected Document referenceDocument;

    protected Key referenceKey = null;

    protected TreeMultimap<String,Attribute<? extends Comparable<?>>> newDocumentAttributes = null;

    protected boolean merge = false;

    public CardinalitySummation(Key topKey, Document doc) {
        this(topKey, doc, false);
    }

    public CardinalitySummation(Key topKey, Document doc, boolean merge) {

        // reduce the key to the document key pieces only and a max cq in order to ensure the top key
        // sorts after the pieces it is summarizing.
        topKey = new Key(topKey.getRow(), topKey.getColumnFamily(), MAX_UNICODE);

        this.merge = merge;

        referenceDocument = doc;

        DatawaveKey parser = new DatawaveKey(topKey);

        newDocumentAttributes = TreeMultimap.create();

        Map<?,?> currentAttr = referenceDocument.getDictionary();

        for (Entry<?,?> attrE : currentAttr.entrySet()) {
            Entry<String,Attribute<?>> attr = (Entry<String,Attribute<?>>) attrE;

            TreeMultimap<String,Attribute<?>> tmpMap = TreeMultimap.create();

            if (attr.getValue() instanceof Attributes) {
                Attributes attrs = (Attributes) attr.getValue();

                NavigableSet<Attribute<? extends Comparable<?>>> attributes = newDocumentAttributes.get(attr.getKey());

                for (Attribute<?> myAttribute : attrs.getAttributes()) {

                    if (log.isTraceEnabled())
                        log.trace("Attributes for " + attr.getKey() + " " + attributes.iterator().hasNext());

                    if (!attributes.isEmpty()) {
                        boolean foundAmongOthers = false;
                        for (Attribute<?> thoseAttributes : attributes) {
                            if (myAttribute instanceof Cardinality) {
                                if (((Cardinality) myAttribute).equals(thoseAttributes)) {
                                    Cardinality card = (Cardinality) thoseAttributes;
                                    Cardinality otherCard = (Cardinality) myAttribute;

                                    merge(card, otherCard, parser, merge);

                                    if (log.isTraceEnabled())
                                        log.trace("Offering to " + attr.getKey() + " value " + card.getContent().getFloorValue() + " "
                                                        + card.getContent().getCeilingValue());
                                    foundAmongOthers = true;
                                    break;
                                }
                            } else
                                throw new RuntimeException("Have " + myAttribute.getClass());
                        }

                        if (!foundAmongOthers) {
                            if (log.isTraceEnabled())
                                log.trace("put attributes " + attr.getKey() + " " + myAttribute.getData());
                            tmpMap.put(attr.getKey(), myAttribute);
                        }

                        newDocumentAttributes.putAll(tmpMap);

                    } else {
                        if (log.isTraceEnabled())
                            log.trace("adding attributes " + attr.getKey() + " " + myAttribute.getData());
                        newDocumentAttributes.put(attr.getKey(), myAttribute);
                    }

                }
            } else {
                if (log.isTraceEnabled())
                    log.trace("Testing " + attr.getKey() + " " + attr.getValue().getData());
                NavigableSet<Attribute<? extends Comparable<?>>> attributes = newDocumentAttributes.get(attr.getKey());
                Attribute<?> attribute = attributes.floor(attr.getValue());

                boolean found = false;
                for (Attribute<?> thoseAttributes : attributes) {
                    if (thoseAttributes.equals(attr.getValue())) {
                        if (log.isTraceEnabled())
                            log.trace("found for " + attr.getKey() + " " + thoseAttributes.getData());
                        Cardinality card = (Cardinality) thoseAttributes;
                        Cardinality otherCard = (Cardinality) attr.getValue();

                        merge(card, otherCard, parser, merge);

                        found = true;
                        break;
                    } else {

                    }
                }

                if (!found) {

                    if (log.isTraceEnabled())
                        log.trace("Don't have " + attr.getKey() + " " + attr.getValue().getData());
                    newDocumentAttributes.put(attr.getKey(), attr.getValue());
                }

            }
        }
        referenceKey = topKey;
    }

    /*
     * (non-Javadoc)
     *
     * @see com.google.common.base.Function#apply(java.lang.Object)
     */

    @SuppressWarnings("unchecked")
    @Override
    public Entry<Key,Document> apply(Entry<Key,Document> input) {

        Document currentDoc = new Document();

        Key topKey = input.getKey();

        // reduce the key to the document key pieces only and a max cq in order to ensure the top key
        // sorts after the pieces it is summarizing.
        topKey = new Key(topKey.getRow(), topKey.getColumnFamily(), MAX_UNICODE);

        DatawaveKey parser = new DatawaveKey(topKey);

        currentDoc = input.getValue();

        Map<?,?> currentAttr = currentDoc.getDictionary();

        for (Entry<?,?> attrE : currentAttr.entrySet()) {
            Entry<String,Attribute<?>> attr = (Entry<String,Attribute<?>>) attrE;

            TreeMultimap<String,Attribute<?>> tmpMap = TreeMultimap.create();
            if (!attr.getKey().equals(Document.DOCKEY_FIELD_NAME)) {
                if (attr.getValue() instanceof Attributes) {
                    Attributes attrs = (Attributes) attr.getValue();

                    NavigableSet<Attribute<? extends Comparable<?>>> attributes = newDocumentAttributes.get(attr.getKey());

                    for (Attribute<?> myAttribute : attrs.getAttributes()) {

                        if (log.isTraceEnabled())
                            log.trace("Attributes for " + attr.getKey() + " " + attributes.iterator().hasNext());

                        if (!attributes.isEmpty()) {
                            boolean foundAmongOthers = false;
                            for (Attribute<?> thoseAttributes : attributes) {
                                if (myAttribute instanceof Cardinality) {
                                    if (((Cardinality) myAttribute).equals(thoseAttributes)) {
                                        Cardinality card = (Cardinality) thoseAttributes;
                                        Cardinality otherCard = (Cardinality) myAttribute;

                                        merge(card, otherCard, parser, merge);

                                        if (log.isTraceEnabled())
                                            log.trace("Offering to " + attr.getKey() + " value " + card.getContent().getFloorValue() + " "
                                                            + card.getContent().getCeilingValue());
                                        foundAmongOthers = true;
                                        break;
                                    }
                                } else
                                    throw new RuntimeException("Have " + myAttribute.getClass());
                            }

                            if (!foundAmongOthers) {
                                if (log.isTraceEnabled())
                                    log.trace("put attributes " + attr.getKey() + " " + myAttribute.getData());
                                tmpMap.put(attr.getKey(), myAttribute);
                            }

                            newDocumentAttributes.putAll(tmpMap);

                        } else {
                            if (log.isTraceEnabled())
                                log.trace("adding attributes " + attr.getKey() + " " + myAttribute.getData());
                            newDocumentAttributes.put(attr.getKey(), myAttribute);
                        }

                    }
                } else {
                    if (log.isTraceEnabled())
                        log.trace("Testing " + attr.getKey() + " " + attr.getValue().getData());
                    NavigableSet<Attribute<? extends Comparable<?>>> attributes = newDocumentAttributes.get(attr.getKey());
                    Attribute<?> attribute = attributes.floor(attr.getValue());

                    boolean found = false;
                    for (Attribute<?> thoseAttributes : attributes) {
                        if (thoseAttributes.equals(attr.getValue())) {
                            if (log.isTraceEnabled())
                                log.trace("found for " + attr.getKey() + " " + thoseAttributes.getData());
                            Cardinality card = (Cardinality) thoseAttributes;
                            Cardinality otherCard = (Cardinality) attr.getValue();

                            merge(card, otherCard, parser, merge);

                            found = true;
                            break;
                        } else {

                        }
                    }

                    if (!found) {

                        if (log.isTraceEnabled())
                            log.trace("Don't have " + attr.getKey() + " " + attr.getValue().getData());
                        newDocumentAttributes.put(attr.getKey(), attr.getValue());
                    }

                }
            }
        }

        referenceDocument = new Document();
        if (log.isTraceEnabled())
            log.trace("entries" + newDocumentAttributes.entries());
        referenceDocument.putAll(newDocumentAttributes.entries().iterator(), false);
        if (log.isTraceEnabled())
            log.trace("currentDoc" + referenceDocument);
        referenceKey = topKey;
        return Maps.immutableEntry(topKey, referenceDocument);
    }

    public Entry<Key,Document> getTop() {
        return Maps.immutableEntry(referenceKey, referenceDocument);
    }

    protected void merge(Cardinality originalCardinality, Cardinality cardinalityToMerge, DatawaveKey keyParser, boolean merge) {
        if (merge) {
            try {
                originalCardinality.getContent().merge(cardinalityToMerge.getContent());
            } catch (CardinalityMergeException e) {
                throw new RuntimeException();
            }
        } else {
            originalCardinality.getContent().getEstimate().offer(keyParser.getUid());
        }
    }
}
