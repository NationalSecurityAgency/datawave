package datawave.query.function;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.TreeMultimap;

import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Cardinality;
import datawave.query.attributes.Document;
import datawave.query.attributes.FieldValueCardinality;
import datawave.query.tables.facets.FacetedConfiguration;

/**
 *
 */
public class FacetedGrouping implements Function<Entry<Key,Document>,Entry<Key,Document>> {

    private static final Text EMPTY_TEXT = new Text();
    private static final Logger log = Logger.getLogger(FacetedGrouping.class);

    protected FacetedConfiguration config;

    protected TreeMultimap<String,FieldValueCardinality> cachedAttributeRanges = TreeMultimap.create();

    public FacetedGrouping(FacetedConfiguration config) {
        this.config = config;

    }

    /*
     * (non-Javadoc)
     *
     * @see com.google.common.base.Function#apply(java.lang.Object)
     */

    @Override
    public Entry<Key,Document> apply(Entry<Key,Document> input) {

        Document currentDoc = input.getValue();
        // list of document attributes to update.
        TreeMultimap<String,Attribute<?>> newDocumentAttributes = TreeMultimap.create();
        Key topKey = null;

        if (topKey == null)
            topKey = input.getKey();
        currentDoc = input.getValue();

        Map<?,?> currentAttr = currentDoc.getDictionary();

        for (Entry<?,?> attrE : currentAttr.entrySet()) {
            Entry<String,Attribute<?>> attr = (Entry<String,Attribute<?>>) attrE;

            if (!attr.getKey().equals(Document.DOCKEY_FIELD_NAME)) {
                if (attr.getValue() instanceof Attributes) {

                    Attributes newAttrs = new Attributes(attr.getValue().isToKeep());

                    Set<Attribute<? extends Comparable<?>>> attributes = ((Attributes) attr.getValue()).getAttributes();
                    if (log.isTraceEnabled())
                        log.trace(attr.getKey() + " is attributes, size is " + attributes.size());

                    Collection<FieldValueCardinality> cardList = cachedAttributeRanges.get(attr.getKey());

                    // we already know that we will exceed the list size
                    if (cardList.size() + attributes.size() > config.getMaximumFacetGroupCount()) {
                        if (log.isTraceEnabled())
                            log.trace("cardinality exceeds maximum facet count");
                        cardList = adjustAttributeGrouping(cardList, attributes);

                    }

                    List<Cardinality> newCardList = Lists.newArrayList();
                    for (FieldValueCardinality fvcBucket : cardList) {

                        FieldValueCardinality fvc = new FieldValueCardinality();
                        fvc.setContent(fvcBucket.getFloorValue());
                        fvc.setCeiling(fvcBucket.getCeilingValue());

                        // for cardinalities, only use the visibility metadata
                        Key metadata = new Key(EMPTY_TEXT, EMPTY_TEXT, EMPTY_TEXT, attr.getValue().getColumnVisibility(), -1);

                        Cardinality card = new Cardinality(fvc, attr.getValue().getMetadata(), newAttrs.isToKeep());
                        newCardList.add(card);
                    }

                    for (Attribute<? extends Comparable<?>> myAttributeList : attributes) {

                        Cardinality card = (Cardinality) myAttributeList;

                        boolean foundBucket = false;
                        for (Cardinality fvcBucket : newCardList) {
                            if (fvcBucket.getContent().isWithin(card.getContent())) {
                                try {
                                    fvcBucket.getContent().merge(card.getContent());
                                    foundBucket = true;
                                } catch (CardinalityMergeException e) {
                                    throw new RuntimeException(e);
                                }
                                break;
                            }
                        }

                        if (!foundBucket) {
                            newCardList.add(card);
                        }

                    }

                    for (Cardinality cardBucket : newCardList) {

                        newAttrs.add(cardBucket);

                        cachedAttributeRanges.put(attr.getKey(), cardBucket.getContent());

                    }

                    newDocumentAttributes.put(attr.getKey(), newAttrs);

                } // ignore none Attributes attributes
                else {
                    if (log.isTraceEnabled())
                        log.trace(attr.getKey() + " is " + attr.getValue().getClass());
                }
            }
        }

        if (log.isTraceEnabled())
            log.trace("entries" + newDocumentAttributes.entries());
        for (Entry<String,Attribute<?>> newAttr : newDocumentAttributes.entries()) {
            currentDoc.replace(newAttr.getKey(), newAttr.getValue(), false);
        }
        if (log.isTraceEnabled())
            log.trace("currentDoc" + currentDoc);
        return Maps.immutableEntry(topKey, currentDoc);
    }

    /**
     * @param cardList
     *            list of cardinalities
     * @param attributes
     *            set of attributes
     * @return the list of cardinalities
     */
    private List<FieldValueCardinality> adjustAttributeGrouping(Collection<FieldValueCardinality> cardList,
                    Set<Attribute<? extends Comparable<?>>> attributes) {

        boolean found = false;

        List<FieldValueCardinality> newCardList = Lists.newArrayList();
        List<FieldValueCardinality> fixNewList = Lists.newArrayList(cardList);
        for (Attribute<? extends Comparable<?>> attribute : attributes) {
            Cardinality card = (Cardinality) attribute;
            found = false;
            for (FieldValueCardinality fvcCard : cardList) {

                card.getContent().isWithin(fvcCard);
                found = true;
                break;
            }
            if (found)
                continue;

            fixNewList.add(card.getContent());
        }

        if (fixNewList.size() > config.getMaximumFacetGroupCount()) {
            // we've exceeded, so let's get a minimum adjustment factor
            final int groupAdjustmentFactor = (int) Math.ceil((double) fixNewList.size() / config.getMaximumFacetGroupCount());

            // partition the list using the previously created adjustment factor.
            // for (List<FieldValueCardinality> list : Iterables.partition(fixNewList, groupAdjustmentFactor)) {
            Collections.sort(fixNewList);
            for (int i = 0; i < fixNewList.size(); i += groupAdjustmentFactor) {

                FieldValueCardinality first = fixNewList.get(i);
                FieldValueCardinality last = fixNewList.get(i + (groupAdjustmentFactor - 1));
                FieldValueCardinality newCard = new FieldValueCardinality();
                newCard.setContent(first.getFloorValue());
                newCard.setCeiling(last.getCeilingValue());
                if (log.isTraceEnabled())
                    log.trace("Creating new bucket " + first.getFloorValue() + " " + last.getCeilingValue());
                newCardList.add(newCard);
                if (newCardList.size() + 1 > config.getMaximumFacetGroupCount()) {
                    newCard.setCeiling(Iterables.getLast(fixNewList).getCeilingValue());
                    break;
                }
            }

        } else {
            newCardList = fixNewList;
        }

        return newCardList;
    }
}
