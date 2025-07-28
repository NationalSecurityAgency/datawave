package datawave.query.function;

import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.query.Constants;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Content;
import datawave.query.attributes.Document;
import datawave.query.attributes.Numeric;
import datawave.query.util.Tuple2;
import datawave.util.StringUtils;

/**
 * <p>
 * LimitFields will reduce the attributes in a document given the limits specified for fields. Attributes that are in the set of hits for a document will never
 * be dropped. Also matching field sets can be specified which will avoid dropping fields in the same group when the values match between the fields in a
 * matching field set. For example given the following field/values:
 * </p>
 * <ul>
 * <li>NAME.PERSON.1 = sam</li>
 * <li>AGE.PERSON.1 = 10</li>
 * <li>NAME.PERSON.2 = frank</li>
 * <li>AGE.PERSON.2 = 11</li>
 * <li>ACTOR.ACTOR.1 = sam</li>
 * <li>FILM.ACTOR.1 = Johnny Goes Home</li>
 * <li>ACTOR.ACTOR.2 = frank</li>
 * <li>FILM.ACTOR.2 = Johnny Head</li>
 * </ul>
 * <ul>
 * <li>and limit fields NAME=-1, AGE=-1, FILM=-1</li>
 * <li>and a matching field set of NAME=ACTOR</li>
 * <li>and a hit term of FILM.ACTOR.1=Johnny Goes Home</li>
 * </ul>
 * <p>
 * In this case the following fields should be returned:
 * </p>
 * <ul>
 * <li>ACTOR.ACTOR.1 = sam</li>
 * <li>FILM.ACTOR.1 = Johnny Goes Home</li>
 * <li>NAME.PERSON.1 = sam</li>
 * <li>AGE.PERSON.1 = 10</li>
 * </ul>
 */
public class LimitFields implements Function<Entry<Key,Document>,Entry<Key,Document>> {

    private static final Logger log = Logger.getLogger(LimitFields.class);

    public static final String ORIGINAL_COUNT_SUFFIX = "_ORIGINAL_COUNT";

    // A map of fields and the number of values to limit the fields by.
    private final Map<String,Integer> limitFieldsMap;

    // A collection of field sets where if the values match then those values should not be dropped.
    private final Set<Set<String>> matchingFieldSets;

    /**
     * Return the commonality (instance) and grouping context of the given key.
     *
     * @param key
     *            the key
     * @return the commonality and grouping context
     */
    static Tuple2<String,String> getCommonalityAndGroupingContext(String key) {
        String[] splits = StringUtils.split(key, '.');
        if (splits.length >= 3) {
            // return the first group and last group (a.k.a the instance in the first group)
            return new Tuple2<>(splits[1], splits[splits.length - 1]);
        }
        return null;
    }

    public LimitFields(Map<String,Integer> limitFieldsMap, Set<Set<String>> matchingFieldSets) {
        this.limitFieldsMap = limitFieldsMap;
        this.matchingFieldSets = matchingFieldSets;
        if (log.isTraceEnabled())
            log.trace("limitFieldsMap set to:" + limitFieldsMap);
    }

    @Override
    public Entry<Key,Document> apply(Entry<Key,Document> entry) {
        Document document = entry.getValue();
        LimitFieldsTracker tracker = new LimitFieldsTracker(matchingFieldSets);

        findHits(document, tracker);
        retainNonHitsForMatchingFieldSets(document, tracker);
        retainNonHitsUpToLimit(document, tracker);
        reduceDocument(document, tracker);

        return entry;
    }

    /**
     * Find all direct hits in the document.
     *
     * @param document
     *            the document
     * @param tracker
     *            the tracker
     */
    private void findHits(Document document, LimitFieldsTracker tracker) {
        Multimap<String,String> hitTermMap = getHitTermMap(document);
        Set<Attribute<?>> hitTermAttributes = getHitTermAttributes(document);

        // first pass is to set all of the hits to be kept, the misses to drop, and count em all
        for (Map.Entry<String,Attribute<? extends Comparable<?>>> de : document.entrySet()) {
            String keyWithGrouping = de.getKey();
            String keyNoGrouping = removeGrouping(keyWithGrouping);

            // if there is an _ANYFIELD_ entry in the limitFieldsMap, then insert every key that is not yet in the map, using the
            // limit value for _ANYFIELD_
            if (isLimited(Constants.ANY_FIELD) && !isLimited(keyNoGrouping)) {
                limitField(keyNoGrouping, getLimit(Constants.ANY_FIELD));
                log.trace("added " + keyNoGrouping + " - " + getLimit(keyNoGrouping) + " to the limitFieldsMap because of the _ANYFIELD_ entry");
            }

            if (isLimited(keyNoGrouping)) { // look for the key without the grouping context
                if (log.isTraceEnabled()) {
                    log.trace("limitFieldsMap contains " + keyNoGrouping);
                }

                Attribute<?> attr = de.getValue();
                if (attr instanceof Attributes) {
                    Attributes attrs = (Attributes) attr;
                    Set<Attribute<? extends Comparable<?>>> attrSet = attrs.getAttributes();
                    for (Attribute<? extends Comparable<?>> value : attrSet) {
                        evaluateForHit(tracker, hitTermMap, hitTermAttributes, keyWithGrouping, keyNoGrouping, value);
                    }
                } else {
                    evaluateForHit(tracker, hitTermMap, hitTermAttributes, keyWithGrouping, keyNoGrouping, attr);
                }
            }
        }
    }

    /**
     * Return a hit term map constructed from the document's {@value JexlEvaluation#HIT_TERM_FIELD} entry.
     *
     * @param document
     *            the document
     * @return the hit term map
     */
    private Multimap<String,String> getHitTermMap(Document document) {
        Multimap<String,String> attrMap = HashMultimap.create();
        fillHitTermMap(document.get(JexlEvaluation.HIT_TERM_FIELD), attrMap);
        return attrMap;
    }

    /**
     * Fills the given map with hit terms extracted from the given attribute, recursively so if the attribute is an {@link Attributes}.
     *
     * @param attr
     *            the attribute
     * @param attrMap
     *            the map
     */
    private void fillHitTermMap(Attribute<?> attr, Multimap<String,String> attrMap) {
        if (attr != null) {
            if (attr instanceof Attributes) {
                Attributes attrs = (Attributes) attr;
                for (Attribute<?> at : attrs.getAttributes()) {
                    fillHitTermMap(at, attrMap);
                }
            } else if (attr instanceof Content) {
                Content content = (Content) attr;
                // split the content into its fieldname:value
                String contentString = content.getContent();
                int colonPos = contentString.indexOf(Constants.COLON);
                attrMap.put(contentString.substring(0, colonPos), contentString.substring(colonPos + 1));
            }
        }
    }

    /**
     * Return the sets of singular attributes in the given document from the {@link JexlEvaluation#HIT_TERM_FIELD} entry.
     *
     * @param document
     *            the document
     * @return the set of individual attributes
     */
    private Set<Attribute<?>> getHitTermAttributes(Document document) {
        Set<Attribute<?>> attributesSet = new HashSet<>();
        Attribute<?> attributes = document.get(JexlEvaluation.HIT_TERM_FIELD);
        fillHitTermSet(attributes, attributesSet);
        return attributesSet;
    }

    /**
     * Adds singular attributes to the given set, recursively so if the attribute is an {@link Attributes}.
     *
     * @param attr
     *            the attribute
     * @param attributesSet
     *            the set
     */
    private void fillHitTermSet(Attribute<?> attr, Set<Attribute<?>> attributesSet) {
        if (attr != null) {
            if (attr instanceof Attributes) {
                Attributes attrs = (Attributes) attr;
                for (Attribute<?> at : attrs.getAttributes()) {
                    fillHitTermSet(at, attributesSet);
                }
            } else if (attr instanceof Content) {
                Content content = (Content) attr;
                if (content.getSource() != null) {
                    attributesSet.add(content.getSource());
                }
            }
        }
    }

    /**
     * Evaluate the given key and attribute to see if it is a hit. Hits will be marked as a hit in the tracker. Non-hits will be marked in the tracker as both a
     * non hit and a potential hit, and the attribute will be set as not to keep.
     *
     * @param tracker
     *            the tracker
     * @param hitTermMap
     *            the hit term map
     * @param hitTermAttributes
     *            the hit term attributes
     * @param keyWithGrouping
     *            the key with the grouping context
     * @param keyNoGrouping
     *            the key without the grouping context
     * @param value
     *            the attribute
     */
    private void evaluateForHit(LimitFieldsTracker tracker, Multimap<String,String> hitTermMap, Set<Attribute<?>> hitTermAttributes, String keyWithGrouping,
                    String keyNoGrouping, Attribute<? extends Comparable<?>> value) {
        if (isHit(keyWithGrouping, value, hitTermMap, hitTermAttributes)) {
            tracker.incrementHit(keyNoGrouping);
            tracker.addHit(keyNoGrouping, value);
        } else {
            value.setToKeep(false);
            tracker.incrementNonHit(keyNoGrouping);
            tracker.incrementAttributesToDrop();
            tracker.addPotential(keyNoGrouping, keyWithGrouping, value);
        }
        tracker.incrementFieldCount(keyNoGrouping);
    }

    /**
     * Determine whether this attribute is one of the hits. It is a hit if it has a matching value, or if another attribute in the same group has a hit. This
     * allows us to keep all attributes that are part of the same group.
     *
     * @param keyWithGrouping
     *            the string key
     * @param attr
     *            the attribute
     * @param hitTermMap
     *            the hit term map
     * @param hitTermAttributes
     *            hit term attributes from the document
     * @return true if a hit
     */
    private boolean isHit(String keyWithGrouping, Attribute<?> attr, Multimap<String,String> hitTermMap, Set<Attribute<?>> hitTermAttributes) {
        if (hitTermMap.containsKey(keyWithGrouping) && hitTermAttributes.contains(attr)) {
            return true;
        }

        // If not already returned as a value match, then lets include those that are
        // part of the same group and instance as some other hit.
        if (!hitTermMap.isEmpty()) {
            Tuple2<String,String> keyTokens = LimitFields.getCommonalityAndGroupingContext(keyWithGrouping);
            if (keyTokens != null) {
                String keyWithGroupingCommonality = keyTokens.first();
                String keyWithGroupingSuffix = keyTokens.second();

                for (String key : hitTermMap.keySet()) {
                    // Get the commonality from the hit term key.
                    Tuple2<String,String> commonalityAndGroupingContext = LimitFields.getCommonalityAndGroupingContext(key);
                    if (commonalityAndGroupingContext != null) {
                        String hitTermKeyCommonality = commonalityAndGroupingContext.first();
                        String hitTermGroup = commonalityAndGroupingContext.second();
                        if (hitTermKeyCommonality.equals(keyWithGroupingCommonality) && keyWithGroupingSuffix.equals(hitTermGroup)) {
                            return true;
                        }
                    }
                }
            }
        }

        return false;
    }

    /**
     * Retain all non-hits where a field is limited and has a matching group to the matching field sets.
     *
     * @param document
     *            the document
     * @param tracker
     *            the tracker
     */
    private void retainNonHitsForMatchingFieldSets(Document document, LimitFieldsTracker tracker) {
        // This pass is to process the limited fields that have matching groups
        tracker.processMatches();
        if (tracker.hasMatches()) {
            for (Map.Entry<String,Attribute<? extends Comparable<?>>> de : document.entrySet()) {
                String keyWithGrouping = de.getKey();
                String keyNoGrouping = removeGrouping(keyWithGrouping);

                // if this was a limited field
                if (isLimited(keyNoGrouping)) {

                    // if we have matching group
                    if (tracker.isMatchingGroup(keyWithGrouping)) {
                        Attribute<?> attr = de.getValue();
                        if (attr instanceof Attributes) {
                            Attributes attrs = (Attributes) attr;
                            Set<Attribute<? extends Comparable<?>>> attrSet = attrs.getAttributes();

                            for (Attribute<? extends Comparable<?>> value : attrSet) {
                                // if this was an attribute previously set to not keep, then it is one of the misses (not a hit)
                                if (!value.isToKeep()) {
                                    markNonHitAsHit(keyNoGrouping, tracker, value);
                                }
                            }
                        } else {
                            // if this was an attribute previously set to not keep, then it is one of the misses (not a hit)
                            if (!attr.isToKeep()) {
                                markNonHitAsHit(keyNoGrouping, tracker, attr);
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * Retain non-hits up to the limits established for specified limited fields.
     *
     * @param document
     *            the document
     * @param tracker
     *            the tracker
     */
    private void retainNonHitsUpToLimit(Document document, LimitFieldsTracker tracker) {
        // Third pass is to set any misses back to be kept if the limit allows
        for (Map.Entry<String,Attribute<? extends Comparable<?>>> de : document.entrySet()) {
            String keyWithGrouping = de.getKey();
            String keyNoGrouping = removeGrouping(keyWithGrouping);

            // Look for the key without the grouping context
            if (isLimited(keyNoGrouping)) {
                int limit = getLimit(keyNoGrouping);

                // Short circuit if we are not actually limiting this field.
                // This is keeping with the original logic where a negative limit means to keep only hits
                if (limit <= 0) {
                    continue;
                }

                int keepers = tracker.getTotalHits(keyNoGrouping);
                int missesToSet = Math.min(limit - keepers, tracker.getTotalNonHits(keyNoGrouping));

                // if we have misses yet to keep
                if (missesToSet > 0) {
                    Attribute<?> attr = de.getValue();
                    if (attr instanceof Attributes) {
                        Attributes attrs = (Attributes) attr;
                        Set<Attribute<? extends Comparable<?>>> attrSet = attrs.getAttributes();

                        for (Attribute<? extends Comparable<?>> value : attrSet) {
                            // if this was an attribute previously set to not keep, then it is one of the misses (not a hit)
                            if (!value.isToKeep()) {
                                markNonHitAsHit(keyNoGrouping, tracker, value);
                                missesToSet--;
                                if (missesToSet == 0) {
                                    break;
                                }
                            }
                        }
                    } else {
                        // if this was an attribute previously set to not keep, then it is one of the misses (not a hit)
                        if (!attr.isToKeep()) {
                            markNonHitAsHit(keyNoGrouping, tracker, attr);
                        }
                    }
                }
            }
        }
    }

    /**
     * Return the given key without its grouping context.
     *
     * @param key
     *            the key
     * @return the key stripped of its grouping context
     */
    private String removeGrouping(String key) {
        // if we have grouping context on, remove the grouping context
        int index = key.indexOf('.');
        if (index != -1) {
            key = key.substring(0, index);
        }
        return key;
    }

    /**
     * Mark the given attribute as to keep, and move a non-hit to a hit in the tracker.
     *
     * @param keyNoGrouping
     *            the key without grouping context
     * @param tracker
     *            the tracker
     * @param attribute
     *            the attribute
     */
    private void markNonHitAsHit(String keyNoGrouping, LimitFieldsTracker tracker, Attribute<?> attribute) {
        attribute.setToKeep(true);
        tracker.incrementHit(keyNoGrouping);
        tracker.decrementNonHit(keyNoGrouping);
        tracker.decrementAttributesToDrop();
    }

    /**
     * Reduce the document down, removing any attributes not marked as to keep, and add entries that note the original counts of any field entries that were
     * subsequently removed.
     *
     * @param document
     *            the document
     * @param tracker
     *            the tracker
     */
    private void reduceDocument(Document document, LimitFieldsTracker tracker) {
        if (tracker.getAttributesToDrop() > 0) {
            // Reduce the document to those to keep.
            document.reduceToKeep();

            // Generate fields for original counts.
            for (String keyNoGrouping : tracker.getFields()) {
                // only generate an original count if a field was reduced
                int keepers = tracker.getTotalHits(keyNoGrouping);
                int originalCount = tracker.getFieldCount(keyNoGrouping);
                if (originalCount > keepers) {
                    document.put(keyNoGrouping + ORIGINAL_COUNT_SUFFIX, new Numeric(originalCount, document.getMetadata(), document.isToKeep()), true);

                    // Some sanity checks.
                    int missesRemaining = tracker.getTotalNonHits(keyNoGrouping);
                    int limit = getLimit(keyNoGrouping);
                    int missesToSet = Math.min(limit - keepers, missesRemaining);
                    if (missesToSet > 0) {
                        log.error("Failed to limit fields correctly, " + missesToSet + " attributes failed to be included");
                        throw new RuntimeException(
                                        "Failed to limit fields correctly, " + missesToSet + ' ' + keyNoGrouping + " attributes failed to be included");
                    }
                }
            }
        }
    }

    /**
     * Return whether the given field is limited
     *
     * @param field
     *            the field
     * @return true if the field is limited, or false otherwise
     */
    private boolean isLimited(String field) {
        return this.limitFieldsMap.containsKey(field);
    }

    /**
     * Return the limit for the given field.
     *
     * @param field
     *            the field
     * @return the limit for the field
     */
    private int getLimit(String field) {
        return this.limitFieldsMap.get(field);
    }

    /**
     * Limit the field to the specified limit
     *
     * @param field
     *            the field
     * @param limit
     *            the limit
     */
    private void limitField(String field, int limit) {
        this.limitFieldsMap.put(field, limit);
    }
}
