package datawave.query.function;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;

import datawave.query.Constants;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Content;
import datawave.query.attributes.Document;
import datawave.query.attributes.Numeric;
import datawave.query.attributes.PreNormalizedAttribute;
import datawave.query.attributes.TypeAttribute;

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

    private static final Logger log = LoggerFactory.getLogger(LimitFields.class);

    public static final String ORIGINAL_COUNT_SUFFIX = "_ORIGINAL_COUNT";

    private static final String COLON = ":";

    // A map of fields and the number of values to limit the fields by.
    private final Map<String,Integer> limitFieldsMap;

    // A collection of field sets where if the values match then those values should not be dropped.
    private final Set<Set<String>> matchingFieldSets;

    // _ANYFIELD_ is present in the limit fields map
    private boolean anyFieldLimitExists;

    // _ANYFIELD_ value as the limit field
    private int anyFieldLimitValue;

    public LimitFields(Map<String,Integer> limitFieldsMap, Set<Set<String>> matchingFieldSets) {
        this.limitFieldsMap = limitFieldsMap;
        this.matchingFieldSets = matchingFieldSets;
        if (limitFieldsMap.containsKey(Constants.ANY_FIELD)) {
            this.anyFieldLimitExists = true;
            this.anyFieldLimitValue = limitFieldsMap.get(Constants.ANY_FIELD);
        }
        log.trace("limitFieldsMap set to:{}", limitFieldsMap);
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
        HitTermContext hitTermContext = getHitTermContext(document);

        // first pass is to set all of the hits to be kept, the misses to drop, and count em all
        for (Map.Entry<String,Attribute<? extends Comparable<?>>> de : document.entrySet()) {
            String keyWithGrouping = de.getKey();
            String keyNoGrouping = removeGrouping(keyWithGrouping);

            // if there is an _ANYFIELD_ entry in the limitFieldsMap, then insert every key that is not yet in the map, using the
            // limit value for _ANYFIELD_
            if (anyFieldLimitExists && !isLimited(keyNoGrouping)) {
                limitField(keyNoGrouping, anyFieldLimitValue);
                if (log.isTraceEnabled()) {
                    log.trace("added {} - {} to the limitFieldsMap because of the _ANYFIELD_ entry", keyNoGrouping, getLimit(keyNoGrouping));
                }
            }

            if (isLimited(keyNoGrouping)) { // look for the key without the grouping context
                log.trace("limitFieldsMap contains {}", keyNoGrouping);
                FieldName fieldName = FieldName.parse(keyWithGrouping);
                Attribute<?> attr = de.getValue();
                if (attr instanceof Attributes) {
                    Attributes attrs = (Attributes) attr;
                    Set<Attribute<? extends Comparable<?>>> attrSet = attrs.getAttributes();
                    for (Attribute<? extends Comparable<?>> value : attrSet) {
                        evaluateForHit(tracker, hitTermContext, fieldName, keyNoGrouping, value);
                    }
                } else {
                    evaluateForHit(tracker, hitTermContext, fieldName, keyNoGrouping, attr);
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
    private HitTermContext getHitTermContext(Document document) {
        HitTermContext.Builder builder = new HitTermContext.Builder();
        fillHitTermBuilder(document.get(JexlEvaluation.HIT_TERM_FIELD), builder);
        return builder.build();
    }

    /**
     * Fills the given map with hit terms extracted from the given attribute, recursively so if the attribute is an {@link Attributes}.
     *
     * @param attr
     *            the attribute
     */
    private void fillHitTermBuilder(Attribute<?> attr, HitTermContext.Builder builder) {
        if (attr != null) {
            if (attr instanceof Attributes) {
                Attributes attrs = (Attributes) attr;
                for (Attribute<?> at : attrs.getAttributes()) {
                    fillHitTermBuilder(at, builder);
                }
            } else if (attr instanceof Content) {
                Content content = (Content) attr;
                // split the content into its fieldname:value
                String contentString = content.getContent();
                String fieldName = contentString.substring(0, contentString.indexOf(COLON));
                if (content.getSource() != null) {
                    builder.putHitField(fieldName, content.getSource());
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
     * @param hitTermContext
     *            the hit term context
     * @param fieldName
     *            the parsed field name
     * @param keyNoGrouping
     *            the key without the grouping context
     * @param value
     *            the attribute
     */
    private void evaluateForHit(LimitFieldsTracker tracker, HitTermContext hitTermContext, FieldName fieldName, String keyNoGrouping,
                                Attribute<? extends Comparable<?>> value) {
        if (isHit(fieldName, value, hitTermContext)) {
            tracker.incrementHit(keyNoGrouping);
            tracker.addHit(keyNoGrouping, value);
        } else {
            value.setToKeep(false);
            tracker.incrementNonHit(keyNoGrouping);
            tracker.incrementAttributesToDrop();
            tracker.addPotential(keyNoGrouping, fieldName.getName(), value);
        }
        tracker.incrementFieldCount(keyNoGrouping);
    }

    /**
     * Determine whether this attribute is one of the hits. It is a hit if it has a matching value, or if another attribute in the same group has a hit. This
     * allows us to keep all attributes that are part of the same group.
     *
     * @param fieldName
     *            the parsed field name
     * @param attr
     *            the attribute
     * @param hitTermContext
     *            the hit term context
     * @return true if a hit
     */
    private boolean isHit(FieldName fieldName, Attribute<?> attr, HitTermContext hitTermContext) {
        if (hitTermContext.isEmpty()) {
            return false;
        } else if (hitTermContext.containsFieldWithGrouping(fieldName.getName()) && hitTermContext.isAttributeHitTerm(attr)) {
            return true;
        }

        // If not already returned as a value match, then lets include those that are
        // part of the same group and instance as some other hit.
        if (fieldName.isGrouped() && hitTermContext.hasGroupAndInstance(fieldName.getGroupAndInstance())) {
            return true;
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
                        log.error("Failed to limit fields correctly, {} attributes failed to be included", missesToSet);
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

    static class HitTermContext {
        private final Set<String> termNames;
        private final Set<FieldName.GroupAndInstance> groupingSet;
        private final Set<Attribute<?>> termAttributes;
        // hit term value string to the metadata keys of the hit term attributes carrying that value. Multiple hit terms
        // may share a value (e.g. the same value under different visibilities, or in event and index forms).
        private final Map<String,Set<Key>> termDataMap;

        HitTermContext(Set<String> fieldNames, Set<FieldName.GroupAndInstance> groupingSet, Set<Attribute<?>> attributes) {
            this.termNames = fieldNames;
            this.groupingSet = groupingSet;
            this.termAttributes = attributes;
            this.termDataMap = new HashMap<>(attributes.size());
            for (Attribute<?> attr : attributes) {
                this.termDataMap.computeIfAbsent(getDataString(attr), k -> new HashSet<>()).add(attr.getMetadata());
            }
        }

        /**
         * Checks if the group and instance is present in the context
         *
         * @param token
         *            the group and instance to test
         * @return true if the group and instance is present, otherwise false
         */
        boolean hasGroupAndInstance(FieldName.GroupAndInstance token) {
            return groupingSet.contains(token);
        }

        /**
         * Checks if the context is empty
         *
         * @return true if the context is empty, otherwise false
         */
        boolean isEmpty() {
            return termNames.isEmpty();
        }

        /**
         * Check if the context contains the field as a hit-term
         *
         * @param keyWithGrouping
         *            the key to check
         * @return true if the context contains the key, otherwise false
         */
        boolean containsFieldWithGrouping(String keyWithGrouping) {
            return termNames.contains(keyWithGrouping);
        }

        /**
         * This method is preferred over a simple <code>set.contains()</code> call because the attribute being compared may not be a TypeAttribute.
         *
         * @param attr
         *            the attribute being compared
         * @return true if the attribute is also a hit term
         */
        boolean isAttributeHitTerm(Attribute<?> attr) {
            // Check if the attribute value matches one of the hit-term source values
            Set<Key> hitTermKeys = termDataMap.get(getDataString(attr));

            // Check if the get was successful (meaning a value match) and then double-check the attribute's key
            // matches one of the hit-term attributes carrying that value
            return hitTermKeys != null && hitTermKeys.contains(attr.getMetadata());
        }

        /**
         * Gets the hit term attributes in context
         *
         * @return the hit term attributes
         */
        Collection<Attribute<?>> getHitTermAttributes() {
            return termAttributes;
        }

        /**
         * Get the group and instance set for hit-term fields
         *
         * @return the group and instance set
         */
        Set<FieldName.GroupAndInstance> getGroupAndInstanceSet() {
            return groupingSet;
        }

        private static String getDataString(Attribute<?> attr) {
            if (attr instanceof PreNormalizedAttribute) {
                return ((PreNormalizedAttribute) attr).getValue();
            }
            if (attr instanceof TypeAttribute) {
                return ((TypeAttribute<?>) attr).getType().getDelegateAsString();
            }
            return String.valueOf(attr.getData());
        }

        static class Builder {
            private final Set<String> hitTermFields;
            private final HashSet<Attribute<?>> hitTermSourceAttributes;

            Builder() {
                this.hitTermFields = new HashSet<>();
                this.hitTermSourceAttributes = new HashSet<>();
            }

            HitTermContext.Builder putHitField(String fieldName, Attribute<?> sourceAttribute) {
                hitTermFields.add(fieldName);
                hitTermSourceAttributes.add(sourceAttribute);
                return this;
            }

            HitTermContext build() {
                Set<FieldName.GroupAndInstance> groupSet = new HashSet<>(hitTermFields.size());
                for (String field : hitTermFields) {
                    FieldName fieldName = FieldName.parse(field);
                    if (fieldName.isGrouped()) {
                        groupSet.add(fieldName.getGroupAndInstance());
                    }
                }
                return new HitTermContext(hitTermFields, groupSet, hitTermSourceAttributes);
            }
        }
    }
}
