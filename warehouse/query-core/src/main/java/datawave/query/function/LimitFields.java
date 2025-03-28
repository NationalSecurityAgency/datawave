package datawave.query.function;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Content;
import datawave.query.attributes.Document;
import datawave.query.attributes.Numeric;
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

    // A map of fields and the number of values to limit the fields by
    private final Map<String,Integer> limitFieldsMap;

    // A collection of field sets where if the values match then those values
    // should not be dropped
    private final Set<Set<String>> matchingFieldSets;

    public LimitFields(Map<String,Integer> limitFieldsMap, Set<Set<String>> matchingFieldSets) {
        this.limitFieldsMap = limitFieldsMap;
        this.matchingFieldSets = matchingFieldSets;
        if (log.isTraceEnabled())
            log.trace("limitFieldsMap set to:" + limitFieldsMap);
    }

    @Override
    public Entry<Key,Document> apply(Entry<Key,Document> entry) {
        Document document = entry.getValue();
        Multimap<String,String> hitTermMap = this.getHitTermMap(document);
        Set<Attribute<?>> hitTermAttributes = getHitTermAttributes(document);

        CountMap countForFieldMap = new CountMap();
        CountMap countMissesRemainingForFieldMap = new CountMap();
        CountMap countKeepersForFieldMap = new CountMap();

        MatchingFieldGroups matchingFieldGroups = new MatchingFieldGroups(matchingFieldSets);

        int attributesToDrop = 0;

        // first pass is to set all of the hits to be kept, the misses to drop, and count em all
        for (Map.Entry<String,Attribute<? extends Comparable<?>>> de : document.entrySet()) {
            String keyWithGrouping = de.getKey();
            String keyNoGrouping = removeGrouping(keyWithGrouping);

            // if there is an _ANYFIELD_ entry in the limitFieldsMap, then insert every key that is not yet in the map, using the
            // limit value for _ANYFIELD_
            if (this.limitFieldsMap.containsKey("_ANYFIELD_") && this.limitFieldsMap.containsKey(keyNoGrouping) == false) {
                this.limitFieldsMap.put(keyNoGrouping, this.limitFieldsMap.get("_ANYFIELD_"));
                log.trace("added " + keyNoGrouping + " - " + this.limitFieldsMap.get(keyNoGrouping) + " to the limitFieldsMap because of the _ANYFIELD_ entry");
            }

            if (this.limitFieldsMap.containsKey(keyNoGrouping)) { // look for the key without the grouping context
                if (log.isTraceEnabled())
                    log.trace("limitFieldsMap contains " + keyNoGrouping);

                Attribute<?> attr = de.getValue();
                int keepers = countKeepersForFieldMap.get(keyNoGrouping);
                int missesRemaining = countMissesRemainingForFieldMap.get(keyNoGrouping);
                int total = countForFieldMap.get(keyNoGrouping);
                if (attr instanceof Attributes) {
                    Attributes attrs = (Attributes) attr;
                    Set<Attribute<? extends Comparable<?>>> attrSet = attrs.getAttributes();
                    for (Attribute<? extends Comparable<?>> value : attrSet) {
                        if (isHit(keyWithGrouping, value, hitTermMap, hitTermAttributes)) {
                            keepers++;
                            matchingFieldGroups.addHit(keyNoGrouping, value);
                        } else {
                            value.setToKeep(false);
                            missesRemaining++;
                            attributesToDrop++;
                            matchingFieldGroups.addPotential(keyNoGrouping, keyWithGrouping, value);
                        }
                        total++;
                    }
                } else {
                    if (isHit(keyWithGrouping, attr, hitTermMap, hitTermAttributes)) {
                        keepers++;
                        matchingFieldGroups.addHit(keyNoGrouping, attr);
                    } else {
                        attr.setToKeep(false);
                        missesRemaining++;
                        attributesToDrop++;
                        matchingFieldGroups.addPotential(keyNoGrouping, keyWithGrouping, attr);
                    }
                    total++;
                }
                countKeepersForFieldMap.put(keyNoGrouping, keepers);
                countMissesRemainingForFieldMap.put(keyNoGrouping, missesRemaining);
                countForFieldMap.put(keyNoGrouping, total);
            }
        }

        // the second pass is to process the limited fields that have matching groups
        matchingFieldGroups.processMatches();
        if (matchingFieldGroups.hasMatches()) {
            for (Map.Entry<String,Attribute<? extends Comparable<?>>> de : document.entrySet()) {
                String keyWithGrouping = de.getKey();
                String keyNoGrouping = removeGrouping(keyWithGrouping);

                // if this was a limited field
                if (this.limitFieldsMap.containsKey(keyNoGrouping)) {

                    int keepers = countKeepersForFieldMap.get(keyNoGrouping);
                    int missesRemaining = countMissesRemainingForFieldMap.get(keyNoGrouping);

                    // if we have matching group
                    if (matchingFieldGroups.isMatchingGroup(keyWithGrouping)) {
                        boolean foundMiss = false;
                        Attribute<?> attr = de.getValue();
                        if (attr instanceof Attributes) {
                            Attributes attrs = (Attributes) attr;
                            Set<Attribute<? extends Comparable<?>>> attrSet = attrs.getAttributes();

                            for (Attribute<? extends Comparable<?>> value : attrSet) {
                                // if this was an attribute previously set to not keep, then it is one of the misses (not a hit)
                                if (!value.isToKeep()) {
                                    value.setToKeep(true);
                                    keepers++;
                                    missesRemaining--;
                                    attributesToDrop--;
                                    foundMiss = true;
                                }
                            }
                        } else {
                            // if this was an attribute previously set to not keep, then it is one of the misses (not a hit)
                            if (!attr.isToKeep()) {
                                attr.setToKeep(true);
                                keepers++;
                                missesRemaining--;
                                attributesToDrop--;
                                foundMiss = true;
                            }
                        }

                        if (foundMiss) {
                            countKeepersForFieldMap.put(keyNoGrouping, keepers);
                            countMissesRemainingForFieldMap.put(keyNoGrouping, missesRemaining);
                        }
                    }
                }
            }
        }

        // third pass is to set any misses back to be kept if the limit allows
        for (Map.Entry<String,Attribute<? extends Comparable<?>>> de : document.entrySet()) {
            String keyWithGrouping = de.getKey();
            String keyNoGrouping = removeGrouping(keyWithGrouping);

            // look for the key without the grouping context
            if (this.limitFieldsMap.containsKey(keyNoGrouping)) {
                int limit = this.limitFieldsMap.get(keyNoGrouping);

                // short circuit if we are not actually limiting this field.
                // this is keeping with the original logic where a negative limit means to keep only hits
                if (limit <= 0) {
                    continue;
                }

                int keepers = countKeepersForFieldMap.get(keyNoGrouping);
                int missesRemaining = countMissesRemainingForFieldMap.get(keyNoGrouping);
                int missesToSet = Math.min(limit - keepers, missesRemaining);

                // if we have misses yet to keep
                if (missesToSet > 0) {
                    boolean foundMiss = false;
                    Attribute<?> attr = de.getValue();
                    if (attr instanceof Attributes) {
                        Attributes attrs = (Attributes) attr;
                        Set<Attribute<? extends Comparable<?>>> attrSet = attrs.getAttributes();

                        for (Attribute<? extends Comparable<?>> value : attrSet) {
                            // if this was an attribute previously set to not keep, then it is one of the misses (not a hit)
                            if (!value.isToKeep()) {
                                value.setToKeep(true);
                                keepers++;
                                missesRemaining--;
                                attributesToDrop--;
                                foundMiss = true;
                                missesToSet--;
                                if (missesToSet == 0) {
                                    break;
                                }
                            }
                        }
                    } else {
                        // if this was an attribute previously set to not keep, then it is one of the misses (not a hit)
                        if (!attr.isToKeep()) {
                            attr.setToKeep(true);
                            keepers++;
                            missesRemaining--;
                            attributesToDrop--;
                            foundMiss = true;
                        }
                    }

                    if (foundMiss) {
                        countKeepersForFieldMap.put(keyNoGrouping, keepers);
                        countMissesRemainingForFieldMap.put(keyNoGrouping, missesRemaining);
                    }
                }
            }
        }

        if (attributesToDrop > 0) {
            // reduce the document to those to keep
            document.reduceToKeep();

            // generate fields for original counts
            for (String keyNoGrouping : countForFieldMap.keySet()) {
                // only generate an original count if a field was reduced
                int keepers = countKeepersForFieldMap.get(keyNoGrouping);
                int originalCount = countForFieldMap.get(keyNoGrouping);
                if (originalCount > keepers) {
                    document.put(keyNoGrouping + ORIGINAL_COUNT_SUFFIX, new Numeric(originalCount, document.getMetadata(), document.isToKeep()), true);

                    // some sanity checks
                    int missesRemaining = countMissesRemainingForFieldMap.get(keyNoGrouping);
                    int limit = this.limitFieldsMap.get(keyNoGrouping);
                    int missesToSet = Math.min(limit - keepers, missesRemaining);
                    if (missesToSet > 0) {
                        log.error("Failed to limit fields correctly, " + missesToSet + " attributes failed to be included");
                        throw new RuntimeException(
                                        "Failed to limit fields correctly, " + missesToSet + ' ' + keyNoGrouping + " attributes failed to be included");
                    }
                }
            }
        }

        return entry;
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

        // if not already returned as a value match, then lets include those that are
        // part of the same group and instance as some other hit.
        if (!hitTermMap.isEmpty()) {
            String[] keyTokens = LimitFields.getCommonalityAndGroupingContext(keyWithGrouping);
            if (keyTokens != null) {
                String keyWithGroupingCommonality = keyTokens[0];
                String keyWithGroupingSuffix = keyTokens[1];

                for (String key : hitTermMap.keySet()) {
                    // get the commonality from the hit term key
                    String[] commonalityAndGroupingContext = LimitFields.getCommonalityAndGroupingContext(key);
                    if (commonalityAndGroupingContext != null) {
                        String hitTermKeyCommonality = commonalityAndGroupingContext[0];
                        String hitTermGroup = commonalityAndGroupingContext[1];
                        if (hitTermKeyCommonality.equals(keyWithGroupingCommonality) && keyWithGroupingSuffix.equals(hitTermGroup)) {
                            return true;
                        }
                    }
                }
            }
        }

        return false;
    }

    static String[] getCommonalityAndGroupingContext(String in) {
        String[] splits = StringUtils.split(in, '.');
        if (splits.length >= 3) {
            // return the first group and last group (a.k.a the instance in the first group)
            return new String[] {splits[1], splits[splits.length - 1]};
        }
        return null;
    }

    private Multimap<String,String> getHitTermMap(Document document) {
        Multimap<String,String> attrMap = HashMultimap.create();
        fillHitTermMap(document.get(JexlEvaluation.HIT_TERM_FIELD), attrMap);
        return attrMap;
    }

    private Set<Attribute<?>> getHitTermAttributes(Document document) {
        Set<Attribute<?>> attributesSet = new HashSet<>();
        Attribute<?> attributes = document.get(JexlEvaluation.HIT_TERM_FIELD);
        fillHitTermSet(attributes, attributesSet);
        return attributesSet;
    }

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
                attrMap.put(contentString.substring(0, contentString.indexOf(":")), contentString.substring(contentString.indexOf(":") + 1));
            }
        }
    }

    protected String removeGrouping(String key) {
        // if we have grouping context on, remove the grouping context
        int index = key.indexOf('.');
        if (index != -1) {
            key = key.substring(0, index);
        }
        return key;
    }

    /**
     * A map that assumes a value for missing keys.
     */
    public static class CountMap extends HashMap<String,Integer> {
        private static final Integer ZERO = Integer.valueOf(0);

        @Override
        public Integer get(Object key) {
            return getOrDefault(key, ZERO);
        }

        @Override
        public Integer put(String key, Integer value) {
            if (value > 0) {
                return super.put(key, value);
            } else {
                value = get(key);
                remove(key);
                return value;
            }
        }
    }
}
