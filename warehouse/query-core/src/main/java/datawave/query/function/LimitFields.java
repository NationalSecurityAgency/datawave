package datawave.query.function;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.data.type.Type;
import datawave.query.Constants;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Content;
import datawave.query.attributes.Document;
import datawave.query.attributes.Numeric;
import datawave.util.StringUtils;
import org.apache.accumulo.core.data.Key;
import org.apache.log4j.Logger;
import org.javatuples.Pair;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

/**
 * This class provides a functional iterator that will reduce the documents to the attributes to keep for any fields that have been given a limit via the
 * {@code limit.fields} feature. Priority will be given to attributes that are considered a hit. If the total number of hits found for a field is fewer than its
 * limit, then any additional non-hit attributes up to the limit will also be retained.
 */
public class LimitFields implements Function<Entry<Key,Document>,Entry<Key,Document>> {
    
    public static final String ORIGINAL_COUNT_SUFFIX = "ORIGINAL_COUNT";
    
    private static final Logger log = Logger.getLogger(LimitFields.class);
    
    private final Map<String,Integer> limitFieldsMap;
    
    public LimitFields(Map<String,Integer> limitFieldsMap) {
        this.limitFieldsMap = limitFieldsMap;
        if (log.isTraceEnabled()) {
            log.trace("limitFieldsMap set to:" + limitFieldsMap);
        }
    }
    
    @Override
    public Entry<Key,Document> apply(Entry<Key,Document> entry) {
        Preconditions.checkNotNull(entry, "Entry must not be null");
        
        Document document = entry.getValue();
        CountTracker countTracker = new CountTracker();
        
        // First identify all the hits to be kept, the non-hits to drop, and count them.
        findAllHits(document, countTracker);
        // If there are any fields that have fewer hits than their limit, retain non-hits up to the limit and do not drop them.
        keepNonHitsUpToLimit(document, countTracker);
        // Finally, if there are any non-hits at this point, drop them.
        reduceDocument(document, countTracker);
        
        return entry;
    }
    
    /**
     * Identify and find all hits and non-hits for each attribute in the document. If a hit term value is found to be a non-hit, it will be set as not to keep.
     * A record of the total number of hits and non-hits, as well as total times a field is seen will be recorded in the given count tracker.
     * 
     * @param document
     *            the document
     * @param tracker
     *            the count tracker
     */
    private void findAllHits(Document document, CountTracker tracker) {
        Multimap<String,String> hitTermMap = this.getHitTermMap(document);
        for (Map.Entry<String,Attribute<? extends Comparable<?>>> documentEntry : document.entrySet()) {
            String originalKey = documentEntry.getKey();
            String keyWithoutGrouping = removeGroupingContext(originalKey);
            
            // if there is an _ANYFIELD_ entry in the limitFieldsMap, then insert every key that is not yet in the map, using the
            // limit value for _ANYFIELD_
            if (this.limitFieldsMap.containsKey(Constants.ANY_FIELD) && !this.limitFieldsMap.containsKey(keyWithoutGrouping)) {
                this.limitFieldsMap.put(keyWithoutGrouping, this.limitFieldsMap.get(Constants.ANY_FIELD));
                log.trace("added " + keyWithoutGrouping + " - " + this.limitFieldsMap.get(keyWithoutGrouping)
                                + " to the limitFieldsMap because of the _ANYFIELD_ entry");
            }
            
            // If there is a limit set for the field, identify all hits and non-hits. Ensure we're checking against the key without its grouping context.
            if (this.limitFieldsMap.containsKey(keyWithoutGrouping)) {
                if (log.isTraceEnabled()) {
                    log.trace("limitFieldsMap contains " + keyWithoutGrouping);
                }
                Attribute<?> attribute = documentEntry.getValue();
                // If we have an Attributes, evaluate each attribute that it has.
                if (attribute instanceof Attributes) {
                    Attributes attributes = (Attributes) attribute;
                    attributes.getAttributes().forEach((attr) -> evaluateForHit(originalKey, keyWithoutGrouping, attr, tracker, hitTermMap));
                } else {
                    evaluateForHit(originalKey, keyWithoutGrouping, attribute, tracker, hitTermMap);
                }
            }
        }
    }
    
    /**
     * Determine if the given attribute is a hit, and record the resulting evaluation the count tracker. If the attribute is not a hit, it will be marked to be
     * dropped.
     * 
     * @param key
     *            the original document entry key with the grouping context
     * @param keyWithoutGrouping
     *            the document entry key without the grouping context
     * @param attribute
     *            the attribute to evaluate
     * @param tracker
     *            the count tracker
     * @param hitTermMap
     *            the hit term map
     */
    private void evaluateForHit(String key, String keyWithoutGrouping, Attribute<?> attribute, CountTracker tracker, Multimap<String,String> hitTermMap) {
        if (isHit(key, attribute, hitTermMap)) {
            // This is a hit.
            tracker.incrementHit(keyWithoutGrouping);
        } else {
            // This is a non-hit. Mark the attribute to be dropped.
            attribute.setToKeep(false);
            tracker.incrementNonHit(keyWithoutGrouping);
        }
        tracker.incrementFieldCount(keyWithoutGrouping);
    }
    
    /**
     * In some cases, the number of hits found for a particular field will be fewer than the limit set for the field. In this case, ensure that we retain the
     * maximum number of entries for the field up to the limit by marking non-hits as to keep until we've reached the limit.
     * 
     * @param document
     *            the document
     * @param tracker
     *            the count tracker
     */
    private void keepNonHitsUpToLimit(Document document, CountTracker tracker) {
        // Perform a second pass to set any non-hits back to be kept if the limit allows.
        for (Map.Entry<String,Attribute<? extends Comparable<?>>> documentEntry : document.entrySet()) {
            String originalKey = documentEntry.getKey();
            String keyWithoutGrouping = removeGroupingContext(originalKey);
            
            // Look for the key without the grouping context.
            if (this.limitFieldsMap.containsKey(keyWithoutGrouping)) {
                int limit = this.limitFieldsMap.get(keyWithoutGrouping);
                
                // A negative limit indicates that we should keep only hits. Do not attempt to retain any non-hits.
                if (limit <= 0) {
                    continue;
                }
                
                // Determine if we have non-hits that we can keep and still stay within the limits.
                int nonHitsToKeep = Math.min(limit - tracker.getTotalHits(keyWithoutGrouping), tracker.getTotalNonHits(keyWithoutGrouping));
                if (nonHitsToKeep > 0) {
                    Attribute<?> attribute = documentEntry.getValue();
                    if (attribute instanceof Attributes) {
                        Attributes attrs = (Attributes) attribute;
                        // @formatter:off
                        attrs.getAttributes().stream()
                                        .filter((attr) -> !attr.isToKeep()) // Find previously recorded non-hits.
                                        .limit(nonHitsToKeep) // Limit the number of non-hits to the established max.
                                        .forEach((attr) -> retainNonHit(keyWithoutGrouping, attr, tracker)); // Mark them as to keep.
                        // @formatter:on
                    } else {
                        // if this is an attribute previously set to not keep, then it is a non-hit that we should retain.
                        if (!attribute.isToKeep()) {
                            retainNonHit(keyWithoutGrouping, attribute, tracker);
                        }
                    }
                }
            }
        }
    }
    
    /**
     * Mark the given attribute as to keep, and increment the total hits for the key, and decrement the total non-hits for the key.
     * 
     * @param key
     *            the key
     * @param attribute
     *            the attribute
     * @param tracker
     *            the count tracker
     */
    private void retainNonHit(String key, Attribute<?> attribute, CountTracker tracker) {
        attribute.setToKeep(true);
        tracker.incrementHit(key);
        tracker.decrementNonHit(key);
    }
    
    /**
     * If there are any non-hits remaining according to the count tracker, drop them from the document.
     * 
     * @param document
     *            the document
     * @param tracker
     *            the count tracker
     */
    private void reduceDocument(Document document, CountTracker tracker) {
        if (tracker.hasNonHits()) {
            // Reduce the document to the values marked as to keep.
            document.reduceToKeep();
            
            // Generate original counts for each field.
            CountMap fieldCounts = tracker.fieldCounts;
            for (String fieldKey : fieldCounts.keySet()) {
                // Generate a record of the original count only if the number of entries for the field was reduced.
                int totalKept = tracker.getTotalHits(fieldKey);
                int originalTotal = fieldCounts.get(fieldKey);
                if (originalTotal > totalKept) {
                    document.put(fieldKey + ORIGINAL_COUNT_SUFFIX, new Numeric(originalTotal, document.getMetadata(), document.isToKeep()), true, false);
                    
                    // Perform some sanity checks. Specifically, if we have non-hits remaining, and we did not retain the maximum number of entries up to the
                    // limit for the field, then something went wrong where we should have retained more of the non-hits.
                    int limit = this.limitFieldsMap.get(fieldKey);
                    int missingAttributes = Math.min(limit - totalKept, tracker.getTotalNonHits(fieldKey));
                    if (missingAttributes > 0) {
                        log.error("Failed to limit fields correctly, " + missingAttributes + " attributes failed to be included");
                        throw new RuntimeException("Failed to limit fields correctly, " + missingAttributes + ' ' + fieldKey
                                        + " attributes failed to be included");
                    }
                }
            }
        }
    }
    
    /**
     * Determine whether this attribute is one of the hits. It is a hit if it has a matching value, or if another attribute in the same group has a hit. This
     * allows us to keep all attributes that are part of the same group.
     * 
     * @param key
     *            the key
     * @param attribute
     *            the attribute
     * @param hitTermMap
     *            the hit term map
     * @return true if the attribute is considered a hit, or false otherwise
     */
    private boolean isHit(String key, Attribute<?> attribute, Multimap<String,String> hitTermMap) {
        // Determine if this attribute is a hit based on whether it has a matching value.
        if (hitTermMap.containsKey(key)) {
            Object data = attribute.getData();
            Class<?> clazz = attribute.getData().getClass();
            for (Object hitValue : hitTermMap.get(key)) {
                try {
                    // If data is a Type, ensure hitValue is correctly parsed from it.
                    if (Type.class.isAssignableFrom(clazz)) {
                        Type<?> type = (Type<?>) clazz.newInstance();
                        type.setDelegateFromString(String.valueOf(hitValue));
                        hitValue = type;
                    }
                    // Compare the hitValue to data. If data is not a Type, a simple equals still suffices.
                    if (data.equals(hitValue)) {
                        return true;
                    }
                } catch (Exception ignored) {}
            }
        }
        
        // If there was not a matching value, determine if this attribute is part of the same group and instance (subgroup) as some other hit.
        if (!hitTermMap.isEmpty()) {
            Optional<Pair<String,String>> keyTokens = getGroupAndSubGroup(key);
            if (keyTokens.isPresent()) {
                for (String hitTermKey : hitTermMap.keySet()) {
                    // get the commonality from the hit term key
                    Optional<Pair<String,String>> hitTermKeyTokens = getGroupAndSubGroup(hitTermKey);
                    if (hitTermKeyTokens.isPresent() && keyTokens.get().equals(hitTermKeyTokens.get())) {
                        return true;
                    }
                }
            }
        }
        
        return false;
    }
    
    /**
     * Return a {@link Pair} where {@link Pair#getValue0()} returns the group and {@link Pair#getValue1()} returns the instance (the subgroup) if they can be
     * parsed from the given key. Otherwise, an empty {@link Optional} will be returned.
     * 
     * @param key
     *            the key to parse the group and instance from
     * @return a {@link Pair} with the group and instance, or an empty {@link Optional} if they cannot be parsed
     */
    private Optional<Pair<String,String>> getGroupAndSubGroup(String key) {
        String[] splits = StringUtils.split(key, '.');
        if (splits.length >= 3) {
            // Return the first group and last group (aka the instance in the first group).
            return Optional.of(Pair.with(splits[1], splits[splits.length - 1]));
        }
        return Optional.empty();
    }
    
    /**
     * Return the hit term map for the given document.
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
     * Add each hit term and value found in the given attribute to the given map.
     * 
     * @param attribute
     *            the attribute
     * @param map
     *            the map
     */
    private void fillHitTermMap(Attribute<?> attribute, Multimap<String,String> map) {
        if (attribute != null) {
            if (attribute instanceof Attributes) {
                Attributes attributes = (Attributes) attribute;
                attributes.getAttributes().forEach((child) -> fillHitTermMap(child, map));
            } else if (attribute instanceof Content) {
                // Split the content into its fieldName:value and add it to the map.
                String contentString = ((Content) attribute).getContent();
                int delimiterPos = contentString.indexOf(":");
                String fieldName = contentString.substring(0, delimiterPos);
                String value = contentString.substring(delimiterPos + 1);
                map.put(fieldName, value);
            }
        }
    }
    
    /**
     * Return a subset of the key without the grouping context.
     *
     * @param key
     *            the key
     * @return the key without the grouping context if present
     */
    private String removeGroupingContext(String key) {
        int index = key.indexOf('.');
        if (index != -1) {
            key = key.substring(0, index);
        }
        return key;
    }
    
    /**
     * A tracker that provides convenience methods for tracking required counts that can be passed to methods.
     */
    private static class CountTracker {
        private final CountMap fieldCounts = new CountMap();
        private final CountMap hitCounts = new CountMap();
        private final CountMap nonHitCounts = new CountMap();
        
        public void incrementHit(String key) {
            hitCounts.increment(key);
        }
        
        public int getTotalHits(String key) {
            return hitCounts.get(key);
        }
        
        public void incrementNonHit(String key) {
            nonHitCounts.increment(key);
        }
        
        public void decrementNonHit(String key) {
            nonHitCounts.decrement(key);
        }
        
        public int getTotalNonHits(String key) {
            return nonHitCounts.get(key);
        }
        
        public boolean hasNonHits() {
            return !nonHitCounts.isEmpty();
        }
        
        public void incrementFieldCount(String key) {
            fieldCounts.increment(key);
        }
    }
    
    /**
     * A map that assumes a value for missing keys.
     */
    private static class CountMap extends HashMap<String,Integer> {
        
        public Integer increment(String key) {
            return modifyValueBy(key, 1);
        }
        
        public void decrement(String key) {
            modifyValueBy(key, -1);
        }
        
        private Integer modifyValueBy(String key, int modifier) {
            Integer value = get(key);
            value = value + modifier;
            return put(key, value);
        }
        
        /**
         * Return the value associated with the given key. If no mapping was found for the key, a default value of zero will be returned.
         *
         * @param key
         *            the key
         * @return the value associated with the key, or zero if no mapping was found for the key
         */
        @Override
        public Integer get(Object key) {
            return getOrDefault(key, 0);
        }
        
        /**
         * Associate the given value with the given key if and only if the value is greater than zero. If the value is zero or less, the mapping for the key
         * will be removed from this map, and the previous associated value for the key will be returned.
         *
         * @param key
         *            the key
         * @param value
         *            the value
         * @return the previous value associated with the key
         */
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
