package datawave.query.function;

import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Iterables;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import datawave.data.type.Type;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Content;
import datawave.query.attributes.Document;
import datawave.query.attributes.Numeric;
import datawave.util.StringUtils;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class LimitFields implements Function<Entry<Key,Document>,Entry<Key,Document>> {
    
    private static final Logger log = Logger.getLogger(LimitFields.class);
    
    public static final String ORIGINAL_COUNT_SUFFIX = "ORIGINAL_COUNT";
    
    private Map<String,Integer> limitFieldsMap;
    
    public LimitFields(Map<String,Integer> limitFieldsMap) {
        this.limitFieldsMap = limitFieldsMap;
        if (log.isTraceEnabled())
            log.trace("limitFieldsMap set to:" + limitFieldsMap);
    }
    
    @Override
    public Entry<Key,Document> apply(Entry<Key,Document> entry) {
        // key is the limited field name with _ORIGINAL_COUNT appended,
        // value will be set to the original count of that field in the document
        Map<String,Integer> limitedFieldCounts = new HashMap<>();
        Document document = entry.getValue();
        Map<String,String> hitTermMap = this.getHitTermMap(document);
        
        Multimap<String,Attribute<? extends Comparable<?>>> reducedMap = LinkedListMultimap.create();
        Map<String,Integer> countForFieldMap = Maps.newHashMap();
        
        // maps from the key with NO grouping context to a multimap of
        // key WITH grouping context to attributes:
        // DIRECTION : [DIRECTION.1 : [over,under], DIRECTION.2 : [sideways,down]]
        LoadingCache<String,Multimap<String,Attribute<? extends Comparable<?>>>> hits = CacheBuilder.newBuilder().build(
                        new CacheLoader<String,Multimap<String,Attribute<? extends Comparable<?>>>>() {
                            public Multimap<String,Attribute<? extends Comparable<?>>> load(String key) {
                                return LinkedListMultimap.create();
                            }
                        });
        // maps from the key with NO grouping context to a multimap of
        // key WITH grouping context to attributes:
        // DIRECTION : [DIRECTION.1 : [over,under], DIRECTION.2 : [sideways,down]]
        @SuppressWarnings("serial")
        LoadingCache<String,Multimap<String,Attribute<? extends Comparable<?>>>> misses = CacheBuilder.newBuilder().build(
                        new CacheLoader<String,Multimap<String,Attribute<? extends Comparable<?>>>>() {
                            public Multimap<String,Attribute<? extends Comparable<?>>> load(String key) {
                                return LinkedListMultimap.create();
                            }
                        });
        
        for (Map.Entry<String,Attribute<? extends Comparable<?>>> de : document.entrySet()) {
            String keyWithGrouping = de.getKey();
            String keyNoGrouping = keyWithGrouping;
            // if we have grouping context on, remove the grouping context
            if (keyNoGrouping.indexOf('.') != -1) {
                keyNoGrouping = keyNoGrouping.substring(0, keyNoGrouping.indexOf('.'));
            }
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
                int limit = this.limitFieldsMap.get(keyNoGrouping); // used below if you un-comment to get all hits
                if (attr instanceof Attributes) {
                    Attributes attrs = (Attributes) attr;
                    Set<Attribute<? extends Comparable<?>>> attrSet = attrs.getAttributes();
                    for (Attribute<? extends Comparable<?>> value : attrSet) {
                        manageHitsAndMisses(keyWithGrouping, keyNoGrouping, value, hitTermMap, hits, misses, countForFieldMap);
                    }
                    
                } else {
                    manageHitsAndMisses(keyWithGrouping, keyNoGrouping, attr, hitTermMap, hits, misses, countForFieldMap);
                }
            }
        }
        for (String keyNoGrouping : countForFieldMap.keySet()) {
            int limit = this.limitFieldsMap.get(keyNoGrouping);
            Multimap<String,Attribute<? extends Comparable<?>>> hitMap = hits.getUnchecked(keyNoGrouping);
            for (String keyWithGrouping : hitMap.keySet()) {
                for (Attribute<? extends Comparable<?>> value : hitMap.get(keyWithGrouping)) {
                    // if(limit <= 0) break; // comment this line if you want to get ALL hits even if the limit is exceeded
                    reducedMap.put(keyWithGrouping, value);
                    limit--;
                }
            }
            Multimap<String,Attribute<? extends Comparable<?>>> missMap = misses.getUnchecked(keyNoGrouping);
            for (String keyWithGrouping : missMap.keySet()) {
                for (Attribute<? extends Comparable<?>> value : missMap.get(keyWithGrouping)) {
                    if (limit <= 0)
                        break;
                    reducedMap.put(keyWithGrouping, value);
                    limit--;
                }
            }
            if (log.isTraceEnabled()) {
                log.trace("reducedMap:" + reducedMap);
                log.trace("mapOfHits:" + hits.asMap());
                log.trace("mapOfMisses:" + misses.asMap());
            }
            
            // only generate an original count if a field was reduced
            if (countForFieldMap.get(keyNoGrouping) > this.limitFieldsMap.get(keyNoGrouping)) {
                limitedFieldCounts.put(keyNoGrouping + ORIGINAL_COUNT_SUFFIX, countForFieldMap.get(keyNoGrouping));
            }
            
        }
        
        // mutate the document with the changes collected in the above loop
        applyCounts(document, limitedFieldCounts);
        Map<String,Multimap<String,Attribute<? extends Comparable<?>>>> toRemove = Maps.newLinkedHashMap();
        toRemove.putAll(hits.asMap());
        toRemove.putAll(misses.asMap());
        makeReduction(document, toRemove, reducedMap);
        return entry;
    }
    
    private void manageHitsAndMisses(String keyWithGrouping, String keyNoGrouping, Attribute<?> attr, Map<String,String> hitTermMap,
                    LoadingCache<String,Multimap<String,Attribute<? extends Comparable<?>>>> mapOfHits,
                    LoadingCache<String,Multimap<String,Attribute<? extends Comparable<?>>>> mapOfMisses, Map<String,Integer> countForFieldMap) {
        if (log.isTraceEnabled())
            log.trace("in - manageHitsAndMisses(" + keyWithGrouping + "," + keyNoGrouping + "," + attr + "," + hitTermMap + "," + mapOfHits.asMap() + ","
                            + mapOfMisses.asMap() + "," + countForFieldMap);
        if (hitTermMap.containsKey(keyWithGrouping)) {
            
            Object s = attr.getData();
            Class<?> clazz = attr.getData().getClass();
            try {
                Object hitValue = hitTermMap.get(keyWithGrouping);
                if (Type.class.isAssignableFrom(clazz)) {
                    Type<?> thing = (Type<?>) clazz.newInstance();
                    thing.setDelegateFromString(hitTermMap.get(keyWithGrouping));
                    hitValue = thing;
                } // otherwise, s is not a Type, just compare to value in hitTermMap using 'equals'
                if (s.equals(hitValue)) {
                    mapOfHits.getUnchecked(keyNoGrouping).put(keyWithGrouping, attr);
                } else {
                    mapOfMisses.getUnchecked(keyNoGrouping).put(keyWithGrouping, attr);
                }
            } catch (InstantiationException | IllegalAccessException e) {
                log.error(e);
            }
        } else if (!hitTermMap.isEmpty() && limitFieldsMap.containsKey(keyNoGrouping)) {
            
            try {
                String[] keyTokens = LimitFields.getCommonalityAndGroupingContext(keyWithGrouping);
                if (keyTokens != null) {
                    String keyWithGroupingCommonality = keyTokens[0];
                    String keyWithGroupingSuffix = keyTokens[1];
                    
                    for (String key : hitTermMap.keySet()) {
                        // get the commonality from the hit term key
                        String[] commonalityAndGroupingContext = LimitFields.getCommonalityAndGroupingContext(key);
                        if (commonalityAndGroupingContext != null) {
                            String hitTermKeyCommonality = commonalityAndGroupingContext[0];
                            if (hitTermKeyCommonality.equals(keyWithGroupingCommonality) && key.endsWith(keyWithGroupingSuffix)) {
                                mapOfHits.getUnchecked(keyNoGrouping).put(keyWithGrouping, attr);
                            } else {
                                mapOfMisses.getUnchecked(keyNoGrouping).put(keyWithGrouping, attr);
                            }
                        } else {
                            mapOfMisses.getUnchecked(keyNoGrouping).put(keyWithGrouping, attr);
                        }
                    }
                } else {
                    mapOfMisses.getUnchecked(keyNoGrouping).put(keyWithGrouping, attr);
                }
                
            } catch (Throwable ex) {
                // if ANYTHING went wrong here, just put it into the misses
                mapOfMisses.getUnchecked(keyNoGrouping).put(keyWithGrouping, attr);
            }
        } else {
            mapOfMisses.getUnchecked(keyNoGrouping).put(keyWithGrouping, attr);
        }
        if (countForFieldMap.get(keyNoGrouping) == null) {
            countForFieldMap.put(keyNoGrouping, 1);
        } else {
            countForFieldMap.put(keyNoGrouping, countForFieldMap.get(keyNoGrouping) + 1);
        }
        if (log.isTraceEnabled())
            log.trace("out - manageHitsAndMisses(" + keyWithGrouping + "," + keyNoGrouping + "," + attr + "," + hitTermMap + "," + mapOfHits.asMap() + ","
                            + mapOfMisses.asMap() + "," + countForFieldMap);
    }
    
    static String[] getCommonalityAndGroupingContext(String in) {
        String[] splits = StringUtils.split(in, '.');
        if (splits.length >= 3) {
            return new String[] {splits[1], splits[splits.length - 1]};
        }
        return null;
    }
    
    private Map<String,String> getHitTermMap(Document document) {
        Map<String,String> attrMap = new HashMap<>();
        fillHitTermMap(document.get(JexlEvaluation.HIT_TERM_FIELD), attrMap);
        return attrMap;
    }
    
    private void fillHitTermMap(Attribute<?> attr, Map<String,String> attrMap) {
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
    
    /**
     * Adds new fields to the document to hold the original count of any fields that have been reduced by the limit.fields parameter. The new fields are named
     * like this: {the.field.that.was.limited}_ORIGINAL_COUNT
     * 
     * @param doc
     * @param limitedFieldCounts
     */
    private void applyCounts(Document doc, Map<String,Integer> limitedFieldCounts) {
        if (!limitedFieldCounts.entrySet().isEmpty()) {
            ColumnVisibility docVisibility = doc.getColumnVisibility();
            for (Entry<String,Integer> limitedFieldCountEntry : limitedFieldCounts.entrySet()) {
                doc.put(limitedFieldCountEntry.getKey(), new Numeric(limitedFieldCountEntry.getValue(), doc.getMetadata(), doc.isToKeep()), true, false);
            }
        }
    }
    
    /**
     * for any field that should be limited, change the document's collection of those fields by emptying it, then adding back the ones that should be kept. For
     * example, if LOAD_DATE should be limited to 2, get the document's collection LOAD_DATE attribute, if it is an Attributes, then replace s, and replace it
     * with the (smaller) collection in the reducedSetMap
     * 
     * @param document
     */
    private void makeReduction(Document document, Map<String,Multimap<String,Attribute<? extends Comparable<?>>>> toRemove,
                    Multimap<String,Attribute<? extends Comparable<?>>> reducedMap) {
        
        if (log.isTraceEnabled())
            log.trace("reducedMap:" + reducedMap);
        if (log.isTraceEnabled())
            log.trace("toRemove:" + toRemove);
        for (Multimap<String,Attribute<? extends Comparable<?>>> toRemoveMultimap : toRemove.values()) {
            for (Entry<String,Attribute<? extends Comparable<?>>> entry : toRemoveMultimap.entries()) {
                document.remove(entry.getKey());
                if (log.isTraceEnabled())
                    log.trace("removed " + entry.getKey() + " and its value " + entry.getValue() + " from document");
            }
        }
        
        for (Entry<String,Attribute<? extends Comparable<?>>> entry : reducedMap.entries()) {
            document.put(entry, true);
            if (log.isTraceEnabled())
                log.trace("put " + entry + " into document");
        }
    }
}
