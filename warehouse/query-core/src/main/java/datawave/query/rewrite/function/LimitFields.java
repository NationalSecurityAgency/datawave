package datawave.query.rewrite.function;

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
import datawave.query.rewrite.attributes.Attribute;
import datawave.query.rewrite.attributes.Attributes;
import datawave.query.rewrite.attributes.Content;
import datawave.query.rewrite.attributes.Document;
import datawave.query.rewrite.attributes.Numeric;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LimitFields implements Function<Entry<Key,Document>,Entry<Key,Document>> {
    
    private static final Logger log = Logger.getLogger(LimitFields.class);
    
    public final static String ORIGINAL_COUNT_SUFFIX = "ORIGINAL_COUNT";
    
    public final static Pattern PREFIX_SUFFIX_PATTERN = Pattern.compile("([A-Za-z0-9]+[\\._])[A-Za-z0-9\\.]*\\.([A-Za-z0-9]+)");
    
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
                int limit = this.limitFieldsMap.get(keyNoGrouping);
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
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        } else if (!hitTermMap.isEmpty() && limitFieldsMap.containsKey(keyNoGrouping)) {
            
            try {
                Matcher matcher = PREFIX_SUFFIX_PATTERN.matcher(keyWithGrouping);
                if (matcher.matches()) {
                    String keyWithGroupingPrefix = matcher.group(1);
                    String keyWithGroupingSuffix = matcher.group(2);
                    for (String key : hitTermMap.keySet()) {
                        if (key.startsWith(keyWithGroupingPrefix) && key.endsWith(keyWithGroupingSuffix)) {
                            mapOfHits.getUnchecked(keyNoGrouping).put(keyWithGrouping, attr);
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
    
    private Map<String,String> getHitTermMap(Document document) {
        Attribute<?> attr = document.get("HIT_TERM");
        Map<String,String> attrMap = new HashMap<>();
        if (attr instanceof Attributes) {
            Attributes attrs = (Attributes) attr;
            for (Attribute<?> at : attrs.getAttributes()) {
                if (at instanceof Content) {
                    Content content = (Content) at;
                    // split the content into its fieldname:value
                    String contentString = content.getContent();
                    String[] fieldValue = new String[] {contentString.substring(0, contentString.indexOf(":")),
                            contentString.substring(contentString.indexOf(":") + 1)};
                    attrMap.put(fieldValue[0], fieldValue[1]);
                }
            }
        } else if (attr instanceof Content) {
            Content content = (Content) attr;
            // split the content into its fieldname:value
            String[] fieldValue = Iterables.toArray(Splitter.on(":").omitEmptyStrings().trimResults().split(content.getContent()), String.class);
            attrMap.put(fieldValue[0], fieldValue[1]);
        }
        return attrMap;
    }
    
    /**
     * Adds new fields to the document to hold the original count of any fields that have been reduced by the limit.fields parameter. The new fields are named
     * like this: {the.field.that.was.limited}_ORIGINAL_COUNT
     * 
     * @param doc
     * @param limitedFieldCounts
     */
    private void applyCounts(Document doc, Map<String,Integer> limitedFieldCounts) {
        if (limitedFieldCounts.entrySet().size() > 0) {
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
