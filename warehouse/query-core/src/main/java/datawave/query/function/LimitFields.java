package datawave.query.function;

import com.google.common.base.Function;
import com.google.common.collect.Maps;
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
    
    protected void increment(String key, Map<String,Integer> map) {
        if (map.containsKey(key)) {
            map.put(key, map.get(key) + 1);
        } else {
            map.put(key, 1);
        }
    }
    
    protected void decrement(String key, Map<String,Integer> map) {
        int value = get(key, map);
        if (value > 0) {
            value = value - 1;
            if (value > 0) {
                map.put(key, value);
            } else {
                map.remove(key);
            }
        }
    }
    
    protected int get(String key, Map<String,Integer> map) {
        Integer value = map.get(key);
        return (value != null ? value : 0);
    }
    
    @Override
    public Entry<Key,Document> apply(Entry<Key,Document> entry) {
        // key is the limited field name with _ORIGINAL_COUNT appended,
        // value will be set to the original count of that field in the document
        Map<String,Integer> limitedFieldCounts = new HashMap<>();
        Document document = entry.getValue();
        Map<String,String> hitTermMap = this.getHitTermMap(document);
        
        Map<String,Integer> countForFieldMap = Maps.newHashMap();
        Map<String,Integer> countMissesRemainingForFieldMap = Maps.newHashMap();
        Map<String,Integer> countKeepersForFieldMap = Maps.newHashMap();
        
        // first pass is to set all of the hits to be kept, the misses to drop, and count em all
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
                if (attr instanceof Attributes) {
                    Attributes attrs = (Attributes) attr;
                    Set<Attribute<? extends Comparable<?>>> attrSet = attrs.getAttributes();
                    for (Attribute<? extends Comparable<?>> value : attrSet) {
                        if (isHit(keyWithGrouping, value, hitTermMap)) {
                            increment(keyNoGrouping, countKeepersForFieldMap);
                        } else {
                            value.setToKeep(false);
                            increment(keyNoGrouping, countMissesRemainingForFieldMap);
                        }
                        increment(keyNoGrouping, countForFieldMap);
                    }
                } else {
                    if (isHit(keyWithGrouping, attr, hitTermMap)) {
                        increment(keyNoGrouping, countKeepersForFieldMap);
                    } else {
                        attr.setToKeep(false);
                        increment(keyNoGrouping, countMissesRemainingForFieldMap);
                    }
                    increment(keyNoGrouping, countForFieldMap);
                }
            }
        }
        
        // second pass is to set any misses back to be kept if the limit allows
        for (Map.Entry<String,Attribute<? extends Comparable<?>>> de : document.entrySet()) {
            String keyWithGrouping = de.getKey();
            String keyNoGrouping = keyWithGrouping;
            // if we have grouping context on, remove the grouping context
            if (keyNoGrouping.indexOf('.') != -1) {
                keyNoGrouping = keyNoGrouping.substring(0, keyNoGrouping.indexOf('.'));
            }
            
            if (this.limitFieldsMap.containsKey(keyNoGrouping)) { // look for the key without the grouping context
                int limit = this.limitFieldsMap.get(keyNoGrouping); // used below if you un-comment to get all hits
                
                // short circuit if we are not actually limiting this field.
                // this is keeping with the original logic where a negative limit means to keep only hits
                if (limit < 0) {
                    continue;
                }
                
                int missesToSet = Math.min(limit - get(keyNoGrouping, countKeepersForFieldMap), get(keyNoGrouping, countMissesRemainingForFieldMap));
                
                // if we have misses yet to keep
                if (missesToSet > 0) {
                    
                    Attribute<?> attr = de.getValue();
                    if (attr instanceof Attributes) {
                        Attributes attrs = (Attributes) attr;
                        Set<Attribute<? extends Comparable<?>>> attrSet = attrs.getAttributes();
                        
                        for (Attribute<? extends Comparable<?>> value : attrSet) {
                            // if this was a miss previously set to not keep
                            if (!value.isToKeep()) {
                                value.setToKeep(true);
                                increment(keyNoGrouping, countKeepersForFieldMap);
                                decrement(keyNoGrouping, countMissesRemainingForFieldMap);
                                missesToSet--;
                                if (missesToSet == 0) {
                                    break;
                                }
                            }
                        }
                    } else {
                        // if this was a miss previously set to not keep
                        if (!attr.isToKeep()) {
                            attr.setToKeep(true);
                            increment(keyNoGrouping, countKeepersForFieldMap);
                            decrement(keyNoGrouping, countMissesRemainingForFieldMap);
                            missesToSet--;
                        }
                    }
                }
            }
        }
        
        // reduce the document to those to keep
        document.reduceToKeep();
        
        for (String keyNoGrouping : countForFieldMap.keySet()) {
            // only generate an original count if a field was reduced
            if (get(keyNoGrouping, countForFieldMap) > get(keyNoGrouping, countKeepersForFieldMap)) {
                ColumnVisibility docVisibility = document.getColumnVisibility();
                document.put(keyNoGrouping + ORIGINAL_COUNT_SUFFIX,
                                new Numeric(get(keyNoGrouping, countForFieldMap), document.getMetadata(), document.isToKeep()), true, false);
            }
        }
        
        return entry;
    }
    
    /**
     * Determine whether this attribute is one of the hits. It is a hit if it has a matching value, or if another attribute in the same group has a hit. This
     * allows us to keep all attributes that are part of the same group.
     * 
     * @param keyWithGrouping
     * @param attr
     * @param hitTermMap
     * @return true if a hit
     */
    private boolean isHit(String keyWithGrouping, Attribute<?> attr, Map<String,String> hitTermMap) {
        try {
            
            if (hitTermMap.containsKey(keyWithGrouping)) {
                Object s = attr.getData();
                Class<?> clazz = attr.getData().getClass();
                Object hitValue = hitTermMap.get(keyWithGrouping);
                if (Type.class.isAssignableFrom(clazz)) {
                    Type<?> thing = (Type<?>) clazz.newInstance();
                    thing.setDelegateFromString(hitTermMap.get(keyWithGrouping));
                    hitValue = thing;
                } // otherwise, s is not a Type, just compare to value in hitTermMap using 'equals'
                if (s.equals(hitValue)) {
                    return true;
                }
            }
            
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
            
        } catch (Exception e) {
            return false;
        }
        return false;
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
    
}
