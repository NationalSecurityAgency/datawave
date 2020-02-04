package datawave.query.jexl.lookups;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A map if valueLists. This contains convenience methods to add key, value pairs to the map.
 */
@SuppressWarnings("serial")
public class IndexLookupMap implements Map<String,ValueSet>, Serializable {
    private Set<String> patterns = new HashSet<>();
    private Map<String,ValueSet> index = new HashMap<>();
    private boolean exceededKeyThreshold = false;
    private int keyThreshold = -1;
    private int valueThreshold = -1;
    
    public IndexLookupMap(int keyThreshold, int valueThreshold) {
        this.keyThreshold = keyThreshold;
        this.valueThreshold = valueThreshold;
    }
    
    public boolean isKeyThresholdExceeded() {
        return this.exceededKeyThreshold;
    }
    
    public int size() {
        checkExceededAndThrow("size");
        return index.size();
    }
    
    public boolean isEmpty() {
        return !exceededKeyThreshold && index.isEmpty();
    }
    
    public boolean containsKey(Object key) {
        checkExceededAndThrow("containsKey");
        return index.containsKey(key);
    }
    
    public boolean containsValue(Object value) {
        checkExceededAndThrow("containsValue");
        return index.containsValue(value);
    }
    
    public ValueSet get(Object key) {
        checkExceededAndThrow("get");
        return index.get(key);
    }
    
    public boolean put(String key, String value) {
        testExceeded(key);
        if (exceededKeyThreshold) {
            return false;
        }
        if (!index.containsKey(key)) {
            index.put(key, new ValueSet(valueThreshold));
        }
        return index.get(key).add(value);
    }
    
    public ValueSet put(String key, ValueSet value) {
        testExceeded(key);
        checkExceededAndThrow("put");
        return index.put(key, value);
    }
    
    public ValueSet remove(Object key) {
        checkExceededAndThrow("remove");
        return index.remove(key);
    }
    
    public boolean putAll(String key, Collection<String> values) {
        testExceeded(key);
        if (!index.containsKey(key)) {
            index.put(key, new ValueSet(valueThreshold));
        }
        return index.get(key).addAll(values);
    }
    
    public void putAll(Map<? extends String,? extends ValueSet> m) {
        for (Entry<? extends String,? extends ValueSet> entry : m.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }
    
    public void clear() {
        checkExceededAndThrow("clear");
        // cheaper than clear()
        index = new HashMap<>();
    }
    
    public Set<String> keySet() {
        checkExceededAndThrow("keySet");
        return index.keySet();
    }
    
    public Collection<ValueSet> values() {
        checkExceededAndThrow("values");
        return index.values();
    }
    
    public Set<Entry<String,ValueSet>> entrySet() {
        checkExceededAndThrow("entrySet");
        return index.entrySet();
    }
    
    public boolean equals(Object o) {
        if (!(o instanceof IndexLookupMap))
            return false;
        IndexLookupMap other = (IndexLookupMap) o;
        return index.equals(other.index) && exceededKeyThreshold == other.exceededKeyThreshold && keyThreshold == other.keyThreshold
                        && keyThreshold == other.valueThreshold;
    }
    
    public int hashCode() {
        return index.hashCode() + Boolean.valueOf(exceededKeyThreshold).hashCode() + keyThreshold + valueThreshold;
    }
    
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        if (exceededKeyThreshold) {
            builder.append("ExceededThreshold of ").append(keyThreshold);
        } else {
            builder.append(index);
        }
        return builder.toString();
    }
    
    private void testExceeded(String key) {
        
        if (keyThreshold > 0 && !exceededKeyThreshold && !index.containsKey(key) && (size() + 1 > keyThreshold)) {
            markExceeded();
        }
    }
    
    private void markExceeded() {
        clear();
        exceededKeyThreshold = true;
    }
    
    private void checkExceededAndThrow(String method) {
        if (exceededKeyThreshold) {
            throw new ExceededThresholdException("Cannot perform " + method + " operation once the map has exceeded its key threshold");
        }
    }
    
    public void retainFields(Collection<String> fieldNamesToRetain) {
        Map<String,ValueSet> replacementIndex = new HashMap<>();
        for (Entry<String,ValueSet> fieldNameToTerms : index.entrySet()) {
            String key = fieldNameToTerms.getKey();
            if (fieldNamesToRetain.contains(key)) {
                replacementIndex.put(key, fieldNameToTerms.getValue());
            }
        }
        index = replacementIndex;
    }
    
    public void removeFields(Collection<String> fieldNamesToRemove) {
        Map<String,ValueSet> replacementIndex = new HashMap<>();
        for (Entry<String,ValueSet> fieldNameToTerms : index.entrySet()) {
            String key = fieldNameToTerms.getKey();
            if (!fieldNamesToRemove.contains(key)) {
                replacementIndex.put(key, fieldNameToTerms.getValue());
            }
        }
        index = replacementIndex;
    }
    
    /**
     * 
     */
    public void setKeyThresholdExceeded() {
        exceededKeyThreshold = true;
        
    }
    
    public void setPatterns(Set<String> patterns) {
        this.patterns = patterns;
    }
    
    public Set<String> getPatterns() {
        return this.patterns;
    }
}
