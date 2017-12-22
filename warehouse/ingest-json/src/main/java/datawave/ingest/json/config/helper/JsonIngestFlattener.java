package datawave.ingest.json.config.helper;

import com.google.common.base.Preconditions;
import com.google.common.collect.Multimap;
import datawave.ingest.json.util.JsonObjectFlattener;
import datawave.ingest.json.util.JsonObjectFlattenerImpl;

import java.util.Map;
import java.util.Set;

/**
 * <p>
 * Custom {@link JsonObjectFlattener} implementation that has a {@link JsonDataTypeHelper} instance in order to utilize some of its behaviors, e.g., its
 * key/value normalization behavior, whitelist/blacklist options, etc
 */
public class JsonIngestFlattener extends JsonObjectFlattenerImpl {
    
    protected final JsonDataTypeHelper jsonDataTypeHelper;
    
    protected JsonIngestFlattener(String pathDelimiter, Set<String> mapKeyWhitelist, Set<String> mapKeyBlacklist, FlattenMode flattenMode,
                    String occurrenceDelimiter, JsonElementNameNormalizer nameNormalizer, MapKeyValueNormalizer keyValueNormalizer,
                    JsonDataTypeHelper jsonDataTypeHelper) {
        super(pathDelimiter, mapKeyWhitelist, mapKeyBlacklist, flattenMode, occurrenceDelimiter, nameNormalizer, keyValueNormalizer);
        this.jsonDataTypeHelper = jsonDataTypeHelper;
    }
    
    @Override
    protected void mapPut(String currentPath, String currentValue, Multimap<String,String> map, Map<String,Integer> occurrenceCounts) {
        String key = this.keyValueNormalizer.normalizeMapKey(currentPath, currentValue);
        String value = this.keyValueNormalizer.normalizeMapValue(currentValue, key);
        if (!ignoreKeyValue(key, value)) {
            
            if (this.flattenMode == FlattenMode.GROUPED || this.flattenMode == FlattenMode.GROUPED_AND_NORMAL) {
                
                if (this.flattenMode == FlattenMode.GROUPED_AND_NORMAL) {
                    // Build typical GROUPED key suffix, but with NORMAL key prefix instead
                    key = getNormalKeyFromGroupedContext(key) + getFieldAndContextSuffix(key, value, incrementCount(key, occurrenceCounts));
                } else {
                    key = getFieldAndContextSuffix(key, value, incrementCount(key, occurrenceCounts));
                }
            }
            
            map.put(key, value);
        }
    }
    
    @Override
    protected String getFieldAndContextSuffix(String fieldPath, String value, int occurrence) {
        int fieldNameIndex = fieldPath.lastIndexOf(this.pathDelimiter) + 1;
        if (fieldNameIndex == 0) {
            /*
             * We avoid adding the context suffix to root-level fields here, since there's no nesting for those, and since there's little chance that it would
             * ever be useful, except *maybe* in root-level arrays where the position of its elements has some meaning to the end user...
             * 
             * For example, root-level array keys would otherwise be surfaced here as FOO.FOO_0, FOO.FOO_1, FOO.FOO_2, etc. We drop that context here. If you
             * need it, then just override this method in a subclass
             */
            return fieldPath;
        }
        if (this.flattenMode == FlattenMode.GROUPED_AND_NORMAL) {
            // Only return the suffix
            return this.pathDelimiter + fieldPath + this.occurrenceDelimiter + occurrence;
        } else {
            return fieldPath.substring(fieldNameIndex) + this.pathDelimiter + fieldPath + this.occurrenceDelimiter + occurrence;
        }
    }
    
    @Override
    protected String getNormalKeyFromGroupedContext(String groupedKey) {
        Preconditions.checkArgument(this.flattenMode == FlattenMode.GROUPED_AND_NORMAL);
        // First, strip any context ordinals and associated delimiters
        String normalKey = groupedKey.replaceAll("_\\d+", "");
        // Replace periods with underscrores
        normalKey = normalKey.replaceAll("\\.", "_");
        if (normalKey.equals(groupedKey)) {
            // Root primitive. No path. No group context
            return "";
        }
        return normalKey;
    }
    
    public static class Builder extends JsonObjectFlattenerImpl.Builder {
        
        protected JsonDataTypeHelper jsonDataTypeHelper = null;
        
        public Builder jsonDataTypeHelper(JsonDataTypeHelper jsonDataTypeHelper) {
            this.jsonDataTypeHelper = jsonDataTypeHelper;
            return this;
        }
        
        @Override
        public JsonIngestFlattener build() {
            if (null == this.jsonDataTypeHelper) {
                this.jsonDataTypeHelper = new JsonDataTypeHelper();
                this.jsonDataTypeHelper.setJsonObjectFlattenMode(this.flattenMode);
            }
            
            if (null == this.nameNormalizer) {
                this.nameNormalizer = new DefaultJsonElementNameNormalizer(this.jsonDataTypeHelper);
            }
            
            if (null == this.keyValueNormalizer) {
                this.keyValueNormalizer = new DefaultMapKeyValueNormalizer(this.jsonDataTypeHelper);
            }
            
            if (this.flattenMode == FlattenMode.GROUPED || this.flattenMode == FlattenMode.GROUPED_AND_NORMAL) {
                // Force pathDelimiter and occurrenceDelimiter per DW's grouping requirements
                return new JsonIngestFlattener(".", this.fieldNameWhitelist, this.fieldNameBlacklist, this.flattenMode, "_", this.nameNormalizer,
                                this.keyValueNormalizer, this.jsonDataTypeHelper);
            }
            
            return new JsonIngestFlattener(this.pathDelimiter, this.fieldNameWhitelist, this.fieldNameBlacklist, this.flattenMode, this.occurrenceDelimiter,
                            this.nameNormalizer, this.keyValueNormalizer, this.jsonDataTypeHelper);
        }
    }
    
    static public final class DefaultMapKeyValueNormalizer implements MapKeyValueNormalizer {
        
        private JsonDataTypeHelper jsonDataTypeHelper;
        
        private DefaultMapKeyValueNormalizer() {}
        
        public DefaultMapKeyValueNormalizer(JsonDataTypeHelper jsonDataTypeHelper) {
            this.jsonDataTypeHelper = jsonDataTypeHelper;
        }
        
        @Override
        public String normalizeMapKey(String key, String value) throws IllegalStateException {
            return key.toUpperCase();
        }
        
        @Override
        public String normalizeMapValue(String value, String key) throws IllegalStateException {
            return this.jsonDataTypeHelper.clean(key, value);
        }
    }
    
    static public final class DefaultJsonElementNameNormalizer implements JsonElementNameNormalizer {
        
        private JsonDataTypeHelper jsonDataTypeHelper;
        
        private DefaultJsonElementNameNormalizer() {}
        
        public DefaultJsonElementNameNormalizer(JsonDataTypeHelper jsonDataTypeHelper) {
            this.jsonDataTypeHelper = jsonDataTypeHelper;
        }
        
        @Override
        public String normalizeElementName(String elementName, String parentKey) throws IllegalStateException {
            
            // No periods allowed in DW base field names
            elementName = elementName.replaceAll("\\.", "");
            switch (this.jsonDataTypeHelper.getJsonObjectFlattenMode()) {
                case GROUPED:
                case GROUPED_AND_NORMAL:
                    // Also strip underscores since that's our occurrence delimiter
                    elementName = elementName.replaceAll("_", "");
                    break;
            // Other normalizations perhaps?
            }
            
            return elementName;
        }
    }
}
