package datawave.ingest.json.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * <p>
 * This flattener implementation should be thread-safe, as no changes to internal state are permitted post-construction. Defaults to
 * {@link JsonObjectFlattener.FlattenMode#NORMAL}
 */
public class JsonObjectFlattenerImpl implements JsonObjectFlattener {

    /**
     * Default path separator
     */
    public static final String DEFAULT_PATH_DELIMITER = ".";

    /**
     * Default separator for a path segment and its integer occurrence value. Ignored unless {@link FlattenMode#GROUPED} is used.
     */
    public static final String DEFAULT_OCCURRENCE_DELIMITER = "_";

    protected final FlattenMode flattenMode;
    protected final String pathDelimiter;
    protected final JsonElementNameNormalizer nameNormalizer;
    protected final MapKeyValueNormalizer keyValueNormalizer;
    protected final Set<String> mapKeyAllowlist;
    protected final Set<String> mapKeyDisallowlist;
    protected final String occurrenceDelimiter;
    protected final boolean addArrayIndexToFieldName;

    protected JsonObjectFlattenerImpl(Builder builder) {
        this.pathDelimiter = builder.pathDelimiter;
        this.mapKeyAllowlist = builder.fieldNameAllowlist != null ? new HashSet<>(builder.fieldNameAllowlist) : null;
        this.mapKeyDisallowlist = builder.fieldNameDisallowlist != null ? new HashSet<>(builder.fieldNameDisallowlist) : null;
        this.flattenMode = builder.flattenMode;
        this.occurrenceDelimiter = builder.occurrenceDelimiter;

        // If a GROUPED* mode is enabled, then we force addArrayIndexToFieldName to false, since the additional
        // context is redundant in those cases. The property is really only relevant for NORMAL mode usage

        this.addArrayIndexToFieldName = (this.flattenMode == FlattenMode.GROUPED || this.flattenMode == FlattenMode.GROUPED_AND_NORMAL) ? false
                        : builder.addArrayIndexToFieldName;

        if (this.flattenMode == FlattenMode.GROUPED) {
            if (this.pathDelimiter.equals(this.occurrenceDelimiter)) {
                throw new IllegalStateException("path delimiter and occurrence delimiter cannot be the same");
            }
        }

        if (null == builder.nameNormalizer) {
            this.nameNormalizer = (elementName, parentKey) -> defaultNormalizeJsonElementName(elementName);
        } else {
            this.nameNormalizer = builder.nameNormalizer;
        }

        if (null == builder.keyValueNormalizer) {
            this.keyValueNormalizer = new MapKeyValueNormalizer() {
                @Override
                public String normalizeMapKey(String key, String value) throws IllegalStateException {
                    return defaultNormalizeMapKey(key, value);
                }

                @Override
                public String normalizeMapValue(String value, String key) throws IllegalStateException {
                    return defaultNormalizeMapValue(value, key);
                }
            };
        } else {
            this.keyValueNormalizer = builder.keyValueNormalizer;
        }

    }

    @Override
    public Multimap<String,String> flatten(JsonObject object) throws IllegalStateException {
        Multimap<String,String> map = HashMultimap.create();
        flatten(object, map);
        return map;
    }

    @Override
    public void flatten(JsonObject object, Multimap<String,String> map) throws NullPointerException, IllegalStateException {
        Preconditions.checkNotNull(map, "'map' argument cannot be null");
        Map<String,Integer> occurrenceCounts = null;
        if (this.flattenMode == FlattenMode.GROUPED || this.flattenMode == FlattenMode.GROUPED_AND_NORMAL) {
            occurrenceCounts = new HashMap<>();
        }
        addKeysToMap("", object, map, occurrenceCounts);
    }

    @Override
    public FlattenMode getFlattenMode() {
        return this.flattenMode;
    }

    protected void addKeysToMap(String currentPath, JsonElement element, Multimap<String,String> map, Map<String,Integer> occurrenceCounts) {

        if (null == element || element.isJsonNull()) {
            // Don't add nulls
            return;

        } else if (element.isJsonObject()) {

            switch (this.flattenMode) {
                case SIMPLE:
                    if (!currentPath.isEmpty()) {
                        // No recursion in simple mode
                        return;
                    }
                    break;
                case GROUPED:
                case GROUPED_AND_NORMAL:
                    if (!currentPath.isEmpty()) {
                        // Append occurrence delimiter + ordinal suffix
                        currentPath = currentPath + this.occurrenceDelimiter + incrementCount(currentPath, occurrenceCounts);
                    }
                    break;
            }

            JsonObject jsonObject = element.getAsJsonObject();
            Iterator<Map.Entry<String,JsonElement>> iter = jsonObject.entrySet().iterator();
            String pathPrefix = currentPath.isEmpty() ? currentPath : currentPath + this.pathDelimiter;

            while (iter.hasNext()) {
                Map.Entry<String,JsonElement> entry = iter.next();
                addKeysToMap(pathPrefix + this.nameNormalizer.normalizeElementName(entry.getKey(), currentPath), entry.getValue(), map, occurrenceCounts);
            }
        } else if (element.isJsonArray()) {

            JsonArray jsonArray = element.getAsJsonArray();
            for (int i = 0; i < jsonArray.size(); i++) {

                if (jsonArray.get(i).isJsonPrimitive()) {
                    mapPut(currentPath, jsonArray.get(i).getAsString(), map, occurrenceCounts);
                } else {

                    if (this.addArrayIndexToFieldName) {
                        addKeysToMap(currentPath + this.pathDelimiter + i, jsonArray.get(i), map, occurrenceCounts);
                    } else {
                        addKeysToMap(currentPath, jsonArray.get(i), map, occurrenceCounts);
                    }
                }
            }
        } else if (element.isJsonPrimitive()) {

            JsonPrimitive primitive = element.getAsJsonPrimitive();
            mapPut(currentPath, primitive.getAsString(), map, occurrenceCounts);
        }
    }

    protected String defaultNormalizeJsonElementName(String name) {
        switch (this.flattenMode) {
            case GROUPED:
            case GROUPED_AND_NORMAL:
                if (name.lastIndexOf(this.pathDelimiter) > -1) {
                    throw new IllegalStateException("'" + this.pathDelimiter + "' path delimiter found in json element name: '" + name + "'");
                }
                if (name.lastIndexOf(this.occurrenceDelimiter) > -1) {
                    throw new IllegalStateException("'" + this.occurrenceDelimiter + "' occurrence delimiter found in json element name: '" + name + "'");
                }
                break;
        }
        return name;
    }

    protected void mapPut(String currentPath, String currentValue, Multimap<String,String> map, Map<String,Integer> occurrenceCounts) {
        String key = this.keyValueNormalizer.normalizeMapKey(currentPath, currentValue);
        String value = this.keyValueNormalizer.normalizeMapValue(currentValue, key);
        if (!ignoreKeyValue(key, value)) {

            if (this.flattenMode == FlattenMode.GROUPED || this.flattenMode == FlattenMode.GROUPED_AND_NORMAL) {

                if (this.flattenMode == FlattenMode.GROUPED_AND_NORMAL) {
                    // At this point we have everything we need to build the 'NORMAL', non-grouped key
                    map.put(getNormalKeyFromGroupedContext(key), value);
                }
                // Build key with fieldname + context suffix
                key = getFieldAndContextSuffix(key, value, incrementCount(key, occurrenceCounts));
            }

            map.put(key, value);
        }
    }

    protected String getNormalKeyFromGroupedContext(String groupedKey) {
        // By default, we just strip any context ordinals and associated delimiters. Override as needed
        // Note: Will probably fail miserably if your occurrenceDelimiter is a special character
        return groupedKey.replaceAll(this.occurrenceDelimiter + "\\d+", "");
    }

    /**
     * Returns the fully-qualified fieldname + grouping suffix for the specified fieldPath
     *
     * @param fieldPath
     *            path to the field, not including the trailing 'occurrence' identifier
     * @param value
     *            value associated with the specified field. Ignored here, but may be important to subclasses
     * @param occurrence
     *            ordinal position of the field within its current tree level
     * @return field name + context suffix for the specified value
     */
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
        return fieldPath.substring(fieldNameIndex) + this.pathDelimiter + fieldPath + this.occurrenceDelimiter + occurrence;
    }

    private String defaultNormalizeMapKey(String key, String value) {
        return key.toUpperCase();
    }

    private String defaultNormalizeMapValue(String value, String key) {
        return value != null && !value.isEmpty() ? value.trim() : null;
    }

    /**
     * Uses disallowlist and allowlist to determine whether or not the key/value pair should be ignored
     *
     * @param key
     *            key to evaluate
     * @param value
     *            value to evaluate. Ignored here, but may be important for subclasses
     * @return true, if key/value pair should be ignored
     */
    protected boolean ignoreKeyValue(String key, String value) {
        if (null == value || value.isEmpty()) {
            return true;
        }
        if (null != mapKeyDisallowlist) {
            if (mapKeyDisallowlist.contains(key)) {
                return true;
            }
        }
        if (null != mapKeyAllowlist && !mapKeyAllowlist.isEmpty()) {
            if (!mapKeyAllowlist.contains(key)) {
                return true;
            }
        }
        return false;
    }

    protected int incrementCount(String elementName, Map<String,Integer> occurrenceCounts) {
        int count = 0;
        if (occurrenceCounts.containsKey(elementName)) {
            count = occurrenceCounts.get(elementName) + 1;
        }
        occurrenceCounts.put(elementName, count);
        return count;
    }

    public static class Builder implements JsonObjectFlattener.Builder<JsonObjectFlattenerImpl> {

        protected String pathDelimiter = DEFAULT_PATH_DELIMITER;
        protected Set<String> fieldNameAllowlist = null;
        protected Set<String> fieldNameDisallowlist = null;
        protected JsonElementNameNormalizer nameNormalizer = null;
        protected MapKeyValueNormalizer keyValueNormalizer = null;
        protected boolean addArrayIndexToFieldName = true;
        protected FlattenMode flattenMode = FlattenMode.NORMAL;
        protected String occurrenceDelimiter = DEFAULT_OCCURRENCE_DELIMITER;

        @Override
        public Builder pathDelimiter(String pathDelimiter) throws NullPointerException {
            Preconditions.checkNotNull(pathDelimiter, "delimiter cannot be null");
            this.pathDelimiter = pathDelimiter;
            return this;
        }

        @Override
        public Builder mapKeyAllowlist(Set<String> mapKeyAllowlist) {
            this.fieldNameAllowlist = mapKeyAllowlist;
            return this;
        }

        @Override
        public Builder mapKeyDisallowlist(Set<String> mapKeyDisallowlist) {
            this.fieldNameDisallowlist = mapKeyDisallowlist;
            return this;
        }

        @Override
        public Builder flattenMode(FlattenMode flattenMode) {
            this.flattenMode = flattenMode;
            return this;
        }

        @Override
        public Builder occurrenceInGroupDelimiter(String delimiter) throws NullPointerException {
            Preconditions.checkNotNull(pathDelimiter, "delimiter cannot be null");
            this.occurrenceDelimiter = delimiter;
            return this;
        }

        @Override
        public Builder addArrayIndexToFieldName(boolean addArrayIndexToFieldName) {
            this.addArrayIndexToFieldName = addArrayIndexToFieldName;
            return this;
        }

        @Override
        public Builder mapKeyValueNormalizer(MapKeyValueNormalizer normalizer) {
            Preconditions.checkNotNull(normalizer, "normalizer cannot be null");
            this.keyValueNormalizer = normalizer;
            return this;
        }

        @Override
        public Builder jsonElementNameNormalizer(JsonElementNameNormalizer normalizer) {
            Preconditions.checkNotNull(normalizer, "normalizer cannot be null");
            this.nameNormalizer = normalizer;
            return this;
        }

        @Override
        public JsonObjectFlattener build() {
            return new JsonObjectFlattenerImpl(this);
        }
    }
}
