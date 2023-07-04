package datawave.ingest.json.util;

import java.util.Set;

import com.google.common.collect.Multimap;
import com.google.gson.JsonObject;

/**
 * <p>
 * Transforms arbitrary Json into a flattened {@link com.google.common.collect.Multimap} representation, where each key denotes the fully-qualified path to one
 * or more distinct primitive values. Each path segment making up a key is separated by a configured delimiter according to the configured {@link FlattenMode}
 * and other options
 *
 * <p>
 * See {@link FlattenMode} enum values for information on supported behaviors
 * <p>
 * See {@link JsonObjectFlattener.Builder} methods for other config options
 *
 */
public interface JsonObjectFlattener {

    /**
     * <p>
     * Supported modes: {@link #SIMPLE}, {@link #NORMAL}, {@link #GROUPED}, {@link #GROUPED_AND_NORMAL}
     */
    enum FlattenMode {
        /**
         * <p>
         * Ignores nested objects. Only retrieves root-level primitives, including primitives within root-level arrays. Array primitives will be represented as
         * multi-valued keys in the resulting map. If your json is flat already, or if you wish to ignore nested objects for whatever reason, this mode should
         * suffice.
         *
         * <p>
         * If you require complete tree traversal, see {@link #NORMAL} and {@link #GROUPED} modes.
         */
        SIMPLE,

        /**
         * <p>
         * Traverses and flattens the entire Json tree, unlike {@link #SIMPLE} mode. Given a nested <b>fieldname</b> property @ level 4 in the tree and path
         * delimiter of "<b>.</b>", the flattened result would take the form
         *
         * <blockquote> <b>greatgrandparent.grandparent.parent.fieldname = value</b> </blockquote>
         */
        NORMAL,

        /**
         * <p>
         * Same as {@link #NORMAL} mode, but instead we append the hierarchical context onto 'FIELDNAME' as a suffix, with additional information to identify
         * the distinct occurrence. For example,
         *
         * <blockquote> <b>FIELDNAME.greatgrandparent_0.grandparent_1.parent_3.fieldname_0 = value</b> </blockquote>
         * <p>
         * ...where '_#' denotes the specific occurrence of the element within the given level of the hierarchy.
         *
         * <p>
         * This form of flattening is convenient for systems like DataWave that are able to leverage the additional context during query processing. If your
         * data is nested with repeating field names and you will need the ability to (easily) disambiguate them based on relative position within the original
         * hierarchy, then you should probably use this option
         */
        GROUPED,

        /**
         * <p>
         * Creates both {@link #NORMAL} and {@link #GROUPED} keys for maximum flexibility in terms of indexing and query options, at the expense of greater
         * storage cost.
         *
         * <p>
         * Implementors may decide to emit two distinct keys in the map, one {@code NORMAL} key and one {@code GROUPED} key, or a single key with the
         * {@link #NORMAL} mode field name applied as a prefix concatenated with the suffix from {@link #GROUPED} mode
         */
        GROUPED_AND_NORMAL;
    }

    /**
     * Flattens the specified json
     *
     * @param object
     *            {@link JsonObject} instance to flatten
     * @return {@link com.google.common.collect.Multimap} instance with the flattened keys and associated values
     * @throws IllegalStateException
     *             if {@link FlattenMode#GROUPED} is used and {@link Builder#pathDelimiter} value is found to exist already within the object's key names
     */
    Multimap<String,String> flatten(JsonObject object) throws IllegalStateException;

    /**
     * Flattens the specified json
     *
     * @param object
     *            {@link JsonObject} instance to flatten
     * @param map
     *            {@link com.google.common.collect.Multimap} instance to receive the flattened keys and associated values
     * @throws IllegalStateException
     *             if {@link FlattenMode#GROUPED} is used and {@link Builder#pathDelimiter} is found to exist already within a json property name
     * @throws NullPointerException
     *             if map is null
     */
    void flatten(JsonObject object, Multimap<String,String> map) throws IllegalStateException, NullPointerException;

    /**
     * <p>
     * Allows clients to specify custom normalization behavior per their needs to affect the final presentation of flattened keys and their values to the
     * {@link Multimap} instance
     *
     * <p>
     * It's the resulting normalized key that {@link JsonObjectFlattener} implementations should use to compare against blacklist and whitelist sets, with
     * normalization applied as close to the actual {@link Multimap#put(Object, Object)} operation as possible
     */
    interface MapKeyValueNormalizer {

        /**
         * Takes a candidate key and its associated value and returns the "normalized" version of the key.
         *
         * @param key
         *            candidate {@link Multimap} key, non-normalized
         * @param value
         *            (optional) value for the key, in case normalization of the key happens to depend on its associated value
         * @return the normalized candidate key
         * @throws IllegalStateException
         *             if the key, or key/value combination, represents an invalid state per the client
         */
        String normalizeMapKey(String key, String value) throws IllegalStateException;

        /**
         * Takes a candidate value and its associated key and returns the "normalized" version of the value.
         *
         * @param value
         *            value for the key
         * @param key
         *            (optional) candidate {@link Multimap} key, normalized, in case normalization of the value happens to depend on its associated key
         * @return the normalized candidate value
         * @throws IllegalStateException
         *             if the value, or key/value combination, represents an invalid state per the client
         */
        String normalizeMapValue(String value, String key) throws IllegalStateException;

        /**
         * No op impl for convenience
         */
        class NoOp implements MapKeyValueNormalizer {

            @Override
            public String normalizeMapKey(String key, String value) throws IllegalStateException {
                // No op
                return key;
            }

            @Override
            public String normalizeMapValue(String value, String key) throws IllegalStateException {
                // No op
                return value;
            }
        }
    }

    /**
     * <p>
     * Gives clients the ability to validate and/or transform an individual json element name at the earliest point possible, ie, at the point that it is first
     * encountered during traversal of the tree
     */
    interface JsonElementNameNormalizer {

        /**
         * Takes a json element key name and returns its "normalized" version
         *
         * @param elementName
         *            json element name, non-normalized
         * @param parentKey
         *            (optional) non-normalized, flattened parent key for context, if needed
         * @return the normalized json element name
         * @throws IllegalStateException
         *             if the element name represents an invalid state for the client for whatever reason
         */
        String normalizeElementName(String elementName, String parentKey) throws IllegalStateException;

        /**
         * No op impl for convenience
         */
        class NoOp implements JsonElementNameNormalizer {
            @Override
            public String normalizeElementName(String elementName, String parentKey) throws IllegalStateException {
                // No op
                return elementName;
            }
        }
    }

    /**
     * Gets the current flatten mode
     *
     * @return current mode
     */
    FlattenMode getFlattenMode();

    interface Builder<T extends JsonObjectFlattener> {
        /**
         * Sets delimiter to be used as the path separator in field names.
         *
         * @param pathDelimiter
         *            delimiter to use as path separator
         * @return builder instance
         * @throws NullPointerException
         *             if delimiter argument is null
         */
        Builder<T> pathDelimiter(String pathDelimiter) throws NullPointerException;

        /**
         * Only map keys matching those in whitelist will be added to the flattened map. If used in conjunction with blacklist, blacklist takes precedence
         *
         * @param mapKeyWhitelist
         *            whitelisted keys
         * @return builder instance
         */
        Builder<T> mapKeyWhitelist(Set<String> mapKeyWhitelist);

        /**
         * Map keys within blacklist will be excluded from the flattened map. If used in conjunction with whitelist, blacklist takes precedence
         *
         * @param mapKeyBlacklist
         *            blacklisted keys
         * @return builder instance
         */
        Builder<T> mapKeyBlacklist(Set<String> mapKeyBlacklist);

        /**
         * <p>
         * Sets the {@link FlattenMode} to be used. If {@link FlattenMode#GROUPED} is used, the raw json property names may be validated within
         * {@link #flatten(JsonObject)} and {@link #flatten(JsonObject, Multimap)} to ensure that they do not already contain the configured path delimiter.
         *
         * <p>
         * If they do, then {@link IllegalStateException} may result. Additionally, if the values chosen for path delimiter and occurrence are the same, then
         * {@link IllegalStateException} should be thrown by {@link #build()}
         *
         * @param mode
         *            mode to be used for flattening
         * @return builder instance
         */
        Builder<T> flattenMode(FlattenMode mode);

        /**
         * Ignored unless {@link FlattenMode#GROUPED} is applied. The separator to use between a field name and its ordinal position within its given parent
         * grouping.
         *
         * @param delimiter
         *            delimiter to use between group names and their integer position, as part of the grouping context
         * @return builder instance
         * @throws NullPointerException
         *             if delimiter argument is null
         */
        Builder<T> occurrenceInGroupDelimiter(String delimiter) throws NullPointerException;

        /**
         * <p>
         * Sets the {@link MapKeyValueNormalizer} to be applied to flattened keys just prior to {@link Multimap#put(Object, Object)} and also prior to
         * whitelist/blacklist checks. That is, create your whitelist and blacklist sets based on your ideal, "normalized" key structure
         *
         * @param normalizer
         *            normalizer instance
         * @return builder instance
         */
        Builder<T> mapKeyValueNormalizer(MapKeyValueNormalizer normalizer);

        /**
         * Sets the {@link JsonElementNameNormalizer} to be applied to individual element names as they are encountered during tree traversal, for the purposes
         * of validation and/or transformation
         *
         * @param normalizer
         *            {@link JsonElementNameNormalizer} instance
         * @return builder instance
         */
        Builder<T> jsonElementNameNormalizer(JsonElementNameNormalizer normalizer);

        /**
         * Set to true if you want an element's array index to be added to the flattened field name, for uniqueness. If set to false, then all array element
         * values will be added to the Multimap under the same key
         *
         * @param addArrayIndexToFieldName
         *            true or false
         * @return builder instance
         */
        Builder<T> addArrayIndexToFieldName(boolean addArrayIndexToFieldName);

        /**
         * Creates the flattener instance
         *
         * @return {@link JsonObjectFlattener} instance
         * @throws IllegalStateException
         *             if {@link FlattenMode#GROUPED} is used and the values chosen for {@link #pathDelimiter} and {@link #occurrenceInGroupDelimiter} are equal
         */
        JsonObjectFlattener build() throws IllegalStateException;
    }
}
