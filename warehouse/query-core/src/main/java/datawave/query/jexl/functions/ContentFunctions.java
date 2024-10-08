package datawave.query.jexl.functions;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

import datawave.query.attributes.ValueTuple;
import datawave.query.postprocessing.tf.TermOffsetMap;
import datawave.util.StringUtils;

/**
 * <p>
 * Jexl functions that determine if a set of offset lists occur within some arbitrary distance. The functions must be given at least two terms. If the function
 * encounters any errors in the arguments, it will return false. Thus, improper function calls will appear to return no results to the user.
 * </p>
 *
 * <p>
 * To support within() queries on all unicode terms, the terms are presented as an array of Strings and the integer offset lists are stored in the
 * <code>Map&lt;String, List&lt;Integer&gt;&gt;</code>. There should be an entry in the map for each term provided in the function call
 * </p>
 *
 * <b>Functions</b>
 * <ul>
 * <li>content:within(int, map, term1, term2, ...)
 * <ul>
 * <li>Returns true if the terms occur within the specified distance of each other</li>
 * <li>The distance parameter is the maximum acceptable distance (term offset) between the terms provided</li>
 * <li>For example, for the phrase "the quick brown fox" content:within(2, 'quick', 'brown', 'fox') will return true because the difference in word offsets one
 * and three is less than or equal to two {@code (3 - 1 <= 2)}. Searching for {@code content:within(1, 'quick', 'brown', 'fox')} will fail because it is
 * impossible for three terms to have a minimum distance of two.</li>
 * </ul>
 * </li>
 * <li>content:within(zone, int, map, term1, term2, ...)
 * <ul>
 * <li>Same as content:within() but with a zone specified</li>
 * </ul>
 * </li>
 * <li>content:adjacent(map, term1, term2, ...)
 * <ul>
 * <li>Calls within() with a distance of num(terms) - 1, meaning that all of the terms occur next to each other</li>
 * <li>e.g. content:adjacent(map, term1, term2, term3) will call content:within(2, map, term1, term2, term3)</li>
 * </ul>
 * </li>
 * <li>content:adjacent(zone, map, term1, term2, ...)
 * <ul>
 * <li>Same as content:adjacent() but with a zone specified</li>
 * </ul>
 * </li>
 * <li>content:phrase(map, term1, term2, ...)
 * <ul>
 * <li>Only matches true on documents that contain the terms adjacent to each other in the order provided. Synonyms at the same position are considered
 * adjacent.</li>
 * </ul>
 * </li>
 * <li>content:phrase(zone, map, term1, term2, ...)
 * <ul>
 * <li>Same as content:phrase() but with a zone specified</li>
 * </ul>
 * </li>
 * </ul>
 *
 * NOTE: The JexlFunctionArgumentDescriptorFactory is implemented by ContentFunctionsDescriptor. This is kept as a separate class to reduce accumulo
 * dependencies on other jars.
 */
@JexlFunctions(descriptorFactory = "datawave.query.jexl.functions.ContentFunctionsDescriptor")
public class ContentFunctions {
    private static final Logger log = Logger.getLogger(ContentFunctions.class);

    public static final String TERM_OFFSET_MAP_JEXL_VARIABLE_NAME = "termOffsetMap";
    public static final String CONTENT_FUNCTION_NAMESPACE = "content";
    public static final String CONTENT_WITHIN_FUNCTION_NAME = "within";
    public static final String CONTENT_ADJACENT_FUNCTION_NAME = "adjacent";
    public static final String CONTENT_PHRASE_FUNCTION_NAME = "phrase";
    public static final String CONTENT_SCORED_PHRASE_FUNCTION_NAME = "scoredPhrase";

    /**
     * Determine if the given offset lists have any permutation of across each offset list that is within the distance given.
     *
     * @param distance
     *            The maximum acceptable distance for the terms to occur near each other
     * @param termOffsetMap
     *            A map of terms and their offset lists
     * @param terms
     *            The array of terms
     * @return a collection of fields that satisfy the content function
     */
    public static Collection<String> within(int distance, TermOffsetMap termOffsetMap, String... terms) {
        return new ContentUnorderedEvaluator(Collections.emptySet(), distance, Float.NEGATIVE_INFINITY, termOffsetMap, terms).evaluate();
    }

    /**
     * Like {@link #within(int, TermOffsetMap, String...)} but limited to only finding a match in a single zone.
     *
     * @param zone
     *            The zone or zones to search within
     * @param distance
     *            The maximum acceptable distance for the terms to occur near each other
     * @param termOffsetMap
     *            A map of terms and their offset lists
     * @param terms
     *            The array of terms
     * @return a collection of fields that satisfy the content function
     * @see #within(int, TermOffsetMap, String...)
     */
    public static Collection<String> within(Object zone, int distance, TermOffsetMap termOffsetMap, String... terms) {
        return new ContentUnorderedEvaluator(getFields(zone), distance, Float.NEGATIVE_INFINITY, termOffsetMap, terms).evaluate();
    }

    /**
     * Like {@link #within(int, TermOffsetMap, String...)} but limited to only finding a match in a single zone.
     *
     * @param zones
     *            The zone or zones to search within
     * @param distance
     *            The maximum acceptable distance for the terms to occur near each other
     * @param termOffsetMap
     *            A map of terms and their offset lists
     * @param terms
     *            The array of terms
     * @return a collection of fields that satisfy the content function
     * @see #within(int, TermOffsetMap, String...)
     */
    public static Collection<String> within(Iterable<?> zones, int distance, TermOffsetMap termOffsetMap, String... terms) {
        return new ContentUnorderedEvaluator(getFields(zones), distance, Float.NEGATIVE_INFINITY, termOffsetMap, terms).evaluate();
    }

    /**
     * A wrapper function around searching for a occurrence where the terms exist next to each other Calls {@link #within} with a distance argument of the
     * length of {@code offsets}
     *
     * @param termOffsetMap
     *            A map of terms and their offset lists
     * @param terms
     *            The array of terms
     * @return a collection of fields that satisfy the content function
     */
    public static Collection<String> adjacent(TermOffsetMap termOffsetMap, String... terms) {
        return within(terms.length - 1, termOffsetMap, terms);
    }

    /**
     * Wrapper around {@link #adjacent} in which a zone is provided
     *
     * @param zone
     *            The zone the phrase must occur in
     * @param termOffsetMap
     *            A map of terms and their offset lists
     * @param terms
     *            The array of terms
     * @return a collection of fields that satisfy the content function
     */
    public static Collection<String> adjacent(Object zone, TermOffsetMap termOffsetMap, String... terms) {
        return within(getFields(zone), terms.length - 1, termOffsetMap, terms);
    }

    /**
     * Wrapper around {@link #adjacent} in which a zone is provided
     *
     * @param zone
     *            The zone the phrase must occur in
     * @param termOffsetMap
     *            A map of terms and their offset lists
     * @param terms
     *            The array of terms
     * @return a collection of fields that satisfy the content function
     */
    public static Collection<String> adjacent(Iterable<?> zone, TermOffsetMap termOffsetMap, String... terms) {
        return within(getFields(zone), terms.length - 1, termOffsetMap, terms);
    }

    /**
     * Searches for the terms occurring in the order specified
     *
     * @param termOffsetMap
     *            A map of terms and their offset lists
     * @param terms
     *            The array of terms
     * @return a collection of fields that satisfy the content function
     */
    public static Collection<String> phrase(TermOffsetMap termOffsetMap, String... terms) {
        return new ContentOrderedEvaluator(Collections.emptySet(), 1, Float.NEGATIVE_INFINITY, termOffsetMap, terms).evaluate();
    }

    /**
     * Wrapper around {@link #phrase} in which a zone is provided
     *
     * @param zone
     *            The zone the phrase must occur in
     * @param termOffsetMap
     *            A map of terms and their offset lists
     * @param terms
     *            The array of terms
     * @return a collection of fields that satisfy the content function
     */
    public static Collection<String> phrase(Object zone, TermOffsetMap termOffsetMap, String... terms) {
        return new ContentOrderedEvaluator(getFields(zone), 1, Float.NEGATIVE_INFINITY, termOffsetMap, terms).evaluate();
    }

    /**
     * Determine if we have a phrase for any zone under the specified score.
     *
     * @param minScore
     *            The lowest score allowed for a term
     * @param termOffsetMap
     *            A map of terms and their offset lists
     * @param terms
     *            The array of terms
     * @return a collection of fields that satisfy the content function
     */
    public static Collection<String> scoredPhrase(Number minScore, TermOffsetMap termOffsetMap, String... terms) {
        return new ContentOrderedEvaluator(Collections.emptySet(), 1, minScore.floatValue(), termOffsetMap, terms).evaluate();
    }

    /**
     * Wrapper around {@link #scoredPhrase} in which a zone is provided
     *
     * @param zone
     *            The zone the phrase must occure in
     * @param minScore
     *            The lowest score allowed for a term
     * @param termOffsetMap
     *            A map of terms and their offset lists
     * @param terms
     *            The array of terms
     * @return a collection of fields that satisfy the content function
     */
    public static Collection<String> scoredPhrase(Object zone, Number minScore, TermOffsetMap termOffsetMap, String... terms) {
        return new ContentOrderedEvaluator(getFields(zone), 1, minScore.floatValue(), termOffsetMap, terms).evaluate();
    }

    /**
     * Wrapper around {@link #phrase} in which a zone is provided
     *
     * @param zone
     *            The zone the phrase must occur in
     * @param termOffsetMap
     *            A map of terms and their offset lists
     * @param terms
     *            The array of terms
     * @return a collection of fields that satisfy the content function
     */
    public static Collection<String> phrase(Iterable<?> zone, TermOffsetMap termOffsetMap, String... terms) {
        return new ContentOrderedEvaluator(getFields(zone), 1, Float.NEGATIVE_INFINITY, termOffsetMap, terms).evaluate();
    }

    /**
     * Get the fields from a zone object. The zone can be an iterable, a value tuple, or a string. Once converted to string form, it will split it by the '|'
     * character. This allows us to specify a zone as something pulled from the context, or as a literal string.
     *
     * @param zone
     *            a zone object
     * @return the set of field names extracted from the "zone" object
     */
    private static Set<String> getFields(Object zone) {
        Set<String> fields = new HashSet<>();
        if (zone != null) {
            if (zone instanceof Iterable) {
                for (Object z : ((Iterable<?>) zone)) {
                    fields.addAll(getFields(z));
                }
            } else {
                String z = ValueTuple.getFieldName(zone);
                String[] parts = StringUtils.split(z, '|');
                fields.addAll(Arrays.asList(parts));
            }
        }
        return fields;
    }

}
