package datawave.test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Content;
import datawave.query.attributes.Document;
import datawave.query.function.JexlEvaluation;

/**
 * Utility class for asserting expected hit terms.
 * <p>
 * A hit term takes the form of <code>FIELD:value</code>. This utility can assert hit terms that are either required or optional.
 * <p>
 * A required hit term must be present in every result. Each set of required terms can be evaluated as 'all of' or 'any of'
 * <p>
 * An optional hit term might be present in any result. Each set of optional terms can be evaluated as 'all of' or 'any of'
 * <p>
 * In the single term example: <code>F:1</code>
 * <ul>
 * <li>requiredAllOf: {F:1}</li>
 * </ul>
 * In the simple intersection example: <code>F:1 and F:2</code>
 * <ul>
 * <li>requiredAllOf: {F:1, F:2}</li>
 * </ul>
 * In the simple union example: <code>F:1 or F:2</code>
 * <ul>
 * <li>requiredAnyOf: {F:1, F:2}</li>
 * </ul>
 * In the intersection with nested union case: <code>F:1 and (F:2 or F:3)</code>
 * <ul>
 * <li>requiredAllOf: {F:1}</li>
 * <li>requiredAnyOf: {F:2, F:3}</li>
 * </ul>
 * In the union with nested intersection case: <code>F:1 or (F:2 and F:3)</code>
 * <ul>
 * <li>optionalAllOf: {F:1}</li>
 * <li>optionalAllOf: {F:2, F:3}</li>
 * </ul>
 * In the double nested intersection case: <code>(F:1 and F:2) or (F:3 and F:4)</code>
 * <ul>
 * <li>optionalAllOf: {F:1, F:2}</li>
 * <li>optionalAllOf: {F:3, F:4}</li>
 * </ul>
 * In the double nested union case: <code>(F:1 or F:2) and (F:3 or F:4)</code>
 * <ul>
 * <li>optionalAnyOf: {F:1, F:2}</li>
 * <li>optionalAnyOf: {F:3, F:4}</li>
 * </ul>
 * <p>
 * Google {@link Preconditions} are used instead of JUnit assertions for portability. DataWave currently supports both JUnit 4 and JUnit 5, so preconditions are
 * used until the codebase converges on a single version of JUnit.
 */
public class HitTermAssertions {

    private static final Logger log = LoggerFactory.getLogger(HitTermAssertions.class);

    private static final String HIT_TERM_FIELD = JexlEvaluation.HIT_TERM_FIELD;

    private final List<Set<String>> requiredAllOf = new ArrayList<>();
    private final List<Set<String>> requiredAnyOf = new ArrayList<>();
    private final List<Set<String>> optionalAllOf = new ArrayList<>();
    private final List<Set<String>> optionalAnyOf = new ArrayList<>();

    private final Set<String> discovered = new HashSet<>();

    private boolean expectNoHitTerms = true;
    private boolean validated = true;

    public HitTermAssertions() {
        // no-op
    }

    /**
     * This should be called before or after every test
     */
    public void resetState() {
        requiredAllOf.clear();
        requiredAnyOf.clear();
        optionalAllOf.clear();
        optionalAnyOf.clear();
        discovered.clear();
        validated = true;
        expectNoHitTerms = false;
    }

    /**
     * Add a collection of hit terms to the set of required hit terms, evaluated as 'all of'
     *
     * @param hits
     *            a collection of hit terms
     */
    public void withRequiredAllOf(String... hits) {
        expectNoHitTerms = false;
        requiredAllOf.add(Set.of(hits));
    }

    /**
     * Add a collection of hit terms to the set of required hit terms, evaluated as 'any of'
     *
     * @param hits
     *            a collection of hit terms
     */
    public void withRequiredAnyOf(String... hits) {
        expectNoHitTerms = false;
        requiredAnyOf.add(Set.of(hits));
    }

    /**
     * Add a collection of hits to the list of optional hit terms, evaluated as 'all of'
     *
     * @param hits
     *            a collection of hit terms
     */
    public void withOptionalAllOf(String... hits) {
        expectNoHitTerms = false;
        optionalAllOf.add(Set.of(hits));
    }

    /**
     * Add a collection of hits to the list of optional hit terms, evaluated as 'any of'
     *
     * @param hits
     *            a collection of hit terms
     */
    public void withOptionalAnyOf(String... hits) {
        expectNoHitTerms = false;
        optionalAnyOf.add(Set.of(hits));
    }

    public void expectNoHitTerms() {
        expectNoHitTerms = true;
    }

    /**
     * Method that helps a test determine if a hit term is expected
     *
     * @return true if a hit term is expected
     */
    public boolean hitTermExpected() {
        return !requiredAllOf.isEmpty() || !requiredAnyOf.isEmpty() || !optionalAllOf.isEmpty() || !optionalAnyOf.isEmpty();
    }

    /**
     * Assert hit terms for the provided collection of documents
     *
     * @param documents
     *            a collection of documents
     * @return true if all documents are valid
     */
    public boolean assertHitTerms(Collection<Document> documents) {
        boolean allValid = true;
        for (Document document : documents) {
            boolean validated = assertHitTerms(document);
            if (!validated) {
                allValid = false;
            }
        }
        return allValid;
    }

    /**
     * Assert hit terms for a single document
     *
     * @param document
     *            the document
     * @return true if all documents are valid
     */
    public boolean assertHitTerms(Document document) {
        // reset the discovered hit terms for each document
        discovered.clear();

        Preconditions.checkNotNull(document, "Expected document to be non-null");

        boolean anyHitTermSet = !requiredAllOf.isEmpty() || !requiredAnyOf.isEmpty() || !optionalAllOf.isEmpty() || !optionalAnyOf.isEmpty();
        Preconditions.checkState((expectNoHitTerms != anyHitTermSet), "Invalid configuration: cannot expect hit terms and set expectNoHitTerm to false");

        Set<String> hits = extractHitTermsFromDocument(document);

        if (expectNoHitTerms) {
            if (!hits.isEmpty()) {
                validated = false;
                log.warn("Expected hit terms to be empty, but found {}", hits);
            }
            return validated;
        }

        if (anyHitTermSet && hits.isEmpty()) {
            log.warn("Expected hit terms but document contained no hit terms");
            return false;
        }

        if (!anyHitTermSet && !hits.isEmpty()) {
            log.warn("No hit terms expected but found hit terms: {}", hits);
            return false;
        }

        allRequiredHitTermsFound(hits);
        atLeastOneRequiredHitTermFound(hits);

        Set<String> extraHits = Sets.difference(hits, discovered);
        if (!extraHits.isEmpty()) {
            log.warn("Found unexpected hit terms:: {}", extraHits);
            Preconditions.checkArgument(extraHits.isEmpty(), "Found unexpected hit terms: " + extraHits);
        }

        return validated;
    }

    /**
     * Extract all HIT_TERMs from the document. Will throw an exception if a hit term is not a Content attribute
     *
     * @param document
     *            the document
     * @return the set of hit terms
     */
    private Set<String> extractHitTermsFromDocument(Document document) {
        if (!document.containsKey(HIT_TERM_FIELD)) {
            if (!expectNoHitTerms) {
                log.warn("Document did not contain any hit terms");
            }
            return Collections.emptySet();
        }

        Attribute<?> attribute = document.get(HIT_TERM_FIELD);
        Set<String> hits = new HashSet<>();
        if (attribute instanceof Attributes) {
            for (Attribute<?> attr : ((Attributes) attribute).getAttributes()) {
                Preconditions.checkArgument(attr instanceof Content, "HIT_TERM was not a Content attribute");
                Content content = (Content) attr;
                hits.add(content.getContent());
            }
        } else {
            Preconditions.checkArgument(attribute instanceof Content, "HIT_TERM was not a Content attribute");
            Content content = (Content) attribute;
            hits.add(content.getContent());
        }

        return hits;
    }

    /**
     * Verify that each required hit term is present in the set of extracted hit terms
     *
     * @param hits
     *            the set of extracted hit terms
     */
    private void allRequiredHitTermsFound(Set<String> hits) {
        for (Set<String> requiredHits : requiredAllOf) {
            if (hits.containsAll(requiredHits)) {
                discovered.addAll(requiredHits);
                continue;
            }

            // otherwise we have a problem
            validated = false;

            Set<String> found = Sets.union(requiredHits, hits);
            Set<String> missing = Sets.difference(hits, requiredHits);
            log.warn("Expected to find all of: {}", requiredHits);
            log.warn("Found: {}", found);
            log.warn("Missing: {}", missing);
        }

        for (Set<String> requiredHits : requiredAnyOf) {
            boolean found = false;
            for (String requiredHit : requiredHits) {
                if (hits.contains(requiredHit)) {
                    found = true;
                    discovered.add(requiredHit);
                }
            }

            if (!found) {
                log.warn("expected to find at least one of required hits: {}", requiredHits);
                log.warn("returned hit terms: {}", hits);
                validated = false;
            }
        }
    }

    /**
     * At least one of the optional sets of hit terms must contain a match
     *
     * @param hits
     *            the set of extracted hit terms
     */
    private void atLeastOneRequiredHitTermFound(Set<String> hits) {
        boolean anyOptionalFound = false;
        for (Set<String> optionalHits : optionalAllOf) {
            if (hits.containsAll(optionalHits)) {
                anyOptionalFound = true;
                discovered.addAll(optionalHits);
            }
        }

        for (Set<String> optionalHits : optionalAnyOf) {
            for (String optionalHit : optionalHits) {
                if (hits.contains(optionalHit)) {
                    anyOptionalFound = true;
                    discovered.add(optionalHit);
                }
            }
        }

        if (requiredAllOf.isEmpty() && requiredAnyOf.isEmpty() && !anyOptionalFound) {
            // safety check: optional hits are the only hits configured and none were found, fail validation.
            validated = false;
        }
    }
}
