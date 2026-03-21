package datawave.query.function;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;

import datawave.query.predicate.TimeFilter;

/**
 * Per-document runtime state used by {@code document:match(...)} evaluation.
 * <p>
 * This context carries the raw {@code d}-column entries retained for a candidate document, the configured size limits used while decoding those payloads, the
 * merged offset results accumulated across one or more {@code document:match(...)} calls, grouped first by matched string and then by view, and the first
 * matched {@code d}-column key whose visibility should be applied to the derived {@code DOCUMENT_MATCHES} attribute.
 */
public class DocumentMatchContext {
    public static final int DEFAULT_MAX_ENCODED_SIZE = 256 * 1024 * 1024;
    public static final int DEFAULT_MAX_DECODED_SIZE = 384 * 1024 * 1024;

    /**
     * Immutable runtime limits for {@code document:match(...)} payload processing.
     */
    public static class Limits {
        private final int maxEncodedValueSize;
        private final int maxDecodedValueSize;

        /**
         * @param maxEncodedValueSize
         *            maximum allowed encoded payload size in bytes
         * @param maxDecodedValueSize
         *            maximum allowed decoded payload size in bytes
         */
        public Limits(int maxEncodedValueSize, int maxDecodedValueSize) {
            this.maxEncodedValueSize = maxEncodedValueSize;
            this.maxDecodedValueSize = maxDecodedValueSize;
        }

        /**
         * @return the maximum encoded payload size, in bytes
         */
        public int getMaxEncodedValueSize() {
            return maxEncodedValueSize;
        }

        /**
         * @return the maximum decoded payload size, in bytes
         */
        public int getMaxDecodedValueSize() {
            return maxDecodedValueSize;
        }
    }

    private final List<Entry<Key,Value>> dEntries;
    private final Limits limits;
    private final Map<String,Map<String,Set<Integer>>> mergedMatches = new LinkedHashMap<>();
    private Key firstMatchingEntry;
    private boolean visibilityMismatchLogged = false;

    public DocumentMatchContext(List<Entry<Key,Value>> dEntries, Limits limits) {
        this.dEntries = dEntries;
        this.limits = limits;
    }

    public DocumentMatchContext(List<Entry<Key,Value>> dEntries, int maxEncodedValueSize) {
        this(dEntries, new Limits(maxEncodedValueSize, DEFAULT_MAX_DECODED_SIZE));
    }

    public DocumentMatchContext(List<Entry<Key,Value>> dEntries, int maxEncodedValueSize, int maxDecodedValueSize) {
        this(dEntries, new Limits(maxEncodedValueSize, maxDecodedValueSize));
    }

    /**
     * Builds a context from already-aggregated document entries using the default encoded and decoded payload limits.
     *
     * @param entries
     *            aggregated document entries
     * @param timeFilter
     *            optional time filter to apply while selecting {@code d}-column entries
     * @return a context containing only eligible {@code d}-column entries
     */
    public static DocumentMatchContext from(List<Entry<Key,Value>> entries, TimeFilter timeFilter) {
        return from(entries, timeFilter, new Limits(DEFAULT_MAX_ENCODED_SIZE, DEFAULT_MAX_DECODED_SIZE));
    }

    /**
     * Builds a context from already-aggregated document entries using a caller-supplied encoded payload limit and the default decoded payload limit.
     *
     * @param entries
     *            aggregated document entries
     * @param timeFilter
     *            optional time filter to apply while selecting {@code d}-column entries
     * @param maxEncodedValueSize
     *            maximum allowed encoded payload size in bytes
     * @return a context containing only eligible {@code d}-column entries
     */
    public static DocumentMatchContext from(List<Entry<Key,Value>> entries, TimeFilter timeFilter, int maxEncodedValueSize) {
        return from(entries, timeFilter, new Limits(maxEncodedValueSize, DEFAULT_MAX_DECODED_SIZE));
    }

    /**
     * Builds a context from already-aggregated document entries using explicit encoded and decoded payload limits.
     *
     * @param entries
     *            aggregated document entries
     * @param timeFilter
     *            optional time filter to apply while selecting {@code d}-column entries
     * @param maxEncodedValueSize
     *            maximum allowed encoded payload size in bytes
     * @param maxDecodedValueSize
     *            maximum allowed decoded payload size in bytes
     * @return a context containing only eligible {@code d}-column entries
     */
    public static DocumentMatchContext from(List<Entry<Key,Value>> entries, TimeFilter timeFilter, int maxEncodedValueSize, int maxDecodedValueSize) {
        return from(entries, timeFilter, new Limits(maxEncodedValueSize, maxDecodedValueSize));
    }

    /**
     * Builds a context from already-aggregated document entries using explicit runtime limits.
     *
     * @param entries
     *            aggregated document entries
     * @param timeFilter
     *            optional time filter to apply while selecting {@code d}-column entries
     * @param limits
     *            payload-processing limits
     * @return a context containing only eligible {@code d}-column entries
     */
    public static DocumentMatchContext from(List<Entry<Key,Value>> entries, TimeFilter timeFilter, Limits limits) {
        List<Entry<Key,Value>> dEntries = new ArrayList<>();
        for (Entry<Key,Value> entry : entries) {
            if (entry.getKey().getColumnFamily().toString().equals("d") && (timeFilter == null || timeFilter.apply(entry))) {
                dEntries.add(entry);
            }
        }
        return new DocumentMatchContext(dEntries, limits);
    }

    public List<Entry<Key,Value>> getdEntries() {
        return Collections.unmodifiableList(dEntries);
    }

    public int getMaxEncodedValueSize() {
        return limits.getMaxEncodedValueSize();
    }

    public int getMaxDecodedValueSize() {
        return limits.getMaxDecodedValueSize();
    }

    public Limits getLimits() {
        return limits;
    }

    /**
     * Clears merged match state before evaluating a new document.
     */
    public void clearMergedMatches() {
        mergedMatches.clear();
        firstMatchingEntry = null;
        visibilityMismatchLogged = false;
    }

    /**
     * Merges per-call matches into the document-wide result set.
     *
     * @param search
     *            the literal string matched by the invocation
     * @param matches
     *            matches produced by one {@code document:match(...)} invocation, keyed by view name
     */
    public void mergeMatches(String search, Map<String,List<Integer>> matches) {
        Map<String,Set<Integer>> searchMatches = mergedMatches.computeIfAbsent(search, key -> new LinkedHashMap<>());
        for (Entry<String,List<Integer>> entry : matches.entrySet()) {
            searchMatches.computeIfAbsent(entry.getKey(), key -> new LinkedHashSet<>()).addAll(entry.getValue());
        }
    }

    /**
     * @return a defensive copy of the merged document-wide match results
     */
    public Map<String,Map<String,List<Integer>>> getMergedMatches() {
        Map<String,Map<String,List<Integer>>> matches = new LinkedHashMap<>();
        for (Entry<String,Map<String,Set<Integer>>> searchEntry : mergedMatches.entrySet()) {
            Map<String,List<Integer>> viewMatches = new LinkedHashMap<>();
            for (Entry<String,Set<Integer>> viewEntry : searchEntry.getValue().entrySet()) {
                viewMatches.put(viewEntry.getKey(), new ArrayList<>(viewEntry.getValue()));
            }
            matches.put(searchEntry.getKey(), viewMatches);
        }
        return matches;
    }

    /**
     * @return the first {@code d}-column key that matched during evaluation, or {@code null} if no match has been recorded yet
     */
    public Key getFirstMatchingEntry() {
        return firstMatchingEntry;
    }

    /**
     * @return the visibility from the first matched {@code d}-column key, or {@code null} if no match has been recorded yet
     */
    public ColumnVisibility getFirstMatchingColumnVisibility() {
        if (firstMatchingEntry == null) {
            return null;
        }
        return firstMatchingEntry.getColumnVisibilityParsed();
    }

    /**
     * Records a matched {@code d}-column key and detects whether its visibility differs from the first matched key for the document.
     *
     * @param key
     *            the matched {@code d}-column key
     * @return {@code true} if the key differs in visibility from the first matched key, otherwise {@code false}
     */
    public boolean recordMatchingEntry(Key key) {
        if (firstMatchingEntry == null) {
            firstMatchingEntry = key;
            return false;
        }
        return !firstMatchingEntry.getColumnVisibilityData().equals(key.getColumnVisibilityData());
    }

    /**
     * @return {@code true} if a visibility mismatch has not yet been logged for the current document
     */
    public boolean shouldLogVisibilityMismatch() {
        return !visibilityMismatchLogged;
    }

    /**
     * Marks the current document as having already logged a visibility mismatch.
     */
    public void markVisibilityMismatchLogged() {
        visibilityMismatchLogged = true;
    }
}
