package datawave.query.function;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import datawave.query.predicate.TimeFilter;

/**
 * Per-document runtime state used by {@code document:match(...)} evaluation.
 * <p>
 * This context carries the raw {@code d}-column entries retained for a candidate document, the configured size limits used while decoding those payloads, and
 * the per-{@code d}-column {@link DocumentMatchResults} accumulated across one or more {@code document:match(...)} calls for a single evaluation. Context
 * instances are expected to be created fresh for each evaluation pass rather than reused across multiple documents or repeated evaluations of the same
 * document.
 */
public class DocumentMatchContext {
    public static final int DEFAULT_MAX_ENCODED_SIZE = 256 * 1024 * 1024;
    public static final int DEFAULT_MAX_DECODED_SIZE = 384 * 1024 * 1024;
    public static final int DEFAULT_MAX_ENCODED_CONTEXT_SIZE = 256 * 1024 * 1024;

    /**
     * Immutable runtime limits for {@code document:match(...)} payload processing.
     */
    public static class Limits {
        private final int maxEncodedValueSize;
        private final int maxDecodedValueSize;
        private final int maxEncodedContextSize;

        /**
         * @param maxEncodedValueSize
         *            maximum allowed encoded payload size in bytes
         * @param maxDecodedValueSize
         *            maximum allowed decoded payload size in bytes
         * @param maxEncodedContextSize
         *            maximum allowed aggregate encoded payload size retained for a document, in bytes
         */
        public Limits(int maxEncodedValueSize, int maxDecodedValueSize, int maxEncodedContextSize) {
            this.maxEncodedValueSize = maxEncodedValueSize;
            this.maxDecodedValueSize = maxDecodedValueSize;
            this.maxEncodedContextSize = maxEncodedContextSize;
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

        /**
         * @return the maximum aggregate encoded payload size retained for a document, in bytes
         */
        public int getMaxEncodedContextSize() {
            return maxEncodedContextSize;
        }
    }

    private final List<Entry<Key,Value>> documentEntries;
    private final Limits limits;
    private final Map<Key,DocumentMatchResults> matches = new LinkedHashMap<>();

    /**
     * Creates a per-evaluation match context for the retained {@code d}-column entries of a single candidate document.
     *
     * @param documentEntries
     *            retained {@code d}-column entries for the document being evaluated
     * @param limits
     *            payload-processing limits applied during decode and match extraction
     */
    public DocumentMatchContext(List<Entry<Key,Value>> documentEntries, Limits limits) {
        this.documentEntries = documentEntries;
        this.limits = limits;
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
     * @return a fresh context containing only eligible {@code d}-column entries for a single evaluation pass
     */
    public static DocumentMatchContext from(List<Entry<Key,Value>> entries, TimeFilter timeFilter, Limits limits) {
        List<Entry<Key,Value>> documentEntries = new ArrayList<>();
        for (Entry<Key,Value> entry : entries) {
            if (entry.getKey().getColumnFamily().toString().equals("d") && (timeFilter == null || timeFilter.apply(entry))) {
                documentEntries.add(entry);
            }
        }
        return new DocumentMatchContext(documentEntries, limits);
    }

    /**
     * @return the retained {@code d}-column entries available to {@code document:match(...)} during the current evaluation
     */
    public List<Entry<Key,Value>> getDocumentEntries() {
        return Collections.unmodifiableList(documentEntries);
    }

    public int getMaxEncodedValueSize() {
        return limits.getMaxEncodedValueSize();
    }

    public int getMaxDecodedValueSize() {
        return limits.getMaxDecodedValueSize();
    }

    public int getMaxEncodedContextSize() {
        return limits.getMaxEncodedContextSize();
    }

    /**
     * @return the payload-processing limits associated with this evaluation context
     */
    public Limits getLimits() {
        return limits;
    }

    /**
     * Records per-call matches in the per-{@code d}-column document-wide result set.
     *
     * @param key
     *            the matched {@code d}-column key
     * @param search
     *            the literal string matched by the invocation
     * @param offsets
     *            starting offsets found in the matched view
     */
    public void addMatches(Key key, String search, List<Integer> offsets) {
        matches.computeIfAbsent(key, DocumentMatchResults::new).addMatches(search, offsets);
    }

    /**
     * Returns the per-entry match results accumulated during this evaluation.
     *
     * @return an immutable snapshot view of the accumulated per-{@code d}-column match results
     */
    public List<DocumentMatchResults> getMatches() {
        return List.copyOf(matches.values());
    }
}
