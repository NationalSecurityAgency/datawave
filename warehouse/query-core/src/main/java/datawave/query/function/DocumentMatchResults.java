package datawave.query.function;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.data.Key;

import com.google.common.annotations.VisibleForTesting;

import datawave.query.data.parsers.DatawaveKey;

/**
 * Match results for a single matched {@code d}-column entry.
 * <p>
 * A {@code d}-column entry has a single view name, so matches are grouped only by search string within that view.
 */
public class DocumentMatchResults {
    public static final String VIEW_FIELD = "view";
    public static final String MATCHES_FIELD = "matches";

    private final Key key;
    private final Map<String,SortedSet<Integer>> matches = new LinkedHashMap<>();

    /**
     * Creates an empty result container for a single matched {@code d}-column entry.
     *
     * @param key
     *            the matched {@code d}-column key
     */
    public DocumentMatchResults(Key key) {
        this.key = key;
    }

    private DocumentMatchResults(DocumentMatchResults other) {
        this.key = other.key;
        for (Map.Entry<String,SortedSet<Integer>> searchEntry : other.matches.entrySet()) {
            this.matches.put(searchEntry.getKey(), new TreeSet<>(searchEntry.getValue()));
        }
    }

    public Key getKey() {
        return key;
    }

    /**
     * @return the single view name associated with this matched {@code d}-column entry
     */
    @VisibleForTesting
    public String getView() {
        return Objects.toString(new DatawaveKey(key).getFieldName(), "");
    }

    /**
     * Records offsets for a literal search string within this entry's view.
     *
     * @param search
     *            the matched literal string
     * @param offsets
     *            ordered starting offsets where the string was found
     */
    public void addMatches(String search, List<Integer> offsets) {
        matches.computeIfAbsent(search, ignored -> new TreeSet<>()).addAll(offsets);
    }

    /**
     * @param search
     *            a matched literal string
     * @return {@code true} if this entry contains offsets for the supplied search string
     */
    public boolean containsSearch(String search) {
        return matches.containsKey(search);
    }

    /**
     * Builds the JSON-ready payload for this entry in the form {@code {"view":"...","matches":{search:[offsets]}}}.
     *
     * @return a payload map suitable for serialization into the {@code DOCUMENT_MATCHES} attribute, or an empty map if no matches are present
     */
    public Map<String,Object> getPayload() {
        Map<String,Object> payload = new LinkedHashMap<>();
        String view = getView();
        if (view == null || matches.isEmpty()) {
            return payload;
        }
        payload.put(VIEW_FIELD, view);
        payload.put(MATCHES_FIELD, matches);
        return payload;
    }

    /**
     * @return a defensive copy of this entry's match results
     */
    public DocumentMatchResults copy() {
        return new DocumentMatchResults(this);
    }
}
