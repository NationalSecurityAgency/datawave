package datawave.query.function;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.data.Key;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;

import datawave.query.data.parsers.DatawaveKey;

/**
 * Match results for a single matched {@code d}-column entry.
 * <p>
 * A {@code d}-column entry has a single view name, so matches are grouped only by search string within that view.
 */
public class DocumentMatchResults {
    private static final Gson GSON = new Gson();

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
     * Builds the JSON-ready payload for this entry in the form {@code {"view":"...","matches":{search:[offsets]}}}.
     *
     * @return a payload map suitable for serialization into the {@code DOCUMENT_MATCHES} attribute, or an empty map if no matches are present
     */
    private Map<String,Object> getPayload() {
        Map<String,Object> payload = new LinkedHashMap<>();
        String view = getView();
        if (view == null || matches.isEmpty()) {
            return payload;
        }
        payload.put(VIEW_FIELD, view);
        Map<String,List<Integer>> jsonMatches = new LinkedHashMap<>();
        for (Map.Entry<String,SortedSet<Integer>> matchEntry : matches.entrySet()) {
            jsonMatches.put(matchEntry.getKey(), new ArrayList<>(matchEntry.getValue()));
        }
        payload.put(MATCHES_FIELD, jsonMatches);
        return payload;
    }

    /**
     * Serializes this entry's payload into the {@code DOCUMENT_MATCHES} JSON representation.
     *
     * @return JSON string representation, or an empty string if no matches were recorded for the entry
     */
    public String toJson() {
        Map<String,Object> payload = getPayload();
        if (payload.isEmpty()) {
            return "";
        }
        return GSON.toJson(payload);
    }
}
