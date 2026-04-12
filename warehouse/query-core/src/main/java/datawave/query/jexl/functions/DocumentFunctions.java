package datawave.query.jexl.functions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.log4j.Logger;

import com.google.gson.Gson;

import datawave.query.data.parsers.DatawaveKey;
import datawave.query.function.DocumentMatchContext;
import datawave.query.table.parser.ContentKeyValueFactory;

/**
 * Evaluation-phase JEXL functions for inspecting decoded shard-table {@code d}-column content.
 * <p>
 * The current namespace exposes {@code document:match(...)} which decodes base64-encoded, gzip-compressed document payloads, performs case-sensitive literal
 * substring matching, and returns the matched search string when any eligible {@code d}-column matches. Detailed per-entry offsets are accumulated in the
 * supplied {@link DocumentMatchContext} and later serialized into {@code DOCUMENT_MATCHES} attributes by the surrounding evaluation flow.
 */
@JexlFunctions(descriptorFactory = "datawave.query.jexl.functions.DocumentFunctionsDescriptor")
public class DocumentFunctions {
    private static final Logger log = Logger.getLogger(DocumentFunctions.class);
    private static final Gson GSON = new Gson();

    public static final String DOCUMENT_FUNCTION_NAMESPACE = "document";
    public static final String DOCUMENT_MATCH_FUNCTION_NAME = "match";
    public static final String DOCUMENT_MATCH_CONTEXT_JEXL_VARIABLE_NAME = "documentMatchContext";
    public static final String DOCUMENT_MATCHES = "DOCUMENT_MATCHES";

    /**
     * Evaluates the internal form of {@code document:match(STRING)} across all eligible views for the current document.
     *
     * @param context
     *            per-document context supplied by the evaluation pipeline
     * @param search
     *            literal substring to search for
     * @return the matched search string if any eligible {@code d}-column matches, or an empty string if no match is found
     */
    public static String match(DocumentMatchContext context, String search) {
        return match(null, context, search);
    }

    /**
     * Evaluates the internal form of {@code document:match(VIEWNAME, STRING)} against the current document.
     * <p>
     * Matching is case-sensitive and literal. If {@code viewName} ends with {@code *}, it is treated as a prefix match against the view portion of the
     * {@code d}-column qualifier. Oversized or undecodable payloads are skipped as non-matching. Matches from this invocation are accumulated in the supplied
     * {@link DocumentMatchContext} on a per-{@code d}-column basis so the resulting {@code DOCUMENT_MATCHES} attributes can preserve each source visibility.
     *
     * @param viewName
     *            optional exact or prefix-matched view selector; {@code null} means evaluate all views
     * @param context
     *            per-document context supplied by the evaluation pipeline
     * @param search
     *            literal substring to search for
     * @return the matched search string if any eligible {@code d}-column matches, or an empty string if no match is found
     */
    public static String match(String viewName, DocumentMatchContext context, String search) {
        if (context == null || search == null) {
            if (log.isDebugEnabled()) {
                log.debug("Skipping document:match evaluation because context or search term was null");
            }
            return "";
        }

        if (log.isDebugEnabled()) {
            log.debug("Evaluating document:match for search [" + search + "] view filter [" + viewName + "] across " + context.getDocumentEntries().size()
                            + " d-column entries");
        }

        boolean matched = false;
        for (Entry<Key,Value> entry : context.getDocumentEntries()) {
            String candidateView = Objects.toString(new DatawaveKey(entry.getKey()).getFieldName(), "");
            if (!matchesView(viewName, candidateView)) {
                if (log.isDebugEnabled()) {
                    log.debug("Skipping d-column entry " + entry.getKey() + " because view [" + candidateView + "] does not match filter [" + viewName + "]");
                }
                continue;
            }
            byte[] encoded = entry.getValue().get();
            if (encoded.length > context.getMaxEncodedValueSize()) {
                log.debug("Skipping oversized d-column payload of " + encoded.length + " bytes for view " + candidateView);
                continue;
            }

            try {
                String decoded = decode(encoded, context.getMaxDecodedValueSize());
                List<Integer> offsets = findOffsets(decoded, search);
                if (!offsets.isEmpty()) {
                    if (log.isDebugEnabled()) {
                        log.debug("document:match found offsets " + offsets + " for search [" + search + "] in view [" + candidateView + "] using key "
                                        + entry.getKey());
                    }
                    context.addMatches(entry.getKey(), search, offsets);
                    matched = true;
                } else if (log.isDebugEnabled()) {
                    log.debug("document:match found no offsets for search [" + search + "] in view [" + candidateView + "] using key " + entry.getKey());
                }
            } catch (IOException | IllegalArgumentException e) {
                log.debug("Unable to decode d-column payload for view " + candidateView, e);
            }
        }
        if (log.isDebugEnabled()) {
            log.debug("document:match produced matched=" + matched + " for search [" + search + "]");
        }
        return matched ? search : "";
    }

    /**
     * Determines whether a candidate view satisfies the requested selector.
     *
     * @param expectedView
     *            requested view selector; {@code null} matches all views and a trailing {@code *} indicates prefix matching
     * @param candidateView
     *            extracted view name for the current {@code d}-column
     * @return {@code true} if the candidate view should be evaluated
     */
    static boolean matchesView(String expectedView, String candidateView) {
        if (expectedView == null) {
            return true;
        }
        if (expectedView.endsWith("*")) {
            String prefix = expectedView.substring(0, expectedView.length() - 1);
            return candidateView.startsWith(prefix);
        }
        return expectedView.equals(candidateView);
    }

    /**
     * Decodes a base64-encoded, gzip-compressed {@code d}-column payload while enforcing a maximum decoded size.
     *
     * @param encoded
     *            encoded payload bytes from the shard table
     * @param maxDecodedValueSize
     *            maximum allowed decoded payload size in bytes
     * @return the decoded UTF-8 content
     * @throws IOException
     *             if the payload cannot be decoded or if the decoded size exceeds the configured limit
     */
    static String decode(byte[] encoded, int maxDecodedValueSize) throws IOException {
        return ContentKeyValueFactory.decodeAndDecompressContentAsString(encoded, maxDecodedValueSize);
    }

    /**
     * Finds all starting character offsets for a literal substring, including overlapping matches.
     *
     * @param decoded
     *            decoded document content
     * @param search
     *            literal substring to search for
     * @return ordered starting offsets for each match
     */
    static List<Integer> findOffsets(String decoded, String search) {
        List<Integer> offsets = new ArrayList<>();
        if (search.isEmpty()) {
            return offsets;
        }
        int index = decoded.indexOf(search);
        while (index >= 0) {
            offsets.add(index);
            index = decoded.indexOf(search, index + 1);
        }
        return offsets;
    }

    /**
     * Serializes one per-entry {@code DOCUMENT_MATCHES} payload in the form {@code {"view":"...","matches":{search:[offsets]}}}.
     *
     * @param payload
     *            per-entry payload built from a single matched {@code d}-column
     * @return JSON string representation, or an empty string if the payload is empty
     */
    public static String toDocumentMatchesJson(Map<String,Object> payload) {
        if (payload.isEmpty()) {
            return "";
        }
        return GSON.toJson(payload);
    }
}
