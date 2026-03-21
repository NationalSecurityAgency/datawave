package datawave.query.jexl.functions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.GZIPInputStream;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.log4j.Logger;

import com.google.gson.Gson;

import datawave.query.function.DocumentMatchContext;

/**
 * Evaluation-phase JEXL functions for inspecting decoded shard-table {@code d}-column content.
 * <p>
 * The current namespace exposes {@code document:match(...)} which decodes base64-encoded, gzip-compressed document payloads, performs case-sensitive literal
 * substring matching, and returns a JSON object keyed first by matched string, then by view name, with starting character offsets as the leaf values.
 * Per-document state is supplied explicitly through {@link DocumentMatchContext} by the surrounding evaluation flow.
 */
@JexlFunctions(descriptorFactory = "datawave.query.jexl.functions.DocumentFunctionsDescriptor")
public class DocumentFunctions {
    private static final Logger log = Logger.getLogger(DocumentFunctions.class);
    private static final Gson GSON = new Gson();

    public static final String DOCUMENT_FUNCTION_NAMESPACE = "document";
    public static final String DOCUMENT_MATCH_FUNCTION_NAME = "match";
    public static final String DOCUMENT_MATCH_CONTEXT_JEXL_VARIABLE_NAME = "documentMatchContext";
    public static final String DOCUMENT_MATCHES = "DOCUMENT_MATCHES";
    private static final int DECODE_BUFFER_SIZE = 4096;

    /**
     * Evaluates the internal form of {@code document:match(STRING)} across all eligible views for the current document.
     *
     * @param context
     *            per-document context supplied by the evaluation pipeline
     * @param search
     *            literal substring to search for
     * @return a JSON object for this invocation keyed by matched string and then by view name, or an empty string if no match is found
     */
    public static String match(DocumentMatchContext context, String search) {
        return match(null, context, search);
    }

    /**
     * Evaluates the internal form of {@code document:match(VIEWNAME, STRING)} against the current document.
     * <p>
     * Matching is case-sensitive and literal. If {@code viewName} ends with {@code *}, it is treated as a prefix match against the view portion of the
     * {@code d}-column qualifier. Oversized or undecodable payloads are skipped as non-matching. Matches from this invocation are merged into the document-wide
     * result set stored in the supplied {@link DocumentMatchContext}.
     *
     * @param viewName
     *            optional exact or prefix-matched view selector; {@code null} means evaluate all views
     * @param context
     *            per-document context supplied by the evaluation pipeline
     * @param search
     *            literal substring to search for
     * @return a JSON object for this invocation keyed by matched string and then by view name, or an empty string if no match is found
     */
    public static String match(String viewName, DocumentMatchContext context, String search) {
        if (context == null || search == null) {
            if (log.isDebugEnabled()) {
                log.debug("Skipping document:match evaluation because context or search term was null");
            }
            return "";
        }

        if (log.isDebugEnabled()) {
            log.debug("Evaluating document:match for search [" + search + "] view filter [" + viewName + "] across " + context.getdEntries().size()
                            + " d-column entries");
        }

        Map<String,List<Integer>> matches = new LinkedHashMap<>();
        for (Entry<Key,Value> entry : context.getdEntries()) {
            String candidateView = extractViewName(entry.getKey());
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
                    if (context.recordMatchingEntry(entry.getKey()) && context.shouldLogVisibilityMismatch()) {
                        log.info("document:match encountered differing d-column visibilities for document " + context.getFirstMatchingEntry().getRow() + '/'
                                        + context.getFirstMatchingEntry().getColumnFamily() + "; using visibility from first matched d-column "
                                        + context.getFirstMatchingEntry() + " and ignoring differing visibility on " + entry.getKey());
                        context.markVisibilityMismatchLogged();
                    }
                    matches.computeIfAbsent(candidateView, k -> new ArrayList<>()).addAll(offsets);
                } else if (log.isDebugEnabled()) {
                    log.debug("document:match found no offsets for search [" + search + "] in view [" + candidateView + "] using key " + entry.getKey());
                }
            } catch (IOException | IllegalArgumentException e) {
                log.debug("Unable to decode d-column payload for view " + candidateView, e);
            }
        }

        context.mergeMatches(search, matches);
        if (log.isDebugEnabled()) {
            log.debug("document:match merged matches for search [" + search + "]: " + matches);
        }
        return toJson(search, matches);
    }

    /**
     * Extracts the view name from a {@code d}-column qualifier whose layout is expected to be {@code datatype\0uid\0view}.
     *
     * @param key
     *            shard-table {@code d}-column key
     * @return the extracted view name, or an empty string if the qualifier does not have the expected structure
     */
    static String extractViewName(Key key) {
        String cq = key.getColumnQualifier().toString();
        int firstNull = cq.indexOf('\0');
        if (firstNull < 0) {
            return "";
        }
        int secondNull = cq.indexOf('\0', firstNull + 1);
        if (secondNull < 0 || secondNull + 1 >= cq.length()) {
            return "";
        }
        return cq.substring(secondNull + 1);
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
        byte[] decodedBytes;
        try (ByteArrayInputStream bais = new ByteArrayInputStream(encoded)) {
            decodedBytes = Base64.getMimeDecoder().decode(bais.readAllBytes());
        }

        try (ByteArrayInputStream decodedInput = new ByteArrayInputStream(decodedBytes);
                        GZIPInputStream gzipInputStream = new GZIPInputStream(decodedInput);
                        ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[DECODE_BUFFER_SIZE];
            int read;
            int totalRead = 0;
            while ((read = gzipInputStream.read(buffer)) >= 0) {
                totalRead += read;
                if (totalRead > maxDecodedValueSize) {
                    throw new IOException("Decoded d-column payload exceeded configured limit of " + maxDecodedValueSize + " bytes");
                }
                baos.write(buffer, 0, read);
            }
            return baos.toString(StandardCharsets.UTF_8);
        } catch (IOException e) {
            if (decodedBytes.length > maxDecodedValueSize) {
                throw new IOException("Decoded d-column payload exceeded configured limit of " + maxDecodedValueSize + " bytes", e);
            }
            return new String(decodedBytes, StandardCharsets.UTF_8);
        }
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
     * Serializes matches for one {@code document:match(...)} invocation to the JSON payload shape stored in {@code DOCUMENT_MATCHES}.
     *
     * @param search
     *            literal string matched by the invocation
     * @param matches
     *            map of view name to ordered character offsets
     * @return JSON string representation, or an empty string if the map is empty
     */
    public static String toJson(String search, Map<String,List<Integer>> matches) {
        if (matches.isEmpty()) {
            return "";
        }
        Map<String,Map<String,List<Integer>>> payload = new LinkedHashMap<>();
        payload.put(search, matches);
        return GSON.toJson(payload);
    }

    /**
     * Serializes merged document-wide matches to the JSON payload stored in {@code DOCUMENT_MATCHES}.
     *
     * @param matches
     *            map of matched string to per-view ordered character offsets
     * @return JSON string representation, or an empty string if the map is empty
     */
    public static String toJson(Map<String,Map<String,List<Integer>>> matches) {
        if (matches.isEmpty()) {
            return "";
        }
        return GSON.toJson(matches);
    }
}
