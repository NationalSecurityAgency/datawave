package datawave.query.jexl.functions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.jupiter.api.Test;

import datawave.query.function.DocumentMatchContext;

/**
 * Unit tests for {@link DocumentFunctions} covering view selection, matching semantics, payload limits, and per-{@code d}-column result accumulation.
 */
public class DocumentFunctionsTest {
    /**
     * Verifies that {@code document:match(STRING)} searches all available views and returns the matched search string when any view matches.
     */
    @Test
    public void testMatchAcrossAllViews() throws Exception {
        DocumentMatchContext context = new DocumentMatchContext(List.of(entry("test\0uid\0BODY", "scar car"), entry("test\0uid\0META", "carpet")),
                        new DocumentMatchContext.Limits(1024, DocumentMatchContext.DEFAULT_MAX_DECODED_SIZE,
                                        DocumentMatchContext.DEFAULT_MAX_ENCODED_CONTEXT_SIZE));

        String result = DocumentFunctions.match(context, "car");

        assertEquals("car", result);
        assertEquals(2, context.getMatches().size());
    }

    /**
     * Verifies that a trailing {@code *} in the requested view name performs prefix matching across views.
     */
    @Test
    public void testWildcardViewMatch() throws Exception {
        DocumentMatchContext context = new DocumentMatchContext(
                        List.of(entry("test\0uid\0BODY", "car"), entry("test\0uid\0BODY_TEXT", "car car"), entry("test\0uid\0META", "car")),
                        new DocumentMatchContext.Limits(1024, DocumentMatchContext.DEFAULT_MAX_DECODED_SIZE,
                                        DocumentMatchContext.DEFAULT_MAX_ENCODED_CONTEXT_SIZE));

        String result = DocumentFunctions.match("BODY*", context, "car");

        assertEquals("car", result);
    }

    /**
     * Verifies that overlapping substring matches are reported with all starting offsets.
     */
    @Test
    public void testOverlappingMatches() throws Exception {
        DocumentMatchContext context = new DocumentMatchContext(List.of(entry("test\0uid\0BODY", "banana")), new DocumentMatchContext.Limits(1024,
                        DocumentMatchContext.DEFAULT_MAX_DECODED_SIZE, DocumentMatchContext.DEFAULT_MAX_ENCODED_CONTEXT_SIZE));

        String result = DocumentFunctions.match("BODY", context, "ana");

        assertEquals("ana", result);
    }

    /**
     * Verifies that matching is case-sensitive.
     */
    @Test
    public void testCaseSensitiveMatch() throws Exception {
        DocumentMatchContext context = new DocumentMatchContext(List.of(entry("test\0uid\0BODY", "scar car")), new DocumentMatchContext.Limits(1024,
                        DocumentMatchContext.DEFAULT_MAX_DECODED_SIZE, DocumentMatchContext.DEFAULT_MAX_ENCODED_CONTEXT_SIZE));

        assertTrue(DocumentFunctions.match(context, "Car").isEmpty());
    }

    /**
     * Verifies that a null context is treated as a non-match.
     */
    @Test
    public void testNullContextIsNonMatch() {
        assertTrue(DocumentFunctions.match(null, "car").isEmpty());
    }

    /**
     * Verifies that a null search term is treated as a non-match.
     */
    @Test
    public void testNullSearchIsNonMatch() throws Exception {
        DocumentMatchContext context = new DocumentMatchContext(List.of(entry("test\0uid\0BODY", "scar car")), new DocumentMatchContext.Limits(1024,
                        DocumentMatchContext.DEFAULT_MAX_DECODED_SIZE, DocumentMatchContext.DEFAULT_MAX_ENCODED_CONTEXT_SIZE));

        assertTrue(DocumentFunctions.match(context, null).isEmpty());
        assertTrue(DocumentFunctions.match("BODY", context, null).isEmpty());
    }

    /**
     * Verifies that an empty search term is treated as a non-match.
     */
    @Test
    public void testEmptySearchIsNonMatch() throws Exception {
        DocumentMatchContext context = new DocumentMatchContext(List.of(entry("test\0uid\0BODY", "scar car")), new DocumentMatchContext.Limits(1024,
                        DocumentMatchContext.DEFAULT_MAX_DECODED_SIZE, DocumentMatchContext.DEFAULT_MAX_ENCODED_CONTEXT_SIZE));

        assertTrue(DocumentFunctions.match(context, "").isEmpty());
        assertTrue(context.getMatches().isEmpty());
    }

    /**
     * Verifies that encoded payloads larger than the configured limit are skipped as non-matching.
     */
    @Test
    public void testOversizedPayloadIsNonMatch() throws Exception {
        Map.Entry<Key,Value> entry = entry("test\0uid\0BODY", "scar car");
        DocumentMatchContext context = new DocumentMatchContext(List.of(entry), new DocumentMatchContext.Limits(entry.getValue().get().length - 1,
                        DocumentMatchContext.DEFAULT_MAX_DECODED_SIZE, DocumentMatchContext.DEFAULT_MAX_ENCODED_CONTEXT_SIZE));

        assertTrue(DocumentFunctions.match(context, "car").isEmpty());
    }

    /**
     * Verifies that decoded payloads larger than the configured limit are skipped as non-matching.
     */
    @Test
    public void testOversizedDecodedPayloadIsNonMatch() throws Exception {
        DocumentMatchContext context = new DocumentMatchContext(List.of(entry("test\0uid\0BODY", "scar car")),
                        new DocumentMatchContext.Limits(1024, 3, DocumentMatchContext.DEFAULT_MAX_ENCODED_CONTEXT_SIZE));

        assertTrue(DocumentFunctions.match(context, "car").isEmpty());
    }

    /**
     * Verifies that an empty {@code d}-entry set yields no match.
     */
    @Test
    public void testNoDocumentEntriesIsNonMatch() {
        assertTrue(DocumentFunctions.match(new DocumentMatchContext(List.of(), new DocumentMatchContext.Limits(1024,
                        DocumentMatchContext.DEFAULT_MAX_DECODED_SIZE, DocumentMatchContext.DEFAULT_MAX_ENCODED_CONTEXT_SIZE)), "car").isEmpty());
    }

    /**
     * Verifies that undecodable payloads are treated as non-matching rather than failing evaluation.
     */
    @Test
    public void testDecodeFailureIsNonMatch() {
        DocumentMatchContext context = new DocumentMatchContext(List.of(Map.entry(new Key("row", "d", "test\0uid\0BODY"), new Value("not-base64".getBytes()))),
                        new DocumentMatchContext.Limits(1024, DocumentMatchContext.DEFAULT_MAX_DECODED_SIZE,
                                        DocumentMatchContext.DEFAULT_MAX_ENCODED_CONTEXT_SIZE));

        assertTrue(DocumentFunctions.match(context, "car").isEmpty());
    }

    /**
     * Verifies that MIME-style base64 payloads with trailing CRLF line breaks still decode and match correctly.
     */
    @Test
    public void testMatchWithBase64LineBreaks() throws Exception {
        DocumentMatchContext context = new DocumentMatchContext(List.of(entryWithEncodedSuffix("test\0uid\0BODY", "/* Origins */  Fix.", "\r\n")),
                        new DocumentMatchContext.Limits(1024, DocumentMatchContext.DEFAULT_MAX_DECODED_SIZE,
                                        DocumentMatchContext.DEFAULT_MAX_ENCODED_CONTEXT_SIZE));

        String result = DocumentFunctions.match("BODY", context, "Origins");

        assertEquals("Origins", result);
    }

    /**
     * Verifies that payloads stored as plain base64-encoded UTF-8 text still decode and match when gzip expansion is not possible.
     */
    @Test
    public void testMatchWithBase64OnlyPayload() {
        DocumentMatchContext context = new DocumentMatchContext(List.of(base64OnlyEntry("test\0uid\0BODY", "/* Origins */  Fix.")),
                        new DocumentMatchContext.Limits(1024, DocumentMatchContext.DEFAULT_MAX_DECODED_SIZE,
                                        DocumentMatchContext.DEFAULT_MAX_ENCODED_CONTEXT_SIZE));

        String result = DocumentFunctions.match("BODY", context, "Origins");

        assertEquals("Origins", result);
    }

    /**
     * Verifies that multiple {@code document:match(...)} calls accumulate results on a per-{@code d}-column basis for document output.
     */
    @Test
    public void testMatchAccumulatesPerEntryResultsAcrossCalls() throws Exception {
        DocumentMatchContext context = new DocumentMatchContext(List.of(entry("test\0uid\0BODY", "scar car"), entry("test\0uid\0CONTENT2", "lawyer car")),
                        new DocumentMatchContext.Limits(1024, DocumentMatchContext.DEFAULT_MAX_DECODED_SIZE,
                                        DocumentMatchContext.DEFAULT_MAX_ENCODED_CONTEXT_SIZE));

        assertEquals("car", DocumentFunctions.match("BODY", context, "car"));
        assertEquals("lawyer", DocumentFunctions.match("CONTENT2", context, "lawyer"));
        assertEquals(2, context.getMatches().size());
        assertTrue(context.getMatches().stream().anyMatch(matches -> matches.containsSearch("car")));
        assertTrue(context.getMatches().stream().anyMatch(matches -> matches.containsSearch("lawyer")));
        assertTrue(context.getMatches().stream().anyMatch(matches -> "BODY".equals(matches.getView())));
        assertTrue(context.getMatches().stream().anyMatch(matches -> "CONTENT2".equals(matches.getView())));
    }

    /**
     * Verifies that repeated {@code document:match(...)} calls against the same {@code d}-column accumulate beneath that single entry payload.
     */
    @Test
    public void testMatchAccumulatesSameEntryAcrossCalls() throws Exception {
        DocumentMatchContext context = new DocumentMatchContext(List.of(entry("test\0uid\0BODY", "scar car lawyer")), new DocumentMatchContext.Limits(1024,
                        DocumentMatchContext.DEFAULT_MAX_DECODED_SIZE, DocumentMatchContext.DEFAULT_MAX_ENCODED_CONTEXT_SIZE));

        assertEquals("car", DocumentFunctions.match("BODY", context, "car"));
        assertEquals("lawyer", DocumentFunctions.match("BODY", context, "lawyer"));
        assertEquals(1, context.getMatches().size());
        assertEquals("BODY", context.getMatches().get(0).getView());
        assertEquals("{\"view\":\"BODY\",\"matches\":{\"car\":[1,5],\"lawyer\":[9]}}",
                        DocumentFunctions.toDocumentMatchesJson(context.getMatches().get(0).getPayload()));
    }

    /**
     * Verifies that repeated identical {@code document:match(...)} calls against the same {@code d}-column do not duplicate offsets for the same search term.
     */
    @Test
    public void testMatchRepeatsSameSearchWithinEntryAcrossCalls() throws Exception {
        DocumentMatchContext context = new DocumentMatchContext(List.of(entry("test\0uid\0BODY", "scar car")), new DocumentMatchContext.Limits(1024,
                        DocumentMatchContext.DEFAULT_MAX_DECODED_SIZE, DocumentMatchContext.DEFAULT_MAX_ENCODED_CONTEXT_SIZE));

        assertEquals("car", DocumentFunctions.match("BODY", context, "car"));
        assertEquals("car", DocumentFunctions.match("BODY", context, "car"));
        assertEquals(1, context.getMatches().size());
        assertEquals("{\"view\":\"BODY\",\"matches\":{\"car\":[1,5]}}", DocumentFunctions.toDocumentMatchesJson(context.getMatches().get(0).getPayload()));
    }

    /**
     * Builds a test {@code d}-column entry with an empty visibility.
     *
     * @param cq
     *            column qualifier to use
     * @param content
     *            decoded content to encode into the value
     * @return encoded test entry
     * @throws Exception
     *             if test payload creation fails
     */
    private Map.Entry<Key,Value> entry(String cq, String content) throws Exception {
        return entry(cq, content, "");
    }

    /**
     * Builds a test {@code d}-column entry with caller-supplied visibility and gzip+base64 encoded content.
     *
     * @param cq
     *            column qualifier to use
     * @param content
     *            decoded content to encode into the value
     * @param visibility
     *            column visibility to attach to the key
     * @return encoded test entry
     * @throws Exception
     *             if test payload creation fails
     */
    private Map.Entry<Key,Value> entry(String cq, String content, String visibility) throws Exception {
        return entryWithEncodedSuffix(cq, content, visibility, "");
    }

    /**
     * Builds a test {@code d}-column entry with caller-supplied visibility and an optional suffix appended to the encoded payload.
     *
     * @param cq
     *            column qualifier to use
     * @param content
     *            decoded content to encode into the value
     * @param visibility
     *            column visibility to attach to the key
     * @param encodedSuffix
     *            suffix bytes to append after base64 encoding, such as {@code \r\n}
     * @return encoded test entry
     * @throws Exception
     *             if test payload creation fails
     */
    private Map.Entry<Key,Value> entryWithEncodedSuffix(String cq, String content, String visibility, String encodedSuffix) throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        OutputStream b64s = java.util.Base64.getEncoder().wrap(bos);
        GZIPOutputStream gzip = new GZIPOutputStream(b64s);
        gzip.write(content.getBytes());
        gzip.close();
        b64s.close();
        if (!encodedSuffix.isEmpty()) {
            bos.write(encodedSuffix.getBytes());
        }
        bos.close();
        return new AbstractMap.SimpleEntry<>(new Key("row", "d", cq, visibility), new Value(bos.toByteArray()));
    }

    /**
     * Builds a test {@code d}-column entry with an empty visibility and an optional suffix appended to the encoded payload.
     *
     * @param cq
     *            column qualifier to use
     * @param content
     *            decoded content to encode into the value
     * @param encodedSuffix
     *            suffix bytes to append after base64 encoding, such as {@code \r\n}
     * @return encoded test entry
     * @throws Exception
     *             if test payload creation fails
     */
    private Map.Entry<Key,Value> entryWithEncodedSuffix(String cq, String content, String encodedSuffix) throws Exception {
        return entryWithEncodedSuffix(cq, content, "", encodedSuffix);
    }

    /**
     * Builds a test {@code d}-column entry whose value is only base64-encoded UTF-8 text.
     *
     * @param cq
     *            column qualifier to use
     * @param content
     *            decoded content to encode into the value
     * @return encoded test entry
     */
    private Map.Entry<Key,Value> base64OnlyEntry(String cq, String content) {
        byte[] encoded = java.util.Base64.getEncoder().encode(content.getBytes());
        return new AbstractMap.SimpleEntry<>(new Key("row", "d", cq), new Value(encoded));
    }
}
