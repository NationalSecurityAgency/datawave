package datawave.microservice.annotation.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Regression tests for {@link AnnotationControllerV1}'s error-response serialization. Previously {@code jsonNotFound}/{@code jsonError} built their JSON
 * response bodies via raw string concatenation (e.g. {@code "{\"message\":\"" + message + "\"}"}), which produced malformed or unsafe JSON whenever
 * {@code message} contained a quote, backslash, or other character requiring JSON escaping -- plausible, since messages interpolate exception text and
 * user-supplied identifiers. Verifies that the current Jackson-based serialization (exposed package-private/{@code @VisibleForTesting} for exactly this
 * purpose) always produces valid, parseable JSON, regardless of the message content.
 */
class TestAnnotationControllerV1JsonErrors {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void testJsonErrorEscapesQuotesAndBackslashes() throws Exception {
        String message = "Internal error fetching annotation: could not parse \"foo\\bar\" identifier";

        ResponseEntity<String> response = AnnotationControllerV1.jsonError(message);

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getStatusCode());
        Map<String,Object> parsed = MAPPER.readValue(response.getBody(), Map.class);
        assertEquals(message, parsed.get("message"), "the original message must round-trip exactly through JSON serialization");
    }

    @Test
    void testJsonNotFoundEscapesQuotesAndBackslashes() throws Exception {
        String message = "No internal identifier found for 'idType:some\"weird\\id'";

        ResponseEntity<String> response = AnnotationControllerV1.jsonNotFound(message);

        assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
        Map<String,Object> parsed = MAPPER.readValue(response.getBody(), Map.class);
        assertEquals(message, parsed.get("message"), "the original message must round-trip exactly through JSON serialization");
    }

    @Test
    void testJsonErrorProducesValidJsonForControlCharactersAndNewlines() throws Exception {
        String message = "line one\nline two\tend";

        ResponseEntity<String> response = AnnotationControllerV1.jsonError(message);

        // if the response body were not valid JSON (e.g. due to unescaped control characters), this parse would throw
        Map<String,Object> parsed = MAPPER.readValue(response.getBody(), Map.class);
        assertTrue(parsed.containsKey("message"));
        assertEquals(message, parsed.get("message"));
    }

    @Test
    void testToJsonMessageProducesValidJson() throws Exception {
        String message = "simple message";

        String json = AnnotationControllerV1.toJsonMessage(message);

        Map<String,Object> parsed = MAPPER.readValue(json, Map.class);
        assertEquals(message, parsed.get("message"));
    }
}
