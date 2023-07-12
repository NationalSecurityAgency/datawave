package datawave.query.tables.async.event;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.google.common.collect.Sets;

class FieldSetsTest {

    final Set<String> fieldSet = Sets.newHashSet("F1", "F2", "F3", "F4");
    final String serializedFieldSet = "F1,F2,F3,F4";

    @Test
    void testFieldSetSerDe() {
        String serialized = FieldSets.serializeFieldSet(fieldSet);
        assertEquals(serializedFieldSet, serialized);

        Set<String> deserialized = FieldSets.deserializeFieldSet(serialized);
        assertEquals(fieldSet, deserialized);
    }

    @Test
    void testFieldSetSerDeWithEmptySet() {
        String serialized = FieldSets.serializeFieldSet(Collections.emptySet());
        assertEquals("", serialized);
        assertEquals(Collections.emptySet(), FieldSets.deserializeFieldSet(""));
    }

    @Test
    void testFieldSetSerDeWithCompression() throws IOException {
        String serialized = FieldSets.serializeFieldSet(fieldSet);
        assertEquals(serializedFieldSet, serialized);

        // now compress
        String expectedCompressed = "H4sIAAAAAAAAAGNgYOB2M9RxM9JxM9ZxMwEAvA/FWA8AAAA=";
        String compressed = FieldSets.compressFieldSet(serialized);
        assertEquals(expectedCompressed, compressed);

        // deser
        assertEquals(serializedFieldSet, FieldSets.decompressFieldSet(compressed));
    }

    @Test
    void testFieldSetSerDeWithCompressionWithEmptySet() throws IOException {
        String serialized = FieldSets.serializeFieldSet(Collections.emptySet());
        assertEquals("", serialized);

        String expectedCompressed = "H4sIAAAAAAAAAGNgYGAAABzfRCEEAAAA";
        String compressed = FieldSets.compressFieldSet("");
        assertEquals(expectedCompressed, compressed);

        String deserialized = FieldSets.decompressFieldSet(compressed);
        assertEquals("", deserialized);
    }

    @Test
    void testFieldSetSerDeWithCompressionWithAlternateCharset() throws IOException {
        String expectedUTF16 = "H4sIAAAAAAAAAGNgYJD495/BjcGQQQdIGoFJYzBpAgDrbj06HAAAAA==";
        String compressedWithUTF16 = FieldSets.compressFieldSet(serializedFieldSet, StandardCharsets.UTF_16);
        assertEquals(expectedUTF16, compressedWithUTF16);

        String decompressedUTF16 = FieldSets.decompressFieldSet(compressedWithUTF16, StandardCharsets.UTF_16);
        assertEquals(serializedFieldSet, decompressedUTF16);
    }

    @Test
    void testCompressionWithMisMatchedCharsets() throws IOException {
        String expectedUTF8 = "H4sIAAAAAAAAAGNgYOB2M9RxM9JxM9ZxMwEAvA/FWA8AAAA=";
        String expectedUTF16 = "H4sIAAAAAAAAAGNgYJD495/BjcGQQQdIGoFJYzBpAgDrbj06HAAAAA==";

        String compressedWithUTF8 = FieldSets.compressFieldSet(serializedFieldSet, StandardCharsets.UTF_8);
        assertEquals(expectedUTF8, compressedWithUTF8);

        String compressedWithUTF16 = FieldSets.compressFieldSet(serializedFieldSet, StandardCharsets.UTF_16);
        assertEquals(expectedUTF16, compressedWithUTF16);

        // different charsets should produce different strings
        assertNotEquals(compressedWithUTF8, compressedWithUTF16);

        String decompressedUTF8WithUTF16 = FieldSets.decompressFieldSet(compressedWithUTF8, StandardCharsets.UTF_16);
        String decompressedUTF16WithUTF8 = FieldSets.decompressFieldSet(compressedWithUTF16, StandardCharsets.UTF_8);

        assertNotEquals(serializedFieldSet, decompressedUTF8WithUTF16);
        assertNotEquals(serializedFieldSet, decompressedUTF16WithUTF8);
    }
}
