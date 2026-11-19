package example.marking.spi;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class MarkingEnvelopeTest {
    @Test
    void canTransportAnUnknownSchemaWithoutAProviderOrMutableAliases() {
        byte[] source = {42};
        MarkingEnvelope original = new MarkingEnvelope("future.schema", 3, source);
        source[0] = 0;
        byte[] delivered = original.payload();
        MarkingEnvelope copy = new MarkingEnvelope(original.schemaId(), original.version(), delivered);
        delivered[0] = 0;

        assertArrayEquals(new byte[] {42}, original.payload());
        assertEquals(original, copy);
        assertEquals(original.hashCode(), copy.hashCode());
        assertNotEquals(copy, new MarkingEnvelope("other.schema", 3, copy.payload()));
        assertNotEquals(copy, new MarkingEnvelope(copy.schemaId(), 4, copy.payload()));
    }

    @Test
    void envelopeRequiresAnIdentifiableSchema() {
        assertThrows(IllegalArgumentException.class, () -> new MarkingEnvelope(" ", 1, new byte[0]));
        assertThrows(IllegalArgumentException.class, () -> new MarkingEnvelope("example", 0, new byte[0]));
    }

    @Test
    void cannotPerformMarkingOperationsWithoutAProvider() {
        // This module deliberately has no provider dependency or service descriptor.
        assertEquals("No MarkingProvider registered", assertThrows(IllegalStateException.class, MarkingProvider::load).getMessage());
    }
}
