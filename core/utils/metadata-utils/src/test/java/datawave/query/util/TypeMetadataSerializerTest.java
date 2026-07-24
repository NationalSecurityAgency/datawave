package datawave.query.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class TypeMetadataSerializerTest {

    @Test
    public void testRoundTrip() {
        TypeMetadata typeMetadata = new TypeMetadata();
        typeMetadata.put("FIELD1", "ingestA", "LcNoDiacriticsType");
        typeMetadata.put("FIELD1", "ingestB", "NumberType");
        typeMetadata.put("FIELD2", "ingestA", "DateType");

        TypeMetadataSerializer serializer = new TypeMetadataSerializer();
        String serialized = serializer.serialize(typeMetadata);
        TypeMetadata deserialized = serializer.deserialize(serialized);

        assertEquals(typeMetadata, deserialized);
        assertEquals(typeMetadata.fold(), deserialized.fold());
    }

    @Test
    public void testRoundTripOfEmptyTypeMetadata() {
        TypeMetadata typeMetadata = new TypeMetadata();

        TypeMetadataSerializer serializer = new TypeMetadataSerializer();
        String serialized = serializer.serialize(typeMetadata);
        TypeMetadata deserialized = serializer.deserialize(serialized);

        assertEquals(typeMetadata, deserialized);
        assertEquals(true, deserialized.isEmpty());
    }

    /**
     * A single serializer instance is meant to be reused across many calls within the same thread, so serializing a smaller instance after a larger one must
     * not leak leftover bytes from the previous call's internal buffer.
     */
    @Test
    public void testInstanceReuseAcrossDifferentlySizedInstances() {
        TypeMetadata large = new TypeMetadata();
        for (int i = 0; i < 50; i++) {
            large.put("FIELD_" + i, "ingestA", "LcNoDiacriticsType");
        }
        TypeMetadata small = new TypeMetadata();
        small.put("FIELD1", "ingestA", "LcNoDiacriticsType");

        TypeMetadataSerializer serializer = new TypeMetadataSerializer();

        String serializedLarge = serializer.serialize(large);
        assertEquals(large, serializer.deserialize(serializedLarge));

        String serializedSmall = serializer.serialize(small);
        assertEquals(small, serializer.deserialize(serializedSmall));

        // re-serializing the large instance again must still be unaffected by the intervening small call
        String serializedLargeAgain = serializer.serialize(large);
        assertEquals(large, serializer.deserialize(serializedLargeAgain));
    }
}
