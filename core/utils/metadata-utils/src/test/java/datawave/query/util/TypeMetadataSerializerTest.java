package datawave.query.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;

public class TypeMetadataSerializerTest {

    private static final String[] NORMALIZERS = {"datawave.data.type.LcNoDiacriticsType", "datawave.data.type.NumberType", "datawave.data.type.DateType"};

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

    private TypeMetadata buildTypeMetadata(int ingestTypeCount, int fieldCount) {
        TypeMetadata typeMetadata = new TypeMetadata();
        for (int i = 0; i < ingestTypeCount; i++) {
            for (int f = 0; f < fieldCount; f++) {
                typeMetadata.put("FIELD_" + f, "ingest" + i, NORMALIZERS[f % NORMALIZERS.length]);
            }
        }
        return typeMetadata;
    }

    private byte[] toKryoBytes(TypeMetadata typeMetadata) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (Output output = new Output(baos)) {
            typeMetadata.write(new Kryo(), output);
        }
        return baos.toByteArray();
    }

    /**
     * The Kryo form has to stay at least as compact as the {@link TypeMetadata#toString()} form it can replace, since both travel as an iterator option.
     * Writing each ingest type and normalizer type name once and referring to it by index thereafter is what keeps it there: a normalizer type name is
     * otherwise repeated for every field it is held against, so the payload grows with the product of the field and ingest type counts rather than their sum.
     */
    @Test
    public void testKryoFormIsSmallerThanTheStringForm() {
        for (int ingestTypeCount : new int[] {1, 2, 5, 20}) {
            for (int fieldCount : new int[] {25, 250}) {
                TypeMetadata typeMetadata = buildTypeMetadata(ingestTypeCount, fieldCount);

                int stringLength = typeMetadata.toString().getBytes(StandardCharsets.UTF_8).length;
                int kryoLength = toKryoBytes(typeMetadata).length;

                String combination = ingestTypeCount + " ingest types x " + fieldCount + " fields";
                assertTrue(kryoLength < stringLength, combination + ": Kryo length " + kryoLength + " should be under the string length " + stringLength);
            }
        }
    }

    /**
     * Base64 encoding costs the Kryo form a third of its size on the way out, which leaves the transported string at rough parity with
     * {@link TypeMetadata#toString()} for a single ingest type. Every additional ingest type adds a full set of index pairs to the string form but only re-uses
     * the mini-map entries in the Kryo form, so from two ingest types on the encoded payload is the smaller of the two.
     */
    @Test
    public void testSerializedFormIsSmallerThanTheStringFormForMultipleIngestTypes() {
        for (int ingestTypeCount : new int[] {2, 5, 20}) {
            for (int fieldCount : new int[] {25, 250}) {
                TypeMetadata typeMetadata = buildTypeMetadata(ingestTypeCount, fieldCount);

                int stringLength = typeMetadata.toString().getBytes(StandardCharsets.UTF_8).length;
                int serializedLength = new TypeMetadataSerializer().serialize(typeMetadata).getBytes(StandardCharsets.UTF_8).length;

                String combination = ingestTypeCount + " ingest types x " + fieldCount + " fields";
                assertTrue(serializedLength < stringLength,
                                combination + ": serialized length " + serializedLength + " should be under the string length " + stringLength);
            }
        }
    }
}
