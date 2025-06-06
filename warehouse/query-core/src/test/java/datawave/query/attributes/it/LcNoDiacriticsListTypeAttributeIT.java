package datawave.query.attributes.it;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.junit.jupiter.api.Test;

import datawave.data.normalizer.LcNoDiacriticsNormalizer;
import datawave.data.type.LcNoDiacriticsListType;
import datawave.data.type.ListType;
import datawave.data.type.Type;
import datawave.query.attributes.TypeAttribute;
import datawave.query.attributes.TypeAttributeIT;

/**
 * Serialization integration test for the {@link LcNoDiacriticsListType} is a {@link ListType} and uses the {@link LcNoDiacriticsNormalizer}
 */
public class LcNoDiacriticsListTypeAttributeIT extends TypeAttributeIT {

    private final String normalizedData = "ab,cd,ef";
    private final String nonNormalizedData = "âB,cD,ëF";

    @Test
    public void testNormalizer() {
        List<String> expected = List.of("ab", "cd", "ef");

        LcNoDiacriticsListType listType = new LcNoDiacriticsListType(normalizedData);
        List<String> normalized = listType.normalizeToMany(normalizedData);
        assertEquals(expected, normalized);

        listType = new LcNoDiacriticsListType(nonNormalizedData);
        normalized = listType.normalizeToMany(nonNormalizedData);
        assertEquals(expected, normalized);
    }

    @Override
    protected Type<?> getType() {
        return new LcNoDiacriticsListType();
    }

    @Override
    protected String getTypeShortName() {
        return "LC_ND_LIST";
    }

    @Override
    protected TypeAttribute<?> createNormalizedAttribute() {
        return createAttribute(normalizedData);
    }

    @Override
    protected TypeAttribute<?> createNonNormalizedAttribute() {
        return createAttribute(nonNormalizedData);
    }

    @Test
    public void testKryoReadWrite() {
        testKryoReadWriteTimes(NORMALIZED, createNormalizedAttribute());
        testKryoReadWriteTimes(NON_NORMALIZED, createNonNormalizedAttribute());
    }

    @Test
    public void testKryoValuePreservation() {
        // serializing full type name: 60, 63
        // serializing type name index: 20, 23
        verifyKryoPreservesValue(createNormalizedAttribute(), 20);
        verifyKryoPreservesValue(createNonNormalizedAttribute(), 23);
    }

    @Test
    public void testDataReadWrite() {
        testDataReadWriteTimes(NORMALIZED, createNormalizedAttribute());
        testDataReadWriteTimes(NON_NORMALIZED, createNonNormalizedAttribute());
    }

    @Test
    public void testDataValuePreservation() {
        // serializing full type name: 68, 70
        // serializing type name index: 24, 26
        verifyDataPreservesValue(createNormalizedAttribute(), 24);
        verifyDataPreservesValue(createNonNormalizedAttribute(), 26);
    }
}
