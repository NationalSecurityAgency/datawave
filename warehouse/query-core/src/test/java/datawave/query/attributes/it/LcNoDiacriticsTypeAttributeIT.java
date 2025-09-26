package datawave.query.attributes.it;

import org.junit.jupiter.api.Test;

import datawave.data.normalizer.LcNoDiacriticsNormalizer;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.Type;
import datawave.query.attributes.TypeAttribute;
import datawave.query.attributes.TypeAttributeIT;

/**
 * Serialization integration test for the {@link LcNoDiacriticsType} which uses the {@link LcNoDiacriticsNormalizer}
 */
public class LcNoDiacriticsTypeAttributeIT extends TypeAttributeIT {

    @Override
    protected Type<?> getType() {
        return new LcNoDiacriticsType();
    }

    @Override
    protected String getTypeShortName() {
        return "LC_ND";
    }

    @Override
    protected TypeAttribute<?> createNormalizedAttribute() {
        return createAttribute("cafe");
    }

    @Override
    protected TypeAttribute<?> createNonNormalizedAttribute() {
        return createAttribute("café");
    }

    @Test
    public void testKryoReadWrite() {
        testKryoReadWriteTimes(NORMALIZED, createNormalizedAttribute());
        testKryoReadWriteTimes(NON_NORMALIZED, createNonNormalizedAttribute());
    }

    @Test
    public void testKryoValuePreservation() {
        // serializing full type name: 52, 54
        // serializing type name index: 16, 18
        // serialize hash code: 21, 23
        verifyKryoPreservesValue(createNormalizedAttribute(), 21);
        verifyKryoPreservesValue(createNonNormalizedAttribute(), 23);
    }

    @Test
    public void testDataReadWrite() {
        testDataReadWriteTimes(NORMALIZED, createNormalizedAttribute());
        testDataReadWriteTimes(NON_NORMALIZED, createNonNormalizedAttribute());
    }

    @Test
    public void testDataValuePreservation() {
        // 20, 21
        verifyDataPreservesValue(createNormalizedAttribute(), 20);
        verifyDataPreservesValue(createNonNormalizedAttribute(), 21);
    }
}
