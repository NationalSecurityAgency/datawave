package datawave.query.attributes.it;

import org.junit.jupiter.api.Test;

import datawave.data.normalizer.LcNormalizer;
import datawave.data.type.LcType;
import datawave.data.type.Type;
import datawave.query.attributes.TypeAttribute;
import datawave.query.attributes.TypeAttributeIT;

/**
 * Serialization integration test for the {@link LcType} which uses the {@link LcNormalizer}
 */
public class LcTypeAttributeIT extends TypeAttributeIT {

    @Override
    protected Type<?> getType() {
        return new LcType();
    }

    @Override
    protected String getTypeShortName() {
        return "LC";
    }

    @Override
    protected TypeAttribute<?> createNormalizedAttribute() {
        return createAttribute("value");
    }

    @Override
    protected TypeAttribute<?> createNonNormalizedAttribute() {
        return createAttribute("Value");
    }

    @Test
    public void testKryoReadWrite() {
        testKryoReadWriteTimes(NORMALIZED, createNormalizedAttribute());
        testKryoReadWriteTimes(NON_NORMALIZED, createNonNormalizedAttribute());
    }

    @Test
    public void testKryoValuePreservation() {
        // serializing full type name: 41
        // serializing type name index: 17
        verifyKryoPreservesValue(createNormalizedAttribute(), 17);
        verifyKryoPreservesValue(createNonNormalizedAttribute(), 17);
    }

    @Test
    public void testDataReadWrite() {
        testDataReadWriteTimes(NORMALIZED, createNormalizedAttribute());
        testDataReadWriteTimes(NON_NORMALIZED, createNonNormalizedAttribute());
    }

    @Test
    public void testDataValuePreservation() {
        // serializing full type name: 49
        // serializing type name index: 21
        verifyDataPreservesValue(createNormalizedAttribute(), 21);
        verifyDataPreservesValue(createNonNormalizedAttribute(), 21);
    }
}
