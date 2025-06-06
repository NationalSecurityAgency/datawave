package datawave.query.attributes.it;

import org.junit.jupiter.api.Test;

import datawave.data.normalizer.NumberNormalizer;
import datawave.data.type.ListType;
import datawave.data.type.NumberListType;
import datawave.data.type.Type;
import datawave.query.attributes.TypeAttribute;
import datawave.query.attributes.TypeAttributeIT;

/**
 * Serialization integration test for the {@link NumberListType} which is a {@link ListType} and uses the {@link NumberNormalizer}
 */
public class NumberListTypeAttributeIT extends TypeAttributeIT {

    @Override
    protected Type<?> getType() {
        return new NumberListType();
    }

    @Override
    protected String getTypeShortName() {
        return "NUM_LIST";
    }

    @Override
    protected TypeAttribute<?> createNormalizedAttribute() {
        return createAttribute("+bE2.1,+bE2.2,+bE2.3");
    }

    @Override
    protected TypeAttribute<?> createNonNormalizedAttribute() {
        return createAttribute("21,22,23");
    }

    @Test
    public void testKryoSerialization() {
        testKryoReadWriteTimes(NORMALIZED, createNormalizedAttribute());
        testKryoReadWriteTimes(NON_NORMALIZED, createNonNormalizedAttribute());
    }

    @Test
    public void testKryoReadWrite() {
        // serializing full type name: 64, 52
        // serializing type name index: 32, 20
        verifyKryoPreservesValue(createNormalizedAttribute(), 32);
        verifyKryoPreservesValue(createNonNormalizedAttribute(), 20);
    }

    @Test
    public void testDataReadWrite() {
        testDataReadWriteTimes(NORMALIZED, createNormalizedAttribute());
        testDataReadWriteTimes(NON_NORMALIZED, createNonNormalizedAttribute());
    }

    @Test
    public void verifyDataPreservesValue() {
        // serializing full type name: 72, 60
        // serializing type name index: 36, 24
        verifyDataPreservesValue(createNormalizedAttribute(), 36);
        verifyDataPreservesValue(createNonNormalizedAttribute(), 24);
    }
}
