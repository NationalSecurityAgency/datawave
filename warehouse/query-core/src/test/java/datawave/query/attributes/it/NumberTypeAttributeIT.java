package datawave.query.attributes.it;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import datawave.data.normalizer.NumberNormalizer;
import datawave.data.type.NumberType;
import datawave.data.type.Type;
import datawave.query.attributes.TypeAttribute;
import datawave.query.attributes.TypeAttributeIT;

/**
 * Serialization integration test for the {@link NumberType} which uses the {@link NumberNormalizer}
 */
public class NumberTypeAttributeIT extends TypeAttributeIT {

    private final NumberNormalizer normalizer = new NumberNormalizer();

    @Test
    public void testNormalizations() {
        assertEquals("25", normalizer.denormalize("25").toString());
        assertEquals("+bE2.5", normalizer.normalize("25"));
    }

    @Override
    protected Type<?> getType() {
        return new NumberType();
    }

    @Override
    protected String getTypeShortName() {
        return "NUM";
    }

    @Override
    protected TypeAttribute<?> createNormalizedAttribute() {
        return createAttribute(normalizer.normalize("25"));
    }

    @Override
    protected TypeAttribute<?> createNonNormalizedAttribute() {
        return createAttribute("25");
    }

    @Test
    public void testKryoReadWrite() {
        testKryoReadWriteTimes(NORMALIZED, createNormalizedAttribute());
        testKryoReadWriteTimes(NON_NORMALIZED, createNonNormalizedAttribute());
    }

    @Test
    public void testKryoValuePreservation() {
        // serializing full type name: 42
        // serializing type name index: 14
        // post kryo optimization: 20
        // serialize hash code: 25
        verifyKryoPreservesValue(createNormalizedAttribute(), 25);
        verifyKryoPreservesValue(createNonNormalizedAttribute(), 25);
    }

    @Test
    public void testDataReadWrite() {
        testDataReadWriteTimes(NORMALIZED, createNormalizedAttribute());
        testDataReadWriteTimes(NON_NORMALIZED, createNonNormalizedAttribute());
    }

    @Test
    public void testDataValuePreservation() {
        // serializing full type name: 50
        // serializing type name index: 18
        verifyDataPreservesValue(createNormalizedAttribute(), 18);
        verifyDataPreservesValue(createNonNormalizedAttribute(), 18);
    }
}
