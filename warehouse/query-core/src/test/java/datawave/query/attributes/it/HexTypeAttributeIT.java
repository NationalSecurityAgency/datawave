package datawave.query.attributes.it;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import datawave.data.normalizer.HexStringNormalizer;
import datawave.data.type.HexStringType;
import datawave.data.type.Type;
import datawave.query.attributes.TypeAttribute;
import datawave.query.attributes.TypeAttributeIT;

/**
 * Serialization integration test for the {@link HexStringType} which uses the {@link HexStringNormalizer}
 */
public class HexTypeAttributeIT extends TypeAttributeIT {

    @Override
    protected Type<?> getType() {
        return new HexStringType();
    }

    @Override
    protected String getTypeShortName() {
        return "HEX";
    }

    @Override
    protected TypeAttribute<?> createNormalizedAttribute() {
        return createAttribute("a1b2c3");
    }

    @Override
    protected TypeAttribute<?> createNonNormalizedAttribute() {
        return createAttribute("A1B2C3");
    }

    @Test
    public void testNormalizer() {
        String data = "A1B2C3";
        HexStringNormalizer normalizer = new HexStringNormalizer();
        assertEquals("a1b2c3", normalizer.normalize(data));
        assertEquals("A1B2C3", normalizer.denormalize(data));

        String normalized = "a1b2c3";
        assertEquals("a1b2c3", normalizer.normalize(normalized));
        assertEquals("a1b2c3", normalizer.denormalize(normalized));
    }

    @Test
    public void testKryoReadWrite() {
        // individual serialization times remained the same after optimizing kryo serialization to avoid normalizers
        testKryoReadWriteTimes(NORMALIZED, createNormalizedAttribute());
        testKryoReadWriteTimes(NON_NORMALIZED, createNonNormalizedAttribute());
    }

    @Test
    public void testKryoValuePreservation() {
        // serializing full type name: 49
        // serializing type name index: 18
        // post kryo optimization: 24
        // serialize hash code: 29
        verifyKryoPreservesValue(createNormalizedAttribute(), 29);
        verifyKryoPreservesValue(createNonNormalizedAttribute(), 29);
    }

    @Test
    public void testDataReadWrite() {
        testDataReadWriteTimes(NORMALIZED, createNormalizedAttribute());
        testDataReadWriteTimes(NON_NORMALIZED, createNonNormalizedAttribute());
    }

    @Test
    public void testDataValuePreservation() {
        // serializing full type name: 57
        // serializing type name index: 22
        verifyDataPreservesValue(createNormalizedAttribute(), 22);
        verifyDataPreservesValue(createNonNormalizedAttribute(), 22);
    }
}
