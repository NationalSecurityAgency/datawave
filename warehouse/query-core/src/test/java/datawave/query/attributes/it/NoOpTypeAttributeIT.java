package datawave.query.attributes.it;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import datawave.data.normalizer.NoOpNormalizer;
import datawave.data.type.NoOpType;
import datawave.data.type.Type;
import datawave.query.attributes.TypeAttribute;
import datawave.query.attributes.TypeAttributeIT;

/**
 * Serialization integration test for the {@link NoOpType} which uses the {@link NoOpNormalizer}
 */
public class NoOpTypeAttributeIT extends TypeAttributeIT {

    private final NoOpNormalizer normalizer = new NoOpNormalizer();

    /**
     * Returns a {@link NoOpType} for this test.
     *
     * @return the Type
     */
    protected Type<?> getType() {
        return new NoOpType();
    }

    @Override
    protected String getTypeShortName() {
        return "NO_OP";
    }

    @Test
    public void testNormalizations() {
        String data = "value";
        assertEquals(data, normalizer.denormalize(data));
        assertEquals(data, normalizer.normalize(data));

        data = "VaLuE";
        assertEquals(data, normalizer.denormalize(data));
        assertEquals(data, normalizer.normalize(data));
    }

    @Override
    protected TypeAttribute<?> createNormalizedAttribute() {
        return createAttribute("value");
    }

    @Override
    protected TypeAttribute<?> createNonNormalizedAttribute() {
        return createAttribute("VaLuE");
    }

    @Override
    protected TypeAttribute<?> createAttribute(String data) {
        Type<?> type = getType();
        type.setDelegateFromString(data);
        return new TypeAttribute<>(type, docKey, false);
    }

    @Test
    public void testKryoReadWrite() {
        testKryoReadWriteTimes(NORMALIZED, createNormalizedAttribute());
        testKryoReadWriteTimes(NON_NORMALIZED, createNonNormalizedAttribute());
    }

    @Test
    public void testKryoValuePreservation() {
        // serializing full type name: 43
        // serializing type name index: 17
        // serialize hash code: 22
        verifyKryoPreservesValue(createNormalizedAttribute(), 22);
        verifyKryoPreservesValue(createNonNormalizedAttribute(), 22);
    }

    @Test
    public void testDataSerialization() {
        testDataReadWriteTimes(NORMALIZED, createNormalizedAttribute());
        testDataReadWriteTimes(NON_NORMALIZED, createNonNormalizedAttribute());
    }

    @Test
    public void testDataValuePreservation() {
        // serializing full type name: 51
        // serializing type name index: 21
        verifyDataPreservesValue(createNormalizedAttribute(), 21);
        verifyDataPreservesValue(createNonNormalizedAttribute(), 21);
    }
}
