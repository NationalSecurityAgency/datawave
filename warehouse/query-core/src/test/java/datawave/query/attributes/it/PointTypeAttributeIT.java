package datawave.query.attributes.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import datawave.data.normalizer.Normalizer;
import datawave.data.normalizer.PointNormalizer;
import datawave.data.type.PointType;
import datawave.data.type.Type;
import datawave.query.attributes.TypeAttribute;
import datawave.query.attributes.TypeAttributeIT;

/**
 * Serialization integration test for the {@link PointType} which uses the {@link PointNormalizer}
 */
public class PointTypeAttributeIT extends TypeAttributeIT {

    @Override
    protected Type<?> getType() {
        return new PointType();
    }

    @Override
    protected String getTypeShortName() {
        return "POINT";
    }

    @Test
    public void testNormalizer() {
        String data = "POINT(0 0)";
        Normalizer<?> normalizer = new PointNormalizer();
        assertEquals("1f0aaaaaaaaaaaaaaa", normalizer.normalize(data));
        assertEquals("POINT (0 0)", normalizer.denormalize(data).toString());

        String normalized = "1f0aaaaaaaaaaaaaaa";
        assertEquals("1f0aaaaaaaaaaaaaaa", normalizer.normalize(normalized));
        assertThrows(IllegalArgumentException.class, () -> normalizer.denormalize(normalized).toString());
    }

    @Override
    protected TypeAttribute<?> createNormalizedAttribute() {
        // the PointType cannot handle a normalized value, so don't exercise that code path
        return createAttribute("POINT (0 0)");
    }

    @Override
    protected TypeAttribute<?> createNonNormalizedAttribute() {
        return createAttribute("POINT (0 0)");
    }

    @Test
    public void testKryoSerialization() {
        testKryoReadWriteTimes(NORMALIZED, createNormalizedAttribute());
        testKryoReadWriteTimes(NON_NORMALIZED, createNonNormalizedAttribute());
    }

    @Test
    public void testKryoValuePreservation() {
        // serializing full type name: 50
        // serializing type name index: 23
        // kryo optimization: 41
        verifyKryoPreservesValue(createNormalizedAttribute(), 41);
        verifyKryoPreservesValue(createNonNormalizedAttribute(), 41);
    }

    @Test
    public void testDataReadWrite() {
        testDataReadWriteTimes(NORMALIZED, createNormalizedAttribute());
        testDataReadWriteTimes(NON_NORMALIZED, createNonNormalizedAttribute());
    }

    @Test
    public void testDataPreservesValues() {
        // serializing full type name: 58
        // serializing type name index: 27
        verifyDataPreservesValue(createNormalizedAttribute(), 27);
        verifyDataPreservesValue(createNonNormalizedAttribute(), 27);
    }
}
