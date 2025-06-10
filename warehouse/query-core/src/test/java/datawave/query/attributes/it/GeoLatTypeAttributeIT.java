package datawave.query.attributes.it;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import datawave.data.normalizer.GeoLatNormalizer;
import datawave.data.normalizer.Normalizer;
import datawave.data.type.GeoLatType;
import datawave.data.type.Type;
import datawave.query.attributes.TypeAttribute;
import datawave.query.attributes.TypeAttributeIT;

/**
 * Serialization integration test for the {@link GeoLatType} which uses the {@link GeoLatNormalizer}
 */
public class GeoLatTypeAttributeIT extends TypeAttributeIT {

    @Override
    protected Type<?> getType() {
        return new GeoLatType();
    }

    @Override
    protected String getTypeShortName() {
        return "GEO_LAT";
    }

    @Override
    protected TypeAttribute<?> createNormalizedAttribute() {
        return createAttribute("+bE9");
    }

    @Override
    protected TypeAttribute<?> createNonNormalizedAttribute() {
        return createAttribute("90");
    }

    @Test
    public void testNormalizer() {
        String data = "90";
        Normalizer<?> normalizer = new GeoLatNormalizer();
        assertEquals("+bE9", normalizer.normalize(data));
        assertEquals("90", normalizer.denormalize(data));

        data = "-75.32";
        assertEquals("!YE2.468", normalizer.normalize(data));
        assertEquals("-75.32", normalizer.denormalize(data));

        data = "+bE9";
        assertEquals("+bE9", normalizer.normalize(data));
        assertEquals("9E+1", normalizer.denormalize(data));

        data = "!YE2.468";
        assertEquals("!YE2.468", normalizer.normalize(data));
        assertEquals("-75.32", normalizer.denormalize(data));
    }

    @Test
    public void testKryoReadWrite() {
        testKryoReadWriteTimes(NORMALIZED, createNormalizedAttribute());
        testKryoReadWriteTimes(NON_NORMALIZED, createNonNormalizedAttribute());
    }

    @Test
    public void testKryoValuePreservation() {
        // serializing full type name: 44, 42
        // serializing type name index: 16, 14
        // kryo optimization: 20, 18
        verifyKryoPreservesValue(createNormalizedAttribute(), 20);
        verifyKryoPreservesValue(createNonNormalizedAttribute(), 18);
    }

    @Test
    public void testDataReadWrite() {
        testDataReadWriteTimes(NORMALIZED, createNormalizedAttribute());
        testDataReadWriteTimes(NON_NORMALIZED, createNonNormalizedAttribute());
    }

    @Test
    public void testDataValuePreservation() {
        // serializing full type name: 52, 50
        // serializing type name index: 20, 18
        verifyDataPreservesValue(createNormalizedAttribute(), 20);
        verifyDataPreservesValue(createNonNormalizedAttribute(), 18);
    }
}
