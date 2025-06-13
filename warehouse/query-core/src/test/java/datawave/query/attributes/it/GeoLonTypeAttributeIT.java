package datawave.query.attributes.it;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import datawave.data.normalizer.GeoLonNormalizer;
import datawave.data.normalizer.Normalizer;
import datawave.data.type.GeoLonType;
import datawave.data.type.Type;
import datawave.query.attributes.TypeAttribute;
import datawave.query.attributes.TypeAttributeIT;

/**
 * Serialization integration test for the {@link GeoLonType} which uses the {@link GeoLonNormalizer}
 */
public class GeoLonTypeAttributeIT extends TypeAttributeIT {

    @Override
    protected Type<?> getType() {
        return new GeoLonType();
    }

    @Override
    protected String getTypeShortName() {
        return "GEO_LON";
    }

    @Override
    protected TypeAttribute<?> createNormalizedAttribute() {
        return createAttribute("+cE1.2");
    }

    @Override
    protected TypeAttribute<?> createNonNormalizedAttribute() {
        return createAttribute("90");
    }

    @Test
    public void testNormalizer() {
        String data = "120";
        Normalizer<?> normalizer = new GeoLonNormalizer();
        assertEquals("+cE1.2", normalizer.normalize(data));
        assertEquals("120", normalizer.denormalize(data));

        data = "-133.33";
        assertEquals("!XE8.6667", normalizer.normalize(data));
        assertEquals("-133.33", normalizer.denormalize(data));

        data = "+cE1.2";
        assertEquals("+cE1.2", normalizer.normalize(data));
        assertEquals("1.2E+2", normalizer.denormalize(data));

        data = "!XE8.6667";
        assertEquals("!XE8.6667", normalizer.normalize(data));
        assertEquals("-133.33", normalizer.denormalize(data));
    }

    @Test
    public void testKryoReadWrite() {
        testKryoReadWriteTimes(NORMALIZED, createNormalizedAttribute());
        testKryoReadWriteTimes(NON_NORMALIZED, createNonNormalizedAttribute());
    }

    @Test
    public void testKryoValuePreservation() {
        // serializing full type name: 46, 42
        // serializing type name index: 18, 14
        // kryo optimization: 24, 18
        verifyKryoPreservesValue(createNormalizedAttribute(), 24);
        verifyKryoPreservesValue(createNonNormalizedAttribute(), 18);
    }

    @Test
    public void testDataReadWrite() {
        testDataReadWriteTimes(NORMALIZED, createNormalizedAttribute());
        testDataReadWriteTimes(NON_NORMALIZED, createNonNormalizedAttribute());
    }

    @Test
    public void testDataPreservesValue() {
        // serializing full type name: 54, 50
        // serializing type name index: 22, 18
        verifyDataPreservesValue(createNormalizedAttribute(), 22);
        verifyDataPreservesValue(createNonNormalizedAttribute(), 18);
    }
}
