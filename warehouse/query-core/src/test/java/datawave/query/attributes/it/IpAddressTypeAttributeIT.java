package datawave.query.attributes.it;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import datawave.data.normalizer.IpAddressNormalizer;
import datawave.data.type.IpAddressType;
import datawave.data.type.Type;
import datawave.query.attributes.TypeAttribute;
import datawave.query.attributes.TypeAttributeIT;

/**
 * Serialization integration test for the {@link IpAddressType} which uses the {@link IpAddressNormalizer}
 */
public class IpAddressTypeAttributeIT extends TypeAttributeIT {

    private final IpAddressNormalizer normalizer = new IpAddressNormalizer();

    @Test
    public void testNormalizations() {
        String input = "192.168.1.1";
        assertEquals(input, normalizer.denormalize(input).toString());
        assertEquals("192.168.001.001", normalizer.normalize(input));
    }

    @Override
    protected Type<?> getType() {
        return new IpAddressType();
    }

    @Override
    protected String getTypeShortName() {
        return "IP";
    }

    @Override
    protected TypeAttribute<?> createNormalizedAttribute() {
        return createAttribute(normalizer.normalize("192.168.1.1"));
    }

    @Override
    protected TypeAttribute<?> createNonNormalizedAttribute() {
        return createAttribute(normalizer.denormalize("192.168.1.1").toString());
    }

    @Test
    public void testKryoReadWrite() {
        testKryoReadWriteTimes(NORMALIZED, createNormalizedAttribute());
        testKryoReadWriteTimes(NON_NORMALIZED, createNonNormalizedAttribute());
    }

    @Test
    public void testKryoValuePreservation() {
        // serializing full type name: 54
        // serializing type name index: 23
        verifyKryoPreservesValue(createNormalizedAttribute(), 23);
        verifyKryoPreservesValue(createNonNormalizedAttribute(), 23);
    }

    @Test
    public void testDataReadWrite() {
        testDataReadWriteTimes(NORMALIZED, createNormalizedAttribute());
        testDataReadWriteTimes(NON_NORMALIZED, createNonNormalizedAttribute());
    }

    @Test
    public void testDataPreservesValues() {
        // serializing full type name: 62
        // serializing type name index: 27
        verifyDataPreservesValue(createNormalizedAttribute(), 27);
        verifyDataPreservesValue(createNonNormalizedAttribute(), 27);
    }
}
