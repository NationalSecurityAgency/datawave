package datawave.query.attributes.it;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.data.normalizer.IpAddressNormalizer;
import datawave.data.type.IpAddressType;
import datawave.data.type.Type;
import datawave.query.attributes.TypeAttribute;
import datawave.query.attributes.TypeAttributeIT;

/**
 * Serialization integration test for the {@link IpAddressType} which uses the {@link IpAddressNormalizer}
 */
public class IpAddressTypeAttributeIT extends TypeAttributeIT {

    private static final Logger log = LoggerFactory.getLogger(IpAddressTypeAttributeIT.class);

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
    protected TypeAttribute<?> createNormalizedAttribute() {
        return createAttribute(normalizer.normalize("192.168.1.1"));
    }

    @Override
    protected TypeAttribute<?> createNonNormalizedAttribute() {
        return createAttribute(normalizer.denormalize("192.168.1.1").toString());
    }

    @Test
    public void testKryoSerialization() {
        writeKryo(NORMALIZED, createNormalizedAttribute(), log);
        writeKryo(NON_NORMALIZED, createNonNormalizedAttribute(), log);
    }

    @Test
    public void testKryoDeserialization() {
        readKryo(NORMALIZED, createNormalizedAttribute(), log);
        readKryo(NON_NORMALIZED, createNonNormalizedAttribute(), log);
    }

    @Test
    public void testKryoReadWrite() {
        readWriteKryo(createNormalizedAttribute());
        readWriteKryo(createNonNormalizedAttribute());
    }

    @Test
    public void testDataSerialization() {
        writeDataOutput(NORMALIZED, createNormalizedAttribute(), log);
        writeDataOutput(NON_NORMALIZED, createNonNormalizedAttribute(), log);
    }

    @Test
    public void testDataDeserialization() {
        readDataInput(NORMALIZED, createNormalizedAttribute(), log);
        readDataInput(NON_NORMALIZED, createNonNormalizedAttribute(), log);
    }

    @Test
    public void testDataReadWrite() {
        readWriteData(createNormalizedAttribute());
        readWriteData(createNonNormalizedAttribute());
    }
}
