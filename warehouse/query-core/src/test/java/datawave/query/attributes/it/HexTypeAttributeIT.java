package datawave.query.attributes.it;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.data.normalizer.HexStringNormalizer;
import datawave.data.type.HexStringType;
import datawave.data.type.Type;
import datawave.query.attributes.TypeAttribute;
import datawave.query.attributes.TypeAttributeIT;

/**
 * Serialization integration test for the {@link HexStringType} which uses the {@link HexStringNormalizer}
 */
public class HexTypeAttributeIT extends TypeAttributeIT {

    private static final Logger log = LoggerFactory.getLogger(HexTypeAttributeIT.class);

    @Override
    protected Type<?> getType() {
        return new HexStringType();
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
