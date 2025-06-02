package datawave.query.attributes.it;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.data.normalizer.NumberNormalizer;
import datawave.data.type.NumberType;
import datawave.data.type.Type;
import datawave.query.attributes.TypeAttribute;
import datawave.query.attributes.TypeAttributeIT;

/**
 * Serialization integration test for the {@link NumberType} which uses the {@link NumberNormalizer}
 */
public class NumberTypeAttributeIT extends TypeAttributeIT {

    private static final Logger log = LoggerFactory.getLogger(NumberTypeAttributeIT.class);

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
    protected TypeAttribute<?> createNormalizedAttribute() {
        return createAttribute(normalizer.normalize("25"));
    }

    @Override
    protected TypeAttribute<?> createNonNormalizedAttribute() {
        return createAttribute("25");
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
