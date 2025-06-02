package datawave.query.attributes.it;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.data.normalizer.LcNormalizer;
import datawave.data.type.LcType;
import datawave.data.type.Type;
import datawave.query.attributes.TypeAttribute;
import datawave.query.attributes.TypeAttributeIT;

/**
 * Serialization integration test for the {@link LcType} which uses the {@link LcNormalizer}
 */
public class LcTypeAttributeIT extends TypeAttributeIT {

    private static final Logger log = LoggerFactory.getLogger(LcTypeAttributeIT.class);

    @Override
    protected Type<?> getType() {
        return new LcType();
    }

    @Override
    protected TypeAttribute<?> createNormalizedAttribute() {
        return createAttribute("value");
    }

    @Override
    protected TypeAttribute<?> createNonNormalizedAttribute() {
        return createAttribute("Value");
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
