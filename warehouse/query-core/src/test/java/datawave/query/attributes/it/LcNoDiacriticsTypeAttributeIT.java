package datawave.query.attributes.it;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.data.normalizer.LcNoDiacriticsNormalizer;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.Type;
import datawave.query.attributes.TypeAttribute;
import datawave.query.attributes.TypeAttributeIT;

/**
 * Serialization integration test for the {@link LcNoDiacriticsType} which uses the {@link LcNoDiacriticsNormalizer}
 */
public class LcNoDiacriticsTypeAttributeIT extends TypeAttributeIT {

    private static final Logger log = LoggerFactory.getLogger(LcNoDiacriticsTypeAttributeIT.class);

    @Override
    protected Type<?> getType() {
        return new LcNoDiacriticsType();
    }

    @Override
    protected TypeAttribute<?> createNormalizedAttribute() {
        return createAttribute("cafe");
    }

    @Override
    protected TypeAttribute<?> createNonNormalizedAttribute() {
        return createAttribute("café");
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
        writeDataOutput(NORMALIZED, createNormalizedAttribute(), log);
        writeDataOutput(NON_NORMALIZED, createNonNormalizedAttribute(), log);
    }
}
