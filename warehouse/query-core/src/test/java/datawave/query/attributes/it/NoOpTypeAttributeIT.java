package datawave.query.attributes.it;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.data.normalizer.NoOpNormalizer;
import datawave.data.type.NoOpType;
import datawave.data.type.Type;
import datawave.query.attributes.TypeAttribute;
import datawave.query.attributes.TypeAttributeIT;

/**
 * Serialization integration test for the {@link NoOpType} which uses the {@link NoOpNormalizer}
 */
public class NoOpTypeAttributeIT extends TypeAttributeIT {

    private static final Logger log = LoggerFactory.getLogger(NoOpTypeAttributeIT.class);

    private final NoOpNormalizer normalizer = new NoOpNormalizer();

    /**
     * Returns a {@link NoOpType} for this test.
     *
     * @return the Type
     */
    protected Type<?> getType() {
        return new NoOpType();
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
