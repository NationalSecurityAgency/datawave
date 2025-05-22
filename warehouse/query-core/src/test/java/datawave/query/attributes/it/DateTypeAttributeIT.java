package datawave.query.attributes.it;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.data.normalizer.DateNormalizer;
import datawave.data.type.DateType;
import datawave.data.type.Type;
import datawave.query.attributes.TypeAttribute;
import datawave.query.attributes.TypeAttributeIT;

/**
 * Serialization integration tests for the {@link DateType} which uses the {@link DateNormalizer}
 */
public class DateTypeAttributeIT extends TypeAttributeIT {

    private static final Logger log = LoggerFactory.getLogger(DateTypeAttributeIT.class);

    private final DateNormalizer normalizer = new DateNormalizer();

    @Test
    public void testNormalizer() {
        String date = "2000-12-28T00:00:05.000Z";
        assertEquals(date, normalizer.normalize(date));
        assertEquals("Thu Dec 28 00:00:05 UTC 2000", normalizer.denormalize(date).toString());
    }

    @Override
    protected Type<?> getType() {
        return new DateType();
    }

    @Override
    protected TypeAttribute<?> createNormalizedAttribute() {
        return createAttribute(normalizer.normalize("2000-12-28T00:00:05.000Z"));
    }

    @Override
    protected TypeAttribute<?> createNonNormalizedAttribute() {
        return createAttribute(normalizer.denormalize("2000-12-28T00:00:05.000Z").toString());
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
