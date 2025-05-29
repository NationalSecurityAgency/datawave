package datawave.query.attributes.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.data.normalizer.Normalizer;
import datawave.data.normalizer.PointNormalizer;
import datawave.data.type.PointType;
import datawave.data.type.Type;
import datawave.query.attributes.TypeAttribute;
import datawave.query.attributes.TypeAttributeIT;

/**
 * Serialization integration test for the {@link PointType} which uses the {@link PointNormalizer}
 */
public class PointTypeAttributeIT extends TypeAttributeIT {

    private static final Logger log = LoggerFactory.getLogger(PointTypeAttributeIT.class);

    @Override
    protected Type<?> getType() {
        return new PointType();
    }

    @Test
    public void testNormalizer() {
        String data = "POINT(0 0)";
        Normalizer<?> normalizer = new PointNormalizer();
        assertEquals("1f0aaaaaaaaaaaaaaa", normalizer.normalize(data));
        assertEquals("POINT (0 0)", normalizer.denormalize(data).toString());

        String normalized = "1f0aaaaaaaaaaaaaaa";
        assertEquals("1f0aaaaaaaaaaaaaaa", normalizer.normalize(normalized));
        assertThrows(IllegalArgumentException.class, () -> normalizer.denormalize(normalized).toString());
    }

    @Override
    protected TypeAttribute<?> createNormalizedAttribute() {
        // the PointType cannot handle a normalized value, so don't exercise that code path
        return createAttribute("POINT (0 0)");
    }

    @Override
    protected TypeAttribute<?> createNonNormalizedAttribute() {
        return createAttribute("POINT (0 0)");
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
