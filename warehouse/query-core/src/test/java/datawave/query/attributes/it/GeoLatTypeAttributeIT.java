package datawave.query.attributes.it;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.data.normalizer.GeoLatNormalizer;
import datawave.data.normalizer.Normalizer;
import datawave.data.type.GeoLatType;
import datawave.data.type.Type;
import datawave.query.attributes.TypeAttribute;
import datawave.query.attributes.TypeAttributeIT;

/**
 * Serialization integration test for the {@link GeoLatType} which uses the {@link GeoLatNormalizer}
 */
public class GeoLatTypeAttributeIT extends TypeAttributeIT {

    private static final Logger log = LoggerFactory.getLogger(GeoLatTypeAttributeIT.class);

    @Override
    protected Type<?> getType() {
        return new GeoLatType();
    }

    @Override
    protected TypeAttribute<?> createNormalizedAttribute() {
        return createAttribute("+bE9");
    }

    @Override
    protected TypeAttribute<?> createNonNormalizedAttribute() {
        return createAttribute("90");
    }

    @Test
    public void testNormalizer() {
        String data = "90";
        Normalizer<?> normalizer = new GeoLatNormalizer();
        assertEquals("+bE9", normalizer.normalize(data));
        assertEquals("90", normalizer.denormalize(data));

        data = "-75.32";
        assertEquals("!YE2.468", normalizer.normalize(data));
        assertEquals("-75.32", normalizer.denormalize(data));

        data = "+bE9";
        assertEquals("+bE9", normalizer.normalize(data));
        assertEquals("9E+1", normalizer.denormalize(data));

        data = "!YE2.468";
        assertEquals("!YE2.468", normalizer.normalize(data));
        assertEquals("-75.32", normalizer.denormalize(data));
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
