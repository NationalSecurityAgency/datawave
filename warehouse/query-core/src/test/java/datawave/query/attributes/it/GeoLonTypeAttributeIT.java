package datawave.query.attributes.it;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.data.normalizer.GeoLonNormalizer;
import datawave.data.normalizer.Normalizer;
import datawave.data.type.GeoLonType;
import datawave.data.type.Type;
import datawave.query.attributes.TypeAttribute;
import datawave.query.attributes.TypeAttributeIT;

/**
 * Serialization integration test for the {@link GeoLonType} which uses the {@link GeoLonNormalizer}
 */
public class GeoLonTypeAttributeIT extends TypeAttributeIT {

    private static final Logger log = LoggerFactory.getLogger(GeoLonTypeAttributeIT.class);

    @Override
    protected Type<?> getType() {
        return new GeoLonType();
    }

    @Override
    protected TypeAttribute<?> createNormalizedAttribute() {
        return createAttribute("+cE1.2");
    }

    @Override
    protected TypeAttribute<?> createNonNormalizedAttribute() {
        return createAttribute("90");
    }

    @Test
    public void testNormalizer() {
        String data = "120";
        Normalizer<?> normalizer = new GeoLonNormalizer();
        assertEquals("+cE1.2", normalizer.normalize(data));
        assertEquals("120", normalizer.denormalize(data));

        data = "-133.33";
        assertEquals("!XE8.6667", normalizer.normalize(data));
        assertEquals("-133.33", normalizer.denormalize(data));

        data = "+cE1.2";
        assertEquals("+cE1.2", normalizer.normalize(data));
        assertEquals("1.2E+2", normalizer.denormalize(data));

        data = "!XE8.6667";
        assertEquals("!XE8.6667", normalizer.normalize(data));
        assertEquals("-133.33", normalizer.denormalize(data));
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
