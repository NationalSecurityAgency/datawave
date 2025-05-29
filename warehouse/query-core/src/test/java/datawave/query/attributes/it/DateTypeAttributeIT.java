package datawave.query.attributes.it;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Date;

import org.junit.jupiter.api.BeforeAll;
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

    @BeforeAll
    public static void setupClass() {
        System.setProperty("user.timezone", "GMT");
    }

    @Test
    public void testNormalizer() {
        String data = "2014-10-20T00:00:00.000Z";
        DateNormalizer normalizer = new DateNormalizer();
        String normalized = normalizer.normalize(data);
        assertEquals("2014-10-20T00:00:00.000Z", normalized);

        Date delegate = normalizer.denormalize(data);
        String normalizedDelegate = normalizer.normalizeDelegateType(delegate);
        assertEquals("2014-10-20T00:00:00.000Z", normalizedDelegate);

        data = "2014-10-20T00:00:00.000Z";
        normalized = normalizer.normalize(data);
        assertEquals("2014-10-20T00:00:00.000Z", normalized);

        delegate = normalizer.denormalize(data);
        normalizedDelegate = normalizer.normalizeDelegateType(delegate);
        assertEquals("2014-10-20T00:00:00.000Z", normalizedDelegate);
    }

    @Override
    protected Type<?> getType() {
        return new DateType();
    }

    @Override
    protected TypeAttribute<?> createNormalizedAttribute() {
        return createAttribute("2014-10-20T00:00:00.000Z");
    }

    @Override
    protected TypeAttribute<?> createNonNormalizedAttribute() {
        return createAttribute("Mon Oct 20 00:00:00 UTC 2014");
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
