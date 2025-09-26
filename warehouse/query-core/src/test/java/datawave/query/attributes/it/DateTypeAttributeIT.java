package datawave.query.attributes.it;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Date;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import datawave.data.normalizer.DateNormalizer;
import datawave.data.type.DateType;
import datawave.data.type.Type;
import datawave.query.attributes.TypeAttribute;
import datawave.query.attributes.TypeAttributeIT;

/**
 * Serialization integration tests for the {@link DateType} which uses the {@link DateNormalizer}
 */
public class DateTypeAttributeIT extends TypeAttributeIT {

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
    protected String getTypeShortName() {
        return "DATE";
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
    public void testKryoReadWrite() {
        testKryoReadWriteTimes(NORMALIZED, createNormalizedAttribute());
        testKryoReadWriteTimes(NON_NORMALIZED, createNonNormalizedAttribute());
    }

    @Test
    public void testKryoValuePreservation() {
        // serializing full type name: 62
        // serializing type name index: 36
        // kryo serialization: 43
        // serialize hash code: 48
        verifyKryoPreservesValue(createNormalizedAttribute(), 48);
        verifyKryoPreservesValue(createNonNormalizedAttribute(), 48);
    }

    @Test
    public void testDataReadWrite() {
        testDataReadWriteTimes(NORMALIZED, createNormalizedAttribute());
        testDataReadWriteTimes(NON_NORMALIZED, createNonNormalizedAttribute());
    }

    @Test
    public void testDataValuePreservation() {
        // serializing full type name: 70
        // serializing type name index: 40
        verifyDataPreservesValue(createNormalizedAttribute(), 40);
        verifyDataPreservesValue(createNonNormalizedAttribute(), 40);
    }
}
