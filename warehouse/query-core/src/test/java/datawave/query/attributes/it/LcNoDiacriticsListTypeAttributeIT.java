package datawave.query.attributes.it;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.data.normalizer.LcNoDiacriticsNormalizer;
import datawave.data.type.LcNoDiacriticsListType;
import datawave.data.type.ListType;
import datawave.data.type.Type;
import datawave.query.attributes.TypeAttribute;
import datawave.query.attributes.TypeAttributeIT;

/**
 * Serialization integration test for the {@link LcNoDiacriticsListType} is a {@link ListType} and uses the {@link LcNoDiacriticsNormalizer}
 */
public class LcNoDiacriticsListTypeAttributeIT extends TypeAttributeIT {

    private static final Logger log = LoggerFactory.getLogger(LcNoDiacriticsListTypeAttributeIT.class);

    private final String normalizedData = "ab,cd,ef";
    private final String nonNormalizedData = "âB,cD,ëF";

    @Test
    public void testNormalizer() {
        List<String> expected = List.of("ab", "cd", "ef");

        LcNoDiacriticsListType listType = new LcNoDiacriticsListType(normalizedData);
        List<String> normalized = listType.normalizeToMany(normalizedData);
        assertEquals(expected, normalized);

        listType = new LcNoDiacriticsListType(nonNormalizedData);
        normalized = listType.normalizeToMany(nonNormalizedData);
        assertEquals(expected, normalized);
    }

    @Override
    protected Type<?> getType() {
        return new LcNoDiacriticsListType();
    }

    @Override
    protected TypeAttribute<?> createNormalizedAttribute() {
        return createAttribute(normalizedData);
    }

    @Override
    protected TypeAttribute<?> createNonNormalizedAttribute() {
        return createAttribute(nonNormalizedData);
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
