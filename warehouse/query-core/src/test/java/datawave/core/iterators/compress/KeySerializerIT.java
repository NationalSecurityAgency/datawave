package datawave.core.iterators.compress;

import static datawave.core.iterators.compress.CompressionTestUtil.iterator;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.text.DecimalFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.core.iterators.compress.event.EventSerializationUtil;
import datawave.data.hash.UID;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class KeySerializerIT {

    private final Logger log = LoggerFactory.getLogger(KeySerializerIT.class);

    private final List<Key> input = new ArrayList<>();
    private final List<Key> expected = new ArrayList<>();
    private final List<Key> results = new ArrayList<>();

    private int originalSize = 0;
    private final List<Integer> expectedSize = new ArrayList<>();

    private final long ts = Instant.parse("2011-12-03T10:15:30Z").toEpochMilli();
    private final long ts2 = ts + 1_234_456L;
    private final String uid = UID.builder().newId("abc.def.ghi".getBytes(), (Date) null).toString();
    private final String uid2 = UID.builder().newId("abc.def.ghi".getBytes(), (Date) null, "123").toString();

    private final String row = "20251010_123";
    private final String cf = "datatype\0" + uid;
    private final String cf2 = "datatype\0" + uid2;

    private final DecimalFormat df = new DecimalFormat("#.##");

    @BeforeEach
    public void beforeEach() {
        input.clear();
        expected.clear();
        results.clear();

        originalSize = 0;
        expectedSize.clear();
    }

    @Test
    @Order(1)
    public void testSingleKey() {
        input.add(new Key(row, cf, "FIELD\0value", "VIZ-A", ts));
        expectOriginalSize(60);
        expectSize(69, 69, 69);
        drive("single key");
    }

    @Test
    @Order(2)
    public void testSingleKeyWithGroupingNotation() {
        input.add(new Key(row, cf, "FIELD.1.2\0value", "VIZ-A", ts));
        expectOriginalSize(64);
        expectSize(73, 73, 72);
        drive("single key grouping");
    }

    @Test
    @Order(3)
    public void testSimpleDocument() {
        input.add(new Key(row, cf, "FIELD_A\0value-a", "VIZ-A", ts));
        input.add(new Key(row, cf, "FIELD_B\0value-b", "VIZ-A", ts));
        input.add(new Key(row, cf, "FIELD_C\0value-c", "VIZ-A", ts));
        input.add(new Key(row, cf, "FIELD_X\0value-x", "VIZ-A", ts));
        input.add(new Key(row, cf, "FIELD_Y\0value-y", "VIZ-A", ts));
        input.add(new Key(row, cf, "FIELD_Z\0value-z", "VIZ-A", ts));
        expectOriginalSize(384);
        expectSize(148, 148, 148);
        drive("simple doc");
    }

    @Test
    @Order(4)
    public void testSimpleDocumentWithComplexVisibilities() {
        input.add(new Key(row, cf, "FIELD_A\0value-a", "VIZ-A&(VIZ-B|VIZ-C)", ts));
        input.add(new Key(row, cf, "FIELD_B\0value-b", "VIZ-A&(VIZ-B|VIZ-C)", ts));
        input.add(new Key(row, cf, "FIELD_C\0value-c", "VIZ-A&(VIZ-B|VIZ-C)", ts));
        input.add(new Key(row, cf, "FIELD_X\0value-x", "VIZ-A&(VIZ-B|VIZ-C)", ts));
        input.add(new Key(row, cf, "FIELD_Y\0value-y", "VIZ-A&(VIZ-B|VIZ-C)", ts));
        input.add(new Key(row, cf, "FIELD_Z\0value-z", "VIZ-A&(VIZ-B|VIZ-C)", ts));
        expectOriginalSize(468);
        expectSize(162, 162, 162);
        drive("complex viz");
    }

    @Test
    @Order(5)
    public void testDocumentWithRepeatFields() {
        input.add(new Key(row, cf, "FIELD_A\0value-1", "VIZ-A", ts));
        input.add(new Key(row, cf, "FIELD_A\0value-2", "VIZ-A", ts));
        input.add(new Key(row, cf, "FIELD_A\0value-3", "VIZ-A", ts));
        input.add(new Key(row, cf, "FIELD_B\0value-4", "VIZ-A", ts));
        input.add(new Key(row, cf, "FIELD_B\0value-5", "VIZ-A", ts));
        input.add(new Key(row, cf, "FIELD_C\0value-6", "VIZ-A", ts));
        expectOriginalSize(384);
        expectSize(148, 130, 130);
        drive("repeat fields");
    }

    @Test
    @Order(6)
    public void testDocumentWithGroupingNotation() {
        input.add(new Key(row, cf, "FIELD_A.1\0value-a", "VIZ-A", ts));
        input.add(new Key(row, cf, "FIELD_A.2\0value-b", "VIZ-A", ts));
        input.add(new Key(row, cf, "FIELD_A.3\0value-c", "VIZ-A", ts));
        input.add(new Key(row, cf, "FIELD_B.1\0value-x", "VIZ-A", ts));
        input.add(new Key(row, cf, "FIELD_B.2\0value-y", "VIZ-A", ts));
        input.add(new Key(row, cf, "FIELD_B.3\0value-z", "VIZ-A", ts));
        expectOriginalSize(396);
        expectSize(160, 160, 136);
        drive("repeat fields grouping");
    }

    @Test
    @Order(7)
    public void testLargeDocument() {
        List<String> fields = List.of("FIELD_A", "FIELD_B", "FIELD_C", "FIELD_X", "FIELD_Y", "FIELD_Z");
        int valuesPerField = 10;
        for (String field : fields) {
            for (int i = 0; i < valuesPerField; i++) {
                input.add(new Key(row, cf, field + "\0value-" + i, "VIZ-A", ts));
            }
        }

        expectOriginalSize(3840);
        expectSize(226, 223, 223);
        drive("large document");
    }

    @Test
    @Order(8)
    public void testLargeDocumentWithGroupingNotation() {
        int groupsPerField = 3;
        int valuesPerGroup = 3;
        List<String> fields = List.of("FIELD_A.1.", "FIELD_B.", "FIELD_C.", "FIELD_X.", "FIELD_Y.", "FIELD_Z.1.2.");
        for (String field : fields) {
            for (int i = 0; i < groupsPerField; i++) {
                String fieldWithGroup = field + (i + 1);
                for (int j = 0; j < valuesPerGroup; j++) {
                    input.add(new Key(row, cf, fieldWithGroup + "\0value-" + j, "VIZ-A", ts));
                }
            }
        }

        expectOriginalSize(3618);
        expectSize(236, 240, 245);
        drive("large grouping notation");
    }

    protected void drive(String context) {

        List<Integer> versions = List.of(1, 2, 3);
        EventSerializationUtil util = new EventSerializationUtil();
        util.setCompressionAlgorithm(EventSerializationUtil.GZIP);
        util.setCompressionThreshold(512);

        for (int version : versions) {
            try {
                util.setSerializationVersion(version);
                KeyGroup keyGroup = util.serialize(iterator(input));

                assertEquals(originalSize, size(input), "original size was " + originalSize);
                assertEquals(expectedSize.get(version - 1), keyGroupSize(keyGroup),
                                "expected size was " + expectedSize.get(version - 1) + " for version " + version);
                if (!expected.equals(results)) {
                    for (int k = 0; k < expected.size(); k++) {
                        assertEquals(expected.get(k), results.get(k));
                    }
                }

                String relative = relative(expectedSize.get(version - 1), originalSize);
                log.info("{} {} {} {} {}%", version, context, originalSize, expectedSize.get(version - 1), relative);
            } catch (IOException e) {
                fail("Failed to serialize keys with version: " + version, e);
            }
        }
    }

    protected void expectOriginalSize(int originalSize) {
        this.originalSize = originalSize;
    }

    protected void expectSize(int... sizes) {
        for (int size : sizes) {
            expectSize(size);
        }
    }

    protected void expectSize(int size) {
        this.expectedSize.add(size);
    }

    protected int size(List<Key> keys) {
        int size = 0;
        for (Key key : keys) {
            size += key.getSize();
        }
        return size;
    }

    protected int keyGroupSize(KeyGroup keyGroup) {
        int size = 0;
        for (Pair<Key,Value> pair : keyGroup.getKeyValues()) {
            size += pair.getKey().getSize();
            size += pair.getValue().getSize();
        }
        return size;
    }

    protected String relative(int compressed, int original) {
        return df.format(((float) compressed / (float) original) - 1.0f);
    }
}
