package datawave.query.attributes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.data.type.DateType;
import datawave.data.type.GeoLatType;
import datawave.data.type.GeoLonType;
import datawave.data.type.GeoType;
import datawave.data.type.GeometryType;
import datawave.data.type.HexStringType;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.LcType;
import datawave.data.type.NoOpType;
import datawave.data.type.NumberListType;
import datawave.data.type.NumberType;
import datawave.data.type.PointType;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.function.serializer.KryoDocumentSerializer;
import datawave.query.util.TypeMetadata;

/**
 * Test document serialization with various attributes
 */
public class DocumentTest {

    private static final Logger log = LoggerFactory.getLogger(DocumentTest.class);

    private final String datatype = "datatype";
    private final Key documentKey = new Key("row", "datatype\0uid");
    private final KryoDocumentSerializer serializer = new KryoDocumentSerializer();
    private final KryoDocumentDeserializer deserializer = new KryoDocumentDeserializer();

    private final long startTimeMillis = System.currentTimeMillis();
    private final Random rand = new Random();

    private static final int MAX_ITERATIONS = 250;
    private static final int DOCUMENT_SIZE = 250;

    private Document d;
    private AttributeFactory attributeFactory;

    @BeforeEach
    public void setup() {
        TypeMetadata typeMetadata = new TypeMetadata();
        typeMetadata.put("DATE", datatype, DateType.class.getTypeName());
        typeMetadata.put("GEO_LAT", datatype, GeoLatType.class.getTypeName());
        typeMetadata.put("GEO_LON", datatype, GeoLonType.class.getTypeName());
        typeMetadata.put("GEOMETRY", datatype, GeometryType.class.getTypeName());
        typeMetadata.put("GEO", datatype, GeoType.class.getTypeName());
        typeMetadata.put("HEX", datatype, HexStringType.class.getTypeName());
        typeMetadata.put("LC", datatype, LcType.class.getTypeName());
        typeMetadata.put("LC_ND", datatype, LcNoDiacriticsType.class.getTypeName());
        typeMetadata.put("NO_OP", datatype, NoOpType.class.getTypeName());
        typeMetadata.put("NUM", datatype, NumberType.class.getTypeName());
        typeMetadata.put("NUM_LIST", datatype, NumberListType.class.getTypeName());
        typeMetadata.put("POINT", datatype, PointType.class.getTypeName());

        d = new Document(documentKey, true);
        attributeFactory = new AttributeFactory(typeMetadata);
    }

    @Test
    public void testEmptyDocument() {
        roundTrip(MAX_ITERATIONS, 15);
    }

    @Test
    public void testDocumentWithLcType() {
        Attribute<?> attr = createAttribute("LC", "value");
        d.put("LC", attr);
        roundTrip(MAX_ITERATIONS, 32);
    }

    @Test
    public void testDocumentWithLcNoDiacriticsType() {
        Attribute<?> attr = createAttribute("LC_ND", "value");
        d.put("LC_ND", attr);
        roundTrip(MAX_ITERATIONS, 35);
    }

    @Test
    public void testDocumentWithHexType() {
        Attribute<?> attr = createAttribute("HEX", "a1b2c3");
        d.put("HEX", attr);
        roundTrip(MAX_ITERATIONS, 40);
    }

    @Test
    public void testDocumentWithNumberType() {
        Attribute<?> attr = createAttribute("NUM", "12");
        d.put("NUM", attr);
        roundTrip(MAX_ITERATIONS, 36);
    }

    @Test
    public void testDocumentWithNumberTypeNormalizedValue() {
        Attribute<?> attr = createAttribute("NUM", "+bE1.2");
        d.put("NUM", attr);
        roundTrip(MAX_ITERATIONS, 36);
    }

    @Test
    public void testDocumentWithNumberTypeLargeValue() {
        Attribute<?> attr = createAttribute("NUM", "12456789.987654321");
        d.put("NUM", attr);
        roundTrip(MAX_ITERATIONS, 67);
    }

    @Test
    public void testOneOfEverything() {
        d.put("DATE", createAttribute("DATE", String.valueOf(System.currentTimeMillis())));
        d.put("GEO_LAT", createAttribute("GEO_LAT", "-90"));
        d.put("GEO_LON", createAttribute("GEO_LON", "-180"));
        d.put("HEX", createAttribute("HEX", "a1b2c3"));
        d.put("IP", createAttribute("IP", "192.168.1.1"));
        d.put("IPV4", createAttribute("IPV4", "192.168.1.1"));
        d.put("LC", createAttribute("LC", "value-1"));
        d.put("NC_ND", createAttribute("NC_ND", "value-2"));
        d.put("NC_ND_LIST", createAttribute("NC_ND", "value-2"));
        d.put("NUM", createAttribute("NUM", "25"));
        d.put("NUM_LIST", createAttribute("NUM_LIST", "22,23,24"));
        d.put("POINT", createAttribute("POINT", "POINT(10 10)"));
        roundTrip(MAX_ITERATIONS, 350);
    }

    @Test
    public void testLargeDocument() {
        int size = 10_000;
        for (int i = 0; i < size; i++) {
            Attribute<?> attr = createAttribute("LC", "value-" + i);
            d.put("LC", attr);
        }
        roundTrip(MAX_ITERATIONS, 188006);
    }

    @Test
    public void testSingleFieldedRoundTrips() {
        roundTrip("DATE", DOCUMENT_SIZE, MAX_ITERATIONS, 10028);
        roundTrip("GEO_LAT", DOCUMENT_SIZE, MAX_ITERATIONS, 0);
        roundTrip("GEO_LON", DOCUMENT_SIZE, MAX_ITERATIONS, 0);
        roundTrip("HEX", DOCUMENT_SIZE, MAX_ITERATIONS, 3277);
        roundTrip("LC", DOCUMENT_SIZE, MAX_ITERATIONS, 4256);
        roundTrip("LC_ND", DOCUMENT_SIZE, MAX_ITERATIONS, 4259);
        roundTrip("NUM", DOCUMENT_SIZE, MAX_ITERATIONS, 4520);
        roundTrip("POINT", DOCUMENT_SIZE, MAX_ITERATIONS, 0);
    }

    @Test
    public void testHexRoundTrip() {
        // original size: 11063
        // post hex serialization changes: 11563
        // read times cut by about 35%
        // attribute index optimization: 3277
        roundTrip("HEX", DOCUMENT_SIZE, MAX_ITERATIONS, 3277);
    }

    @Test
    public void testNumberRoundTrip() {
        // original size: 11213
        // post number serialization: 12806
        // attribute write times remained the same, attribute read times were cut by about 50%
        // attribute index optimization: 4520
        roundTrip("NUM", DOCUMENT_SIZE, MAX_ITERATIONS, 4520);
    }

    @Test
    public void testNumberListRoundTrip() {
        // original size: 12994
        // kryo optimization: 18030
        // attribute index optimization: 9731
        roundTrip("NUM_LIST", DOCUMENT_SIZE, MAX_ITERATIONS, 9731);
    }

    @Test
    public void testDateRoundTrip() {
        // original size: 16564
        // kryo optimization: 22564
        // using default DateSerializer dropped the read time by 50%
        // attribute index optimization: 10028
        roundTrip("DATE", DOCUMENT_SIZE, MAX_ITERATIONS, 10028);
    }

    @Test
    public void testPointRoundTrip() {
        roundTrip("POINT", DOCUMENT_SIZE, MAX_ITERATIONS, 0);
    }

    protected void roundTrip(int max, int serializedLength) {
        Entry<Key,Value> entry = serialize(d);
        log.trace("size: {}", entry.getValue().getSize());
        assertEquals(serializedLength, entry.getValue().getSize(), 5.0f);
        for (int i = 0; i < max; i++) {
            Entry<Key,Document> result = deserialize(entry);
            Document d2 = result.getValue();
            assertEquals(d, d2);
        }
    }

    /**
     * Test both read and writes for a single fielded document. Should provide a rough comparison of Type serialization performance
     *
     * @param field
     *            the field, corresponds to a Type
     * @param documentSize
     *            the size of the document to create
     * @param maxIterations
     *            the number of iterations
     * @param serializedLength
     *            the expected length of the serialized document
     */
    protected void roundTrip(String field, int documentSize, int maxIterations, int serializedLength) {
        long readTime = 0;
        long writeTime = 0;
        Document d = createDocumentForField(field, documentSize);

        Entry<Key,Value> entry = null;
        writeTime = System.nanoTime();
        for (int i = 0; i < maxIterations; i++) {
            entry = serialize(d);
        }
        writeTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - writeTime);
        assertNotNull(entry);

        if (serializedLength > 0) {
            // some tests rely on random inputs, do not assert the serialized length in those cases
            float delta = serializedLength * 0.15f;
            assertEquals(serializedLength, entry.getValue().getSize(), delta);
        }

        for (int i = 0; i < maxIterations; i++) {
            long start = System.nanoTime();
            Entry<Key,Document> result = deserialize(entry);
            readTime += System.nanoTime() - start;
            Document d2 = result.getValue();
            assertEquals(d, d2);
        }
        readTime = TimeUnit.NANOSECONDS.toMillis(readTime);

        log.info("{} read: {} write: {}", field, readTime, writeTime);
    }

    protected Document createDocumentForField(String field, int size) {
        d = new Document(documentKey, true);
        for (int i = 0; i < size; i++) {
            Attribute<?> attr = createAttribute(field, i);
            d.put(field, attr);
        }
        return d;
    }

    protected Attribute<?> createAttribute(String field, int index) {
        String value = createValueForFieldAndIndex(field, index);
        return createAttribute(field, value);
    }

    protected Attribute<?> createAttribute(String field, String value) {
        Attribute<?> attr = attributeFactory.create(field, value, documentKey, datatype, true);
        attr.clearMetadata();
        return attr;
    }

    protected String createValueForFieldAndIndex(String field, int index) {
        switch (field) {
            case "DATE":
                return Long.toString(startTimeMillis + index);
            case "GEO_LAT":
                return String.valueOf(-90 + rand.nextInt(180));
            case "GEO_LON":
                return String.valueOf(-180 + rand.nextInt(360));
            case "GEOMETRY":
            case "GEO":
            case "HEX":
                return Integer.toHexString(index);
            case "HIT_TERM":
            case "IP":
            case "IPV4":
                return "192.168.1.1";
            case "LC_ND_LIST":
            case "LC_ND":
            case "LC":
                return "value-" + index;
            case "NO_OP":
            case "NUM_LIST":
                return index + "," + (index + 1) + "," + (index + 2);
            case "NUM":
                return Integer.toString(index);
            case "POINT":
                int x = -10 + rand.nextInt(20);
                int y = -10 + rand.nextInt(20);
                return "POINT(" + x + " " + y + ")";
            default:
                throw new IllegalArgumentException("Unknown field: " + field);
        }
    }

    protected Entry<Key,Value> serialize(Document d) {
        Entry<Key,Document> entry = new AbstractMap.SimpleEntry<>(documentKey, d);
        return serializer.apply(entry);
    }

    protected Entry<Key,Document> deserialize(Entry<Key,Value> entry) {
        return deserializer.apply(entry);
    }

    @Test
    public void testConsumeRawData() {
        Set<Key> keys = Set.of(documentKey);

        Value value = new Value();
        List<Entry<Key,Value>> entries = new ArrayList<>();
        entries.add(new AbstractMap.SimpleEntry<>(new Key("row", "datatype\0uid", "FIELD_A\0value-a"), value));
        entries.add(new AbstractMap.SimpleEntry<>(new Key("row", "datatype\0uid", "FIELD_B\0value-b"), value));
        entries.add(new AbstractMap.SimpleEntry<>(new Key("row", "datatype\0uid", "FIELD_C\0value-c"), value));
        entries.add(new AbstractMap.SimpleEntry<>(new Key("row", "datatype\0uid", "FIELD_D\0value-d"), value));
        entries.add(new AbstractMap.SimpleEntry<>(new Key("row", "datatype\0uid", "FIELD_E\0value-e"), value));

        List<Key> documentKeys = createDocumentKeys();

        int max = 1_000_000;
        for (int i = 0; i < max; i++) {
            Key randomDocumentKey = documentKeys.get(rand.nextInt(documentKeys.size()));
            Document d = new Document(randomDocumentKey, keys, false, entries.iterator(), new TypeMetadata(), null, false, true, null, true, true);
            assertEquals(6, d.size());
        }
    }

    private List<Key> createDocumentKeys() {
        List<Key> keys = new ArrayList<>();
        keys.add(new Key("20250601", "datatype\0uid"));
        keys.add(new Key("20250602", "datatype\0uid"));
        keys.add(new Key("20250603", "datatype\0uid"));
        keys.add(new Key("20250604", "datatype\0uid"));
        keys.add(new Key("20250605", "datatype\0uid"));
        keys.add(new Key("20250606", "datatype\0uid"));
        keys.add(new Key("20250607", "datatype\0uid"));
        keys.add(new Key("20250608", "datatype\0uid"));
        keys.add(new Key("20250609", "datatype\0uid"));
        return keys;
    }
}
