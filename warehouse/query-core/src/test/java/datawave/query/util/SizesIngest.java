package datawave.query.util;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.io.Text;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.util.GeometricShapeFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.data.ColumnFamilyConstants;
import datawave.data.hash.UID;
import datawave.data.normalizer.LcNoDiacriticsNormalizer;
import datawave.data.normalizer.Normalizer;
import datawave.data.normalizer.NumberNormalizer;
import datawave.data.normalizer.PointNormalizer;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NumberType;
import datawave.data.type.PointType;
import datawave.ingest.protobuf.Uid;
import datawave.query.SizesTest.RangeType;
import datawave.util.TableName;
import datawave.util.time.DateHelper;

/**
 * Similar to {@link ColorsIngest} and {@link ShapesIngest}, SizesIngest exercises fully random data and queries
 * <p>
 * Fields are SIZE, COLOR, and SHAPE
 * <p>
 * Sizes are small, medium, and large
 * <p>
 * Colors are red, green, and blue
 * <p>
 * Shapes are square, triangle, and circle
 * <p>
 * Additional fields will be added to get some variety in Attribute Types
 */
public class SizesIngest {

    private static final Logger log = LoggerFactory.getLogger(SizesIngest.class);

    private static final String ROW = "20250606";
    private static final int NUM_SHARDS = 5;
    private static final int EVENTS_PER_SHARD = 10;
    private static final int EXTRA_TYPED_FIELDS_PER_EVENT = 2;
    private static final int EXTRA_NON_TYPED_FIELDS_PER_EVENT = 15;

    private static final ColumnVisibility cv = new ColumnVisibility("ALL");
    private static final String datatype = "size";
    private static final Value EMPTY_VALUE = new Value();

    private final Set<String> indexedFields = Set.of("SIZE", "COLOR", "SHAPE", "VERTEX", "POINT");
    private final List<String> sizeValues = List.of("small", "medium", "large");
    private final List<String> colorValues = List.of("red", "green", "blue");
    private final List<String> shapeValues = List.of("square", "triangle", "circle");
    private final List<String> vertexValues = List.of("4", "3", "0");

    private final List<String> extraFields = List.of("DATE", "GEO_LAT", "GEO_LON", "GEOMETRY", "GEO", "HEX", "LC", "LC_ND", "NO_OP", "NUM");

    private static final GeometricShapeFactory shapeFactory = new GeometricShapeFactory();

    private static final Random rand = new SecureRandom();

    private final int delta = 15; // 15%

    private int numberOfEvents = 0;

    // allows a rough distribution of events based on the value of the SIZE field
    private final int sizeSmallRatio = 1;
    private final int sizeMediumRatio = 5;
    private final int sizeLargeRatio = 10;
    private final int sizeTotal = sizeSmallRatio + sizeMediumRatio + sizeLargeRatio;

    private List<Multimap<String,String>> events;
    private final AccumuloClient client;

    public SizesIngest(AccumuloClient client) {
        this.client = client;
    }

    public void write(RangeType type) throws Exception {
        createTables();
        loadMetadata();
        writeEvents(type);
        log.info("wrote {} events", numberOfEvents);
    }

    private void createTables() throws Exception {
        TableOperations tops = client.tableOperations();
        tops.create(TableName.SHARD);
        tops.create(TableName.SHARD_INDEX);
        tops.create(TableName.METADATA);
    }

    private void loadMetadata() throws Exception {

        try (BatchWriter bw = client.createBatchWriter(TableName.METADATA)) {
            Mutation m = new Mutation("num_shards");
            m.put("ns", ROW + "_" + getNumShards(), new Value());
            bw.addMutation(m);
        }

        try (BatchWriter bw = client.createBatchWriter(TableName.METADATA)) {
            // write metadata for indexed fields
            for (String field : indexedFields) {
                Mutation m = new Mutation(field);
                m.put(ColumnFamilyConstants.COLF_E, new Text(datatype), EMPTY_VALUE);
                // skip F column for now
                m.put(ColumnFamilyConstants.COLF_I, new Text(datatype), EMPTY_VALUE);
                m.put(ColumnFamilyConstants.COLF_T, new Text(datatype + "\0" + normalizerNameForField(field)), EMPTY_VALUE);
                bw.addMutation(m);
            }
        }
    }

    private void writeEvents(RangeType type) throws Exception {
        for (int i = 0; i < NUM_SHARDS; i++) {
            String shard = ROW + "_" + i;
            writeEventsForShard(shard, type);
        }
    }

    private void writeEventsForShard(String shard, RangeType type) throws Exception {
        // each shard gets its own random events
        createRandomEvents();
        writeShardIndex(shard, type);
        writeFieldIndex(shard);
        writeEvent(shard);
    }

    private void writeShardIndex(String shard, RangeType type) throws Exception {
        long ts = DateHelper.parse(ROW).getTime();
        try (BatchWriter bw = client.createBatchWriter(TableName.SHARD_INDEX)) {
            for (Multimap<String,String> event : events) {
                Multimap<String,String> inverted = invert(event);
                for (String value : inverted.keySet()) {
                    Mutation m = new Mutation(value);
                    for (String field : inverted.get(value)) {
                        String uid = uidForEvent(shard, event.get("COUNTER").iterator().next());
                        m.put(field, shard + "\0" + datatype, cv, ts, getValue(type, uid));
                        bw.addMutation(m);
                    }
                }
            }
        }
    }

    private void writeFieldIndex(String shard) throws Exception {
        long ts = DateHelper.parse(ROW).getTime();
        try (BatchWriter bw = client.createBatchWriter(TableName.SHARD)) {
            Mutation m = new Mutation(shard);
            for (Multimap<String,String> event : events) {
                String uid = uidForEvent(shard, event.get("COUNTER").iterator().next());
                // only indexed fields
                for (String field : indexedFields) {
                    for (String value : event.get(field)) {
                        if (value != null) {
                            String normalizedValue = normalizerForField(field).normalize(value);
                            String cf = "fi\0" + field;
                            String cq = normalizedValue + "\0" + datatype + "\0" + uid;
                            m.put(cf, cq, cv, ts, EMPTY_VALUE);
                        }
                    }
                }
            }
            bw.addMutation(m);
        }
    }

    private void writeEvent(String shard) throws Exception {
        long ts = DateHelper.parse(ROW).getTime();
        try (BatchWriter bw = client.createBatchWriter(TableName.SHARD)) {
            Mutation m = new Mutation(shard);
            for (Multimap<String,String> event : events) {
                String uid = uidForEvent(shard, event.get("COUNTER").iterator().next());
                String cf = datatype + "\0" + uid;
                // all fields
                for (String field : event.keySet()) {
                    for (String value : event.get(field)) {
                        String cq = field + "\0" + value;
                        m.put(cf, cq, cv, ts, EMPTY_VALUE);
                    }
                }
            }
            bw.addMutation(m);
        }
    }

    private void createRandomEvents() {
        events = new ArrayList<>();
        for (int i = 0; i < EVENTS_PER_SHARD; i++) {
            events.add(createRandomEvent());
        }
    }

    private Multimap<String,String> createRandomEvent() {
        Multimap<String,String> event = HashMultimap.create();
        event.put("COLOR", getRandomValue(colorValues));
        event.put("SIZE", getRandomSize(sizeValues));
        event.put("SHAPE", getRandomValue(shapeValues));

        // write unique counter
        event.put("COUNTER", String.valueOf(numberOfEvents++));

        addVertices(event);
        addRandomTypedFields(event);

        int extra = rand.nextInt(EXTRA_NON_TYPED_FIELDS_PER_EVENT);
        for (int i = 0; i < extra; i++) {
            event.put("FIELD_" + i, "value-" + i);
        }
        return event;
    }

    private void addVertices(Multimap<String,String> event) {
        Preconditions.checkArgument(event.get("SHAPE").size() == 1);
        int n = getVertexForShape(event.get("SHAPE").iterator().next());
        event.put("VERTEX", String.valueOf(n));

        if (n == 0) {
            return;
        }

        Set<String> points = new HashSet<>();
        while (points.size() < n) {
            points.add(getRandomPoint());
        }

        for (String point : points) {
            event.put("POINT", point);
        }
    }

    private String getRandomValue(List<String> values) {
        return values.get(rand.nextInt(values.size()));
    }

    /**
     * Returns a value of small, medium, or large while adhering to defined ratios
     *
     * @param values
     *            the values
     * @return a size
     */
    private String getRandomSize(List<String> values) {
        int value = rand.nextInt(sizeTotal);
        if (value < sizeSmallRatio) {
            return values.get(0);
        } else if (value < (sizeSmallRatio + sizeMediumRatio)) {
            return values.get(1);
        } else {
            return values.get(2);
        }
    }

    private int getVertexForShape(String shape) {
        switch (shape) {
            case "square":
                return 4;
            case "triangle":
                return 3;
            case "circle":
            default:
                return 0;
        }
    }

    private String getRandomPoint() {
        int x = rand.nextInt(10);
        int y = rand.nextInt(10);
        return "POINT(" + x + " " + y + ")";
    }

    private void addRandomTypedFields(Multimap<String,String> event) {
        int n = rand.nextInt(EXTRA_TYPED_FIELDS_PER_EVENT);
        for (int i = 0; i < n; i++) {
            String field = getRandomValue(extraFields);
            String value = getRandomValueForField(field);
            event.put(field, value);
        }
    }

    private String getRandomValueForField(String field) {
        switch (field) {
            case "DATE":
                return DateHelper.format(new Date(rand.nextInt()));
            case "GEO_LAT":
            case "GEO_LON":
                return getRandomPoint();
            case "GEOMETRY":
            case "GEO":
                return createRandomGeometry();
            case "HEX":
                return Integer.toHexString(rand.nextInt());
            case "LC":
            case "LC_ND":
            case "NO_OP":
                return RandomStringUtils.randomAlphabetic(10, 15);
            case "NUM":
                return RandomStringUtils.randomNumeric(5, 10);
            default:
                throw new RuntimeException("Unknown field: " + field);
        }
    }

    private String createRandomGeometry() {
        shapeFactory.setNumPoints(32);
        shapeFactory.setCentre(new Coordinate(rand.nextInt(15), rand.nextInt(15)));
        shapeFactory.setSize(1.5 * 2);
        return String.valueOf(shapeFactory.createCircle());
    }

    public int getNumShards() {
        return NUM_SHARDS;
    }

    protected String normalizerNameForField(String field) {
        switch (field) {
            case "COLOR":
            case "SHAPE":
            case "SIZE":
                return LcNoDiacriticsType.class.getName();
            case "VERTEX":
                return NumberType.class.getName();
            case "POINT":
                return PointType.class.getName();
            default:
                throw new RuntimeException("Unknown field: " + field);
        }
    }

    protected Normalizer<?> normalizerForField(String field) {
        switch (field) {
            case "COLOR":
            case "SHAPE":
            case "SIZE":
                return new LcNoDiacriticsNormalizer();
            case "VERTEX":
                return new NumberNormalizer();
            case "POINT":
                return new PointNormalizer();
            default:
                throw new RuntimeException("Unknown field: " + field);
        }
    }

    private String uidForEvent(String row, String count) {
        String data = row + count;
        return UID.builder().newId(data.getBytes(), (Date) null).toString();
    }

    /**
     * Inverts and normalized indexed fields from an event for use in the shard index
     *
     * @param event
     *            the event
     * @return the inverted and normalized indexed fields
     */
    private Multimap<String,String> invert(Multimap<String,String> event) {
        Multimap<String,String> inverted = HashMultimap.create();
        for (String key : event.keySet()) {
            // only invert and normalize indexed fields
            if (indexedFields.contains(key)) {
                Normalizer<?> normalizer = normalizerForField(key);
                for (String value : event.get(key)) {
                    // normalize indexed fields
                    String normalizedValue = normalizer.normalize(value);
                    inverted.put(normalizedValue, key);
                }
            }
        }
        return inverted;
    }

    private static Value getValue(RangeType type, String uid) {
        Uid.List.Builder builder = Uid.List.newBuilder();
        if (type.equals(RangeType.DOCUMENT)) {
            builder.setIGNORE(false);
            builder.setCOUNT(1L);
            builder.addUID(uid);
        } else {
            builder.setIGNORE(true);
            builder.setCOUNT(17L); // arbitrary prime number below the 20 max uid limit
        }
        return new Value(builder.build().toByteArray());
    }

}
