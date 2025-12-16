package datawave.query;

import static datawave.query.util.AbstractQueryTest.RangeType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.data.ColumnFamilyConstants;
import datawave.data.hash.UID;
import datawave.data.normalizer.LcNoDiacriticsNormalizer;
import datawave.data.normalizer.Normalizer;
import datawave.data.normalizer.NumberNormalizer;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NumberType;
import datawave.ingest.protobuf.Uid;
import datawave.util.TableName;
import datawave.util.time.DateHelper;

public class MultiNormalizerIngest {

    private static final String OLD_ROW = "20250707_0";
    private static final String NEW_ROW = "20250708_0";

    private static final ColumnVisibility cv = new ColumnVisibility("ALL");
    private static final String datatype = "dt";
    private static final Value EMPTY_VALUE = new Value();

    private final Map<String,Normalizer<?>> normalizerMap = new HashMap<>();
    private final Set<String> indexedFields = Set.of("COLOR", "SIZE");

    private final AccumuloClient client;

    private List<Multimap<String,String>> events;

    public MultiNormalizerIngest(AccumuloClient client) {
        this.client = client;

        normalizerMap.put("SIZE" + OLD_ROW, new LcNoDiacriticsNormalizer());
        normalizerMap.put("SIZE" + NEW_ROW, new NumberNormalizer());
        normalizerMap.put("COLOR" + OLD_ROW, new LcNoDiacriticsNormalizer());
        normalizerMap.put("COLOR" + NEW_ROW, new LcNoDiacriticsNormalizer());
    }

    public void write(RangeType type) throws Exception {
        createTables();
        loadMetadata();
        writeEvents(type);
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
            m.put("ns", "20250707_1", new Value());
            bw.addMutation(m);
        }

        try (BatchWriter bw = client.createBatchWriter(TableName.METADATA)) {
            // write metadata for indexed fields
            Mutation m = new Mutation("SIZE");
            m.put(ColumnFamilyConstants.COLF_E, new Text(datatype), EMPTY_VALUE);
            // skip F column for now
            m.put(ColumnFamilyConstants.COLF_I, new Text(datatype), EMPTY_VALUE);
            m.put(ColumnFamilyConstants.COLF_T, new Text(datatype + "\0" + normalizerNameForField("SIZE", OLD_ROW)), EMPTY_VALUE);
            m.put(ColumnFamilyConstants.COLF_T, new Text(datatype + "\0" + normalizerNameForField("SIZE", NEW_ROW)), EMPTY_VALUE);
            bw.addMutation(m);

            m = new Mutation("COLOR");
            m.put(ColumnFamilyConstants.COLF_E, new Text(datatype), EMPTY_VALUE);
            // skip F column for now
            m.put(ColumnFamilyConstants.COLF_I, new Text(datatype), EMPTY_VALUE);
            m.put(ColumnFamilyConstants.COLF_T, new Text(datatype + "\0" + normalizerNameForField("COLOR", OLD_ROW)), EMPTY_VALUE);
            m.put(ColumnFamilyConstants.COLF_T, new Text(datatype + "\0" + normalizerNameForField("COLOR", NEW_ROW)), EMPTY_VALUE);
            bw.addMutation(m);
        }
    }

    protected String normalizerNameForField(String field, String row) {
        if (field.equals("COLOR")) {
            return LcNoDiacriticsType.class.getName();
        } else if (field.equals("SIZE")) {
            if (row.equals(OLD_ROW)) {
                return LcNoDiacriticsType.class.getName();
            } else if (row.equals(NEW_ROW)) {
                return NumberType.class.getName();
            } else {
                throw new IllegalArgumentException("Unknown row: " + row + " for field: " + field);
            }
        } else {
            throw new IllegalArgumentException("Unknown field: " + field);
        }
    }

    private Normalizer<?> normalizerForField(String field, String row) {
        String key = field + row;
        Normalizer<?> normalizer = normalizerMap.get(key);
        if (normalizer == null) {
            throw new IllegalArgumentException("No normalizer found for key: " + key);
        }
        return normalizer;
    }

    private void writeEvents(RangeType type) throws Exception {
        createEvents();
        writeEventsForShard(OLD_ROW, type);
        writeEventsForShard(NEW_ROW, type);
    }

    private void createEvents() {
        events = new ArrayList<>();
        int counter = 0;
        List<String> values = List.of("1", "2", "3", "4", "5", "6", "7", "8", "9", "10");
        for (String value : values) {
            Multimap<String,String> event = HashMultimap.create();
            event.put("SIZE", value);
            event.put("COLOR", "red");
            event.put("COUNTER", String.valueOf(++counter));
            events.add(event);
        }
    }

    private void writeEventsForShard(String shard, RangeType type) throws Exception {
        writeShardIndex(shard, type);
        writeFieldIndex(shard);
        writeEvent(shard);
    }

    private void writeShardIndex(String shard, RangeType type) throws Exception {
        long ts = DateHelper.parse(shard).getTime();
        try (BatchWriter bw = client.createBatchWriter(TableName.SHARD_INDEX)) {
            for (Multimap<String,String> event : events) {
                Multimap<String,String> inverted = invert(event, shard);
                for (String value : inverted.keySet()) {
                    Mutation m = new Mutation(value);
                    Collection<String> fields = inverted.get(value);
                    for (String field : fields) {
                        String uid = uidForEvent(shard, event.get("COUNTER").iterator().next());
                        m.put(field, shard + "\0" + datatype, cv, ts, getValue(type, uid));
                        bw.addMutation(m);
                    }
                }
            }
        }
    }

    private void writeFieldIndex(String shard) throws Exception {
        long ts = DateHelper.parse(shard).getTime();
        try (BatchWriter bw = client.createBatchWriter(TableName.SHARD)) {
            Mutation m = new Mutation(shard);
            for (Multimap<String,String> event : events) {
                String uid = uidForEvent(shard, event.get("COUNTER").iterator().next());
                // each indexed field is in every event, for now
                for (String field : indexedFields) {
                    Normalizer<?> normalizer = normalizerForField(field, shard);
                    Collection<String> values = event.get(field);
                    for (String value : values) {
                        String normalizedValue = normalizer.normalize(value);
                        String cf = "fi\0" + field;
                        String cq = normalizedValue + "\0" + datatype + "\0" + uid;
                        m.put(cf, cq, cv, ts, EMPTY_VALUE);
                    }
                }
            }
            bw.addMutation(m);
        }
    }

    private void writeEvent(String shard) throws Exception {
        long ts = DateHelper.parse(shard).getTime();
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

    private String uidForEvent(String row, String count) {
        String data = row + count;
        return UID.builder().newId(data.getBytes(), (Date) null).toString();
    }

    /**
     * Invert and normalize indexed field values for insertion into the shard index
     *
     * @param event
     *            the event
     * @param row
     *            the row
     * @return an inverted and normalized hash map
     */
    private Multimap<String,String> invert(Multimap<String,String> event, String row) {
        Multimap<String,String> inverted = HashMultimap.create();
        for (String key : event.keySet()) {
            if (indexedFields.contains(key)) {
                Normalizer<?> normalizer = normalizerForField(key, row);
                Collection<String> values = event.get(key);
                for (String value : values) {
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
