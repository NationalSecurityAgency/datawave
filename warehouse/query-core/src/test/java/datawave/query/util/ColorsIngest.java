package datawave.query.util;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.data.ColumnFamilyConstants;
import datawave.data.hash.UID;
import datawave.data.type.LcNoDiacriticsType;
import datawave.ingest.protobuf.Uid;
import datawave.util.TableName;
import datawave.util.time.DateHelper;

/**
 * Data for multi-day, multi-shard queries.
 * <p>
 * Primary colors are red, yellow, blue
 * <p>
 * Secondary colors are orange, green, purple
 * <p>
 * Complimentary pairs are red-green, yellow-purple, blue-orange
 */
public class ColorsIngest {

    public enum RangeType {
        SHARD, DOCUMENT
    }

    private static final String startDay = "20250301";
    private static final String endDay = "20250331";

    private static final int NUM_SHARDS = 1;
    private static final int NUM_DAYS = 31;

    private static final int NEW_SHARDS = 2;
    private static final String NEW_SHARDS_START = "20250327";

    private static final String datatype = "colors";
    private static final ColumnVisibility cv = new ColumnVisibility("ALL");
    private static final Value EMPTY_VALUE = new Value();

    private static final LongCombiner.VarLenEncoder encoder = new LongCombiner.VarLenEncoder();

    protected static String normalizerForField(String field) {
        switch (field) {
            case "COLOR":
            default:
                return LcNoDiacriticsType.class.getName();
        }
    }

    protected static int getNumShardsForDay(String day) {
        if (day.compareTo(NEW_SHARDS_START) < 0) {
            return NUM_SHARDS;
        } else {
            return NEW_SHARDS;
        }
    }

    public static int getNumShards() {
        return NUM_SHARDS;
    }

    public static int getNewShards() {
        return NEW_SHARDS;
    }

    public static int getNumDays() {
        return NUM_DAYS;
    }

    public static String getStartDay() {
        return startDay;
    }

    public static String getEndDay() {
        return endDay;
    }

    public static void writeData(AccumuloClient client, RangeType type) throws Exception {
        TableOperations tops = client.tableOperations();
        tops.create(TableName.SHARD);
        tops.create(TableName.SHARD_INDEX);
        tops.create(TableName.METADATA);

        Calendar start = getStartOfDay();
        Calendar stop = getStartOfDay();
        stop.add(Calendar.DAY_OF_YEAR, getNumDays());

        Multimap<String,String> e1 = HashMultimap.create();
        e1.put("COLOR", "red");
        Multimap<String,String> e2 = HashMultimap.create();
        e2.put("COLOR", "yellow");
        Multimap<String,String> e3 = HashMultimap.create();
        e3.put("COLOR", "blue");
        List<Multimap<String,String>> events = new ArrayList<>(List.of(e1, e2, e3));

        while (start.compareTo(stop) < 0) {
            String day = DateHelper.format(start.getTime());
            writeEventsForDay(client, type, day, events);
            start.add(Calendar.DAY_OF_YEAR, 1);
        }
    }

    private static Calendar getStartOfDay() {
        Calendar cal = Calendar.getInstance();
        cal.setTime(DateHelper.parse(startDay));
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal;
    }

    protected static void writeEventsForDay(AccumuloClient client, RangeType type, String day, List<Multimap<String,String>> events) throws Exception {
        int numShards = getNumShardsForDay(day);

        // shard index data
        try (BatchWriter bw = client.createBatchWriter(TableName.SHARD_INDEX)) {
            Multimap<String,String> inverted = HashMultimap.create();
            for (Multimap<String,String> event : events) {
                for (String field : event.keySet()) {
                    for (String value : event.get(field)) {
                        inverted.put(value, field);
                    }
                }
            }

            long ts = DateHelper.parse(day).getTime();

            for (String value : inverted.keySet()) {
                Mutation m = new Mutation(value);
                for (String field : inverted.get(value)) {
                    for (int offset = 0; offset < numShards; offset++) {
                        String shard = day + "_" + offset;
                        String uid = uidForEvent(shard, field, value);
                        m.put(field, shard + "\0" + datatype, cv, ts, getValue(type, uid));
                    }
                }
                bw.addMutation(m);
            }
        }

        // shard data
        try (BatchWriter bw = client.createBatchWriter(TableName.SHARD)) {
            for (int offset = 0; offset < numShards; offset++) {
                String shard = day + "_" + offset;
                Mutation m = new Mutation(shard);

                long ts = DateHelper.parse(day).getTime();

                for (Multimap<String,String> event : events) {

                    String colorField = "COLOR";
                    String colorValue = event.get("COLOR").iterator().next();
                    String uid = uidForEvent(shard, colorField, colorValue);
                    String cf = datatype + "\0" + uid;

                    for (String field : event.keySet()) {
                        for (String value : event.get(field)) {
                            m.put(cf, field + "\0" + value, cv, ts, EMPTY_VALUE);
                        }
                    }
                }
                bw.addMutation(m);
            }
        }

        // field index data
        try (BatchWriter bw = client.createBatchWriter(TableName.SHARD)) {
            for (int offset = 0; offset < numShards; offset++) {
                String shard = day + "_" + offset;
                Mutation m = new Mutation(shard);

                long ts = DateHelper.parse(day).getTime();

                for (Multimap<String,String> event : events) {

                    String colorField = "COLOR";
                    String colorValue = event.get("COLOR").iterator().next();
                    String uid = uidForEvent(shard, colorField, colorValue);

                    for (String field : event.keySet()) {
                        for (String value : event.get(field)) {
                            String cf = "fi\0" + field;
                            String cq = value + "\0" + datatype + "\0" + uid;
                            m.put(cf, cq, cv, ts, EMPTY_VALUE);
                        }
                    }
                }
                bw.addMutation(m);
            }
        }

        // metadata table
        try (BatchWriter bw = client.createBatchWriter(TableName.METADATA)) {
            Mutation m = new Mutation("COLOR");
            m.put(ColumnFamilyConstants.COLF_E, new Text(datatype), EMPTY_VALUE);
            // skipping F column for now
            m.put(ColumnFamilyConstants.COLF_I, new Text(datatype), EMPTY_VALUE);
            m.put(ColumnFamilyConstants.COLF_T, new Text(datatype + "\0" + normalizerForField("COLOR")), EMPTY_VALUE);
            bw.addMutation(m);
        }

        try (BatchWriter bw = client.createBatchWriter(TableName.METADATA)) {
            Mutation m = new Mutation("num_shards");
            m.put("ns", startDay + "_" + getNumShards(), new Value());
            m.put("ns", NEW_SHARDS_START + "_" + getNewShards(), new Value());
            bw.addMutation(m);
        }
    }

    private static String uidForEvent(String row, String field, String value) {
        String data = row + field + value;
        return UID.builder().newId(data.getBytes(), (Date) null).toString();
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

    private static Value createValue(long count) {
        return new Value(encoder.encode(count));
    }
}
