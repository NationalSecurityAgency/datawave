package datawave.query.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.javatuples.Pair;

import datawave.data.ColumnFamilyConstants;
import datawave.data.hash.UID;
import datawave.data.type.BaseType;
import datawave.data.type.DateType;
import datawave.data.type.GeometryType;
import datawave.data.type.IpAddressType;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NumberType;
import datawave.data.type.OneToManyNormalizerType;
import datawave.data.type.util.Geometry;
import datawave.ingest.protobuf.Uid;
import datawave.query.QueryTestTableHelper;
import datawave.util.TableName;

/**
 * Test data that contains field index holes.
 */
public class IndexFieldHoleDataIngest {

    public enum Range {
        SHARD {
            public Value getValue(String... uids) {
                Uid.List.Builder builder = Uid.List.newBuilder();
                builder.setCOUNT(50); // Must not be zero.
                builder.setIGNORE(true); // Must be true.
                return new Value(builder.build().toByteArray());
            }
        },
        DOCUMENT {
            public Value getValue(String... uids) {
                Uid.List.Builder builder = Uid.List.newBuilder();
                for (String s : uids) {
                    builder.addUID(s);
                }
                builder.setCOUNT(uids.length);
                builder.setIGNORE(false);
                return new Value(builder.build().toByteArray());
            }
        };

        public abstract Value getValue(String... uids);
    }

    private static final LcNoDiacriticsType lcNoDiacriticsType = new LcNoDiacriticsType();
    private static final IpAddressType ipAddressType = new IpAddressType();
    private static final NumberType numberType = new NumberType();
    private static final DateType dateType = new DateType();
    private static final GeometryType geoType = new GeometryType();

    protected static final String datatype = "test";
    protected static final ColumnVisibility columnVisibility = new ColumnVisibility("ALL");
    protected static final Value emptyValue = new Value(new byte[0]);
    public static final String corleoneUID = UID.builder().newId("Corleone".getBytes(), (Date) null).toString();
    public static final String corleoneChildUID = UID.builder().newId("Corleone".getBytes(), (Date) null, "1").toString();
    public static final String sopranoUID = UID.builder().newId("Soprano".getBytes(), (Date) null).toString();
    public static final String caponeUID = UID.builder().newId("Capone".getBytes(), (Date) null).toString();

    public static final Map<String,Pair<Long,Long>> DEFAULT_COUNTS = new HashMap<>();
    public static final Set<String> EVENT_FIELDS = new HashSet<>();

    static {
        DEFAULT_COUNTS.put("NAME", Pair.with(6L, 6L));
        DEFAULT_COUNTS.put("NOME", Pair.with(6L, 6L));
        DEFAULT_COUNTS.put("GENDER", Pair.with(6L, 6L));
        DEFAULT_COUNTS.put("GENERE", Pair.with(6L, 6L));
        DEFAULT_COUNTS.put("AGE", Pair.with(6L, 6L));
        DEFAULT_COUNTS.put("ETA", Pair.with(7L, 7L));
        DEFAULT_COUNTS.put("GEO", Pair.with(3L, 3L));
        DEFAULT_COUNTS.put("UUID", Pair.with(4L, 4L));
        DEFAULT_COUNTS.put("LOCATION", Pair.with(0L, 2L));
        DEFAULT_COUNTS.put("POSIZIONE", Pair.with(0L, 1L));
        DEFAULT_COUNTS.put("SENTENCE", Pair.with(0L, 1L));
        DEFAULT_COUNTS.put("MAGIC", Pair.with(3L, 0L));
        DEFAULT_COUNTS.put("NUMBER", Pair.with(2L, 0L));
        DEFAULT_COUNTS.put("BIRTH_DATE", Pair.with(5L, 0L));
        DEFAULT_COUNTS.put("DEATH_DATE", Pair.with(4L, 0L));
        DEFAULT_COUNTS.put("NULL1", Pair.with(1L, 1L));
        DEFAULT_COUNTS.put("NULL2", Pair.with(1L, 1L));
        DEFAULT_COUNTS.put("QUOTE", Pair.with(2L, 2L));

        EVENT_FIELDS.add("NAME");
        EVENT_FIELDS.add("NOME");
        EVENT_FIELDS.add("GENDER");
        EVENT_FIELDS.add("GENERE");
        EVENT_FIELDS.add("AGE");
        EVENT_FIELDS.add("ETA");
        EVENT_FIELDS.add("GEO");
        EVENT_FIELDS.add("MAGIC");
        EVENT_FIELDS.add("NUMBER");
        EVENT_FIELDS.add("ETA");
        EVENT_FIELDS.add("UUID");
        EVENT_FIELDS.add("BIRTH_DATE");
        EVENT_FIELDS.add("DEATH_DATE");
        EVENT_FIELDS.add("NULL1");
        EVENT_FIELDS.add("NULL2");
        EVENT_FIELDS.add("QUOTE");
    }

    public static class EventConfig {
        private String date;
        private long time;
        private final Map<String,Pair<Long,Long>> metadataCounts = new HashMap<>(DEFAULT_COUNTS);

        public static EventConfig forDate(String date) {
            return new EventConfig().withDate(date);
        }

        public EventConfig withDate(String date) {
            this.date = date;
            try {
                this.time = new SimpleDateFormat("yyyyMMdd").parse(date).getTime();
            } catch (ParseException e) {
                throw new RuntimeException("Expected yyyyMMdd format for " + date, e);
            }
            return this;
        }

        public EventConfig withMetadataCount(String field, long frequencyCount, long indexCount) {
            this.metadataCounts.put(field, Pair.with(frequencyCount, indexCount));
            return this;
        }

        public String getDate() {
            return date;
        }

        public long getTime() {
            return time;
        }

        public boolean hasCountsForField(String field) {
            return metadataCounts.containsKey(field);
        }

        public Pair<Long,Long> getMetadataCounts(String field) {
            return metadataCounts.get(field);
        }

        public Map<String,Pair<Long,Long>> getMetadataCounts() {
            return metadataCounts;
        }
    }

    public static void writeItAll(AccumuloClient client, Range range, List<EventConfig> eventConfigs) throws Exception {
        BatchWriterConfig bwConfig = new BatchWriterConfig().setMaxMemory(1000L).setMaxLatency(1, TimeUnit.SECONDS).setMaxWriteThreads(1);
        writeShardTable(client, eventConfigs, bwConfig);
        writeShardIndexTable(client, range, eventConfigs, bwConfig);
        writeReverseIndexTable(client, range, eventConfigs, bwConfig);
        writeShardFieldIndexes(client, eventConfigs, bwConfig);
        writeMetadataTable(client, eventConfigs, bwConfig);
        writeForwardModel(client, bwConfig);
        writeReverseModel(client, bwConfig);
    }

    private static void writeShardTable(AccumuloClient client, List<EventConfig> eventConfigs, BatchWriterConfig bwConfig)
                    throws TableNotFoundException, MutationsRejectedException {
        try (BatchWriter bw = client.createBatchWriter(TableName.SHARD, bwConfig)) {
            for (EventConfig config : eventConfigs) {
                // Create a shard for the date.
                String shard = config.getDate() + "_0";
                long timeStamp = config.getTime();

                Mutation mutation = new Mutation(shard);

                // Add the shard mutations.
                mutation.put(datatype + "\u0000" + corleoneUID, "NOME.0" + "\u0000" + "SANTINO", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + corleoneUID, "NOME.1" + "\u0000" + "FREDO", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + corleoneUID, "NOME.2" + "\u0000" + "MICHAEL", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + corleoneUID, "NOME.3" + "\u0000" + "CONSTANZIA", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + corleoneUID, "NOME.4" + "\u0000" + "LUCA", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + corleoneUID, "NOME.5" + "\u0000" + "VINCENT", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + corleoneUID, "GENERE.0" + "\u0000" + "MALE", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + corleoneUID, "GENERE.1" + "\u0000" + "MALE", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + corleoneUID, "GENERE.2" + "\u0000" + "MALE", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + corleoneUID, "GENERE.3" + "\u0000" + "MALE", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + corleoneUID, "GENERE.4" + "\u0000" + "MALE", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + corleoneUID, "GENERE.5" + "\u0000" + "MALE", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + corleoneUID, "ETA.0" + "\u0000" + "24", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + corleoneUID, "ETA.1" + "\u0000" + "22", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + corleoneUID, "ETA.2" + "\u0000" + "20", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + corleoneUID, "ETA.3" + "\u0000" + "18", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + corleoneUID, "ETA.4" + "\u0000" + "40", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + corleoneUID, "ETA.5" + "\u0000" + "22", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + corleoneUID, "MAGIC.0" + "\u0000" + "18", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + corleoneUID, "UUID.0" + "\u0000" + "CORLEONE", columnVisibility, timeStamp, emptyValue);
                // CORLEONE date delta is 70 years
                mutation.put(datatype + "\u0000" + corleoneUID, "BIRTH_DATE" + "\u0000" + "1930-12-28T00:00:05.000Z", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + corleoneUID, "DEATH_DATE" + "\u0000" + "2000-12-28T00:00:05.000Z", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + corleoneUID, "QUOTE" + "\u0000" + "Im gonna make him an offer he cant refuse", columnVisibility, timeStamp,
                                emptyValue);
                mutation.put(datatype + "\u0000" + corleoneUID, "NUMBER" + "\u0000" + "25", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + corleoneUID, "GEO" + "\u0000" + "POINT(10 10)", columnVisibility, timeStamp, emptyValue);

                mutation.put(datatype + "\u0000" + corleoneChildUID, "UUID.0" + "\u0000" + "ANDOLINI", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + corleoneChildUID, "ETA.0" + "\u0000" + "12", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + corleoneChildUID, "BIRTH_DATE" + "\u0000" + "1930-12-28T00:00:05.000Z", columnVisibility, timeStamp,
                                emptyValue);
                mutation.put(datatype + "\u0000" + corleoneChildUID, "DEATH_DATE" + "\u0000" + "2000-12-28T00:00:05.000Z", columnVisibility, timeStamp,
                                emptyValue);

                mutation.put(datatype + "\u0000" + sopranoUID, "NAME.0" + "\u0000" + "ANTHONY", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + sopranoUID, "NAME.1" + "\u0000" + "MEADOW", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + sopranoUID, "GENDER.0" + "\u0000" + "MALE", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + sopranoUID, "GENDER.1" + "\u0000" + "MALE", columnVisibility, timeStamp, emptyValue);
                // to test whether singleton values correctly get matched using the function set methods, only add AGE.1
                // mutation.put(datatype + "\u0000" + sopranoUID, "AGE.0" + "\u0000" + "16", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + sopranoUID, "AGE.0" + "\u0000" + "16", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + sopranoUID, "AGE.1" + "\u0000" + "18", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + sopranoUID, "MAGIC.0" + "\u0000" + "18", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + sopranoUID, "UUID.0" + "\u0000" + "SOPRANO", columnVisibility, timeStamp, emptyValue);
                // soprano date delta is 50 years
                mutation.put(datatype + "\u0000" + sopranoUID, "BIRTH_DATE" + "\u0000" + "1950-12-28T00:00:05.000Z", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + sopranoUID, "DEATH_DATE" + "\u0000" + "2000-12-28T00:00:05.000Z", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + sopranoUID, "QUOTE" + "\u0000" + "If you can quote the rules then you can obey them", columnVisibility,
                                timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + sopranoUID, "GEO" + "\u0000" + "POINT(20 20)", columnVisibility, timeStamp, emptyValue);

                mutation.put(datatype + "\u0000" + caponeUID, "NAME.0" + "\u0000" + "ALPHONSE", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + caponeUID, "NAME.1" + "\u0000" + "FRANK", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + caponeUID, "NAME.2" + "\u0000" + "RALPH", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + caponeUID, "NAME.3" + "\u0000" + "MICHAEL", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + caponeUID, "GENDER.0" + "\u0000" + "MALE", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + caponeUID, "GENDER.1" + "\u0000" + "MALE", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + caponeUID, "GENDER.2" + "\u0000" + "MALE", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + caponeUID, "GENDER.3" + "\u0000" + "MALE", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + caponeUID, "AGE.0" + "\u0000" + "30", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + caponeUID, "AGE.1" + "\u0000" + "34", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + caponeUID, "AGE.2" + "\u0000" + "20", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + caponeUID, "AGE.3" + "\u0000" + "40", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + caponeUID, "MAGIC.0" + "\u0000" + "18", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + caponeUID, "UUID.0" + "\u0000" + "CAPONE", columnVisibility, timeStamp, emptyValue);

                // capone date delta is 89 or 90 years
                mutation.put(datatype + "\u0000" + caponeUID, "BIRTH_DATE.0" + "\u0000" + "1910-12-28T00:00:05.000Z", columnVisibility, timeStamp, emptyValue);
                // add a second date to test function taking an Iterable
                mutation.put(datatype + "\u0000" + caponeUID, "BIRTH_DATE.1" + "\u0000" + "1911-12-28T00:00:05.000Z", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + caponeUID, "DEATH_DATE.0" + "\u0000" + "2000-12-28T00:00:05.000Z", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + caponeUID,
                                "QUOTE" + "\u0000" + "You can get much farther with a kind word and a gun than you can with a kind word alone",
                                columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + caponeUID, "NUMBER" + "\u0000" + "25", columnVisibility, timeStamp, emptyValue);
                mutation.put(datatype + "\u0000" + caponeUID, "GEO" + "\u0000" + "POINT(30 30)", columnVisibility, timeStamp, emptyValue);

                bw.addMutation(mutation);
            }
        }
    }

    private static void writeShardIndexEntry(BatchWriter bw, Map<String,AtomicLong> indexCounts, String field, String value, boolean normalize, String shard,
                    long ts, Value v) throws MutationsRejectedException {
        writeShardIndexEntry(bw, indexCounts, field, value, normalize, 1, shard, ts, v);
    }

    private static void writeShardIndexEntry(BatchWriter bw, Map<String,AtomicLong> indexCounts, String field, String value, boolean normalize, int numEntries,
                    String shard, long ts, Value v) throws MutationsRejectedException {
        writeGlobalIndexEntry(bw, indexCounts, field, value, normalize, false, numEntries, shard, ts, v);
    }

    private static void writeShardReverseIndexEntry(BatchWriter bw, Map<String,AtomicLong> indexCounts, String field, String value, boolean normalize,
                    String shard, long ts, Value v) throws MutationsRejectedException {
        writeShardReverseIndexEntry(bw, indexCounts, field, value, normalize, 1, shard, ts, v);
    }

    private static void writeShardReverseIndexEntry(BatchWriter bw, Map<String,AtomicLong> indexCounts, String field, String value, boolean normalize,
                    int numEntries, String shard, long ts, Value v) throws MutationsRejectedException {
        writeGlobalIndexEntry(bw, indexCounts, field, value, normalize, true, numEntries, shard, ts, v);
    }

    /**
     * Write a shard index or reverse index entry
     *
     * @param bw
     *            The batch write
     * @param indexCounts
     *            The index counts permitted per field
     * @param field
     *            The field
     * @param value
     *            The value
     * @param normalize
     *            Should we normalize the value
     * @param reverse
     *            Should we reverse the value for the reverse index
     * @param numEntries
     *            The number of entries this represents (may be multiple in the event like GENERE.0, GENERE.1, ...)
     * @param shard
     *            The shard
     * @param ts
     *            The timestamp
     * @param v
     *            The value (UID.List)
     * @throws MutationsRejectedException
     */
    private static void writeGlobalIndexEntry(BatchWriter bw, Map<String,AtomicLong> indexCounts, String field, String value, boolean normalize,
                    boolean reverse, int numEntries, String shard, long ts, Value v) throws MutationsRejectedException {
        String normalizedValue = normalize ? normalizerForColumn(field).normalize(value) : value;
        if (reverse) {
            normalizedValue = new StringBuilder(normalizedValue).reverse().toString();
        }
        Mutation mutation = new Mutation(normalizedValue);
        mutation.put(field.toUpperCase(), shard + "\u0000" + datatype, columnVisibility, ts, v);
        AtomicLong count = indexCounts.get(field);
        if (count.get() > 0) {
            count.addAndGet(0 - numEntries);
            bw.addMutation(mutation);
        } else {
            System.out.println("Dropping global index mutation " + new Text(mutation.getRow()) + ' ' + new Text(mutation.getUpdates().get(0).getColumnFamily())
                            + ':' + new Text(mutation.getUpdates().get(0).getColumnQualifier()));
        }
    }

    private static void writeShardIndexTable(AccumuloClient client, Range range, List<EventConfig> eventConfigs, BatchWriterConfig bwConfig)
                    throws TableNotFoundException, MutationsRejectedException {
        try (BatchWriter bw = client.createBatchWriter(TableName.SHARD_INDEX, bwConfig)) {
            for (EventConfig config : eventConfigs) {
                // Create a shard for the date.
                String shard = config.getDate() + "_0";
                long timeStamp = config.getTime();

                Map<String,AtomicLong> indexCounts = config.getMetadataCounts().entrySet().stream()
                                .collect(Collectors.toMap(e -> e.getKey(), e -> new AtomicLong(e.getValue().getValue1())));

                // corleones
                // uuid
                writeShardIndexEntry(bw, indexCounts, "UUID", "CORLEONE", true, shard, timeStamp, range.getValue(corleoneUID));
                writeShardIndexEntry(bw, indexCounts, "UUID", "ANDOLINI", true, shard, timeStamp, range.getValue(corleoneChildUID));
                // names
                writeShardIndexEntry(bw, indexCounts, "NOME", "SANTINO", true, shard, timeStamp, range.getValue(corleoneUID));
                writeShardIndexEntry(bw, indexCounts, "NOME", "FREDO", true, shard, timeStamp, range.getValue(corleoneUID));
                writeShardIndexEntry(bw, indexCounts, "NOME", "MICHAEL", true, shard, timeStamp, range.getValue(corleoneUID));
                writeShardIndexEntry(bw, indexCounts, "NOME", "CONSTANZIA", true, shard, timeStamp, range.getValue(corleoneUID));
                writeShardIndexEntry(bw, indexCounts, "NOME", "LUCA", true, shard, timeStamp, range.getValue(corleoneUID));
                writeShardIndexEntry(bw, indexCounts, "NOME", "VINCENT", true, shard, timeStamp, range.getValue(corleoneUID));
                // genders
                writeShardIndexEntry(bw, indexCounts, "GENERE", "MALE", true, 6, shard, timeStamp, range.getValue(corleoneUID));
                // ages
                writeShardIndexEntry(bw, indexCounts, "ETA", "12", true, shard, timeStamp, range.getValue(corleoneChildUID));
                writeShardIndexEntry(bw, indexCounts, "ETA", "18", true, shard, timeStamp, range.getValue(corleoneUID));
                writeShardIndexEntry(bw, indexCounts, "ETA", "20", true, shard, timeStamp, range.getValue(corleoneUID));
                writeShardIndexEntry(bw, indexCounts, "ETA", "22", true, shard, timeStamp, range.getValue(corleoneUID));
                writeShardIndexEntry(bw, indexCounts, "ETA", "24", true, shard, timeStamp, range.getValue(corleoneUID));
                writeShardIndexEntry(bw, indexCounts, "ETA", "40", true, shard, timeStamp, range.getValue(corleoneUID));
                // geo
                for (String normalized : ((OneToManyNormalizerType<Geometry>) geoType).normalizeToMany("POINT(10 10)")) {
                    writeShardIndexEntry(bw, indexCounts, "GEO", normalized, false, shard, timeStamp, range.getValue(corleoneUID));
                }
                // add some index-only fields
                writeShardIndexEntry(bw, indexCounts, "POSIZIONE", "newyork", false, shard, timeStamp, range.getValue(corleoneUID));
                // add some tokens
                addTokens(bw, indexCounts, shard, timeStamp, range, "QUOTE", "Im gonna make him an offer he cant refuse", corleoneUID);

                // sopranos
                // uuid
                writeShardIndexEntry(bw, indexCounts, "UUID", "SOPRANO", true, shard, timeStamp, range.getValue(sopranoUID));
                // names
                writeShardIndexEntry(bw, indexCounts, "NAME", "ANTHONY", true, shard, timeStamp, range.getValue(sopranoUID));
                writeShardIndexEntry(bw, indexCounts, "NAME", "MEADOW", true, shard, timeStamp, range.getValue(sopranoUID));
                // genders
                writeShardIndexEntry(bw, indexCounts, "GENDER", "MALE", true, 6, shard, timeStamp, range.getValue(sopranoUID, caponeUID));
                // ages
                writeShardIndexEntry(bw, indexCounts, "AGE", "16", true, shard, timeStamp, range.getValue(sopranoUID));
                writeShardIndexEntry(bw, indexCounts, "AGE", "18", true, shard, timeStamp, range.getValue(sopranoUID));
                // geo
                for (String normalized : ((OneToManyNormalizerType<Geometry>) geoType).normalizeToMany("POINT(20 20)")) {
                    writeShardIndexEntry(bw, indexCounts, "GEO", normalized, false, shard, timeStamp, range.getValue(sopranoUID));
                }
                // add some index-only fields
                writeShardIndexEntry(bw, indexCounts, "LOCATION", "newjersey", false, shard, timeStamp, range.getValue(sopranoUID));
                // add some tokens
                addTokens(bw, indexCounts, shard, timeStamp, range, "QUOTE", "If you can quote the rules then you can obey them", sopranoUID);

                // capones
                // uuid
                writeShardIndexEntry(bw, indexCounts, "UUID", "CAPONE", true, shard, timeStamp, range.getValue(caponeUID));
                // names
                writeShardIndexEntry(bw, indexCounts, "NAME", "ALPHONSE", true, shard, timeStamp, range.getValue(caponeUID));
                writeShardIndexEntry(bw, indexCounts, "NAME", "FRANK", true, shard, timeStamp, range.getValue(caponeUID));
                writeShardIndexEntry(bw, indexCounts, "NAME", "RALPH", true, shard, timeStamp, range.getValue(caponeUID));
                writeShardIndexEntry(bw, indexCounts, "NAME", "MICHAEL", true, shard, timeStamp, range.getValue(caponeUID));
                // genders
                // see above: writeShardIndexEntry(bw, indexCounts, "GENDER", "MALE", true, 6, shard, timeStamp, range.getValue(sopranoUID, caponeUID));
                // ages
                writeShardIndexEntry(bw, indexCounts, "AGE", "20", true, shard, timeStamp, range.getValue(caponeUID));
                writeShardIndexEntry(bw, indexCounts, "AGE", "30", true, shard, timeStamp, range.getValue(caponeUID));
                writeShardIndexEntry(bw, indexCounts, "AGE", "34", true, shard, timeStamp, range.getValue(caponeUID));
                writeShardIndexEntry(bw, indexCounts, "AGE", "40", true, shard, timeStamp, range.getValue(caponeUID));
                // geo
                for (String normalized : ((OneToManyNormalizerType<Geometry>) geoType).normalizeToMany("POINT(30 30)")) {
                    writeShardIndexEntry(bw, indexCounts, "GEO", normalized, false, shard, timeStamp, range.getValue(caponeUID));
                }
                // add some index-only fields
                writeShardIndexEntry(bw, indexCounts, "LOCATION", "chicago", false, shard, timeStamp, range.getValue(caponeUID));
                writeShardIndexEntry(bw, indexCounts, "SENTENCE", "11y", false, shard, timeStamp, range.getValue(caponeUID));
                // add some tokens
                addTokens(bw, indexCounts, shard, timeStamp, range, "QUOTE",
                                "You can get much farther with a kind word and a gun than you can with a kind word alone", caponeUID);
            }
        }
    }

    private static void writeReverseIndexTable(AccumuloClient client, Range range, List<EventConfig> eventConfigs, BatchWriterConfig bwConfig)
                    throws TableNotFoundException, MutationsRejectedException {
        try (BatchWriter bw = client.createBatchWriter(TableName.SHARD_RINDEX, bwConfig)) {
            for (EventConfig config : eventConfigs) {
                // Create a shard for the date.
                String shard = config.getDate() + "_0";
                long timeStamp = config.getTime();

                Map<String,AtomicLong> indexCounts = config.getMetadataCounts().entrySet().stream()
                                .collect(Collectors.toMap(e -> e.getKey(), e -> new AtomicLong(e.getValue().getValue1())));

                // corleones
                // uuid
                writeShardReverseIndexEntry(bw, indexCounts, "UUID", "CORLEONE", true, shard, timeStamp, range.getValue(corleoneUID));
                writeShardReverseIndexEntry(bw, indexCounts, "UUID", "ANDOLINI", true, shard, timeStamp, range.getValue(corleoneChildUID));
                // names
                writeShardReverseIndexEntry(bw, indexCounts, "NOME", "SANTINO", true, shard, timeStamp, range.getValue(corleoneUID));
                writeShardReverseIndexEntry(bw, indexCounts, "NOME", "FREDO", true, shard, timeStamp, range.getValue(corleoneUID));
                writeShardReverseIndexEntry(bw, indexCounts, "NOME", "MICHAEL", true, shard, timeStamp, range.getValue(corleoneUID));
                writeShardReverseIndexEntry(bw, indexCounts, "NOME", "CONSTANZIA", true, shard, timeStamp, range.getValue(corleoneUID));
                writeShardReverseIndexEntry(bw, indexCounts, "NOME", "LUCA", true, shard, timeStamp, range.getValue(corleoneUID));
                writeShardReverseIndexEntry(bw, indexCounts, "NOME", "VINCENT", true, shard, timeStamp, range.getValue(corleoneUID));
                // genders
                writeShardReverseIndexEntry(bw, indexCounts, "GENERE", "MALE", true, 6, shard, timeStamp, range.getValue(corleoneUID));
                // ages
                writeShardReverseIndexEntry(bw, indexCounts, "ETA", "12", true, shard, timeStamp, range.getValue(corleoneChildUID));
                writeShardReverseIndexEntry(bw, indexCounts, "ETA", "18", true, shard, timeStamp, range.getValue(corleoneUID));
                writeShardReverseIndexEntry(bw, indexCounts, "ETA", "20", true, shard, timeStamp, range.getValue(corleoneUID));
                writeShardReverseIndexEntry(bw, indexCounts, "ETA", "22", true, shard, timeStamp, range.getValue(corleoneUID));
                writeShardReverseIndexEntry(bw, indexCounts, "ETA", "24", true, shard, timeStamp, range.getValue(corleoneUID));
                writeShardReverseIndexEntry(bw, indexCounts, "ETA", "40", true, shard, timeStamp, range.getValue(corleoneUID));
                // add some index-only fields
                writeShardReverseIndexEntry(bw, indexCounts, "POSIZIONE", "newyork", false, shard, timeStamp, range.getValue(corleoneUID));

                // sopranos
                // uuid
                writeShardReverseIndexEntry(bw, indexCounts, "UUID", "SOPRANO", true, shard, timeStamp, range.getValue(sopranoUID));
                // names
                writeShardReverseIndexEntry(bw, indexCounts, "NAME", "ANTHONY", true, shard, timeStamp, range.getValue(sopranoUID));
                writeShardReverseIndexEntry(bw, indexCounts, "NAME", "MEADOW", true, shard, timeStamp, range.getValue(sopranoUID));
                // genders
                writeShardReverseIndexEntry(bw, indexCounts, "GENDER", "MALE", true, 6, shard, timeStamp, range.getValue(sopranoUID, caponeUID));
                // ages
                writeShardReverseIndexEntry(bw, indexCounts, "AGE", "16", true, shard, timeStamp, range.getValue(sopranoUID));
                writeShardReverseIndexEntry(bw, indexCounts, "AGE", "18", true, shard, timeStamp, range.getValue(sopranoUID));
                // add some index-only fields
                writeShardReverseIndexEntry(bw, indexCounts, "LOCATION", "newjersey", false, shard, timeStamp, range.getValue(sopranoUID));

                // capones
                // uuid
                writeShardReverseIndexEntry(bw, indexCounts, "UUID", "CAPONE", true, shard, timeStamp, range.getValue(caponeUID));
                // names
                writeShardReverseIndexEntry(bw, indexCounts, "NAME", "ALPHONSE", true, shard, timeStamp, range.getValue(caponeUID));
                writeShardReverseIndexEntry(bw, indexCounts, "NAME", "FRANK", true, shard, timeStamp, range.getValue(caponeUID));
                writeShardReverseIndexEntry(bw, indexCounts, "NAME", "RALPH", true, shard, timeStamp, range.getValue(caponeUID));
                writeShardReverseIndexEntry(bw, indexCounts, "NAME", "MICHAEL", true, shard, timeStamp, range.getValue(caponeUID));
                // genders
                // see above: writeReverseShardIndexEntry(bw, indexCounts, "GENDER", "MALE", true, 6, shard, timeStamp, range.getValue(sopranoUID, caponeUID));
                // ages
                writeShardReverseIndexEntry(bw, indexCounts, "AGE", "20", true, shard, timeStamp, range.getValue(caponeUID));
                writeShardReverseIndexEntry(bw, indexCounts, "AGE", "30", true, shard, timeStamp, range.getValue(caponeUID));
                writeShardReverseIndexEntry(bw, indexCounts, "AGE", "34", true, shard, timeStamp, range.getValue(caponeUID));
                writeShardReverseIndexEntry(bw, indexCounts, "AGE", "40", true, shard, timeStamp, range.getValue(caponeUID));
                // add some index-only fields
                writeShardReverseIndexEntry(bw, indexCounts, "LOCATION", "chicago", false, shard, timeStamp, range.getValue(caponeUID));
            }
        }
    }

    private static void writeFieldIndexEntry(BatchWriter bw, Map<String,AtomicLong> indexCounts, String field, String value, boolean normalize, String shard,
                    String uid, long ts) throws MutationsRejectedException {
        writeFieldIndexEntry(bw, indexCounts, field, value, normalize, 1, shard, uid, ts);
    }

    private static void writeFieldIndexEntry(BatchWriter bw, Map<String,AtomicLong> indexCounts, String field, String value, boolean normalize, int numEntries,
                    String shard, String uid, long ts) throws MutationsRejectedException {
        String normalizedValue = normalize ? normalizerForColumn(field).normalize(value) : value;
        Mutation mutation = new Mutation(shard);
        mutation.put("fi\u0000" + field.toUpperCase(), normalizedValue + "\u0000" + datatype + "\u0000" + uid, columnVisibility, ts, emptyValue);
        AtomicLong count = indexCounts.get(field);
        if (count.get() > 0) {
            count.addAndGet(0 - numEntries);
            bw.addMutation(mutation);
        } else {
            System.out.println("Dropping field index mutation " + new Text(mutation.getRow()) + ' ' + new Text(mutation.getUpdates().get(0).getColumnFamily())
                            + ':' + new Text(mutation.getUpdates().get(0).getColumnQualifier()));
        }
    }

    private static void writeShardFieldIndexes(AccumuloClient client, List<EventConfig> eventConfigs, BatchWriterConfig bwConfig)
                    throws TableNotFoundException, MutationsRejectedException {
        try (BatchWriter bw = client.createBatchWriter(TableName.SHARD, bwConfig)) {
            for (EventConfig config : eventConfigs) {
                // Create a shard for the date.
                String shard = config.getDate() + "_0";
                long timeStamp = config.getTime();
                Mutation mutation = new Mutation(shard);

                Map<String,AtomicLong> indexCounts = config.getMetadataCounts().entrySet().stream()
                                .collect(Collectors.toMap(e -> e.getKey(), e -> new AtomicLong(e.getValue().getValue1())));

                // corleones
                // uuid
                writeFieldIndexEntry(bw, indexCounts, "UUID", "CORLEONE", true, shard, corleoneUID, timeStamp);
                writeFieldIndexEntry(bw, indexCounts, "UUID", "ANDOLINI", true, shard, corleoneChildUID, timeStamp);
                // names
                writeFieldIndexEntry(bw, indexCounts, "NOME", "SANTINO", true, shard, corleoneUID, timeStamp);
                writeFieldIndexEntry(bw, indexCounts, "NOME", "FREDO", true, shard, corleoneUID, timeStamp);
                writeFieldIndexEntry(bw, indexCounts, "NOME", "MICHAEL", true, shard, corleoneUID, timeStamp);
                writeFieldIndexEntry(bw, indexCounts, "NOME", "CONSTANZIA", true, shard, corleoneUID, timeStamp);
                writeFieldIndexEntry(bw, indexCounts, "NOME", "LUCA", true, shard, corleoneUID, timeStamp);
                writeFieldIndexEntry(bw, indexCounts, "NOME", "VINCENT", true, shard, corleoneUID, timeStamp);
                // genders
                writeFieldIndexEntry(bw, indexCounts, "GENERE", "MALE", true, 6, shard, corleoneUID, timeStamp);
                // ages
                writeFieldIndexEntry(bw, indexCounts, "ETA", "24", true, shard, corleoneUID, timeStamp);
                writeFieldIndexEntry(bw, indexCounts, "ETA", "22", true, shard, corleoneUID, timeStamp);
                writeFieldIndexEntry(bw, indexCounts, "ETA", "20", true, shard, corleoneUID, timeStamp);
                writeFieldIndexEntry(bw, indexCounts, "ETA", "18", true, shard, corleoneUID, timeStamp);
                writeFieldIndexEntry(bw, indexCounts, "ETA", "40", true, shard, corleoneUID, timeStamp);
                writeFieldIndexEntry(bw, indexCounts, "ETA", "12", true, shard, corleoneChildUID, timeStamp);
                // geo
                for (String normalized : ((OneToManyNormalizerType<Geometry>) geoType).normalizeToMany("POINT(10 10)")) {
                    writeFieldIndexEntry(bw, indexCounts, "GEO", normalized, false, shard, corleoneUID, timeStamp);
                }
                // add some index-only fields
                writeFieldIndexEntry(bw, indexCounts, "POSIZIONE", "newyork", true, shard, corleoneUID, timeStamp);
                addFiTfTokens(bw, indexCounts, shard, timeStamp, "QUOTE", "Im gonna make him an offer he cant refuse", corleoneUID);

                // sopranos
                // uuid
                writeFieldIndexEntry(bw, indexCounts, "UUID", "SOPRANO", true, shard, sopranoUID, timeStamp);
                // names
                writeFieldIndexEntry(bw, indexCounts, "NAME", "ANTHONY", true, shard, sopranoUID, timeStamp);
                writeFieldIndexEntry(bw, indexCounts, "NAME", "MEADOW", true, shard, sopranoUID, timeStamp);
                // genders
                writeFieldIndexEntry(bw, indexCounts, "GENDER", "MALE", true, 2, shard, sopranoUID, timeStamp);
                // ages
                writeFieldIndexEntry(bw, indexCounts, "AGE", "16", true, shard, sopranoUID, timeStamp);
                writeFieldIndexEntry(bw, indexCounts, "AGE", "18", true, shard, sopranoUID, timeStamp);
                // geo
                for (String normalized : ((OneToManyNormalizerType<Geometry>) geoType).normalizeToMany("POINT(20 20)")) {
                    writeFieldIndexEntry(bw, indexCounts, "GEO", normalized, false, shard, corleoneUID, timeStamp);
                }
                // add some index-only fields
                writeFieldIndexEntry(bw, indexCounts, "LOCATION", "newjersey", true, shard, sopranoUID, timeStamp);
                addFiTfTokens(bw, indexCounts, shard, timeStamp, "QUOTE", "If you can quote the rules then you can obey them", sopranoUID);

                // capones
                // uuid
                writeFieldIndexEntry(bw, indexCounts, "UUID", "CAPONE", true, shard, caponeUID, timeStamp);
                // names
                writeFieldIndexEntry(bw, indexCounts, "NAME", "ALPHONSE", true, shard, caponeUID, timeStamp);
                writeFieldIndexEntry(bw, indexCounts, "NAME", "FRANK", true, shard, caponeUID, timeStamp);
                writeFieldIndexEntry(bw, indexCounts, "NAME", "RALPH", true, shard, caponeUID, timeStamp);
                writeFieldIndexEntry(bw, indexCounts, "NAME", "MICHAEL", true, shard, caponeUID, timeStamp);
                // genders
                writeFieldIndexEntry(bw, indexCounts, "GENDER", "MALE", true, 4, shard, caponeUID, timeStamp);
                // ages
                writeFieldIndexEntry(bw, indexCounts, "AGE", "30", true, shard, caponeUID, timeStamp);
                writeFieldIndexEntry(bw, indexCounts, "AGE", "34", true, shard, caponeUID, timeStamp);
                writeFieldIndexEntry(bw, indexCounts, "AGE", "20", true, shard, caponeUID, timeStamp);
                writeFieldIndexEntry(bw, indexCounts, "AGE", "40", true, shard, caponeUID, timeStamp);
                // geo
                for (String normalized : ((OneToManyNormalizerType<Geometry>) geoType).normalizeToMany("POINT(30 30)")) {
                    writeFieldIndexEntry(bw, indexCounts, "GEO", normalized, false, shard, corleoneUID, timeStamp);
                }
                // add some index-only fields
                writeFieldIndexEntry(bw, indexCounts, "LOCATION", "chicago", true, shard, caponeUID, timeStamp);
                writeFieldIndexEntry(bw, indexCounts, "SENTENCE", "11y", true, shard, caponeUID, timeStamp);
                addFiTfTokens(bw, indexCounts, shard, timeStamp, "QUOTE",
                                "You can get much farther with a kind word and a gun than you can with a kind word alone", caponeUID);
            }
        }
    }

    private static Mutation getMetadataTableMutation(Map.Entry<String,Pair<Long,Long>> countEntry, String date) {
        Mutation mutation = new Mutation(countEntry.getKey());
        Pair<Long,Long> counts = countEntry.getValue();
        if (eventField(countEntry.getKey())) {
            mutation.put(ColumnFamilyConstants.COLF_E, new Text(datatype), emptyValue);
        }
        mutation.put(ColumnFamilyConstants.COLF_F, new Text(datatype + "\u0000" + date), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(counts.getValue0())));
        if (counts.getValue1() > 0) {
            mutation.put(ColumnFamilyConstants.COLF_I, new Text(datatype + "\u0000" + date),
                            new Value(SummingCombiner.VAR_LEN_ENCODER.encode(counts.getValue1())));
            mutation.put(ColumnFamilyConstants.COLF_RI, new Text(datatype + "\u0000" + date),
                            new Value(SummingCombiner.VAR_LEN_ENCODER.encode(counts.getValue1())));
        }
        mutation.put(ColumnFamilyConstants.COLF_T, new Text(datatype + "\u0000" + normalizerForColumn(countEntry.getKey()).getClass().getName()), emptyValue);
        return mutation;
    }

    private static void writeMetadataTable(AccumuloClient client, List<EventConfig> eventConfigs, BatchWriterConfig bwConfig)
                    throws TableNotFoundException, MutationsRejectedException {
        try (BatchWriter bw = client.createBatchWriter(QueryTestTableHelper.MODEL_TABLE_NAME, bwConfig)) {
            for (EventConfig config : eventConfigs) {
                String date = config.getDate();

                for (Map.Entry<String,Pair<Long,Long>> fieldCounts : config.getMetadataCounts().entrySet()) {
                    Mutation mutation = getMetadataTableMutation(fieldCounts, date);
                    if (fieldCounts.getKey().equals("QUOTE")) {
                        // a field to test tokens and missing reverse index
                        Mutation mutation2 = new Mutation(mutation.getRow());
                        for (ColumnUpdate update : mutation.getUpdates()) {
                            if (!new Text(update.getColumnFamily()).equals(ColumnFamilyConstants.COLF_I)) {
                                mutation2.put(update.getColumnFamily(), update.getColumnQualifier(), update.getValue());
                            }
                        }
                        mutation2.put(ColumnFamilyConstants.COLF_TF, new Text(datatype), emptyValue);
                        mutation = mutation2;
                    }
                    bw.addMutation(mutation);
                }
            }
        }
    }

    private static void writeForwardModel(AccumuloClient client, BatchWriterConfig bwConfig) throws TableNotFoundException, MutationsRejectedException {
        long timeStamp = System.currentTimeMillis();
        try (BatchWriter bw = client.createBatchWriter(QueryTestTableHelper.MODEL_TABLE_NAME, bwConfig)) {
            Mutation mutation = new Mutation("NAM");
            mutation.put("DATAWAVE", "NAME" + "\u0000" + "forward", columnVisibility, timeStamp, emptyValue);
            mutation.put("DATAWAVE", "NOME" + "\u0000" + "forward", columnVisibility, timeStamp, emptyValue);
            bw.addMutation(mutation);

            mutation = new Mutation("AG");
            mutation.put("DATAWAVE", "AGE" + "\u0000" + "forward", columnVisibility, timeStamp, emptyValue);
            mutation.put("DATAWAVE", "ETA" + "\u0000" + "forward", columnVisibility, timeStamp, emptyValue);
            bw.addMutation(mutation);

            mutation = new Mutation("GEN");
            mutation.put("DATAWAVE", "GENDER" + "\u0000" + "forward", columnVisibility, timeStamp, emptyValue);
            mutation.put("DATAWAVE", "GENERE" + "\u0000" + "forward", columnVisibility, timeStamp, emptyValue);
            bw.addMutation(mutation);

            mutation = new Mutation("LOC");
            mutation.put("DATAWAVE", "LOCATION" + "\u0000" + "forward", columnVisibility, timeStamp, emptyValue);
            mutation.put("DATAWAVE", "POSIZIONE" + "\u0000" + "forward", columnVisibility, timeStamp, emptyValue);
            bw.addMutation(mutation);

            mutation = new Mutation("BOTH_NULL");
            mutation.put("DATAWAVE", "NULL1" + "\u0000" + "forward", columnVisibility, timeStamp, emptyValue);
            mutation.put("DATAWAVE", "NULL2" + "\u0000" + "forward", columnVisibility, timeStamp, emptyValue);
            bw.addMutation(mutation);

            mutation = new Mutation("ONE_NULL");
            mutation.put("DATAWAVE", "NULL1" + "\u0000" + "forward", columnVisibility, timeStamp, emptyValue);
            mutation.put("DATAWAVE", "UUID" + "\u0000" + "forward", columnVisibility, timeStamp, emptyValue);
            bw.addMutation(mutation);
        }
    }

    private static void writeReverseModel(AccumuloClient client, BatchWriterConfig bwConfig) throws TableNotFoundException, MutationsRejectedException {
        long timeStamp = System.currentTimeMillis();
        try (BatchWriter bw = client.createBatchWriter(QueryTestTableHelper.MODEL_TABLE_NAME, bwConfig)) {
            Mutation mutation = new Mutation("NOME");
            mutation.put("DATAWAVE", "NAM" + "\u0000" + "reverse", columnVisibility, timeStamp, emptyValue);
            bw.addMutation(mutation);
            mutation = new Mutation("NAME");
            mutation.put("DATAWAVE", "NAM" + "\u0000" + "reverse", columnVisibility, timeStamp, emptyValue);
            bw.addMutation(mutation);

            mutation = new Mutation("AGE");
            mutation.put("DATAWAVE", "AG" + "\u0000" + "reverse", columnVisibility, timeStamp, emptyValue);
            bw.addMutation(mutation);
            mutation = new Mutation("ETA");
            mutation.put("DATAWAVE", "AG" + "\u0000" + "reverse", columnVisibility, timeStamp, emptyValue);
            bw.addMutation(mutation);

            mutation = new Mutation("GENDER");
            mutation.put("DATAWAVE", "GEN" + "\u0000" + "reverse", columnVisibility, timeStamp, emptyValue);
            bw.addMutation(mutation);
            mutation = new Mutation("GENERE");
            mutation.put("DATAWAVE", "GEN" + "\u0000" + "reverse", columnVisibility, timeStamp, emptyValue);
            bw.addMutation(mutation);

            mutation = new Mutation("LOCATION");
            mutation.put("DATAWAVE", "LOC" + "\u0000" + "reverse", columnVisibility, timeStamp, emptyValue);
            bw.addMutation(mutation);
            mutation = new Mutation("POSIZIONE");
            mutation.put("DATAWAVE", "LOC" + "\u0000" + "reverse", columnVisibility, timeStamp, emptyValue);
            bw.addMutation(mutation);

            mutation = new Mutation("NULL1");
            mutation.put("DATAWAVE", "BOTH_NULL" + "\u0000" + "reverse", columnVisibility, timeStamp, emptyValue);
            bw.addMutation(mutation);
            mutation = new Mutation("NULL2");
            mutation.put("DATAWAVE", "BOTH_NULL" + "\u0000" + "reverse", columnVisibility, timeStamp, emptyValue);
            bw.addMutation(mutation);

            mutation = new Mutation("NULL1");
            mutation.put("DATAWAVE", "ONE_NULL" + "\u0000" + "reverse", columnVisibility, timeStamp, emptyValue);
            bw.addMutation(mutation);
            mutation = new Mutation("UUID");
            mutation.put("DATAWAVE", "ONE_NULL" + "\u0000" + "reverse", columnVisibility, timeStamp, emptyValue);
            bw.addMutation(mutation);
        }
    }

    private static BaseType<?> normalizerForColumn(String column) {
        if ("AGE".equals(column) || "MAGIC".equals(column) || "ETA".equals(column)) {
            return numberType;
        } else if ("FROM_ADDRESS".equals(column) || "TO_ADDRESS".equals(column)) {
            return ipAddressType;
        } else if ("GEO".equals(column)) {
            return geoType;
        } else {
            return lcNoDiacriticsType;
        }
    }

    private static boolean eventField(String column) {
        return EVENT_FIELDS.contains(column);
    }

    private static void addTokens(BatchWriter bw, Map<String,AtomicLong> indexCounts, String shard, long timeStamp, Range range, String field, String phrase,
                    String uid) throws MutationsRejectedException {
        Mutation mutation = new Mutation(lcNoDiacriticsType.normalize(phrase));
        mutation.put(field.toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp, range.getValue(uid));
        bw.addMutation(mutation);

        AtomicLong count = indexCounts.get(field);
        String[] tokens = phrase.split(" ");
        for (String token : tokens) {
            mutation = new Mutation(lcNoDiacriticsType.normalize(token));
            mutation.put(field.toUpperCase(), shard + "\u0000" + datatype, columnVisibility, timeStamp, range.getValue(uid));
            if (count.get() > 0) {
                count.decrementAndGet();
                bw.addMutation(mutation);
            }
        }
    }

    private static void addFiTfTokens(BatchWriter bw, Map<String,AtomicLong> indexCounts, String shard, long timeStamp, String field, String phrase, String uid)
                    throws MutationsRejectedException {
        Mutation fi = new Mutation(shard);
        fi.put("fi\u0000" + field.toUpperCase(), lcNoDiacriticsType.normalize(phrase) + "\u0000" + datatype + "\u0000" + uid, columnVisibility, timeStamp,
                        emptyValue);

        AtomicLong count = indexCounts.get(field);
        String[] tokens = phrase.split(" ");
        for (String token : tokens) {
            if (count.get() > 0) {
                count.decrementAndGet();
                fi.put("fi\u0000" + field.toUpperCase(), lcNoDiacriticsType.normalize(token) + "\u0000" + datatype + "\u0000" + uid, columnVisibility,
                                timeStamp, emptyValue);
                fi.put("tf", datatype + "\u0000" + uid + "\u0000" + lcNoDiacriticsType.normalize(token) + "\u0000" + field, columnVisibility, timeStamp,
                                emptyValue);
            }
        }
        bw.addMutation(fi);
    }
}
