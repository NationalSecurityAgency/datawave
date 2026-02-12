package datawave.query.util;

import static datawave.util.TableName.METADATA;
import static datawave.util.TableName.SHARD;
import static datawave.util.TableName.SHARD_INDEX;
import static datawave.util.TableName.SHARD_RINDEX;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.io.Text;

import datawave.data.ColumnFamilyConstants;
import datawave.ingest.protobuf.Uid;
import datawave.test.MacTestUtil;
import datawave.util.TableName;

public class IndexExpansionIngest {

    private static final Set<String> fields = Set.of("FIELD_A", "FIELD_B", "FIELD_C", "FIELD_D", "FIELD_E");
    private static final Set<String> dates = Set.of("20250101", "20250102", "20250103", "20250104", "20250105");
    private static final Set<String> datatypes = Set.of("datatype-a", "datatype-b", "datatype-c", "datatype-d", "datatype-e");

    private static final Long ts = 1707045600000L;
    private static final Value EMPTY_VALUE = new Value();

    private static final Set<String> extraIndexedFields = Set.of("FIELD_X");

    private static final int maxValuesPerPrefix = 10;

    public static void write(AccumuloClient client, Authorizations auths) throws Exception {
        TableOperations tops = client.tableOperations();
        MacTestUtil.createOrRecreate(tops, SHARD);
        MacTestUtil.createOrRecreate(tops, SHARD_INDEX);
        MacTestUtil.createOrRecreate(tops, SHARD_RINDEX);
        MacTestUtil.createOrRecreate(tops, METADATA);

        Map<String,String> additions = new HashMap<>();
        IteratorUtil.IteratorScope[] scopes = IteratorUtil.IteratorScope.values();
        for (IteratorUtil.IteratorScope scope : scopes) {
            String name = "table.iterator." + scope.name() + ".UIDAggregator";
            String opt = "table.iterator." + scope.name() + ".UIDAggregator.opt.*";

            additions.put(name, "19,datawave.iterators.TotalAggregatingIterator");
            additions.put(opt, "datawave.ingest.table.aggregator.KeepCountOnlyUidAggregator");
        }
        MacTestUtil.addPropertiesAndWait(tops, SHARD_INDEX, additions);
        MacTestUtil.addPropertiesAndWait(tops, SHARD_RINDEX, additions);

        writeMetadata(client);
        writeShardIndex(client, auths);
    }

    private static void writeMetadata(AccumuloClient client) throws Exception {
        BatchWriterConfig bwConfig = new BatchWriterConfig().setMaxMemory(1000L).setMaxLatency(1, TimeUnit.SECONDS).setMaxWriteThreads(1);
        try (BatchWriter bw = client.createBatchWriter(TableName.METADATA, bwConfig)) {
            for (String field : fields) {
                Mutation m = new Mutation(field);
                for (String datatype : datatypes) {
                    Text cq = new Text(datatype);
                    m.put(ColumnFamilyConstants.COLF_I, cq, EMPTY_VALUE);
                    m.put(ColumnFamilyConstants.COLF_E, cq, EMPTY_VALUE);
                    for (String date : dates) {
                        m.put(ColumnFamilyConstants.COLF_F, new Text(datatype + '\u0000' + date), createValue(12L));
                    }
                }
                bw.addMutation(m);
            }

            String lastDate = new TreeSet<>(dates).last();
            for (String field : extraIndexedFields) {
                Mutation m = new Mutation(field);
                for (String datatype : datatypes) {
                    Text cq = new Text(datatype);
                    m.put(ColumnFamilyConstants.COLF_I, cq, EMPTY_VALUE);
                    m.put(ColumnFamilyConstants.COLF_E, cq, EMPTY_VALUE);
                    m.put(ColumnFamilyConstants.COLF_F, new Text(datatype + '\u0000' + lastDate), createValue(12L));
                }
                bw.addMutation(m);
            }

            // write num_shards cache
            String firstDate = new TreeSet<>(dates).first();
            Mutation m = new Mutation("num_shards");
            m.put("ns", firstDate + "_2", EMPTY_VALUE);
            bw.addMutation(m);
        }
    }

    private static void writeShardIndex(AccumuloClient client, Authorizations auths) throws Exception {
        ColumnVisibility viz = new ColumnVisibility(auths.iterator().next());
        Set<String> prefixes = Set.of("aa", "ab", "ac", "ad", "ae");

        BatchWriterConfig bwConfig = new BatchWriterConfig().setMaxMemory(1000L).setMaxLatency(1, TimeUnit.SECONDS).setMaxWriteThreads(1);
        try (BatchWriter bw = client.createBatchWriter(TableName.SHARD_INDEX, bwConfig)) {
            for (String prefix : prefixes) {
                for (int i = 0; i < maxValuesPerPrefix; i++) {
                    String value = prefix + RandomStringUtils.randomAlphabetic(3, 6).toLowerCase();
                    Mutation m = new Mutation(value);
                    for (String field : fields) {
                        Text cf = new Text(field);
                        for (String date : dates) {
                            for (String datatype : datatypes) {
                                Text cq = new Text(date + "_2\0" + datatype);
                                m.put(cf, cq, viz, getUidList());
                            }
                        }
                    }
                    bw.addMutation(m);
                }
            }

            // extra indexed fields have data at the very end of range
            String lastDate = new TreeSet<>(dates).last();
            for (int i = 0; i < maxValuesPerPrefix; i++) {
                String value = "af" + RandomStringUtils.randomAlphabetic(3, 6).toLowerCase();
                Mutation m = new Mutation(value);
                for (String field : extraIndexedFields) {
                    Text cf = new Text(field);
                    for (String datatype : datatypes) {
                        Text cq = new Text(lastDate + "_2\0" + datatype);
                        m.put(cf, cq, viz, getUidList());
                    }
                }
                bw.addMutation(m);
            }

            // unfielded literal expansion data
            String value = "a1b2c3";
            Mutation m = new Mutation(value);
            for (String field : fields) {
                Text cf = new Text(field);
                for (String datatype : datatypes) {
                    Text cq = new Text(lastDate + "_1\0" + datatype);
                    m.put(cf, cq, viz, getUidList());
                }
            }
            bw.addMutation(m);
        }
    }

    private static final LongCombiner.VarLenEncoder encoder = new LongCombiner.VarLenEncoder();

    private static Value createValue(long count) {
        return new Value(encoder.encode(count));
    }

    private static final Random rand = new Random();

    private static Value getUidList() {
        Uid.List.Builder builder = Uid.List.newBuilder();
        builder.setIGNORE(false);
        int count = 1 + rand.nextInt(18);
        builder.setCOUNT(count);
        for (int i = 0; i < count; i++) {
            builder.addUID(RandomStringUtils.randomAlphabetic(12));
        }
        return new Value(builder.build().toByteArray());
    }

}
