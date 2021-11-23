package datawave.experimental.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.data.ColumnFamilyConstants;
import datawave.data.hash.UID;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NumberType;
import datawave.data.type.Type;
import datawave.ingest.protobuf.TermWeight;
import datawave.ingest.protobuf.TermWeightPosition;
import datawave.ingest.protobuf.Uid;
import datawave.query.util.MetadataHelper;
import datawave.query.util.MetadataHelperFactory;
import datawave.util.TableName;

/**
 * Helper class to load some data.
 *
 * TODO -- generate some better data and load/normalize based on metadata entries
 */
public class AccumuloUtil {

    private static final Logger log = Logger.getLogger(AccumuloUtil.class);

    protected InMemoryInstance instance;
    protected AccumuloClient client;

    protected MetadataHelperFactory metadataHelperFactory = new MetadataHelperFactory();
    protected MetadataHelper metadataHelper = null;

    protected Authorizations auths = new Authorizations("ALL");

    protected String dataPath = "src/test/resources/experimental/events.csv";

    protected final ColumnVisibility cv = new ColumnVisibility("ALL");
    protected final long ts = 1607749205000L;
    protected final Value emptyValue = new Value(new byte[0]);

    private static final Type<?> lcNoDiacriticsType = new LcNoDiacriticsType();
    private static final Type<?> numberType = new NumberType();

    // the shard/datatype
    private final String shard = "20201212_0";
    private static final String dt = "dt";
    private static final Text textDt = new Text(dt);

    // the uids
    protected final String uid0 = toUid("uid0");
    protected final String uid1 = toUid("uid1");
    protected final String uid2 = toUid("uid2");
    protected final String uid3 = toUid("uid3");
    protected final String uid4 = toUid("uid4");
    protected final String uid5 = toUid("uid5");
    protected final String uid6 = toUid("uid6");
    protected final String uid7 = toUid("uid7");
    protected final String uid8 = toUid("uid8");
    protected final String uid9 = toUid("uid9");
    // extra uids
    protected final String uid10 = toUid("uid10");
    protected final String uid11 = toUid("uid11");
    protected final String uid12 = toUid("uid12");

    // uids grouped into convenience sets
    private final Set<String> aliceUids = Sets.newHashSet(uid0, uid1, uid2, uid3, uid4, uid5, uid6, uid7, uid8, uid9);
    private final Set<String> bobUids = Sets.newHashSet(uid0, uid1, uid2, uid3, uid4);
    private final Set<String> eveUids = Sets.newHashSet(uid0, uid2, uid4, uid6, uid8);
    private final Set<String> extraUids = Sets.newHashSet(uid10, uid11, uid12);
    private final Set<String> oberonUids = Sets.newHashSet(uid1, uid3, uid5, uid7, uid9);
    private final Set<String> messageUids = Sets.newHashSet(uid0, uid1, uid2, uid3, uid4);

    public void create(String instanceName) {
        instance = new InMemoryInstance(instanceName);
    }

    public AccumuloClient getClient() {
        return this.client;
    }

    public Authorizations getAuths() {
        return this.auths;
    }

    public MetadataHelperFactory getMetadataHelperFactory() {
        return this.metadataHelperFactory;
    }

    public MetadataHelper getMetadataHelper() {
        if (metadataHelper == null) {
            metadataHelper = metadataHelperFactory.createMetadataHelper(client, TableName.METADATA, Collections.singleton(auths));
        }
        return metadataHelper;
    }

    /**
     * Loads data from a test file
     *
     * @throws Exception
     *             if something goes wrong
     */
    public void loadData() throws Exception {
        loadShardTable();
        loadShardIndex();
        loadMetadataTable();
    }

    private void loadShardTable() throws Exception {
        client = new InMemoryAccumuloClient("root", instance);
        if (!client.tableOperations().exists(TableName.SHARD)) {
            client.tableOperations().create(TableName.SHARD);
        }

        //  @formatter:off
        BatchWriterConfig bwConfig = new BatchWriterConfig()
                .setMaxLatency(10, TimeUnit.SECONDS)
                .setMaxMemory(100000L)
                .setMaxWriteThreads(1);
        //  @formatter:on

        try (BatchWriter bw = client.createBatchWriter(TableName.SHARD, bwConfig)) {

            // write some field index entries
            Mutation m = new Mutation(shard);

            try (BufferedReader br = new BufferedReader(new FileReader(new File(dataPath)))) {
                String line;
                while ((line = br.readLine()) != null) {

                    if (line.startsWith("#"))
                        continue;

                    // normalizer determines how mutations are formed
                    String[] parts = line.split(",");
                    switch (parts[3]) {
                        case "IndexOnly":
                            addFieldIndex(m, parts[0], parts[1], parts[2], parts[3]);
                            break;
                        case "TermFrequencyEventOnly":
                            addTermFrequency(m, parts[0], parts[1], parts[2], parts[3]);
                            // original phrase is part of the event
                            addEvent(m, parts[0], parts[1], parts[2], parts[3]);
                            break;
                        case "TermFrequencyIndexOnly":
                            addTermFrequency(m, parts[0], parts[1], parts[2], parts[3]);
                            break;
                        case "EventOnly":
                            addEvent(m, parts[0], parts[1], parts[2], parts[3]);
                            break;
                        case "LcNoDiacritics":
                        default:
                            addEvent(m, parts[0], parts[1], parts[2], parts[3]);
                            addFieldIndex(m, parts[0], parts[1], parts[2], parts[3]);
                    }
                }
            }

            bw.addMutation(m);
            bw.flush();
        }
    }

    private void loadShardIndex() throws Exception {
        Multimap<String,String> dataMap = transformForShardIndex();

        client = new InMemoryAccumuloClient("root", instance);
        if (!client.tableOperations().exists(TableName.SHARD_INDEX)) {
            client.tableOperations().create(TableName.SHARD_INDEX);
            setShardIndexAggregator();
        }

        //  @formatter:off
        BatchWriterConfig bwConfig = new BatchWriterConfig()
                .setMaxLatency(10, TimeUnit.SECONDS)
                .setMaxMemory(100000L)
                .setMaxWriteThreads(1);
        //  @formatter:on

        try (BatchWriter bw = client.createBatchWriter(TableName.SHARD_INDEX, bwConfig)) {
            Mutation m;
            for (String value : new TreeSet<>(dataMap.keySet())) {
                m = new Mutation(value);
                TreeSet<String> datas = new TreeSet<>(dataMap.get(value));
                Multimap<String,String> fieldsToUids = HashMultimap.create();
                for (String data : datas) {
                    String[] parts = data.split(",");
                    fieldsToUids.put(parts[0], parts[1]);
                }

                for (String field : new TreeSet<>(fieldsToUids.keySet())) {
                    m.put(field, shard + "\0dt", cv, ts, toUidList(fieldsToUids.get(field)));
                }

                bw.addMutation(m);
            }

            bw.flush();
        }
    }

    // need to sort
    private Multimap<String,String> transformForShardIndex() {
        Multimap<String,String> data = HashMultimap.create();
        try (BufferedReader br = new BufferedReader(new FileReader(new File(dataPath)))) {
            String line;
            while ((line = br.readLine()) != null) {

                if (line.startsWith("#"))
                    continue;

                // uid, field, value, normalizer
                String[] parts = line.split(",");
                if (parts[3].equals("TermFrequencyIndexOnly") || parts[3].equals("TermFrequencyEventOnly")) {
                    for (String token : parts[2].split(" ")) {
                        data.put(token, parts[1] + "," + parts[0]);
                    }
                } else {
                    String normalizedValue = normalize(parts[2], parts[3]);
                    // put as value = {field, uid}
                    data.put(normalizedValue, parts[1] + "," + parts[0]);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            log.error("Problem reading data from file: " + dataPath);
        }
        return data;
    }

    public void setShardIndexAggregator() throws Exception {
        for (IteratorUtil.IteratorScope scope : IteratorUtil.IteratorScope.values()) {
            String stem = String.format("%s%s.%s", Property.TABLE_ITERATOR_PREFIX, scope.name(), "UIDAggregator");
            // Override the UidAggregator with a mock aggregator to lower the UID.List MAX uid limit.
            client.tableOperations().setProperty(TableName.SHARD_INDEX, stem + ".opt.*", "datawave.ingest.table.aggregator.GlobalIndexUidAggregator");
            // connector.tableOperations().setProperty(TableName.SHARD_RINDEX, stem + ".opt.*", "datawave.query.util.InMemoryGlobalIndexUidAggregator");
        }
    }

    private void loadMetadataTable() throws Exception {
        client = new InMemoryAccumuloClient("root", instance);
        if (!client.tableOperations().exists(TableName.METADATA)) {
            client.tableOperations().create(TableName.METADATA);
        }

        //  @formatter:off
        BatchWriterConfig bwConfig = new BatchWriterConfig()
                .setMaxLatency(10, TimeUnit.SECONDS)
                .setMaxMemory(100000L)
                .setMaxWriteThreads(1);
        //  @formatter:on

        try (BatchWriter bw = client.createBatchWriter(TableName.METADATA, bwConfig)) {

            // E - field to datatype
            // F - count of field given datatype and day
            // I - field is indexed
            // T - field to normalizer
            // TF - is field tokenized
            Mutation m;

            // FIRST_NAME - indexed field
            m = new Mutation("FIRST_NAME");
            m.put(ColumnFamilyConstants.COLF_E, textDt, emptyValue);
            m.put(ColumnFamilyConstants.COLF_F, new Text("dt\u000020201212"), getCountValue(10L));
            m.put(ColumnFamilyConstants.COLF_I, textDt, emptyValue);
            m.put(ColumnFamilyConstants.COLF_T, new Text("dt\u0000datawave.data.type." + LcNoDiacriticsType.class.getSimpleName()), emptyValue);
            bw.addMutation(m);

            // EVENT_ONLY - an event only field
            m = new Mutation("EVENT_ONLY");
            m.put(ColumnFamilyConstants.COLF_E, textDt, emptyValue);
            m.put(ColumnFamilyConstants.COLF_F, new Text("dt\u000020201212"), getCountValue(10L));
            m.put(ColumnFamilyConstants.COLF_T, new Text("dt\u0000datawave.data.type." + LcNoDiacriticsType.class.getSimpleName()), emptyValue);
            bw.addMutation(m);

            // MSG_SIZE - an indexed numeric field
            m = new Mutation("MSG_SIZE");
            m.put(ColumnFamilyConstants.COLF_E, textDt, emptyValue);
            m.put(ColumnFamilyConstants.COLF_F, new Text("dt\u000020201212"), getCountValue(10L));
            m.put(ColumnFamilyConstants.COLF_I, textDt, emptyValue);
            m.put(ColumnFamilyConstants.COLF_T, new Text("dt\u0000datawave.data.type." + NumberType.class.getSimpleName()), emptyValue);
            bw.addMutation(m);

            // MSG - event only tokenized field
            m = new Mutation("MSG");
            m.put(ColumnFamilyConstants.COLF_F, new Text("dt\u000020201212"), getCountValue(10L));
            m.put(ColumnFamilyConstants.COLF_I, textDt, emptyValue);
            m.put(ColumnFamilyConstants.COLF_T, new Text("dt\u0000datawave.data.type." + LcNoDiacriticsType.class.getSimpleName()), emptyValue);
            m.put(ColumnFamilyConstants.COLF_TF, textDt, emptyValue);
            bw.addMutation(m);

            // TOK - index only tokenized field
            m = new Mutation("TOK");
            m.put(ColumnFamilyConstants.COLF_F, new Text("dt\u000020201212"), getCountValue(10L));
            m.put(ColumnFamilyConstants.COLF_I, textDt, emptyValue);
            m.put(ColumnFamilyConstants.COLF_T, new Text("dt\u0000datawave.data.type." + LcNoDiacriticsType.class.getSimpleName()), emptyValue);
            m.put(ColumnFamilyConstants.COLF_TF, textDt, emptyValue);
            bw.addMutation(m);

            bw.flush();
        }
    }

    private void addEvent(Mutation m, String uid, String field, String value, String normalizer) {
        m.put(dt + "\0" + toUid(uid), field + "\0" + normalize(value, normalizer), cv, ts, emptyValue);
    }

    private void addFieldIndex(Mutation m, String uid, String field, String value, String normalizer) {
        m.put("fi\0" + field, normalize(value, normalizer) + "\0" + dt + "\0" + toUid(uid), cv, ts, emptyValue);
    }

    /**
     * Performs simple tokenization and adds field index and term frequency entries for a message
     */
    private void addTermFrequency(Mutation m, String uid, String field, String value, String normalizer) {
        String uid2 = toUid(uid);
        String[] tokens = value.split(" ");
        for (int j = 0; j < tokens.length; j++) {
            String token = tokens[j];
            String cq = dt + "\0" + uid2 + "\0" + normalize(token.toLowerCase(), normalizer) + "\0" + field;
            m.put("tf", cq, cv, ts, getTfValue(j));

            // add tokens to the field index
            addFieldIndex(m, uid, field, token.toLowerCase(), normalizer);
        }
    }

    private Value getTfValue(int position) {
        //  @formatter:off
        TermWeight.Info info = TermWeight.Info.newBuilder()
                .addTermOffset(position)
                .addPrevSkips(0)
                .addScore(TermWeightPosition.positionScoreToTermWeightScore(0.5f))
                .setZeroOffsetMatch(true).build();
        return new Value(info.toByteArray());
        //  @formatter:on
    }

    private Value getCountValue(long count) {
        return new Value(SummingCombiner.VAR_LEN_ENCODER.encode(count));
    }

    private static String normalize(String s, String normalizer) {
        switch (normalizer) {
            case "Number":
                return numberType.normalize(s);
            case "EventOnly":
            case "IndexOnly":
            case "TermFrequencyEventOnly":
            case "TermFrequencyIndexOnly":
            case "LcNoDiacritics":
            default:
                return lcNoDiacriticsType.normalize(s);
        }
    }

    private static String toUid(String s) {
        return UID.builder().newId(s.getBytes(), (Date) null).toString();
    }

    private static Value toUidList(Collection<String> uids) {
        Uid.List.Builder builder = Uid.List.newBuilder();
        for (String s : uids) {
            builder.addUID(toUid(s));
        }
        builder.setCOUNT(uids.size());
        builder.setIGNORE(false);
        return new Value(builder.build().toByteArray());
    }

    public Set<String> getAliceUids() {
        return aliceUids;
    }

    public Set<String> getBobUids() {
        return bobUids;
    }

    public Set<String> getEveUids() {
        return eveUids;
    }

    public Set<String> getOberonUids() {
        return oberonUids;
    }

    public Set<String> getExtraUids() {
        return extraUids;
    }

    public Set<String> getMessageUids() {
        return messageUids;
    }

    // message uids

    public String getUid0() {
        return this.uid0;
    }

    public String getUid1() {
        return this.uid1;
    }

    public String getUid2() {
        return this.uid2;
    }

    public String getUid3() {
        return this.uid3;
    }

    public String getUid4() {
        return this.uid4;
    }

    public String getUid5() {
        return this.uid5;
    }

    public String getUid6() {
        return this.uid6;
    }

    public String getUid7() {
        return this.uid7;
    }

    public String getUid8() {
        return this.uid8;
    }

}
