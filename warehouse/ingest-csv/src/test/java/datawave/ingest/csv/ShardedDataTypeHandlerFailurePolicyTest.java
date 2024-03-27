package datawave.ingest.csv;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.Before;
import org.junit.Test;

import datawave.ingest.csv.mr.handler.ColumnBasedHandlerTestUtil;
import datawave.ingest.csv.mr.input.CSVRecordReader;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.DataTypeHelper;
import datawave.ingest.data.config.ingest.BaseIngestHelper;
import datawave.ingest.mapreduce.handler.DataTypeHandler;
import datawave.ingest.mapreduce.handler.shard.AbstractColumnBasedHandler;

public class ShardedDataTypeHandlerFailurePolicyTest {
    private static final String CONFIG_FILE = "config/ingest/norm-content-interface.xml";
    private static final String CSV_FILE = "/input/my-nci.csv";

    private final String DATA_NAME = "mycsv";
    private final Text VIS = new Text("PRIVATE");
    private final String NB = "\u0000";

    private final long FIRST_TS = 1709208107000L;
    private final long SECOND_TS = 1709226703000L;
    private final long THIRD_TS = 1709294484000L;
    private final String UID = "-3bjkmz.-h4x7jj.eny3am";
    private final Text ROW = new Text("20240229_9");
    private final String DT_UID = NB + DATA_NAME + NB + UID;
    private final Text C_FAM = new Text(DATA_NAME + NB + UID);
    private final Text SHARD_ID_DT = new Text(ROW + NB + DATA_NAME);

    private Configuration conf;
    private FileSplit split;
    private AbstractColumnBasedHandler<Text> abstractHandler;
    private CSVRecordReader reader;

    @Before
    public void setup() {
        conf = new Configuration();
        conf.addResource(ClassLoader.getSystemResource(CONFIG_FILE));

        split = new FileSplit(new Path(Objects.requireNonNull(getClass().getResource(CSV_FILE)).getPath()), 0, 200, new String[0]);
    }

    @Test
    public void testLeave() throws IOException {
        conf.set(DATA_NAME + BaseIngestHelper.DEFAULT_FAILED_NORMALIZATION_POLICY, BaseIngestHelper.FailurePolicy.LEAVE.name());

        initializeTest();
        processFirstRow();

        HashMap<String,Set<Key>> expectedKeys = getSecondRowExpectedKeys();

        // Sharded Indexes added based upon LEAVE normalization policy
        Set<Key> shardKeys = new HashSet<>();
        Set<Key> shardIndexKeys = new HashSet<>();
        Set<Key> shardReverseIndexKeys = new HashSet<>();

        // for the leave policy, clear out the exception but add
        // a failed normalization field

        // event keys
        shardKeys.add(new Key(ROW, C_FAM, new Text("DATE_FIELD\u000020240229 17:11:43"), VIS, SECOND_TS));
        shardKeys.add(new Key(ROW, C_FAM, new Text("FAILED_NORMALIZATION_FIELD\u0000DATE_FIELD"), VIS, SECOND_TS));
        // index keys
        shardKeys.add(new Key(ROW, new Text("fi\u0000DATE_FIELD"), new Text("20240229 17:11:43" + DT_UID), VIS, SECOND_TS));
        shardKeys.add(new Key(ROW, new Text("fi\u0000FAILED_NORMALIZATION_FIELD"), new Text("DATE_FIELD" + DT_UID), VIS, SECOND_TS));
        // shardIndex keys
        long stamp = SECOND_TS - (SECOND_TS % 86400000);
        shardIndexKeys.add(new Key(new Text("20240229 17:11:43"), new Text("DATE_FIELD"), SHARD_ID_DT, VIS, stamp));
        shardIndexKeys.add(new Key(new Text("DATE_FIELD"), new Text("FAILED_NORMALIZATION_FIELD"), SHARD_ID_DT, VIS, stamp));
        // reverse shardIndex keys
        shardReverseIndexKeys.add(new Key(new Text("34:11:71 92204202"), new Text("DATE_FIELD"), SHARD_ID_DT, VIS, stamp));
        shardReverseIndexKeys.add(new Key(new Text("DLEIF_ETAD"), new Text("FAILED_NORMALIZATION_FIELD"), SHARD_ID_DT, VIS, stamp));

        expectedKeys.get("shardKeys").addAll(shardKeys);
        expectedKeys.get("shardIndexKeys").addAll(shardIndexKeys);
        expectedKeys.get("shardReverseIndexKeys").addAll(shardReverseIndexKeys);

        assertTrue("Can you read the second row of records?", reader.nextKeyValue());
        RawRecordContainer event = reader.getEvent();
        assertEquals("Do expected and actual timestamps equal?", SECOND_TS, event.getDate());

        processEvent(abstractHandler, event, expectedKeys.get("shardKeys"), expectedKeys.get("shardIndexKeys"), expectedKeys.get("shardReverseIndexKeys"));

        processThirdRow();

        reader.close();
    }

    @Test
    public void testDrop() throws IOException {
        conf.set(DATA_NAME + BaseIngestHelper.DEFAULT_FAILED_NORMALIZATION_POLICY, BaseIngestHelper.FailurePolicy.DROP.name());

        initializeTest();
        processFirstRow();

        HashMap<String,Set<Key>> expectedKeys = getSecondRowExpectedKeys();

        // Sharded Indexes added based upon LEAVE normalization policy
        Set<Key> shardKeys = new HashSet<>();
        Set<Key> shardIndexKeys = new HashSet<>();
        Set<Key> shardReverseIndexKeys = new HashSet<>();

        // for the drop policy, clear out the exception,
        // clear out the indexed field value and add
        // a failed normalization field

        // event keys
        shardKeys.add(new Key(ROW, C_FAM, new Text("DATE_FIELD\u000020240229 17:11:43"), VIS, SECOND_TS));
        shardKeys.add(new Key(ROW, C_FAM, new Text("FAILED_NORMALIZATION_FIELD\u0000DATE_FIELD"), VIS, SECOND_TS));
        // index keys
        shardKeys.add(new Key(ROW, new Text("fi\u0000FAILED_NORMALIZATION_FIELD"), new Text("DATE_FIELD" + DT_UID), VIS, SECOND_TS));
        // shardIndex keys
        long stamp = SECOND_TS - (SECOND_TS % 86400000);
        shardIndexKeys.add(new Key(new Text("DATE_FIELD"), new Text("FAILED_NORMALIZATION_FIELD"), SHARD_ID_DT, VIS, stamp));
        // reverse shardIndex keys
        shardReverseIndexKeys.add(new Key(new Text("DLEIF_ETAD"), new Text("FAILED_NORMALIZATION_FIELD"), SHARD_ID_DT, VIS, stamp));

        expectedKeys.get("shardKeys").addAll(shardKeys);
        expectedKeys.get("shardIndexKeys").addAll(shardIndexKeys);
        expectedKeys.get("shardReverseIndexKeys").addAll(shardReverseIndexKeys);

        assertTrue("Can you read the second row of records?", reader.nextKeyValue());
        RawRecordContainer event = reader.getEvent();
        assertEquals("Do expected and actual timestamps equal?", SECOND_TS, event.getDate());

        processEvent(abstractHandler, event, expectedKeys.get("shardKeys"), expectedKeys.get("shardIndexKeys"), expectedKeys.get("shardReverseIndexKeys"));

        processThirdRow();

        reader.close();
    }

    @Test
    public void testFail() throws IOException {
        conf.set(DATA_NAME + BaseIngestHelper.DEFAULT_FAILED_NORMALIZATION_POLICY, BaseIngestHelper.FailurePolicy.FAIL.name());

        initializeTest();
        processFirstRow();

        HashMap<String,Set<Key>> expectedKeys = getSecondRowExpectedKeys();

        // Sharded Indexes added based upon LEAVE normalization policy
        Set<Key> shardKeys = new HashSet<>();
        Set<Key> shardIndexKeys = new HashSet<>();
        Set<Key> shardReverseIndexKeys = new HashSet<>();

        // for the fail policy, leave the exception and let the
        // caller (EventMapper) fail the event

        // event keys
        shardKeys.add(new Key(ROW, C_FAM, new Text("DATE_FIELD\u000020240229 17:11:43"), VIS, SECOND_TS));
        // index keys
        shardKeys.add(new Key(ROW, new Text("fi\u0000DATE_FIELD"), new Text("20240229 17:11:43" + DT_UID), VIS, SECOND_TS));
        // shardIndex keys
        long stamp = SECOND_TS - (SECOND_TS % 86400000);
        shardIndexKeys.add(new Key(new Text("20240229 17:11:43"), new Text("DATE_FIELD"), SHARD_ID_DT, VIS, stamp));
        // reverse shardIndex keys
        shardReverseIndexKeys.add(new Key(new Text("34:11:71 92204202"), new Text("DATE_FIELD"), SHARD_ID_DT, VIS, stamp));

        expectedKeys.get("shardKeys").addAll(shardKeys);
        expectedKeys.get("shardIndexKeys").addAll(shardIndexKeys);
        expectedKeys.get("shardReverseIndexKeys").addAll(shardReverseIndexKeys);

        assertTrue("Can you read the second row of records?", reader.nextKeyValue());
        RawRecordContainer event = reader.getEvent();
        assertEquals("Do expected and actual timestamps equal?", SECOND_TS, event.getDate());

        processEvent(abstractHandler, event, expectedKeys.get("shardKeys"), expectedKeys.get("shardIndexKeys"), expectedKeys.get("shardReverseIndexKeys"));

        processThirdRow();

        reader.close();
    }

    private void initializeTest() throws IOException {
        TaskAttemptContextImpl context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        TypeRegistry registry = TypeRegistry.getInstance(context.getConfiguration());
        Type type = registry.get(context.getConfiguration().get(DataTypeHelper.Properties.DATA_NAME));
        type.clearIngestHelper();

        abstractHandler = new AbstractColumnBasedHandler<>();
        abstractHandler.setup(context);

        reader = new CSVRecordReader();
        reader.initialize(split, context);
    }

    private void processFirstRow() throws IOException {
        HashMap<String,Set<Key>> expectedKeys = getExpectedKeysFromFirstRow();

        assertTrue("Can you read the first row of records?", reader.nextKeyValue());
        RawRecordContainer event = reader.getEvent();
        assertEquals("Do event timestamps equal?", FIRST_TS, event.getDate());

        processEvent(abstractHandler, reader.getEvent(), expectedKeys.get("shardKeys"), expectedKeys.get("shardIndexKeys"),
                        expectedKeys.get("shardReverseIndexKeys"));
    }

    private void processThirdRow() throws IOException {
        HashMap<String,Set<Key>> expectedKeys = getExpectedKeysFromThirdRow();

        assertTrue("Can you read the third row of records?", reader.nextKeyValue());
        RawRecordContainer event = reader.getEvent();
        assertEquals("Do event timestamps equal?", THIRD_TS, event.getDate());

        processEvent(abstractHandler, reader.getEvent(), expectedKeys.get("shardKeys"), expectedKeys.get("shardIndexKeys"),
                        expectedKeys.get("shardReverseIndexKeys"));
    }

    private HashMap<String,Set<Key>> getExpectedKeysFromFirstRow() {
        HashMap<String,Set<Key>> expected = new HashMap<>();

        Set<Key> shardKeys = new HashSet<>();
        Set<Key> shardIndexKeys = new HashSet<>();
        Set<Key> shardReverseIndexKeys = new HashSet<>();

        String uid = "7cabjd.k6nbu6.-go983l";
        Text row = new Text("20240229_6");

        String dtUid = NB + DATA_NAME + NB + uid;
        Text cFam = new Text(DATA_NAME + NB + uid);
        Text shardIdDt = new Text(row + NB + DATA_NAME);
        // event keys
        shardKeys.add(new Key(row, cFam, new Text("HEADER_DATE\u00002024-02-29 12:01:47"), VIS, FIRST_TS));
        shardKeys.add(new Key(row, cFam, new Text("HEADER_ID\u0000header_one"), VIS, FIRST_TS));
        shardKeys.add(new Key(row, cFam, new Text("HEADER_NUMBER\u0000111"), VIS, FIRST_TS));
        shardKeys.add(new Key(row, cFam, new Text("HEADER_TEXT_1\u0000text one-one"), VIS, FIRST_TS));
        shardKeys.add(new Key(row, cFam, new Text("HEADER_TEXT_2\u0000text two-one"), VIS, FIRST_TS));
        shardKeys.add(new Key(row, cFam, new Text("DATE_FIELD\u00002024-02-29 12:01:47"), VIS, FIRST_TS));
        // index keys
        shardKeys.add(new Key(row, new Text("fi\u0000HEADER_NUMBER"), new Text("+cE1.11" + dtUid), VIS, FIRST_TS));
        shardKeys.add(new Key(row, new Text("fi\u0000HEADER_DATE"), new Text("2024-02-29T12:01:47.000Z" + dtUid), VIS, FIRST_TS));
        shardKeys.add(new Key(row, new Text("fi\u0000HEADER_ID"), new Text("header_one" + dtUid), VIS, FIRST_TS));
        shardKeys.add(new Key(row, new Text("fi\u0000DATE_FIELD"), new Text("2024-02-29T12:01:47.000Z" + dtUid), VIS, FIRST_TS));
        // shardIndex keys
        long stamp = FIRST_TS - (FIRST_TS % 86400000);
        shardIndexKeys.add(new Key(new Text("+cE1.11"), new Text("HEADER_NUMBER"), shardIdDt, VIS, stamp));
        shardIndexKeys.add(new Key(new Text("2024-02-29T12:01:47.000Z"), new Text("HEADER_DATE"), shardIdDt, VIS, stamp));
        shardIndexKeys.add(new Key(new Text("header_one"), new Text("HEADER_ID"), shardIdDt, VIS, stamp));
        shardIndexKeys.add(new Key(new Text("2024-02-29T12:01:47.000Z"), new Text("DATE_FIELD"), shardIdDt, VIS, stamp));
        // reverse shardIndex keys
        shardReverseIndexKeys.add(new Key(new Text("11.1Ec+"), new Text("HEADER_NUMBER"), shardIdDt, VIS, stamp));
        shardReverseIndexKeys.add(new Key(new Text("Z000.74:10:21T92-20-4202"), new Text("HEADER_DATE"), shardIdDt, VIS, stamp));
        shardReverseIndexKeys.add(new Key(new Text("eno_redaeh"), new Text("HEADER_ID"), shardIdDt, VIS, stamp));
        shardReverseIndexKeys.add(new Key(new Text("Z000.74:10:21T92-20-4202"), new Text("DATE_FIELD"), shardIdDt, VIS, stamp));

        expected.put("shardKeys", shardKeys);
        expected.put("shardIndexKeys", shardIndexKeys);
        expected.put("shardReverseIndexKeys", shardReverseIndexKeys);

        return expected;
    }

    private HashMap<String,Set<Key>> getSecondRowExpectedKeys() {
        HashMap<String,Set<Key>> expected = new HashMap<>();

        Set<Key> shardKeys = new HashSet<>();
        Set<Key> shardIndexKeys = new HashSet<>();
        Set<Key> shardReverseIndexKeys = new HashSet<>();

        // event keys
        shardKeys.add(new Key(ROW, C_FAM, new Text("HEADER_DATE\u00002024-02-29 17:11:43"), VIS, SECOND_TS));
        shardKeys.add(new Key(ROW, C_FAM, new Text("HEADER_ID\u0000header_two"), VIS, SECOND_TS));
        shardKeys.add(new Key(ROW, C_FAM, new Text("HEADER_NUMBER\u0000222"), VIS, SECOND_TS));
        shardKeys.add(new Key(ROW, C_FAM, new Text("HEADER_TEXT_1\u0000text one-two"), VIS, SECOND_TS));
        shardKeys.add(new Key(ROW, C_FAM, new Text("HEADER_TEXT_2\u0000text two-two"), VIS, SECOND_TS));
        // index keys
        shardKeys.add(new Key(ROW, new Text("fi\u0000HEADER_NUMBER"), new Text("+cE2.22" + DT_UID), VIS, SECOND_TS));
        shardKeys.add(new Key(ROW, new Text("fi\u0000HEADER_DATE"), new Text("2024-02-29T17:11:43.000Z" + DT_UID), VIS, SECOND_TS));
        shardKeys.add(new Key(ROW, new Text("fi\u0000HEADER_ID"), new Text("header_two" + DT_UID), VIS, SECOND_TS));
        // shardIndex keys
        long stamp = SECOND_TS - (SECOND_TS % 86400000);
        shardIndexKeys.add(new Key(new Text("+cE2.22"), new Text("HEADER_NUMBER"), SHARD_ID_DT, VIS, stamp));
        shardIndexKeys.add(new Key(new Text("2024-02-29T17:11:43.000Z"), new Text("HEADER_DATE"), SHARD_ID_DT, VIS, stamp));
        shardIndexKeys.add(new Key(new Text("header_two"), new Text("HEADER_ID"), SHARD_ID_DT, VIS, stamp));
        // reverse shardIndex keys
        shardReverseIndexKeys.add(new Key(new Text("22.2Ec+"), new Text("HEADER_NUMBER"), SHARD_ID_DT, VIS, stamp));
        shardReverseIndexKeys.add(new Key(new Text("Z000.34:11:71T92-20-4202"), new Text("HEADER_DATE"), SHARD_ID_DT, VIS, stamp));
        shardReverseIndexKeys.add(new Key(new Text("owt_redaeh"), new Text("HEADER_ID"), SHARD_ID_DT, VIS, stamp));

        expected.put("shardKeys", shardKeys);
        expected.put("shardIndexKeys", shardIndexKeys);
        expected.put("shardReverseIndexKeys", shardReverseIndexKeys);

        return expected;
    }

    private HashMap<String,Set<Key>> getExpectedKeysFromThirdRow() {
        HashMap<String,Set<Key>> expected = new HashMap<>();

        Set<Key> shardKeys = new HashSet<>();
        Set<Key> shardIndexKeys = new HashSet<>();
        Set<Key> shardReverseIndexKeys = new HashSet<>();

        String uid = "2a6d40.b56k6m.pmeuml";
        Text row = new Text("20240301_2");

        String dtUid = NB + DATA_NAME + NB + uid;
        Text cFam = new Text(DATA_NAME + NB + uid);
        Text shardIdDt = new Text(row + NB + DATA_NAME);
        // event keys
        shardKeys.add(new Key(row, cFam, new Text("HEADER_DATE\u00002024-03-01 12:01:24"), VIS, THIRD_TS));
        shardKeys.add(new Key(row, cFam, new Text("HEADER_ID\u0000header_three"), VIS, THIRD_TS));
        shardKeys.add(new Key(row, cFam, new Text("HEADER_NUMBER\u0000333"), VIS, THIRD_TS));
        shardKeys.add(new Key(row, cFam, new Text("HEADER_TEXT_1\u0000text one-three"), VIS, THIRD_TS));
        shardKeys.add(new Key(row, cFam, new Text("HEADER_TEXT_2\u0000text two-three"), VIS, THIRD_TS));
        shardKeys.add(new Key(row, cFam, new Text("DATE_FIELD\u00002024-03-01 12:01:24"), VIS, THIRD_TS));
        // index keys
        shardKeys.add(new Key(row, new Text("fi\u0000HEADER_NUMBER"), new Text("+cE3.33" + dtUid), VIS, THIRD_TS));
        shardKeys.add(new Key(row, new Text("fi\u0000HEADER_DATE"), new Text("2024-03-01T12:01:24.000Z" + dtUid), VIS, THIRD_TS));
        shardKeys.add(new Key(row, new Text("fi\u0000HEADER_ID"), new Text("header_three" + dtUid), VIS, THIRD_TS));
        shardKeys.add(new Key(row, new Text("fi\u0000DATE_FIELD"), new Text("2024-03-01T12:01:24.000Z" + dtUid), VIS, THIRD_TS));
        // shardIndex keys
        long stamp = THIRD_TS - (THIRD_TS % 86400000);
        shardIndexKeys.add(new Key(new Text("+cE3.33"), new Text("HEADER_NUMBER"), shardIdDt, VIS, stamp));
        shardIndexKeys.add(new Key(new Text("2024-03-01T12:01:24.000Z"), new Text("HEADER_DATE"), shardIdDt, VIS, stamp));
        shardIndexKeys.add(new Key(new Text("header_three"), new Text("HEADER_ID"), shardIdDt, VIS, stamp));
        shardIndexKeys.add(new Key(new Text("2024-03-01T12:01:24.000Z"), new Text("DATE_FIELD"), shardIdDt, VIS, stamp));
        // reverse shardIndex keys
        shardReverseIndexKeys.add(new Key(new Text("33.3Ec+"), new Text("HEADER_NUMBER"), shardIdDt, VIS, stamp));
        shardReverseIndexKeys.add(new Key(new Text("Z000.42:10:21T10-30-4202"), new Text("HEADER_DATE"), shardIdDt, VIS, stamp));
        shardReverseIndexKeys.add(new Key(new Text("eerht_redaeh"), new Text("HEADER_ID"), shardIdDt, VIS, stamp));
        shardReverseIndexKeys.add(new Key(new Text("Z000.42:10:21T10-30-4202"), new Text("DATE_FIELD"), shardIdDt, VIS, stamp));

        expected.put("shardKeys", shardKeys);
        expected.put("shardIndexKeys", shardIndexKeys);
        expected.put("shardReverseIndexKeys", shardReverseIndexKeys);

        return expected;
    }

    private void processEvent(DataTypeHandler<Text> handler, RawRecordContainer record, Set<Key> shardKeys, Set<Key> shardIndexKeys,
                    Set<Key> shardReverseIndexKeys) {
        ColumnBasedHandlerTestUtil.processEvent(handler, record, shardKeys, shardIndexKeys, shardReverseIndexKeys);
    }
}
