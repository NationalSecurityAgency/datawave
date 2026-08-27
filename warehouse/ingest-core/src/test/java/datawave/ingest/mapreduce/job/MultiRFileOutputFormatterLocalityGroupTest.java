package datawave.ingest.mapreduce.job;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.spi.crypto.NoCryptoServiceFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import datawave.ingest.config.TableConfigCache;
import datawave.ingest.data.config.ingest.AccumuloHelper;
import datawave.table.constants.TableName;

/**
 * Exercises the real {@link MultiRFileOutputFormatter} (real {@code RFileWriter}, local filesystem) end to end for a table opted into locality-group ordering,
 * driving it the way {@code IngestJob} actually does: keys are built as plain {@link BulkIngestKey}s, serialized (which resolves and bakes in each key's
 * locality-group ordinal, exactly like the reducer's output does), sorted with the real {@link BulkIngestKey.Comparator} operating on the serialized bytes,
 * then deserialized and fed to the formatter's {@link RecordWriter} in that order.
 */
public class MultiRFileOutputFormatterLocalityGroupTest {

    private static final String JOB_ID = "job_201109071404_1";
    private static final String FULLCONTENT = "fullcontent";
    private static final String TERMFREQUENCY = "termfrequency";

    private static final String[] ROWS = {"20240101_0", "20240101_1", "20240101_2"};
    private static final String[] NAMED_GROUP_CFS = {"d", "tf"};
    // fi\0FIELD and datatype\0uid style column families: not part of any named group, so they fall to the default (last) ordinal
    private static final String[] DEFAULT_GROUP_CFS = {"fi\u0000F", "dt\u0000uid"};

    @BeforeEach
    public void resetStatics() throws NoSuchFieldException, IllegalAccessException {
        BulkIngestKeyLocalityGroupLookup.reset();

        // TableConfigCache is itself a JVM-wide singleton; reset it between tests the same way BulkIngestKeyLocalityGroupLookupTest does, so each test's
        // serialized table config is actually re-read rather than reusing whatever a previous test cached.
        Field cache = TableConfigCache.class.getDeclaredField("cache");
        cache.setAccessible(true);
        cache.set(null, null);
    }

    @AfterEach
    public void resetStaticsAfter() {
        BulkIngestKeyLocalityGroupLookup.reset();
    }

    private static ByteSequence bs(String s) {
        return new ArrayByteSequence(s.getBytes(StandardCharsets.UTF_8));
    }

    /** Encode a table's properties exactly as {@code TableConfigurationUtil#serializeTableConfgurationIntoConf} does, and register it as a job output table. */
    private static void setTableConfig(Configuration conf, String table, Map<String,String> props) throws IOException {
        String existing = conf.get(TableConfigurationUtil.JOB_OUTPUT_TABLE_NAMES);
        conf.set(TableConfigurationUtil.JOB_OUTPUT_TABLE_NAMES, null == existing ? table : existing + "," + table);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(new HashMap<>(props));
        }
        conf.set(table + TableConfigurationUtil.TABLE_CONFIGURATION_PROPERTY, Base64.getEncoder().encodeToString(baos.toByteArray()));
    }

    /** A realistic shard-table property set: real Accumulo defaults (so e.g. block size lookups succeed) plus the fullcontent/termfrequency groups. */
    private static Map<String,String> shardTableProperties() {
        Map<String,String> props = new HashMap<>();
        DefaultConfiguration.getInstance().getProperties(props, key -> true);
        props.put(Property.TABLE_LOCALITY_GROUP_PREFIX.getKey() + FULLCONTENT, "d");
        props.put(Property.TABLE_LOCALITY_GROUP_PREFIX.getKey() + TERMFREQUENCY, "tf");
        props.put(Property.TABLE_LOCALITY_GROUPS.getKey(), FULLCONTENT + "," + TERMFREQUENCY);
        return props;
    }

    /** Build a job configuration that opts {@link TableName#SHARD} into locality-group ordering via a real serialized table config, as a job's conf would. */
    private static Configuration buildJobConf(java.nio.file.Path tempDir) throws IOException {
        Configuration conf = new Configuration();
        conf.set(AccumuloHelper.USERNAME, "root");
        conf.set(AccumuloHelper.PASSWORD, Base64.getEncoder().encodeToString("pass".getBytes(StandardCharsets.UTF_8)));
        conf.set(AccumuloHelper.INSTANCE_NAME, "instance");
        conf.set(AccumuloHelper.ZOOKEEPERS, "localhost:2181");

        setTableConfig(conf, TableName.SHARD, shardTableProperties());
        conf.set(TableConfigurationUtil.JOB_OUTPUT_LOCALITY_GROUP_TABLES, TableName.SHARD);
        conf.set(SplitsFile.CONFIGURED_SHARDED_TABLE_NAMES, TableName.SHARD);

        conf.set("mapred.output.dir", tempDir.toString());
        MultiRFileOutputFormatter.setCompressionType(conf, "none");
        return conf;
    }

    private static TaskAttemptContext newContext(Configuration conf) {
        return new TaskAttemptContextImpl(conf, new TaskAttemptID(new TaskID(new JobID(JOB_ID, 1), TaskType.MAP, 1), 1));
    }

    /** {@code d}/{@code tf} keys (2 per row, named groups) and {@code fi\0F}/{@code dt\0uid} keys (2 per row, default group) across several rows. */
    private static List<BulkIngestKey> buildShardKeys() {
        List<BulkIngestKey> keys = new ArrayList<>();
        long ts = 1000L;
        for (String row : ROWS) {
            for (String cf : NAMED_GROUP_CFS) {
                for (int i = 0; i < 2; i++) {
                    keys.add(new BulkIngestKey(new Text(TableName.SHARD), new Key(new Text(row), new Text(cf), new Text("cq" + i), new Text(""), ts++)));
                }
            }
            for (String cf : DEFAULT_GROUP_CFS) {
                for (int i = 0; i < 2; i++) {
                    keys.add(new BulkIngestKey(new Text(TableName.SHARD), new Key(new Text(row), new Text(cf), new Text("cq" + i), new Text(""), ts++)));
                }
            }
        }
        return keys;
    }

    /**
     * Serialize each key (baking in its locality-group ordinal via {@link BulkIngestKey#write}), sort the serialized bytes with the real
     * {@link BulkIngestKey.Comparator} (configured from {@code conf}, exactly as MapReduce would resolve it for the job), then deserialize back to
     * {@link BulkIngestKey}s (adopting the ordinal from the stream) in sorted order. This is the same table+lg+row+cf+... order the reducer's output would be
     * in.
     */
    private static List<BulkIngestKey> sortByLocalityGroup(List<BulkIngestKey> keys, Configuration conf) throws IOException {
        BulkIngestKey.Comparator comparator = new BulkIngestKey.Comparator();
        comparator.setConf(conf);

        List<byte[]> serialized = new ArrayList<>();
        for (BulkIngestKey key : keys) {
            DataOutputBuffer out = new DataOutputBuffer();
            key.write(out);
            serialized.add(Arrays.copyOf(out.getData(), out.getLength()));
        }
        serialized.sort((a, b) -> comparator.compare(a, 0, a.length, b, 0, b.length));

        List<BulkIngestKey> sorted = new ArrayList<>();
        for (byte[] bytes : serialized) {
            BulkIngestKey key = new BulkIngestKey();
            key.readFields(new DataInputStream(new ByteArrayInputStream(bytes)));
            sorted.add(key);
        }
        return sorted;
    }

    private static Path findProducedRFile(MultiRFileOutputFormatter formatter, String table, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.getLocal(conf);
        Path tableDir = new Path(formatter.workDir, table);
        FileStatus[] files = fs.listStatus(tableDir, p -> p.getName().endsWith(formatter.extension));
        assertEquals(1, files.length, "expected exactly one produced RFile in " + tableDir + ": " + Arrays.toString(files));
        return files[0].getPath();
    }

    private static RFile.Reader openReader(Path rfile, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.getLocal(conf);
        FileOperations fops = FileOperations.getInstance();
        FileSKVIterator iter = fops.newReaderBuilder().forFile(rfile.toString(), fs, conf, NoCryptoServiceFactory.NONE)
                        .withTableConfiguration(DefaultConfiguration.getInstance()).build();
        return (RFile.Reader) iter;
    }

    @Test
    public void writesLocalityGroupsAndPreservesLogicalScanOrder(@TempDir java.nio.file.Path tempDir) throws Exception {
        Configuration conf = buildJobConf(tempDir);
        List<BulkIngestKey> keys = buildShardKeys();
        List<BulkIngestKey> sorted = sortByLocalityGroup(keys, conf);

        MultiRFileOutputFormatter formatter = new MultiRFileOutputFormatter();
        TaskAttemptContext context = newContext(conf);
        RecordWriter<BulkIngestKey,Value> writer = formatter.getRecordWriter(context);
        for (BulkIngestKey key : sorted) {
            writer.write(key, new Value(Integer.toString(key.getKey().hashCode()).getBytes(StandardCharsets.UTF_8)));
        }
        writer.close(context);

        Path rfile = findProducedRFile(formatter, TableName.SHARD, conf);
        try (RFile.Reader reader = openReader(rfile, conf)) {
            // exactly the two named groups configured for the table, each holding exactly its one configured column family
            Map<String,ArrayList<ByteSequence>> lgcf = reader.getLocalityGroupCF();
            assertEquals(Collections.singleton(bs("d")), new HashSet<>(lgcf.get(FULLCONTENT)));
            assertEquals(Collections.singleton(bs("tf")), new HashSet<>(lgcf.get(TERMFREQUENCY)));

            reader.seek(new Range(), Collections.emptyList(), false);
            List<Key> scanned = new ArrayList<>();
            while (reader.hasTop()) {
                scanned.add(reader.getTopKey());
                reader.next();
            }

            // every key present, none duplicated/lost -- physically grouping by locality group must not change what a scan returns
            Set<Key> expected = keys.stream().map(BulkIngestKey::getKey).collect(Collectors.toSet());
            assertEquals(expected, new HashSet<>(scanned));
            assertEquals(keys.size(), scanned.size());

            // a plain scan reassembles the locality groups back into normal Key order, regardless of the LG-first physical write order
            for (int i = 1; i < scanned.size(); i++) {
                assertTrue(scanned.get(i - 1).compareTo(scanned.get(i)) <= 0,
                                "scan must return keys in Key order: " + scanned.get(i - 1) + " should sort <= " + scanned.get(i));
            }
        }
    }

    @Test
    public void integrationOrderedInputThrowsOrdinalRegression(@TempDir java.nio.file.Path tempDir) throws Exception {
        Configuration conf = buildJobConf(tempDir);
        List<BulkIngestKey> keys = buildShardKeys();

        // "integration-ordered" -- i.e. the plain Key order this feature did not exist to change. Within a row, cf order is d < dt\0uid < fi\0F < tf, i.e.
        // ordinals 0, 2, 2, 1 -- a regression from ordinal 2 back down to 1 partway through the very first row.
        List<BulkIngestKey> plainKeyOrder = new ArrayList<>(keys);
        plainKeyOrder.sort((a, b) -> a.getKey().compareTo(b.getKey()));

        MultiRFileOutputFormatter formatter = new MultiRFileOutputFormatter();
        TaskAttemptContext context = newContext(conf);
        RecordWriter<BulkIngestKey,Value> writer = formatter.getRecordWriter(context);

        IOException thrown = assertThrows(IOException.class, () -> {
            for (BulkIngestKey key : plainKeyOrder) {
                writer.write(key, new Value(new byte[0]));
            }
        });
        assertTrue(thrown.getMessage().contains(TableName.SHARD), "message should name the table: " + thrown.getMessage());
    }
}
