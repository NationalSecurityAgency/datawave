package datawave.ingest.mapreduce.job.reindex;

import static datawave.ingest.mapreduce.job.reindex.ShardedDataGenerator.createIngestFiles;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.isA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.rfile.RFile;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.VersioningIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.easymock.EasyMockSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import datawave.accumulo.inmemory.InMemoryAccumulo;
import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.ingest.data.config.ingest.AccumuloHelper;

public class ShardReindexVerificationMapperTest extends EasyMockSupport {

    @TempDir
    Path folder;

    private ShardReindexVerificationMapper mapper;
    private Mapper.Context context;
    private Configuration config;
    private InMemoryAccumulo instance;
    private InMemoryAccumuloClient accumuloClient;

    private List<String> dataOptions;

    private File sourceDir1 = null;
    private File sourceDir2 = null;

    @BeforeEach
    public void setup() throws AccumuloSecurityException {
        mapper = new ShardReindexVerificationMapper();
        config = new Configuration();
        context = createMock(Mapper.Context.class);

        instance = new InMemoryAccumulo(this.getClass().toString());
        accumuloClient = new InMemoryAccumuloClient("root", instance);

        expect(context.getConfiguration()).andReturn(config).anyTimes();

        dataOptions = new ArrayList<>();
        dataOptions.add("");
        dataOptions.add("val1");
        dataOptions.add("value 2");
        dataOptions.add("the dog jumped over the grey fence");
        dataOptions.add("seven long nights");
        dataOptions.add("val2");
        dataOptions.add("walking");
    }

    @AfterEach
    public void cleanup() throws IOException {
        if (sourceDir1 != null) {
            FileUtils.deleteDirectory(sourceDir1);
        }
        if (sourceDir2 != null) {
            FileUtils.deleteDirectory(sourceDir2);
        }
    }

    @Test
    public void setupTest_noSources() {
        replayAll();

        assertThrows(IllegalArgumentException.class, () -> mapper.setup(context));

        verifyAll();
    }

    @Test
    public void setupTest_oneSource1Accumulo() {
        replayAll();

        config.set("source1", "ACCUMULO");
        assertThrows(IllegalArgumentException.class, () -> mapper.setup(context));

        verifyAll();
    }

    @Test
    public void setupTest_oneSource1File() {
        replayAll();

        config.set("source1", "FILE");
        assertThrows(IllegalArgumentException.class, () -> mapper.setup(context));

        verifyAll();
    }

    @Test
    public void setupTest_oneSource2Accumulo() {
        replayAll();

        config.set("source2", "ACCUMULO");
        assertThrows(IllegalArgumentException.class, () -> mapper.setup(context));

        verifyAll();
    }

    @Test
    public void setupTest_oneSource2File() {
        replayAll();

        config.set("source2", "FILE");
        assertThrows(IllegalArgumentException.class, () -> mapper.setup(context));

        verifyAll();
    }

    @Test
    public void setupTest_twoSources() {
        replayAll();

        config.set("source1", "ACCUMULO");
        config.set("source2", "FILE");
        assertThrows(IllegalArgumentException.class, () -> mapper.setup(context));

        verifyAll();
    }

    @Test
    public void setupTest_accumuloSourceNeedsAccumuloHelperConfig() {
        replayAll();

        config.set("source1", "ACCUMULO");
        config.set("source1.table", "myTable");
        config.set("source2", "ACCUMULO");
        config.set("source2.table", "myOtherTable");
        assertThrows(IllegalArgumentException.class, () -> mapper.setup(context));

        verifyAll();
    }

    @Test
    public void setupTest_accumuloSourceNeedsTable() {
        replayAll();

        config.set("source1", "ACCUMULO");
        config.set("source2", "ACCUMULO");
        assertThrows(IllegalArgumentException.class, () -> mapper.setup(context));

        verifyAll();
    }

    @Test
    public void setupTest_accumuloSourceUserMustExist() throws Throwable {
        replayAll();

        config.set("source1", "ACCUMULO");
        config.set("source1.table", "myTable");
        config.set("source2", "ACCUMULO");
        config.set("source2.table", "myOtherTable");

        AccumuloHelper.setUsername(config, "applePie");
        AccumuloHelper.setPassword(config, "password".getBytes());
        AccumuloHelper.setZooKeepers(config, "zoo");
        AccumuloHelper.setInstanceName(config, "myInstance");
        // force in this client so no client is created
        mapper.setAccumuloClient(accumuloClient);

        // since this is thrown as a runtime exception caused by an AccumuloSecurityException unwrap this to verify
        assertThrows(AccumuloSecurityException.class, () -> {
            try {
                mapper.setup(context);
            } catch (RuntimeException e) {
                throw e.getCause();
            }
        });

        verifyAll();
    }

    @Test
    public void setupTest_accumuloSourceTableMustExist() throws Throwable {
        replayAll();

        config.set("source1", "ACCUMULO");
        config.set("source1.table", "accumuloSourceTable1");
        config.set("source2", "ACCUMULO");
        config.set("source2.table", "accumuloSourceTable2");

        AccumuloHelper.setUsername(config, "root");
        AccumuloHelper.setPassword(config, "password".getBytes());
        AccumuloHelper.setZooKeepers(config, "zoo");
        AccumuloHelper.setInstanceName(config, "myInstance");

        accumuloClient.tableOperations().create("accumuloSourceTable1");

        // force in so no client will be created inside the mapper
        mapper.setAccumuloClient(accumuloClient);

        // accumulo exception is wrapped up in a runtime exception, unwrap and verify
        assertThrows(TableNotFoundException.class, () -> {
            try {
                mapper.setup(context);
            } catch (RuntimeException e) {
                throw e.getCause();
            }
        });

        verifyAll();
    }

    @Test
    public void setupTest_fileSourceMustHavePaths() {
        replayAll();

        config.set("source1", "FILE");

        config.set("source2", "FILE");
        config.set("source2.files", "/some/path");
        assertThrows(IllegalArgumentException.class, () -> mapper.setup(context));

        verifyAll();
    }

    @Test
    public void setupTest_fileSourceNoPaths() throws Throwable {
        replayAll();

        config.set("source1", "FILE");
        config.set("source1.files", "/some/path/no-rifles");
        config.set("source2", "FILE");
        config.set("source2.files", "/some/other/path/no-rfiles");

        assertThrows(IOException.class, () -> {
            try {
                mapper.setup(context);
            } catch (RuntimeException e) {
                throw e.getCause();
            }
        });

        verifyAll();
    }

    @Test
    public void setupTest_twoSourcesWithAccumuloConfig()
                    throws AccumuloException, TableExistsException, AccumuloSecurityException, IOException, ClassNotFoundException, InterruptedException {
        sourceDir1 = createIngestFiles(dataOptions);

        replayAll();

        config.set("source1", "ACCUMULO");
        AccumuloHelper.setUsername(config, "root");
        AccumuloHelper.setPassword(config, "password".getBytes());
        AccumuloHelper.setZooKeepers(config, "zoo");
        AccumuloHelper.setInstanceName(config, "myInstance");
        config.set("source1.table", "mytable");
        accumuloClient.tableOperations().create("mytable");
        // force the in-memory client so AccumuloHelper is not used
        mapper.setAccumuloClient(accumuloClient);

        config.set("source2", "FILE");
        config.set("source2.files", sourceDir1.getAbsolutePath() + "/shard/magic.rf");
        mapper.setup(context);

        verifyAll();
    }

    @Test
    public void mapTest_compareToSelf() throws IOException, ClassNotFoundException, InterruptedException {
        Mapper.Context context = createMock(Mapper.Context.class);
        Counter mockCounter = createMock(Counter.class);

        expect(context.getConfiguration()).andReturn(config).anyTimes();

        context.progress();
        expectLastCall().anyTimes();

        // ignore all the counters
        expect(context.getCounter("progress", "source1")).andReturn(mockCounter).anyTimes();
        expect(context.getCounter("progress", "source2")).andReturn(mockCounter).anyTimes();
        mockCounter.increment(1);
        expectLastCall().anyTimes();

        sourceDir1 = createIngestFiles(dataOptions);

        replayAll();

        config.set("source1", "FILE");
        config.set("source1.files", sourceDir1.getAbsolutePath() + "/shard/magic.rf");

        config.set("source2", "FILE");
        config.set("source2.files", sourceDir1.getAbsolutePath() + "/shard/magic.rf");
        mapper.setup(context);

        mapper.map(new Range(), "", context);

        // should be no diff as the inputs are identical

        verifyAll();
    }

    @Test
    public void mapTest_compareToSelfExpectDiff() throws IOException, ClassNotFoundException, InterruptedException {
        Mapper.Context context = createMock(Mapper.Context.class);
        Counter mockCounter = createMock(Counter.class);

        expect(context.getConfiguration()).andReturn(config).anyTimes();

        context.progress();
        expectLastCall().anyTimes();

        // ignore all the counters
        expect(context.getCounter("progress", "source1")).andReturn(mockCounter).anyTimes();
        expect(context.getCounter("progress", "source2")).andReturn(mockCounter).anyTimes();
        mockCounter.increment(1);
        expectLastCall().anyTimes();

        sourceDir1 = createIngestFiles(dataOptions);

        // use totally different options to ensure results are different
        List<String> otherOptions = new ArrayList<>();
        otherOptions.add("alpha");
        otherOptions.add("delta");
        otherOptions.add("beta");
        otherOptions.add("omega");
        otherOptions.add("alpha beta delta omega");
        otherOptions.add("alpha beta");
        otherOptions.add("delta omega");
        sourceDir2 = createIngestFiles(otherOptions);

        Counter c = new Counters.Counter();
        expect(context.getCounter(isA(String.class), isA(String.class))).andAnswer(() -> c).anyTimes();
        context.write(isA(Key.class), isA(Value.class));
        expectLastCall().anyTimes();

        replayAll();

        config.set("source1", "FILE");
        config.set("source1.files", sourceDir1.getAbsolutePath() + "/shard/magic.rf");

        config.set("source2", "FILE");
        config.set("source2.files", sourceDir2.getAbsolutePath() + "/shard/magic.rf");
        mapper.setup(context);

        mapper.map(new Range(), "", context);

        verifyAll();

        // should be a non-zero diff since all values are different
        assertTrue(c.getValue() > 0);
    }

    @Test
    public void mapTest_compareToSelfAccumulo() throws IOException, ClassNotFoundException, InterruptedException, TableNotFoundException, AccumuloException,
                    AccumuloSecurityException, TableExistsException {
        Mapper.Context context = createMock(Mapper.Context.class);
        Counter mockCounter = createMock(Counter.class);

        expect(context.getConfiguration()).andReturn(config).anyTimes();

        context.progress();
        expectLastCall().anyTimes();

        // ignore all the counters
        expect(context.getCounter("progress", "source1")).andReturn(mockCounter).anyTimes();
        expect(context.getCounter("progress", "source2")).andReturn(mockCounter).anyTimes();
        mockCounter.increment(1);
        expectLastCall().anyTimes();

        sourceDir1 = createIngestFiles(dataOptions, 10000);
        // create a copy of this data for the other table
        sourceDir2 = Files.createDirectory(folder.resolve("source2")).toFile();

        // copy the rfile generated to the second directory for bulk importing
        File shardDir = new File(sourceDir1, "shard");
        FileUtils.copyDirectory(shardDir, sourceDir2);

        // read what the tables should hold before the bulk imports remove the files
        List<Map.Entry<Key,Value>> expected = readNewestVersions(new File(shardDir, "magic.rf"));
        assertFalse(expected.isEmpty());

        accumuloClient.tableOperations().create("myTable1");
        accumuloClient.tableOperations().importDirectory(sourceDir1.getAbsolutePath() + "/shard").to("myTable1").load();
        accumuloClient.tableOperations().create("myTable2");
        accumuloClient.tableOperations().importDirectory(sourceDir2.getAbsolutePath()).to("myTable2").load();

        // both tables must hold the generated data, otherwise finding no diff below proves nothing
        assertEquals(expected, scan("myTable1"));
        assertEquals(expected, scan("myTable2"));

        replayAll();

        config.set("source1", "ACCUMULO");
        config.set("source1.table", "myTable1");

        config.set("source2", "ACCUMULO");
        config.set("source2.table", "myTable2");

        // set accumulo client info
        AccumuloHelper.setUsername(config, "root");
        AccumuloHelper.setPassword(config, "password".getBytes());
        AccumuloHelper.setZooKeepers(config, "zoo");
        AccumuloHelper.setInstanceName(config, "myInstance");

        mapper.setAccumuloClient(accumuloClient);
        mapper.setup(context);

        mapper.map(new Range(), "", context);

        // should be no diff as the inputs are identical

        verifyAll();
    }

    // the newest version of each entry in the file, which is what a table with the default versioning iterator returns
    private static List<Map.Entry<Key,Value>> readNewestVersions(File rfile) {
        try (Scanner scanner = RFile.newScanner().from(rfile.getAbsolutePath()).build()) {
            scanner.addScanIterator(new IteratorSetting(20, "vers", VersioningIterator.class));
            return toList(scanner);
        }
    }

    private List<Map.Entry<Key,Value>> scan(String table) throws TableNotFoundException {
        try (Scanner scanner = accumuloClient.createScanner(table, Authorizations.EMPTY)) {
            return toList(scanner);
        }
    }

    private static List<Map.Entry<Key,Value>> toList(Scanner scanner) {
        List<Map.Entry<Key,Value>> entries = new ArrayList<>();
        for (Map.Entry<Key,Value> entry : scanner) {
            entries.add(entry);
        }
        return entries;
    }
}
