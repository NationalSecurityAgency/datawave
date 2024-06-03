package datawave.ingest.mapreduce.job.reindex;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.shaded.com.google.common.io.Files;
import org.easymock.EasyMockSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Multimap;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.ingest.AccumuloHelper;
import datawave.ingest.mapreduce.StandaloneStatusReporter;
import datawave.ingest.mapreduce.handler.DataTypeHandler;
import datawave.ingest.mapreduce.handler.shard.AbstractColumnBasedHandler;
import datawave.ingest.mapreduce.job.BulkIngestKey;

public class ShardReindexVerificationMapperTest extends EasyMockSupport {
    private ShardReindexVerificationMapper mapper;
    private Mapper.Context context;
    private Configuration config;
    private InMemoryInstance instance;
    private InMemoryAccumuloClient accumuloClient;

    private File sourceDir1 = null;
    private File sourceDir2 = null;

    @Before
    public void setup() throws AccumuloSecurityException {
        mapper = new ShardReindexVerificationMapper();
        config = new Configuration();
        context = createMock(Mapper.Context.class);

        instance = new InMemoryInstance(this.getClass().toString());
        accumuloClient = new InMemoryAccumuloClient("root", instance);

        expect(context.getConfiguration()).andReturn(config).anyTimes();
    }

    @After
    public void cleanup() throws IOException {
        if (sourceDir1 != null) {
            FileUtils.deleteDirectory(sourceDir1);
        }
        if (sourceDir2 != null) {
            FileUtils.deleteDirectory(sourceDir2);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void setupTest_noSources() {
        replayAll();

        mapper.setup(context);

        verifyAll();
    }

    @Test(expected = IllegalArgumentException.class)
    public void setupTest_oneSource1Accumulo() {
        replayAll();

        config.set("source1", "ACCUMULO");
        mapper.setup(context);

        verifyAll();
    }

    @Test(expected = IllegalArgumentException.class)
    public void setupTest_oneSource1File() {
        replayAll();

        config.set("source1", "FILE");
        mapper.setup(context);

        verifyAll();
    }

    @Test(expected = IllegalArgumentException.class)
    public void setupTest_oneSource2Accumulo() {
        replayAll();

        config.set("source2", "ACCUMULO");
        mapper.setup(context);

        verifyAll();
    }

    @Test(expected = IllegalArgumentException.class)
    public void setupTest_oneSource2File() {
        replayAll();

        config.set("source2", "FILE");
        mapper.setup(context);

        verifyAll();
    }

    @Test(expected = IllegalArgumentException.class)
    public void setupTest_twoSources() {
        replayAll();

        config.set("source1", "ACCUMULO");
        config.set("source2", "FILE");
        mapper.setup(context);

        verifyAll();
    }

    @Test
    public void setupTest_accumuloSourceNeedsAccumuloHelperConfig() {
        throw new NotImplementedException("todo");
    }

    @Test
    public void setupTest_accumuloSourceNeedsTable() {
        throw new NotImplementedException("todo");
    }

    @Test
    public void setupTest_accumuloSourceUserMustExist() {
        throw new NotImplementedException("todo");
    }

    @Test
    public void setupTest_accumuloSourceTableMustExist() {
        throw new NotImplementedException("todo");
    }

    @Test
    public void setupTest_fileSourceMustHavePaths() {
        throw new NotImplementedException("todo");
    }

    @Test
    public void setupTest_fileSourceNoPaths() {
        throw new NotImplementedException("todo");
    }

    @Test
    public void setupTest_fileSourceMissingFileTest() {
        throw new NotImplementedException("todo");
    }

    @Test
    public void setupTest_twoSourcesWithAccumuloConfig()
                    throws AccumuloException, TableExistsException, AccumuloSecurityException, IOException, ClassNotFoundException, InterruptedException {
        sourceDir1 = createIngestFiles();

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

        sourceDir1 = createIngestFiles();

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

    private File createIngestFiles() throws ClassNotFoundException, IOException, InterruptedException {
        File f = Files.createTempDir();
        Configuration config = new Configuration();
        ShardedDataGenerator.setup(config, "samplecsv", 9, "shard", "shardIndex", "shardReverseIndex", "DatawaveMetadata");
        DataTypeHandler handler = ShardedDataGenerator.getDataTypeHandler(config, new TaskAttemptContextImpl(config, new TaskAttemptID()),
                        AbstractColumnBasedHandler.class.getCanonicalName());
        List<String> dataOptions = new ArrayList<>();
        dataOptions.add("");
        dataOptions.add("val1");
        dataOptions.add("value 2");
        dataOptions.add("the dog jumped over the grey fence");
        dataOptions.add("seven long nights");
        dataOptions.add("val2");
        dataOptions.add("walking");
        RawRecordContainer container = ShardedDataGenerator.generateEvent(config, "samplecsv", new Date(),
                        ShardedDataGenerator.generateRawData(9, dataOptions).getBytes(), new ColumnVisibility());
        Multimap<BulkIngestKey,Value> generated = ShardedDataGenerator.process(config, handler, "samplecsv", container, new StandaloneStatusReporter());

        ShardedDataGenerator.writeData(f.getAbsolutePath(), "magic.rf", generated);

        return f;
    }
}
