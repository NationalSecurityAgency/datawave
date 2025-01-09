package datawave.ingest.mapreduce.job.reindex;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.TaskType;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import datawave.ingest.mapreduce.StandaloneStatusReporter;
import datawave.ingest.mapreduce.StandaloneTaskAttemptContext;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.writer.SpillingSortedContextWriter;

public class SpillingSortedContextWriterTest {
    private Configuration conf;
    private SpillingSortedContextWriter spillingSortedContextWriter;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Before
    public void setup() {
        conf = new Configuration();
        spillingSortedContextWriter = new SpillingSortedContextWriter();
    }

    @Test(expected = IllegalStateException.class)
    public void setup_noWorkDir_Test() throws IOException, InterruptedException {
        spillingSortedContextWriter.setup(conf, false);
    }

    @Test(expected = IllegalStateException.class)
    public void setup_workDirNotADirTest() throws IOException, InterruptedException {
        File workDir = folder.newFile("workDir");
        conf.set(SpillingSortedContextWriter.WORK_DIR, workDir.getAbsolutePath());
        spillingSortedContextWriter.setup(conf, false);
    }

    @Test
    public void setupTest() throws IOException, InterruptedException {
        File workDir = folder.newFolder("workDir");
        conf.set(SpillingSortedContextWriter.WORK_DIR, workDir.getAbsolutePath());
        spillingSortedContextWriter.setup(conf, false);
    }

    // allowed
    @Test
    public void setup_testNonExistentDirTest() throws IOException, InterruptedException {
        conf.set(SpillingSortedContextWriter.WORK_DIR, "/some/non/existent/path");
        spillingSortedContextWriter.setup(conf, false);
    }

    @Test
    public void write_singleNoWriteTest() throws IOException, InterruptedException {
        StandaloneTaskAttemptContext context = new StandaloneTaskAttemptContext(conf, new StandaloneStatusReporter());
        File workDir = folder.newFolder("workDir");
        conf.set(SpillingSortedContextWriter.WORK_DIR, workDir.getAbsolutePath());
        spillingSortedContextWriter.setup(conf, false);

        spillingSortedContextWriter.write(new BulkIngestKey(new Text("myTable"), new Key()), new Value(), context);

        // verify that nothing is created in the workDir
        File targetOutput = new File(workDir, "myTable/" + context.getTaskAttemptID());
        assertFalse(targetOutput.exists());
    }

    @Test
    public void write_singleWriteTest() throws IOException, InterruptedException {
        StandaloneTaskAttemptContext context = new StandaloneTaskAttemptContext(conf, new StandaloneStatusReporter());
        File workDir = folder.newFolder("workDir");
        conf.set(SpillingSortedContextWriter.WORK_DIR, workDir.getAbsolutePath());
        spillingSortedContextWriter.setup(conf, false);

        spillingSortedContextWriter.write(new BulkIngestKey(new Text("myTable"), new Key()), new Value(), context);
        spillingSortedContextWriter.commit(context);

        // verify that a single rfile is written
        File targetOutput = new File(workDir, "myTable/" + context.getTaskAttemptID());
        assertTrue(targetOutput.exists());

        File targetFile = new File(workDir, "myTable/" + context.getTaskAttemptID() + "/0.rf");
        assertTrue(targetFile.exists());

        int rfileCount = 0;
        for (File f : targetOutput.listFiles()) {
            if (f.getName().endsWith(".rf")) {
                rfileCount++;
            }
        }

        // verify no other files created
        assertEquals(1, rfileCount);
    }

    @Test
    public void cleanup_flushTest() throws IOException, InterruptedException {
        TaskInputOutputContext mockContext = EasyMock.createMock(TaskInputOutputContext.class);
        File workDir = folder.newFolder("workDir");
        conf.set(SpillingSortedContextWriter.WORK_DIR, workDir.getAbsolutePath());

        EasyMock.expect(mockContext.getTaskAttemptID()).andReturn(new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 1), 1)).anyTimes();
        EasyMock.expect(mockContext.getConfiguration()).andReturn(conf).anyTimes();
        mockContext.write(new BulkIngestKey(new Text("myTable"), new Key()), new Value());

        EasyMock.replay(mockContext);

        spillingSortedContextWriter.setup(conf, false);
        spillingSortedContextWriter.write(new BulkIngestKey(new Text("myTable"), new Key()), new Value(), mockContext);
        spillingSortedContextWriter.cleanup(mockContext);

        EasyMock.verify(mockContext);

        // verify that nothing is left behind
        File targetOutput = new File(workDir, "myTable/" + mockContext.getTaskAttemptID());
        assertFalse(targetOutput.exists());
    }

    @Test
    public void cleanup_duplicateKeySquashedTest() throws IOException, InterruptedException {
        TaskInputOutputContext mockContext = EasyMock.createMock(TaskInputOutputContext.class);
        File workDir = folder.newFolder("workDir");
        conf.set(SpillingSortedContextWriter.WORK_DIR, workDir.getAbsolutePath());

        EasyMock.expect(mockContext.getTaskAttemptID()).andReturn(new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 1), 1)).anyTimes();
        EasyMock.expect(mockContext.getConfiguration()).andReturn(conf).anyTimes();
        mockContext.write(EasyMock.eq(new BulkIngestKey(new Text("myTable"), new Key("setA"))), EasyMock.isA(Value.class));
        EasyMock.expectLastCall().times(1);

        EasyMock.replay(mockContext);

        spillingSortedContextWriter.setup(conf, false);
        spillingSortedContextWriter.write(new BulkIngestKey(new Text("myTable"), new Key("setA")), new Value(), mockContext);
        spillingSortedContextWriter.write(new BulkIngestKey(new Text("myTable"), new Key("setA")), new Value(), mockContext);
        spillingSortedContextWriter.write(new BulkIngestKey(new Text("myTable"), new Key("setA")), new Value(), mockContext);
        spillingSortedContextWriter.write(new BulkIngestKey(new Text("myTable"), new Key("setA")), new Value(), mockContext);
        spillingSortedContextWriter.write(new BulkIngestKey(new Text("myTable"), new Key("setA")), new Value(), mockContext);
        spillingSortedContextWriter.cleanup(mockContext);

        EasyMock.verify(mockContext);
    }

    @Test
    public void cleanup_mergeOfKeysTest() throws IOException, InterruptedException {
        TaskInputOutputContext mockContext = EasyMock.createMock(TaskInputOutputContext.class);
        File workDir = folder.newFolder("workDir");
        conf.set(SpillingSortedContextWriter.WORK_DIR, workDir.getAbsolutePath());

        EasyMock.expect(mockContext.getTaskAttemptID()).andReturn(new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 1), 1)).anyTimes();
        EasyMock.expect(mockContext.getConfiguration()).andReturn(conf).anyTimes();
        mockContext.write(EasyMock.eq(new BulkIngestKey(new Text("myTable"), new Key("setA"))), EasyMock.isA(Value.class));
        EasyMock.expectLastCall().times(5);
        mockContext.write(EasyMock.eq(new BulkIngestKey(new Text("myTable"), new Key("setB"))), EasyMock.isA(Value.class));
        EasyMock.expectLastCall().times(3);

        EasyMock.replay(mockContext);

        spillingSortedContextWriter.setup(conf, false);
        spillingSortedContextWriter.write(new BulkIngestKey(new Text("myTable"), new Key("setA")), new Value("a"), mockContext);
        spillingSortedContextWriter.write(new BulkIngestKey(new Text("myTable"), new Key("setA")), new Value("b"), mockContext);
        spillingSortedContextWriter.write(new BulkIngestKey(new Text("myTable"), new Key("setA")), new Value("c"), mockContext);
        spillingSortedContextWriter.write(new BulkIngestKey(new Text("myTable"), new Key("setA")), new Value("d"), mockContext);
        spillingSortedContextWriter.write(new BulkIngestKey(new Text("myTable"), new Key("setA")), new Value("e"), mockContext);
        spillingSortedContextWriter.commit(mockContext);

        spillingSortedContextWriter.write(new BulkIngestKey(new Text("myTable"), new Key("setB")), new Value("1"), mockContext);
        spillingSortedContextWriter.write(new BulkIngestKey(new Text("myTable"), new Key("setB")), new Value("2"), mockContext);
        spillingSortedContextWriter.write(new BulkIngestKey(new Text("myTable"), new Key("setB")), new Value("3"), mockContext);
        spillingSortedContextWriter.cleanup(mockContext);

        EasyMock.verify(mockContext);

        // verify that nothing is left behind
        File targetOutput = new File(workDir, "myTable/" + mockContext.getTaskAttemptID());
        assertFalse(targetOutput.exists());
    }

    @Test
    public void cleanup_emptyTest() throws IOException, InterruptedException {
        StandaloneTaskAttemptContext context = new StandaloneTaskAttemptContext(conf, new StandaloneStatusReporter());
        File workDir = folder.newFolder("workDir");
        conf.set(SpillingSortedContextWriter.WORK_DIR, workDir.getAbsolutePath());
        spillingSortedContextWriter.setup(conf, false);

        spillingSortedContextWriter.cleanup(context);

        // verify that a single rfile is written
        File targetOutput = new File(workDir, "myTable/" + context.getTaskAttemptID());
        assertFalse(targetOutput.exists());
    }
}
