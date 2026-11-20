package datawave.ingest.mapreduce.job.reindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.ignoreStubs;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.TaskType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import datawave.ingest.mapreduce.StandaloneStatusReporter;
import datawave.ingest.mapreduce.StandaloneTaskAttemptContext;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.writer.SpillingSortedContextWriter;

public class SpillingSortedContextWriterTest {
    private Configuration conf;
    private SpillingSortedContextWriter spillingSortedContextWriter;

    @TempDir
    Path folder;

    @BeforeEach
    public void setup() {
        conf = new Configuration();
        spillingSortedContextWriter = new SpillingSortedContextWriter();
    }

    @Test
    public void setup_noWorkDir_Test() throws IOException, InterruptedException {
        assertThrows(IllegalStateException.class, () -> spillingSortedContextWriter.setup(conf, false));
    }

    @Test
    public void setup_workDirNotADirTest() throws IOException, InterruptedException {
        File workDir = Files.createFile(folder.resolve("workDir")).toFile();
        conf.set(SpillingSortedContextWriter.WORK_DIR, workDir.getAbsolutePath());
        assertThrows(IllegalStateException.class, () -> spillingSortedContextWriter.setup(conf, false));
    }

    @Test
    public void setupTest() throws IOException, InterruptedException {
        File workDir = Files.createDirectory(folder.resolve("workDir")).toFile();
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
        File workDir = Files.createDirectory(folder.resolve("workDir")).toFile();
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
        File workDir = Files.createDirectory(folder.resolve("workDir")).toFile();
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
        TaskInputOutputContext mockContext = mock(TaskInputOutputContext.class);
        File workDir = Files.createDirectory(folder.resolve("workDir")).toFile();
        conf.set(SpillingSortedContextWriter.WORK_DIR, workDir.getAbsolutePath());

        lenient().doReturn(new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 1), 1)).when(mockContext).getTaskAttemptID();
        lenient().doReturn(conf).when(mockContext).getConfiguration();

        spillingSortedContextWriter.setup(conf, false);
        spillingSortedContextWriter.write(new BulkIngestKey(new Text("myTable"), new Key()), new Value(), mockContext);
        spillingSortedContextWriter.cleanup(mockContext);

        verify(mockContext, times(1)).write(new BulkIngestKey(new Text("myTable"), new Key()), new Value());

        // verify that nothing is left behind
        File targetOutput = new File(workDir, "myTable/" + mockContext.getTaskAttemptID());
        assertFalse(targetOutput.exists());

        if (mockContext != null && mockingDetails(mockContext).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockContext));
        }
    }

    @Test
    public void cleanup_duplicateKeySquashedTest() throws IOException, InterruptedException {
        TaskInputOutputContext mockContext = mock(TaskInputOutputContext.class);
        File workDir = Files.createDirectory(folder.resolve("workDir")).toFile();
        conf.set(SpillingSortedContextWriter.WORK_DIR, workDir.getAbsolutePath());

        lenient().doReturn(new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 1), 1)).when(mockContext).getTaskAttemptID();
        lenient().doReturn(conf).when(mockContext).getConfiguration();

        spillingSortedContextWriter.setup(conf, false);
        spillingSortedContextWriter.write(new BulkIngestKey(new Text("myTable"), new Key("setA")), new Value(), mockContext);
        spillingSortedContextWriter.write(new BulkIngestKey(new Text("myTable"), new Key("setA")), new Value(), mockContext);
        spillingSortedContextWriter.write(new BulkIngestKey(new Text("myTable"), new Key("setA")), new Value(), mockContext);
        spillingSortedContextWriter.write(new BulkIngestKey(new Text("myTable"), new Key("setA")), new Value(), mockContext);
        spillingSortedContextWriter.write(new BulkIngestKey(new Text("myTable"), new Key("setA")), new Value(), mockContext);
        spillingSortedContextWriter.cleanup(mockContext);

        verify(mockContext, times(1)).write(eq(new BulkIngestKey(new Text("myTable"), new Key("setA"))), isA(Value.class));

        if (mockContext != null && mockingDetails(mockContext).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockContext));
        }
    }

    @Test
    public void cleanup_mergeOfKeysTest() throws IOException, InterruptedException {
        TaskInputOutputContext mockContext = mock(TaskInputOutputContext.class);
        File workDir = Files.createDirectory(folder.resolve("workDir")).toFile();
        conf.set(SpillingSortedContextWriter.WORK_DIR, workDir.getAbsolutePath());

        lenient().doReturn(new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 1), 1)).when(mockContext).getTaskAttemptID();
        lenient().doReturn(conf).when(mockContext).getConfiguration();

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

        verify(mockContext, times(5)).write(eq(new BulkIngestKey(new Text("myTable"), new Key("setA"))), isA(Value.class));
        verify(mockContext, times(3)).write(eq(new BulkIngestKey(new Text("myTable"), new Key("setB"))), isA(Value.class));

        // verify that nothing is left behind
        File targetOutput = new File(workDir, "myTable/" + mockContext.getTaskAttemptID());
        assertFalse(targetOutput.exists());

        if (mockContext != null && mockingDetails(mockContext).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockContext));
        }
    }

    @Test
    public void cleanup_emptyTest() throws IOException, InterruptedException {
        StandaloneTaskAttemptContext context = new StandaloneTaskAttemptContext(conf, new StandaloneStatusReporter());
        File workDir = Files.createDirectory(folder.resolve("workDir")).toFile();
        conf.set(SpillingSortedContextWriter.WORK_DIR, workDir.getAbsolutePath());
        spillingSortedContextWriter.setup(conf, false);

        spillingSortedContextWriter.cleanup(context);

        // verify that a single rfile is written
        File targetOutput = new File(workDir, "myTable/" + context.getTaskAttemptID());
        assertFalse(targetOutput.exists());
    }
}
