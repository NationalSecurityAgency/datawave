package datawave.ingest.mapreduce.job.writer;

import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.easymock.EasyMockSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import datawave.ingest.mapreduce.StandaloneStatusReporter;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.util.RFileUtil;
import datawave.ingest.test.StandaloneTaskAttemptContext;

public class SpillingSortedContextWriterTest extends EasyMockSupport {
    private SpillingSortedContextWriter writer;
    private Configuration config;
    private File tmpDir;
    private TaskInputOutputContext context;
    private TaskInputOutputContext outputContext;

    @Before
    public void setup() throws IOException {
        writer = new SpillingSortedContextWriter();
        config = new Configuration();
        tmpDir = getTmpDir();
        context = new StandaloneTaskAttemptContext(config, new StandaloneStatusReporter());
        outputContext = createMock(TaskInputOutputContext.class);
        expect(outputContext.getTaskAttemptID()).andReturn(new TaskAttemptID()).anyTimes();
        expect(outputContext.getConfiguration()).andReturn(config).anyTimes();
    }

    @After
    public void cleanup() {
        if (tmpDir != null) {
            cleanup(tmpDir);
        }
    }

    private void cleanup(File dir) {
        if (dir.isDirectory()) {
            for (File child : dir.listFiles()) {
                cleanup(child);
            }
        }

        dir.delete();
    }

    @Test(expected = IllegalStateException.class)
    public void setup_noWorkDirTest() throws IOException, InterruptedException {
        writer.setup(config, true);
    }

    @Test(expected = IllegalStateException.class)
    public void setup_workDirNotDirTest() throws IOException, InterruptedException {
        File tmpFile = File.createTempFile("prefix", "suffix");
        try {
            config.set(SpillingSortedContextWriter.WORK_DIR, tmpFile.toString());
            writer.setup(config, true);
        } finally {
            tmpFile.delete();
        }
    }

    @Test
    public void setup_tmpDirTest() throws IOException, InterruptedException {
        config.set(SpillingSortedContextWriter.WORK_DIR, tmpDir.toString());
        writer.setup(config, true);
    }

    @Test
    public void write_noSpillUntilFlushTest() throws IOException, InterruptedException {
        config.set(SpillingSortedContextWriter.WORK_DIR, tmpDir.toString());
        writer.setup(config, true);
        BulkIngestKey bik = new BulkIngestKey(new Text("testTable"), new Key("apple"));
        writer.write(bik, new Value(), context);

        assertTrue(tmpDir.listFiles().length == 0);
    }

    @Test
    public void commit_spillOnCommitTest() throws IOException, InterruptedException {
        config.set(SpillingSortedContextWriter.WORK_DIR, tmpDir.toString());
        writer.setup(config, true);
        Key testKey = new Key("apple");
        BulkIngestKey bik = new BulkIngestKey(new Text("testTable"), testKey);
        writer.write(bik, new Value(), context);
        writer.commit(context);

        assertTrue(tmpDir.listFiles().length == 1);
        File tableFile = tmpDir.listFiles()[0];
        assertTrue(tableFile.getName().equals("testTable"));
        assertTrue(tableFile.listFiles().length == 1);
        File attemptFile = tableFile.listFiles()[0];
        assertTrue(attemptFile.getName().equals("attempt__0000_r_000000_0"));
        File rf = new File(attemptFile, "0.rf");
        assertTrue(rf.exists());
        Scanner s = RFileUtil.getRFileScanner(config, rf.toString());
        s.setRange(new Range());
        Iterator<Map.Entry<Key,Value>> itr = s.iterator();
        assertTrue(itr.hasNext());
        Map.Entry e = itr.next();
        assertTrue(e.getKey().equals(testKey));
        assertFalse(itr.hasNext());
    }

    @Test
    public void commit_sortOnSpillTest() throws IOException, InterruptedException {
        config.set(SpillingSortedContextWriter.WORK_DIR, tmpDir.toString());
        writer.setup(config, true);
        Key appleKey = new Key("apple");
        Key bananaKey = new Key("banana");
        // write keys out of order
        BulkIngestKey bik = new BulkIngestKey(new Text("testTable"), bananaKey);
        writer.write(bik, new Value(), context);
        bik = new BulkIngestKey(new Text("testTable"), appleKey);
        writer.write(bik, new Value(), context);
        writer.commit(context);

        assertTrue(tmpDir.listFiles().length == 1);
        File tableFile = tmpDir.listFiles()[0];
        assertTrue(tableFile.getName().equals("testTable"));
        assertTrue(tableFile.listFiles().length == 1);
        File attemptFile = tableFile.listFiles()[0];
        assertTrue(attemptFile.getName().equals("attempt__0000_r_000000_0"));
        File rf = new File(attemptFile, "0.rf");
        assertTrue(rf.exists());
        Scanner s = RFileUtil.getRFileScanner(config, rf.toString());
        s.setRange(new Range());
        Iterator<Map.Entry<Key,Value>> itr = s.iterator();
        assertTrue(itr.hasNext());
        Map.Entry e = itr.next();
        assertTrue(e.getKey().equals(appleKey));
        assertTrue(itr.hasNext());
        e = itr.next();
        assertTrue(e.getKey().equals(bananaKey));
        assertFalse(itr.hasNext());
    }

    @Test
    public void commit_filePerFlushTest() throws IOException, InterruptedException {
        config.set(SpillingSortedContextWriter.WORK_DIR, tmpDir.toString());
        writer.setup(config, true);
        Key appleKey = new Key("apple");
        Key bananaKey = new Key("banana");
        // commit after each write
        BulkIngestKey bik = new BulkIngestKey(new Text("testTable"), bananaKey);
        writer.write(bik, new Value(), context);
        writer.commit(context);
        bik = new BulkIngestKey(new Text("testTable"), appleKey);
        writer.write(bik, new Value(), context);
        writer.commit(context);

        assertTrue(tmpDir.listFiles().length == 1);
        File tableFile = tmpDir.listFiles()[0];
        assertTrue(tableFile.getName().equals("testTable"));
        assertTrue(tableFile.listFiles().length == 1);
        File attemptFile = tableFile.listFiles()[0];
        assertTrue(attemptFile.getName().equals("attempt__0000_r_000000_0"));

        File rf = new File(attemptFile, "0.rf");
        assertTrue(rf.exists());
        File rf2 = new File(attemptFile, "1.rf");
        assertTrue(rf2.exists());

        // create a scanner across both files
        Scanner s = RFileUtil.getRFileScanner(config, rf.toString(), rf2.toString());
        s.setRange(new Range());
        Iterator<Map.Entry<Key,Value>> itr = s.iterator();
        assertTrue(itr.hasNext());
        Map.Entry e = itr.next();
        assertTrue(e.getKey().equals(appleKey));
        assertTrue(itr.hasNext());
        e = itr.next();
        assertTrue(e.getKey().equals(bananaKey));
        assertFalse(itr.hasNext());
    }

    @Test
    public void cleanup_filesToContextTest() throws IOException, InterruptedException {
        config.set(SpillingSortedContextWriter.WORK_DIR, tmpDir.toString());
        writer.setup(config, true);
        Key appleKey = new Key("apple");
        Key bananaKey = new Key("banana");
        // commit after each write
        BulkIngestKey bik = new BulkIngestKey(new Text("testTable"), bananaKey);
        writer.write(bik, new Value(), context);
        writer.commit(context);
        bik = new BulkIngestKey(new Text("testTable"), appleKey);
        writer.write(bik, new Value(), context);
        writer.commit(context);

        bik = new BulkIngestKey(new Text("testTable"), appleKey);
        outputContext.write(eq(bik), isA(Value.class));
        bik = new BulkIngestKey(new Text("testTable"), bananaKey);
        outputContext.write(eq(bik), isA(Value.class));

        replayAll();

        writer.cleanup(outputContext);

        verifyAll();
    }

    @Test
    public void rollback_noFilesWrittenTest() throws IOException, InterruptedException {
        config.set(SpillingSortedContextWriter.WORK_DIR, tmpDir.toString());
        writer.setup(config, true);
        Key testKey = new Key("apple");
        BulkIngestKey bik = new BulkIngestKey(new Text("testTable"), testKey);
        writer.write(bik, new Value(), context);
        writer.rollback();
        writer.commit(context);

        assertTrue(tmpDir.listFiles().length == 0);
    }

    @Test
    public void commit_emptyEntriesWriteNothingTest() throws IOException, InterruptedException {
        config.set(SpillingSortedContextWriter.WORK_DIR, tmpDir.toString());
        writer.setup(config, true);
        writer.commit(context);

        assertTrue(tmpDir.listFiles().length == 0);
    }

    private File getTmpDir() throws IOException {
        File tmpFile = File.createTempFile("prefix", "suffix");
        tmpFile.delete();
        tmpFile.mkdir();

        return tmpFile;
    }
}
