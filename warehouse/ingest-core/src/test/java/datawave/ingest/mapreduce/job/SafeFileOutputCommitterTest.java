package datawave.ingest.mapreduce.job;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileExistsException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * This is the FileOutputCommitterTest from after YARN-3027 and YARN-3079 was applied, with some added tests for the SafeFileOutputCommitterTest. Note that this
 * set of tests will also work with the non-patched FileOutputCommitterTest.
 */
public class SafeFileOutputCommitterTest {
    
    private static final String FILEOUTPUTCOMMITTER_ALGORITHM_VERSION = "mapreduce.fileoutputcommitter.algorithm.version";
    private static final Path outDir = new Path(System.getProperty("test.build.data", System.getProperty("java.io.tmpdir")),
                    SafeFileOutputCommitterTest.class.getName());
    
    private static final String SUB_DIR = "SUB_DIR";
    private static final Path OUT_SUB_DIR = new Path(outDir, SUB_DIR);
    
    private static final Log LOG = LogFactory.getLog(SafeFileOutputCommitterTest.class);
    
    private static final String ATTEMPT_0_ID = "attempt_200707121733_0001_m_000000_0";
    private static final String ATTEMPT_1_ID = "attempt_200707121733_0001_m_000001_0";
    
    private static final TaskAttemptID TASK_0_ID = TaskAttemptID.forName(ATTEMPT_0_ID);
    private static final TaskAttemptID TASK_1_ID = TaskAttemptID.forName(ATTEMPT_1_ID);
    
    private static final String PART_FILE_NAME = "part-m-00000";
    private static final String ALTERNATE_BASE_NAME = "segment"; // alternate prefix to "part"
    private static final String ALTERNATE_FILE_NAME = ALTERNATE_BASE_NAME + "-m-00000";
    
    private Text key1 = new Text("key1");
    private Text key2 = new Text("key2");
    private Text val1 = new Text("val1");
    private Text val2 = new Text("val2");
    
    private static boolean patched = false;
    private Configuration configuration;
    
    private static void cleanup() throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = outDir.getFileSystem(conf);
        fs.delete(outDir, true);
        
        // now determine if we have YARN-3027 and YARN-3079 patches applied
        // based on whether the FILEOUTPUTCOMMITTER_ALGORITHM_VERSION static class
        // member exists
        try {
            FileOutputCommitter.class.getDeclaredField("FILEOUTPUTCOMMITTER_ALGORITHM_VERSION");
            patched = true;
        } catch (NoSuchFieldException nsf) {
            patched = false;
        }
    }
    
    @Before
    public void setUp() throws IOException {
        configuration = new Configuration();
        cleanup();
    }
    
    @After
    public void tearDown() throws IOException {
        cleanup();
    }
    
    private void writeOutput(RecordWriter theRecordWriter, TaskAttemptContext context) throws IOException, InterruptedException {
        NullWritable nullWritable = NullWritable.get();
        
        try {
            theRecordWriter.write(key1, val1);
            theRecordWriter.write(null, nullWritable);
            theRecordWriter.write(null, val1);
            theRecordWriter.write(nullWritable, val2);
            theRecordWriter.write(key2, nullWritable);
            theRecordWriter.write(key1, null);
            theRecordWriter.write(null, null);
            theRecordWriter.write(key2, val2);
        } finally {
            theRecordWriter.close(context);
        }
    }
    
    private void writeMapFileOutput(RecordWriter theRecordWriter, TaskAttemptContext context) throws IOException, InterruptedException {
        try {
            for (int key = 0; key < 10; ++key) {
                Text val = (key % 2 == 1) ? val1 : val2;
                theRecordWriter.write(new LongWritable(key), val);
            }
        } finally {
            theRecordWriter.close(context);
        }
    }
    
    private void testRecoveryInternal(int commitVersion, int recoveryVersion) throws Exception {
        Job job = Job.getInstance();
        FileOutputFormat.setOutputPath(job, outDir);
        Configuration conf = job.getConfiguration();
        conf.set(MRJobConfig.TASK_ATTEMPT_ID, ATTEMPT_0_ID);
        conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 1);
        conf.setInt(FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, commitVersion);
        JobContext jContext = new JobContextImpl(conf, TASK_0_ID.getJobID());
        TaskAttemptContext tContext = new TaskAttemptContextImpl(conf, TASK_0_ID);
        FileOutputCommitter committer = new SafeFileOutputCommitter(outDir, tContext);
        
        // setup
        committer.setupJob(jContext);
        committer.setupTask(tContext);
        
        // write output
        TextOutputFormat theOutputFormat = new TextOutputFormat();
        RecordWriter theRecordWriter = theOutputFormat.getRecordWriter(tContext);
        writeOutput(theRecordWriter, tContext);
        
        // do commit
        committer.commitTask(tContext);
        
        Path jobTempDir1 = committer.getCommittedTaskPath(tContext);
        File jtd = new File(jobTempDir1.toUri().getPath());
        if (commitVersion == 1 || !patched) {
            assertTrue("Version 1 commits to temporary dir " + jtd, jtd.exists());
            validateContent(jtd, PART_FILE_NAME);
        } else {
            assertFalse("Version 2 commits to output dir " + jtd, jtd.exists());
        }
        
        // now while running the second app attempt,
        // recover the task output from first attempt
        Configuration conf2 = job.getConfiguration();
        conf2.set(MRJobConfig.TASK_ATTEMPT_ID, ATTEMPT_0_ID);
        conf2.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 2);
        conf2.setInt(FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, recoveryVersion);
        JobContext jContext2 = new JobContextImpl(conf2, TASK_0_ID.getJobID());
        TaskAttemptContext tContext2 = new TaskAttemptContextImpl(conf2, TASK_0_ID);
        FileOutputCommitter committer2 = new SafeFileOutputCommitter(outDir, tContext2);
        committer2.setupJob(tContext2);
        Path jobTempDir2 = committer2.getCommittedTaskPath(tContext2);
        File jtd2 = new File(jobTempDir2.toUri().getPath());
        
        committer2.recoverTask(tContext2);
        if (recoveryVersion == 1 || !patched) {
            assertTrue("Version 1 recovers to " + jtd2, jtd2.exists());
            validateContent(jtd2, PART_FILE_NAME);
        } else {
            assertFalse("Version 2 commits to output dir " + jtd2, jtd2.exists());
            if (commitVersion == 1 || !patched) {
                assertEquals("Version 2  recovery moves to output dir from " + jtd, 0, jtd.list().length);
            }
        }
        
        committer2.commitJob(jContext2);
        validateContent(outDir);
        FileUtil.fullyDelete(new File(outDir.toString()));
    }
    
    // failedAttemptWritesData - refers to whether the first attempt's files should be should contain data (true) or
    // be left empty (false)
    private void failFirstAttemptPassSecond(int commitVersion, int recoveryVersion, boolean failedAttemptWritesData, boolean useDifferentFileName)
                    throws Exception {
        Job job = Job.getInstance(this.configuration);
        FileOutputFormat.setOutputPath(job, outDir);
        Configuration conf = job.getConfiguration();
        conf.set(MRJobConfig.TASK_ATTEMPT_ID, ATTEMPT_0_ID);
        conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 1);
        conf.setInt(FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, commitVersion);
        JobContext jContext = new JobContextImpl(conf, TASK_0_ID.getJobID());
        Configuration confCopy = new Configuration(conf);
        if (useDifferentFileName) {
            confCopy.set("mapreduce.output.basename", ALTERNATE_BASE_NAME);
        }
        TaskAttemptContext tContext1 = new TaskAttemptContextImpl(confCopy, TASK_0_ID);
        FileOutputCommitter committer = new SafeFileOutputCommitter(outDir, tContext1);
        
        // setup
        committer.setupJob(jContext);
        committer.setupTask(tContext1);
        
        // write output
        TextOutputFormat theOutputFormat = new TextOutputFormat();
        
        // empty file created here:
        RecordWriter theRecordWriter = theOutputFormat.getRecordWriter(tContext1);
        
        // output file goes from empty to containing data:
        if (failedAttemptWritesData) {
            writeOutput(theRecordWriter, tContext1);
        }
        
        // Do not call commitTask for attempt #1
        // committer.commitTask(tContext);
        
        // Verify task attempt #1 still has a temporary file in the attempt path
        Path jobTempDir1 = committer.getTaskAttemptPath(tContext1);
        verifyFileInTemporaryDir(jobTempDir1.toUri().getPath(), failedAttemptWritesData, useDifferentFileName ? ALTERNATE_FILE_NAME : PART_FILE_NAME);
        
        // now run and commit a second app attempt
        Configuration conf2 = job.getConfiguration();
        conf2.set(MRJobConfig.TASK_ATTEMPT_ID, ATTEMPT_0_ID);
        conf2.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 2);
        conf2.setInt(FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, recoveryVersion);
        TaskAttemptContext tContext2 = new TaskAttemptContextImpl(conf2, TASK_0_ID);
        
        // setup
        committer.setupTask(tContext2);
        // write output
        TextOutputFormat theOutputFormat2 = new TextOutputFormat();
        
        RecordWriter theRecordWriter2 = theOutputFormat2.getRecordWriter(tContext2);
        writeOutput(theRecordWriter2, tContext2);
        
        // Attempt #2 moves file out from its attempt directory in _temporary and into the commit path
        committer.commitTask(tContext2);
        
        // Just prior to commitJob, when the SafeFileOutputCommitter will look for files left behind, verify that the
        // attempt #1 file still exists in the temporary directory
        if (configuration.getBoolean(SafeFileOutputCommitter.LENIENT_MODE, false)) {
            verifyFileInTemporaryDir(jobTempDir1.toUri().getPath(), failedAttemptWritesData, useDifferentFileName ? ALTERNATE_FILE_NAME : PART_FILE_NAME);
        }
        
        committer.commitJob(jContext);
        
        // Just after commitJob, verify that the attempt #1 file does not still exist in the temporary directory
        if (configuration.getBoolean(SafeFileOutputCommitter.LENIENT_MODE, false)) {
            File attempt1File = new File(jobTempDir1.toUri().getPath() + "/" + PART_FILE_NAME);
            assertFalse("Attempt 1 file should have been eliminated after jobCommit", attempt1File.exists());
        }
        
        validateContent(outDir);
        FileUtil.fullyDelete(new File(outDir.toString()));
    }
    
    private void verifyFileInTemporaryDir(String attempt1Path, boolean assertContainsData, String fileName) {
        File attemptOutputFile = new File(attempt1Path + "/" + fileName);
        assertTrue("Expected file to exist for attempt. " + attemptOutputFile, attemptOutputFile.exists());
        assertTrue("Expected file to be in temporary dir. " + attemptOutputFile, attempt1Path.contains("_temporary"));
        if (assertContainsData) {
            assertTrue("Expected file to be non-empty. " + attemptOutputFile, attemptOutputFile.length() > 0);
        }
    }
    
    @Test(expected = FileExistsException.class)
    public void testFirstAttemptFailsV1BackwardsCompatible() throws Exception {
        failFirstAttemptPassSecond(1, 1, true, false);
    }
    
    @Test(expected = FileExistsException.class)
    public void testFirstAttemptFailsV1BackwardsCompatibleDifferentName() throws Exception {
        failFirstAttemptPassSecond(1, 1, true, false);
    }
    
    @Test
    public void testFirstAttemptFailsV1Permissive() throws Exception {
        configuration.setBoolean(SafeFileOutputCommitter.LENIENT_MODE, true);
        failFirstAttemptPassSecond(1, 1, true, false);
    }
    
    @Test(expected = FileExistsException.class)
    public void testFirstAttemptFailsV1PermissiveDifferentName() throws Exception {
        // in this test, write data for attempt #1 using a different filename than attempt #2
        configuration.setBoolean(SafeFileOutputCommitter.LENIENT_MODE, true);
        failFirstAttemptPassSecond(1, 1, true, true);
    }
    
    @Test(expected = FileExistsException.class)
    public void testFirstAttemptFailsV2BackwardsCompatible() throws Exception {
        failFirstAttemptPassSecond(2, 2, true, false);
    }
    
    @Test(expected = FileExistsException.class)
    public void testFirstAttemptFailsV2BackwardsCompatibleDifferentName() throws Exception {
        failFirstAttemptPassSecond(2, 2, true, true);
    }
    
    @Test
    public void testFirstAttemptFailsV2Permissive() throws Exception {
        configuration.setBoolean(SafeFileOutputCommitter.LENIENT_MODE, true);
        failFirstAttemptPassSecond(2, 2, true, false);
    }
    
    @Test(expected = FileExistsException.class)
    public void testFirstAttemptFailsV2PermissiveDifferentName() throws Exception {
        // in this test, write data for attempt #1 using a different filename than attempt #2
        configuration.setBoolean(SafeFileOutputCommitter.LENIENT_MODE, true);
        failFirstAttemptPassSecond(2, 2, true, true);
    }
    
    @Test(expected = FileExistsException.class)
    public void testFirstAttemptFailsV1_V2BackwardsCompatible() throws Exception {
        failFirstAttemptPassSecond(1, 2, true, false);
    }
    
    @Test(expected = FileExistsException.class)
    public void testFirstAttemptFailsV1_V2BackwardsCompatibleDifferentName() throws Exception {
        failFirstAttemptPassSecond(1, 2, true, true);
    }
    
    @Test
    public void testFirstAttemptFailsV1_V2Permissive() throws Exception {
        configuration.setBoolean(SafeFileOutputCommitter.LENIENT_MODE, true);
        failFirstAttemptPassSecond(1, 2, true, false);
    }
    
    @Test(expected = FileExistsException.class)
    public void testFirstAttemptFailsV1_V2PermissiveDifferentName() throws Exception {
        // in this test, write data for attempt #1 using a different filename than attempt #2
        configuration.setBoolean(SafeFileOutputCommitter.LENIENT_MODE, true);
        failFirstAttemptPassSecond(1, 2, true, true);
    }
    
    @Test(expected = FileExistsException.class)
    public void testFirstAttemptFailsV1BackwardsCompatibleEmptyFile() throws Exception {
        failFirstAttemptPassSecond(1, 1, false, true);
    }
    
    @Test(expected = FileExistsException.class)
    public void testFirstAttemptFailsV1BackwardsCompatibleEmptyAlternateName() throws Exception {
        failFirstAttemptPassSecond(1, 1, false, true);
    }
    
    @Test
    public void testFirstAttemptFailsV1PermissiveEmptyFile() throws Exception {
        configuration.setBoolean(SafeFileOutputCommitter.LENIENT_MODE, true);
        failFirstAttemptPassSecond(1, 1, false, true);
    }
    
    @Test
    public void testFirstAttemptFailsV1PermissiveEmptyAlternateName() throws Exception {
        configuration.setBoolean(SafeFileOutputCommitter.LENIENT_MODE, true);
        failFirstAttemptPassSecond(1, 1, false, true);
    }
    
    @Test(expected = FileExistsException.class)
    public void testFirstAttemptFailsV2BackwardsCompatibleEmptyFile() throws Exception {
        failFirstAttemptPassSecond(2, 2, false, true);
    }
    
    @Test(expected = FileExistsException.class)
    public void testFirstAttemptFailsV2BackwardsCompatibleEmptyAlternateName() throws Exception {
        failFirstAttemptPassSecond(2, 2, false, true);
    }
    
    @Test
    public void testFirstAttemptFailsV2PermissiveEmptyFile() throws Exception {
        configuration.setBoolean(SafeFileOutputCommitter.LENIENT_MODE, true);
        failFirstAttemptPassSecond(2, 2, false, true);
    }
    
    @Test
    public void testFirstAttemptFailsV2PermissiveEmptyAlternateName() throws Exception {
        configuration.setBoolean(SafeFileOutputCommitter.LENIENT_MODE, true);
        failFirstAttemptPassSecond(2, 2, false, true);
    }
    
    @Test(expected = FileExistsException.class)
    public void testFirstAttemptFailsV1_V2BackwardsCompatibleEmptyFile() throws Exception {
        failFirstAttemptPassSecond(1, 2, false, false);
    }
    
    @Test(expected = FileExistsException.class)
    public void testFirstAttemptFailsV1_V2BackwardsCompatibleEmptyAlternateName() throws Exception {
        failFirstAttemptPassSecond(1, 2, false, true);
    }
    
    @Test
    public void testFirstAttemptFailsV1_V2PermissiveEmptyFile() throws Exception {
        configuration.setBoolean(SafeFileOutputCommitter.LENIENT_MODE, true);
        failFirstAttemptPassSecond(1, 2, false, true);
    }
    
    @Test
    public void testFirstAttemptFailsV1_V2PermissiveEmptyAlternateName() throws Exception {
        configuration.setBoolean(SafeFileOutputCommitter.LENIENT_MODE, true);
        failFirstAttemptPassSecond(1, 2, false, true);
    }
    
    @Test
    public void testRecoveryV1() throws Exception {
        testRecoveryInternal(1, 1);
    }
    
    @Test
    public void testRecoveryV2() throws Exception {
        testRecoveryInternal(2, 2);
    }
    
    @Test
    public void testRecoveryUpgradeV1V2() throws Exception {
        testRecoveryInternal(1, 2);
    }
    
    private void validateContent(Path dir) throws IOException {
        validateContent(new File(dir.toUri().getPath()), PART_FILE_NAME);
    }
    
    private void validateContent(File dir, String fileName) throws IOException {
        File expectedFile = new File(dir, fileName);
        assertTrue("Could not find " + expectedFile, expectedFile.exists());
        StringBuffer expectedOutput = new StringBuffer();
        expectedOutput.append(key1).append('\t').append(val1).append("\n");
        expectedOutput.append(val1).append("\n");
        expectedOutput.append(val2).append("\n");
        expectedOutput.append(key2).append("\n");
        expectedOutput.append(key1).append("\n");
        expectedOutput.append(key2).append('\t').append(val2).append("\n");
        String output = slurp(expectedFile);
        assertEquals(output, expectedOutput.toString());
    }
    
    private void validateMapFileOutputContent(FileSystem fs, Path dir, String fileName) throws IOException {
        // map output is a directory with index and data files
        Path expectedMapDir = new Path(dir, fileName);
        assert (fs.getFileStatus(expectedMapDir).isDirectory());
        FileStatus[] files = fs.listStatus(expectedMapDir);
        int fileCount = 0;
        boolean dataFileFound = false;
        boolean indexFileFound = false;
        for (FileStatus f : files) {
            if (f.isFile()) {
                ++fileCount;
                if (f.getPath().getName().equals(MapFile.INDEX_FILE_NAME)) {
                    indexFileFound = true;
                } else if (f.getPath().getName().equals(MapFile.DATA_FILE_NAME)) {
                    dataFileFound = true;
                }
            }
        }
        assert (fileCount > 0);
        assert (dataFileFound && indexFileFound);
    }
    
    private void testCommitterInternal(int version) throws Exception {
        Job job = Job.getInstance();
        FileOutputFormat.setOutputPath(job, outDir);
        Configuration conf = job.getConfiguration();
        conf.set(MRJobConfig.TASK_ATTEMPT_ID, ATTEMPT_0_ID);
        conf.setInt(FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, version);
        JobContext jContext = new JobContextImpl(conf, TASK_0_ID.getJobID());
        TaskAttemptContext tContext = new TaskAttemptContextImpl(conf, TASK_0_ID);
        FileOutputCommitter committer = new SafeFileOutputCommitter(outDir, tContext);
        
        // setup
        committer.setupJob(jContext);
        committer.setupTask(tContext);
        
        // write output
        TextOutputFormat theOutputFormat = new TextOutputFormat();
        RecordWriter theRecordWriter = theOutputFormat.getRecordWriter(tContext);
        writeOutput(theRecordWriter, tContext);
        
        // do commit
        committer.commitTask(tContext);
        committer.commitJob(jContext);
        
        // validate output
        validateContent(outDir);
        FileUtil.fullyDelete(new File(outDir.toString()));
    }
    
    @Test
    public void testCommitterV1() throws Exception {
        testCommitterInternal(1);
    }
    
    @Test
    public void testCommitterV2() throws Exception {
        testCommitterInternal(2);
    }
    
    private void testMapFileOutputCommitterInternal(int version) throws Exception {
        Job job = Job.getInstance();
        FileOutputFormat.setOutputPath(job, outDir);
        Configuration conf = job.getConfiguration();
        conf.set(MRJobConfig.TASK_ATTEMPT_ID, ATTEMPT_0_ID);
        conf.setInt(FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, version);
        JobContext jContext = new JobContextImpl(conf, TASK_0_ID.getJobID());
        TaskAttemptContext tContext = new TaskAttemptContextImpl(conf, TASK_0_ID);
        FileOutputCommitter committer = new SafeFileOutputCommitter(outDir, tContext);
        
        // setup
        committer.setupJob(jContext);
        committer.setupTask(tContext);
        
        // write output
        MapFileOutputFormat theOutputFormat = new MapFileOutputFormat();
        RecordWriter theRecordWriter = theOutputFormat.getRecordWriter(tContext);
        writeMapFileOutput(theRecordWriter, tContext);
        
        // do commit
        committer.commitTask(tContext);
        committer.commitJob(jContext);
        
        // validate output
        validateMapFileOutputContent(FileSystem.get(job.getConfiguration()), outDir, PART_FILE_NAME);
        FileUtil.fullyDelete(new File(outDir.toString()));
    }
    
    @Test
    public void testMapFileOutputCommitterV1() throws Exception {
        testMapFileOutputCommitterInternal(1);
    }
    
    @Test
    public void testMapFileOutputCommitterV2() throws Exception {
        testMapFileOutputCommitterInternal(2);
    }
    
    private void testAbortInternal(int version) throws IOException, InterruptedException {
        Job job = Job.getInstance();
        FileOutputFormat.setOutputPath(job, outDir);
        Configuration conf = job.getConfiguration();
        conf.set(MRJobConfig.TASK_ATTEMPT_ID, ATTEMPT_0_ID);
        conf.setInt(FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, version);
        JobContext jContext = new JobContextImpl(conf, TASK_0_ID.getJobID());
        TaskAttemptContext tContext = new TaskAttemptContextImpl(conf, TASK_0_ID);
        FileOutputCommitter committer = new SafeFileOutputCommitter(outDir, tContext);
        
        // do setup
        committer.setupJob(jContext);
        committer.setupTask(tContext);
        
        // write output
        TextOutputFormat theOutputFormat = new TextOutputFormat();
        RecordWriter theRecordWriter = theOutputFormat.getRecordWriter(tContext);
        writeOutput(theRecordWriter, tContext);
        
        // do abort
        committer.abortTask(tContext);
        File expectedFile = new File(new Path(committer.getWorkPath(), PART_FILE_NAME).toString());
        assertFalse("task temp dir still exists", expectedFile.exists());
        
        committer.abortJob(jContext, JobStatus.State.FAILED);
        expectedFile = new File(new Path(outDir, FileOutputCommitter.PENDING_DIR_NAME).toString());
        assertFalse("job temp dir still exists", expectedFile.exists());
        assertEquals("Output directory not empty", 0, new File(outDir.toString()).listFiles().length);
        FileUtil.fullyDelete(new File(outDir.toString()));
    }
    
    @Test
    public void testAbortV1() throws IOException, InterruptedException {
        testAbortInternal(1);
    }
    
    @Test
    public void testAbortV2() throws IOException, InterruptedException {
        testAbortInternal(2);
    }
    
    public static class FakeFileSystem extends RawLocalFileSystem {
        
        public URI getUri() {
            return URI.create("faildel:///");
        }
        
        @Override
        public boolean delete(Path p, boolean recursive) throws IOException {
            throw new IOException("fake delete failed");
        }
    }
    
    private void testFailAbortInternal(int version) throws IOException, InterruptedException {
        Job job = Job.getInstance();
        Configuration conf = job.getConfiguration();
        conf.set(FileSystem.FS_DEFAULT_NAME_KEY, "faildel:///");
        conf.setClass("fs.faildel.impl", FakeFileSystem.class, FileSystem.class);
        conf.set(MRJobConfig.TASK_ATTEMPT_ID, ATTEMPT_0_ID);
        conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 1);
        conf.setInt(FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, version);
        FileOutputFormat.setOutputPath(job, outDir);
        JobContext jContext = new JobContextImpl(conf, TASK_0_ID.getJobID());
        TaskAttemptContext tContext = new TaskAttemptContextImpl(conf, TASK_0_ID);
        FileOutputCommitter committer = new SafeFileOutputCommitter(outDir, tContext);
        
        // do setup
        committer.setupJob(jContext);
        committer.setupTask(tContext);
        
        // write output
        TextOutputFormat<?,?> theOutputFormat = new TextOutputFormat();
        RecordWriter<?,?> theRecordWriter = theOutputFormat.getRecordWriter(tContext);
        writeOutput(theRecordWriter, tContext);
        
        // do abort
        Throwable th = null;
        try {
            committer.abortTask(tContext);
        } catch (IOException ie) {
            th = ie;
        }
        assertNotNull(th);
        assertTrue(th instanceof IOException);
        assertTrue(th.getMessage().contains("fake delete failed"));
        Path jtd = committer.getJobAttemptPath(jContext);
        File jobTmpDir = new File(jtd.toUri().getPath());
        Path ttd = committer.getTaskAttemptPath(tContext);
        File taskTmpDir = new File(ttd.toUri().getPath());
        File expectedFile = new File(taskTmpDir, PART_FILE_NAME);
        assertTrue(expectedFile + " does not exists", expectedFile.exists());
        
        th = null;
        try {
            committer.abortJob(jContext, JobStatus.State.FAILED);
        } catch (IOException ie) {
            th = ie;
        }
        assertNotNull(th);
        assertTrue(th instanceof IOException);
        assertTrue(th.getMessage().contains("fake delete failed"));
        assertTrue("job temp dir does not exists", jobTmpDir.exists());
        FileUtil.fullyDelete(new File(outDir.toString()));
    }
    
    @Test
    public void testFailAbortV1() throws Exception {
        testFailAbortInternal(1);
    }
    
    @Test
    public void testFailAbortV2() throws Exception {
        testFailAbortInternal(2);
    }
    
    static class RLFS extends RawLocalFileSystem {
        private final ThreadLocal<Boolean> needNull = ThreadLocal.withInitial(() -> true);
        
        public RLFS() {}
        
        @Override
        public FileStatus getFileStatus(Path f) throws IOException {
            if (needNull.get() && OUT_SUB_DIR.toUri().getPath().equals(f.toUri().getPath())) {
                needNull.set(false); // lie once per thread
                return null;
            }
            return super.getFileStatus(f);
        }
    }
    
    private void testConcurrentCommitTaskWithSubDir(int version) throws Exception {
        final Job job = Job.getInstance();
        FileOutputFormat.setOutputPath(job, outDir);
        final Configuration conf = job.getConfiguration();
        conf.set(MRJobConfig.TASK_ATTEMPT_ID, ATTEMPT_0_ID);
        conf.setInt(FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, version);
        
        conf.setClass("fs.file.impl", RLFS.class, FileSystem.class);
        FileSystem.closeAll();
        
        final JobContext jContext = new JobContextImpl(conf, TASK_0_ID.getJobID());
        final FileOutputCommitter amCommitter = new SafeFileOutputCommitter(outDir, jContext);
        amCommitter.setupJob(jContext);
        
        final TaskAttemptContext[] taCtx = new TaskAttemptContextImpl[2];
        taCtx[0] = new TaskAttemptContextImpl(conf, TASK_0_ID);
        taCtx[1] = new TaskAttemptContextImpl(conf, TASK_1_ID);
        
        final TextOutputFormat[] tof = new TextOutputFormat[2];
        for (int i = 0; i < tof.length; i++) {
            tof[i] = new TextOutputFormat() {
                @Override
                public Path getDefaultWorkFile(TaskAttemptContext context, String extension) throws IOException {
                    final FileOutputCommitter foc = (FileOutputCommitter) getOutputCommitter(context);
                    return new Path(new Path(foc.getWorkPath(), SUB_DIR), getUniqueFile(context, getOutputName(context), extension));
                }
            };
        }
        
        final ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            for (int i = 0; i < taCtx.length; i++) {
                final int taskIdx = i;
                executor.submit((Callable<Void>) () -> {
                    final OutputCommitter outputCommitter = tof[taskIdx].getOutputCommitter(taCtx[taskIdx]);
                    outputCommitter.setupTask(taCtx[taskIdx]);
                    final RecordWriter rw = tof[taskIdx].getRecordWriter(taCtx[taskIdx]);
                    writeOutput(rw, taCtx[taskIdx]);
                    outputCommitter.commitTask(taCtx[taskIdx]);
                    return null;
                });
            }
        } finally {
            executor.shutdown();
            while (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
                LOG.info("Awaiting thread termination!");
            }
        }
        
        amCommitter.commitJob(jContext);
        final RawLocalFileSystem lfs = new RawLocalFileSystem();
        lfs.setConf(conf);
        assertFalse("Must not end up with sub_dir/sub_dir", lfs.exists(new Path(OUT_SUB_DIR, SUB_DIR)));
        
        // validate output
        validateContent(OUT_SUB_DIR);
        FileUtil.fullyDelete(new File(outDir.toString()));
    }
    
    @Test
    public void testConcurrentCommitTaskWithSubDirV1() throws Exception {
        testConcurrentCommitTaskWithSubDir(1);
    }
    
    @Test
    public void testConcurrentCommitTaskWithSubDirV2() throws Exception {
        testConcurrentCommitTaskWithSubDir(2);
    }
    
    private void testSafety(int commitVersion) throws Exception {
        Job job = Job.getInstance();
        FileOutputFormat.setOutputPath(job, outDir);
        Configuration conf = job.getConfiguration();
        conf.set(MRJobConfig.TASK_ATTEMPT_ID, ATTEMPT_0_ID);
        conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 1);
        conf.setInt(FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, commitVersion);
        JobContext jContext = new JobContextImpl(conf, TASK_0_ID.getJobID());
        TaskAttemptContext tContext = new TaskAttemptContextImpl(conf, TASK_0_ID);
        FileOutputCommitter committer = new SafeFileOutputCommitter(outDir, tContext);
        
        // setup
        committer.setupJob(jContext);
        committer.setupTask(tContext);
        
        // write output
        TextOutputFormat theOutputFormat = new TextOutputFormat();
        RecordWriter theRecordWriter = theOutputFormat.getRecordWriter(tContext);
        writeOutput(theRecordWriter, tContext);
        
        // close the job prior to committing task (leaving files in temporary dir
        try {
            committer.commitJob(jContext);
            Assert.fail("Expected commit job to fail");
        } catch (Exception e) {
            committer.commitTask(tContext);
            committer.commitJob(jContext);
        }
        validateContent(outDir);
        FileUtil.fullyDelete(new File(outDir.toString()));
    }
    
    @Test
    public void testSafetyWithSubDirV1() throws Exception {
        testSafety(1);
    }
    
    @Test
    public void testSafetyWithSubDirV2() throws Exception {
        testSafety(2);
    }
    
    public static String slurp(File f) throws IOException {
        int len = (int) f.length();
        byte[] buf = new byte[len];
        FileInputStream in = new FileInputStream(f);
        String contents = null;
        try {
            in.read(buf, 0, len);
            contents = new String(buf, "UTF-8");
        } finally {
            in.close();
        }
        return contents;
    }
    
}
