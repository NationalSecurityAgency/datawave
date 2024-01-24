package datawave.ingest.mapreduce.job;

import static datawave.ingest.mapreduce.job.BulkIngestMapFileLoader.COMPLETE_FILE_MARKER;
import static datawave.ingest.mapreduce.job.BulkIngestMapFileLoader.FAILED_FILE_MARKER;
import static datawave.ingest.mapreduce.job.BulkIngestMapFileLoader.INPUT_FILES_MARKER;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.powermock.api.easymock.PowerMock;
import org.powermock.reflect.Whitebox;

import com.google.common.collect.Multimap;

import datawave.common.test.integration.IntegrationTest;
import datawave.common.test.logging.TestLogCollector;
import datawave.common.test.utils.ProcessUtils;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.ingest.BaseIngestHelper;
import datawave.ingest.input.reader.EventRecordReader;
import datawave.ingest.input.reader.LongLineEventRecordReader;

@Category(IntegrationTest.class)
public class BulkIngestMapFileLoaderTest {

    @ClassRule
    public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    protected static final URI FILE_SYSTEM_URI = URI.create("file:///");
    protected static final Logger LOG = Logger.getLogger(BulkIngestMapFileLoaderTest.class);

    private static final String PASSWORD = "secret";

    private static final String USER = "root";

    private static final Authorizations USER_AUTHS = new Authorizations("BAR", "FOO", "PRIVATE", "PUBLIC");

    private static final String METADATA_TABLE = "metadata";
    private static final String METADATA_RFILE_PATH = "/datawave/rfiles/metadata/I3abcdef01.rf";

    private static final String SHARD_TABLE = "shard";
    private static final String SHARD_RFILE_PATH = "/datawave/rfiles/shard/I2abcdef01.rf";

    private static MiniAccumuloCluster cluster;
    private static File tmpDir;
    private static java.nio.file.Path workPath;
    private static java.nio.file.Path flaggedPath;
    private static java.nio.file.Path loadedPath;
    private static URI metadataRfile;
    private static URI shardRfile;

    @Rule
    public final ExpectedSystemExit exit = ExpectedSystemExit.none();

    @Rule
    public TestLogCollector logCollector = new TestLogCollector.Builder().with(BulkIngestMapFileLoader.class, Level.ALL).build();

    protected Level testDriverLevel;

    private List<String> systemProperties;
    private Configuration conf = new Configuration();

    @BeforeClass
    public static void setupClass() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException, IOException,
                    InterruptedException, URISyntaxException {
        tmpDir = temporaryFolder.newFolder();
        cluster = new MiniAccumuloCluster(tmpDir, PASSWORD);
        cluster.start();

        workPath = Paths.get(tmpDir.getAbsolutePath(), "datawave", "ingest", "work");
        Files.createDirectories(workPath);

        flaggedPath = Files.createDirectory(Paths.get(workPath.toString(), "flagged"));
        loadedPath = Files.createDirectory(Paths.get(workPath.toString(), "loaded"));

        metadataRfile = BulkIngestMapFileLoaderTest.class.getResource(METADATA_RFILE_PATH).toURI();
        shardRfile = BulkIngestMapFileLoaderTest.class.getResource(SHARD_RFILE_PATH).toURI();

        try (AccumuloClient client = cluster.createAccumuloClient(USER, new PasswordToken(PASSWORD))) {
            if (!client.tableOperations().exists(METADATA_TABLE)) {
                client.tableOperations().create(METADATA_TABLE);
            }
            if (!client.tableOperations().exists(SHARD_TABLE)) {
                client.tableOperations().create(SHARD_TABLE);
            }
            client.securityOperations().changeUserAuthorizations(USER, USER_AUTHS);
        }
    }

    /**
     * Sets up all inputs required to process a completed ingest job (job.complete) against the running MAC
     *
     * @param jobName
     *            should uniquely identify the bulk load job to be run
     * @param loaderSleepTime
     *            desired sleep time (in ms) for the bulk loader
     *
     * @return BulkIngestMapFileLoader instance for running the job
     * @throws IOException
     */
    private BulkIngestMapFileLoader setupJobComplete(String jobName, int loaderSleepTime) throws IOException {

        Assert.assertFalse("jobName can't be null/empty", jobName == null || jobName.isEmpty());

        java.nio.file.Path metaSrc, metaDest, shardSrc, shardDest, inputFilesPath, inputFile, jobPathsFile;

        String parentDir = workPath.toString();
        String mapFilesDir = "mapFiles";

        Assert.assertFalse(jobName + " directory already exists", Files.exists(Paths.get(parentDir, jobName)));
        Assert.assertFalse(jobName + " flagged directory already exists", Files.exists(Paths.get(flaggedPath.toString(), jobName)));
        Assert.assertFalse(jobName + " loaded directory already exists", Files.exists(Paths.get(loadedPath.toString(), jobName)));

        // Copy metadata rfile into jobName/mapFiles/DW_METADATA_TABLE dir
        metaSrc = Paths.get(metadataRfile);
        metaDest = Files.createDirectories(Paths.get(parentDir, jobName, mapFilesDir, METADATA_TABLE));
        Files.copy(metaSrc, Paths.get(metaDest.toString(), metaSrc.getFileName().toString()));

        // Copy shard rfile into jobName/mapFiles/DW_SHARD_TABLE dir
        shardSrc = Paths.get(shardRfile);
        shardDest = Files.createDirectories(Paths.get(parentDir, jobName, mapFilesDir, SHARD_TABLE));
        Files.copy(shardSrc, Paths.get(shardDest.toString(), shardSrc.getFileName().toString()));

        // Create 'job.paths' marker and associated dummy input file...
        inputFilesPath = Files.createDirectory(Paths.get(flaggedPath.toString(), jobName));
        inputFile = Files.createFile(Paths.get(inputFilesPath.toString(), "dummy"));
        jobPathsFile = Files.createFile(Paths.get(parentDir, jobName, INPUT_FILES_MARKER));
        Files.write(jobPathsFile, inputFile.toString().getBytes(StandardCharsets.UTF_8));

        // Create 'job.complete' marker
        Files.createFile(Paths.get(parentDir, jobName, COMPLETE_FILE_MARKER));

        // @formatter:off
        return new BulkIngestMapFileLoader(
                workPath.toString(),
                "*",
                cluster.getInstanceName(),
                cluster.getZooKeepers(),
                USER,
                new PasswordToken(PASSWORD),
                tmpDir.toURI(),
                tmpDir.toURI(),
                tmpDir.toURI(),
                null,
                new HashMap<>(),
                conf,
                0,
                1,
                new ArrayList<>(),
                loaderSleepTime,
                loaderSleepTime,
                false);
        // @formatter:on
    }

    private void verifyImportedData() throws TableNotFoundException {

        long shardKeyCount = 0;
        long metaKeyCount = 0;

        Collection ranges = Collections.singleton(new Range());
        try (AccumuloClient client = cluster.createAccumuloClient(USER, new PasswordToken(PASSWORD))) {
            // Count shard keys
            BatchScanner scanner = client.createBatchScanner(SHARD_TABLE, USER_AUTHS);
            scanner.setRanges(ranges);
            Iterator it = scanner.iterator();
            while (it.hasNext()) {
                it.next();
                shardKeyCount++;
            }
            scanner.close();

            // Count metadata keys
            scanner = client.createBatchScanner(METADATA_TABLE, USER_AUTHS);
            scanner.setRanges(ranges);
            it = scanner.iterator();
            while (it.hasNext()) {
                it.next();
                metaKeyCount++;
            }
            scanner.close();
        }
        Assert.assertEquals("Unexpected number of shard entries", 16301, shardKeyCount);
        Assert.assertEquals("Unexpected number of metadata entries", 380, metaKeyCount);
    }

    @AfterClass
    public static void teardownClass() throws IOException {
        cluster.close();
    }

    @Test
    public void testShutdownPortAlreadyInUse() throws IOException {
        exit.expectSystemExitWithStatus(-3);
        try (final ServerSocket socket = new ServerSocket(0)) {
            new BulkIngestMapFileLoader(".", null, "test", "localhost:2181", "root", new PasswordToken(""), null, null, null, null, null, null,
                            socket.getLocalPort());
        }
    }

    public static class WrappedPositionedReadable extends InputStream implements PositionedReadable, Seekable {

        protected long position = 0;
        protected ByteArrayInputStream mockedInputStream = null;

        public WrappedPositionedReadable(ByteArrayInputStream mis) {

            mockedInputStream = mis;
        }

        @Override
        public int read(long position, byte[] buffer, int offset, int length) throws IOException {

            this.position = position;

            mockedInputStream.skip(position);

            return mockedInputStream.read(buffer, offset, length);
        }

        @Override
        public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {

            read(position, buffer, offset, length);
        }

        @Override
        public void readFully(long position, byte[] buffer) throws IOException {

            readFully(position, buffer, 0, buffer.length);
        }

        @Override
        public int read() throws IOException {

            return mockedInputStream.read();
        }

        @Override
        public void seek(long pos) throws IOException {

            position = pos;

            mockedInputStream.reset();
            mockedInputStream.skip(pos);
        }

        @Override
        public long getPos() throws IOException {

            return position;
        }

        @Override
        public boolean seekToNewSource(long targetPos) throws IOException {

            return false;
        }

    }

    public static class WrappedLocalFileSystem extends RawLocalFileSystem {

        protected ByteArrayInputStream mockedInputStream = null;
        protected FileStatus[] globStatusResults = null;
        protected boolean mkdirsResults = false;
        protected boolean renameResults = false;
        protected boolean deleteResults = false;
        protected boolean createNewFileResults = false;
        protected Map<String,Boolean> existsResults = null;
        protected boolean renameThrowsException = false;
        protected boolean existsThrowsException = false;
        protected List<String> wrappedFilsSystemCallParameters = new ArrayList<>();

        public List<String> callsLogs() {

            return wrappedFilsSystemCallParameters;
        }

        public WrappedLocalFileSystem(ByteArrayInputStream mis, FileStatus[] gsr, boolean mk, boolean rename, boolean delete, boolean create,
                        Map<String,Boolean> exists, boolean renameThrows, boolean existsThrows) {

            mockedInputStream = mis;
            globStatusResults = gsr;
            mkdirsResults = mk;
            renameResults = rename;
            deleteResults = delete;
            createNewFileResults = create;
            existsResults = exists;
            renameThrowsException = renameThrows;
            existsThrowsException = existsThrows;

        }

        @Override
        public FileStatus[] globStatus(Path pathPattern) throws IOException {

            return globStatusResults;
        }

        @Override
        public FSDataInputStream open(Path f) throws IOException {

            InputStream is = new BulkIngestMapFileLoaderTest.WrappedPositionedReadable(mockedInputStream);

            return new FSDataInputStream(is);
        }

        @Override
        public boolean mkdirs(Path f) throws IOException {

            wrappedFilsSystemCallParameters.add(String.format("FileSystem#mkdirs(%s)", f.toString()));

            return mkdirsResults;
        }

        @Override
        public boolean rename(Path src, Path dst) throws IOException {

            wrappedFilsSystemCallParameters.add(String.format("FileSystem#rename(%s, %s)", src.toString(), dst.toString()));

            if (renameThrowsException) {

                throw new IOException("This is only a test exception - IT CAN BE IGNORED.");
            }

            return renameResults;
        }

        @Override
        public boolean delete(Path p, boolean recursive) throws IOException {

            wrappedFilsSystemCallParameters.add(String.format("FileSystem#delete(%s, %s)", p.toString(), recursive));

            return deleteResults;
        }

        @Override
        public boolean createNewFile(Path f) throws IOException {

            wrappedFilsSystemCallParameters.add(String.format("FileSystem#createNewFile(%s)", f.toString()));

            return createNewFileResults;
        }

        @Override
        public boolean exists(Path f) throws IOException {

            wrappedFilsSystemCallParameters.add(String.format("FileSystem#exists(%s)", f.toString()));

            if (existsThrowsException) {

                throw new IOException("This is only a test exception - IT CAN BE IGNORED.");
            }
            return existsResults.get(f.toString()) != null && existsResults.get(f.toString()) == true;
        }

    }

    public static class TestRecordReader extends RecordReader<Text,RawRecordContainer> implements EventRecordReader {

        @Override
        public void setInputDate(long time) {
            // TODO Auto-generated method stub

        }

        @Override
        public RawRecordContainer getEvent() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            // TODO Auto-generated method stub

        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            return false;
        }

        @Override
        public Text getCurrentKey() throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public RawRecordContainer getCurrentValue() throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public void close() throws IOException {
            // TODO Auto-generated method stub

        }

        @Override
        public String getRawInputFileName() {
            // TODO: Auto-generated method stub
            return null;
        }

        @Override
        public long getRawInputFileTimestamp() {
            // TODO: Auto-generated method stub
            return 0;
        }

        @Override
        public RawRecordContainer enforcePolicy(RawRecordContainer event) {
            return null;
        }

        @Override
        public void initializeEvent(Configuration conf) throws IOException {
            // TODO Auto-generated method stub

        }
    }

    public static class TestIngestHelper extends BaseIngestHelper {

        @Override
        public Multimap<String,NormalizedContentInterface> getEventFields(RawRecordContainer event) {
            // TODO Auto-generated method stub
            return null;
        }

    }

    public static class TestReader extends LongLineEventRecordReader {

    }

    protected ByteArrayInputStream createMockInputStream() {
        return createMockInputStream(null);
    }

    protected ByteArrayInputStream createMockInputStream(String[] additionalEntries) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(baos))) {
                for (int index = 0; index < 3; index++) {
                    bw.write(String.format("/flagged/file%d\n", index));
                }
                if (null != additionalEntries) {
                    for (String entries : additionalEntries) {
                        bw.write(entries);
                    }
                }
            } catch (IOException ex) {}
            return new ByteArrayInputStream(baos.toByteArray());
        } catch (IOException e) {}
        return null;
    }

    protected FileStatus createMockFileStatus() {

        FileStatus mocked = PowerMock.createMock(FileStatus.class);
        PowerMock.replay(mocked);

        return mocked;
    }

    protected FileStatus createMockFileStatus(Path path) throws Exception {

        FileStatus mocked = PowerMock.createMock(FileStatus.class);
        PowerMock.expectPrivate(mocked, "getPath").andReturn(path);

        PowerMock.replay(mocked);

        return mocked;
    }

    protected boolean processOutputContains(List<String> output, String message) {
        boolean results = false;

        for (String msg : output) {

            results = msg.contains(message);

            if (results) {

                break;
            }
        }

        return results;
    }

    protected List<String> retrieveUUTLogs() throws IOException {
        return logCollector.getMessages();
    }

    @Before
    public void setup() throws Exception {
        systemProperties = new ArrayList<>();

        testDriverLevel = BulkIngestMapFileLoaderTest.LOG.getLevel();
        BulkIngestMapFileLoaderTest.LOG.setLevel(Level.ALL);
    }

    @After
    public void teardown() {
        BulkIngestMapFileLoaderTest.LOG.setLevel(testDriverLevel);
    }

    @Test
    public void testMainWithoutArgs() throws IOException, InterruptedException {

        BulkIngestMapFileLoaderTest.LOG.info("testMainWitoutArgs called...");

        try {

            List<String> cmdList = ProcessUtils.buildApplicationCommandLine(BulkIngestMapFileLoader.class.getName(), systemProperties, false);
            String[] cmdArray = ProcessUtils.convertCommandLine(cmdList);

            Map<String,String> newEnvironment = new HashMap<>();
            List<String> dropFromEnvironment = new ArrayList<>();
            File workingDirectory = new File(System.getProperty("user.dir"));

            Process proc = ProcessUtils.runInstance(cmdArray, newEnvironment, dropFromEnvironment, workingDirectory);

            int procResults = proc.waitFor();

            Assert.assertEquals("BulkIngestMapLoader#main failed to return the expected value.", ProcessUtils.SYSTEM_EXIT_MINUS_ONE, procResults);

            List<String> stdOut = ProcessUtils.getStandardOutDumps(proc);

            Assert.assertTrue("BulkIngestMapLoader#main failed to generate the expected error message",
                            processOutputContains(stdOut, "usage: BulkIngestMapFileLoader "));

        } finally {

            BulkIngestMapFileLoaderTest.LOG.info("testMainWitoutArgs completed.");

        }
    }

    @Test
    public void testMainWithSixArgs() throws IOException, InterruptedException {

        BulkIngestMapFileLoaderTest.LOG.info("testMainWithSixArgs called...");

        try {

            List<String> cmdList = ProcessUtils.buildApplicationCommandLine(BulkIngestMapFileLoader.class.getName(), systemProperties, false);

            for (int counter = 0; counter < 6; counter++) {

                cmdList.add(String.format("%d", counter));
            }
            String[] cmdArray = ProcessUtils.convertCommandLine(cmdList);

            Map<String,String> newEnvironment = new HashMap<>();
            List<String> dropFromEnvironment = new ArrayList<>();
            File workingDirectory = new File(System.getProperty("user.dir"));

            Process proc = ProcessUtils.runInstance(cmdArray, newEnvironment, dropFromEnvironment, workingDirectory);

            int procResults = proc.waitFor();

            Assert.assertEquals("BulkIngestMapLoader#main failed to return the expected value.", ProcessUtils.SYSTEM_EXIT_MINUS_TWO, procResults);

            List<String> stdOut = ProcessUtils.getStandardOutDumps(proc);

            Assert.assertTrue("BulkIngestMapLoader#main failed to generate the expected error message",
                            processOutputContains(stdOut, "Configured data types is empty"));

        } finally {

            BulkIngestMapFileLoaderTest.LOG.info("testMainWithSixArgs completed.");

        }
    }

    @Test
    public void testMainWithAllOptionalArgs() throws IOException, InterruptedException {

        BulkIngestMapFileLoaderTest.LOG.info("testMainWithAllOptionalArgs called...");

        try {

            List<String> cmdList = ProcessUtils.buildApplicationCommandLine(BulkIngestMapFileLoader.class.getName(), systemProperties, false);

            for (int counter = 0; counter < 6; counter++) {

                cmdList.add(String.format("%d", counter));
            }

            cmdList.add("-sleepTime");
            cmdList.add("100");

            cmdList.add("-majcThreshold");
            cmdList.add("10");

            cmdList.add("-majcDelay");
            cmdList.add("20");

            cmdList.add("-maxDirectories");
            cmdList.add("15");

            cmdList.add("-numThreads");
            cmdList.add("9");

            cmdList.add("-numAssignThreads");
            cmdList.add("6");

            cmdList.add("-seqFileHdfs");
            cmdList.add(".");

            cmdList.add("-srcHdfs");
            cmdList.add(".");

            cmdList.add("-destHdfs");
            cmdList.add(".");

            cmdList.add("-jt");
            cmdList.add("localhost");

            cmdList.add("-shutdownPort");
            cmdList.add("0");

            cmdList.add("-property1=hello, world!");

            String[] cmdArray = ProcessUtils.convertCommandLine(cmdList);

            Map<String,String> newEnvironment = new HashMap<>();
            List<String> dropFromEnvironment = new ArrayList<>();
            File workingDirectory = new File(System.getProperty("user.dir"));

            Process proc = ProcessUtils.runInstance(cmdArray, newEnvironment, dropFromEnvironment, workingDirectory);

            int procResults = proc.waitFor();

            Assert.assertEquals("BulkIngestMapLoader#main failed to return the expected value.", ProcessUtils.SYSTEM_EXIT_MINUS_TWO, procResults);

            List<String> stdOut = ProcessUtils.getStandardOutDumps(proc);

            Assert.assertTrue("BulkIngestMapLoader#main failed to generate the expected error message",
                            processOutputContains(stdOut, "Configured data types is empty"));

        } finally {

            BulkIngestMapFileLoaderTest.LOG.info("testMainWithAllOptionalArgs completed.");

        }
    }

    /**
     * Use MAC to verify that bulk loader actually loads rfiles into shard, metadata tables successfully
     */
    @Test
    public void testLoaderWithMiniAccumuloCluster() {
        BulkIngestMapFileLoaderTest.LOG.info("testLoaderWithMiniAccumuloCluster called...");

        List<String> log = logCollector.getMessages();
        Assert.assertTrue("Unexpected log messages", log.isEmpty());

        BulkIngestMapFileLoader processor = null;
        try {
            processor = setupJobComplete("job1", 1000);
            new Thread(processor, "map-file-watcher").start();

            // Wait up to 30 secs for the bulk loader to log completion
            for (int i = 1; i <= 15; i++) {
                Thread.sleep(2000);
                if (log.contains("Marking 1 sequence files from flagged to loaded")) {
                    break;
                }
            }

            Assert.assertTrue("Unexpected log output", log.contains("Bringing Map Files online for " + METADATA_TABLE));
            Assert.assertTrue("Unexpected log output", log.contains("Bringing Map Files online for " + SHARD_TABLE));
            Assert.assertTrue("Unexpected log output", log.contains("Completed bringing map files online for " + METADATA_TABLE));
            Assert.assertTrue("Unexpected log output", log.contains("Completed bringing map files online for " + SHARD_TABLE));
            Assert.assertTrue("Unexpected log output", log.contains("Marking 1 sequence files from flagged to loaded"));

            verifyImportedData();

        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            BulkIngestMapFileLoaderTest.LOG.info("testLoaderWithMiniAccumuloCluster completed.");
            if (processor != null) {
                processor.shutdown();
            }
        }
    }

    /**
     * Use MAC to verify that bulk loader fails as expected in the face of invalid rfile(s)
     */
    @Test
    public void testLoadFailedWithMiniAccumuloCluster() {
        BulkIngestMapFileLoaderTest.LOG.info("testLoadFailedWithMiniAccumuloCluster called...");

        List<String> log = logCollector.getMessages();
        Assert.assertTrue("Unexpected log messages", log.isEmpty());

        String jobName = "job2";

        java.nio.file.Path metaRfile, failedMarker;

        // expected marker file
        failedMarker = Paths.get(workPath.toString(), jobName, FAILED_FILE_MARKER);

        BulkIngestMapFileLoader processor = null;
        try {
            // Create/configure 'job2'
            processor = setupJobComplete(jobName, 500);

            // rfile to corrupt...
            metaRfile = Paths.get(workPath.toString(), jobName, "mapFiles", METADATA_TABLE, "I3abcdef01.rf");

            Assert.assertTrue("metadata rfile is missing after setup", Files.exists(metaRfile));

            // Write invalid content...
            Files.delete(metaRfile);
            Files.createFile(metaRfile);
            Files.write(metaRfile, "Invalid rfile content here".getBytes(StandardCharsets.UTF_8));

            String expectedMsg = "Error importing files into table " + METADATA_TABLE + " from directory file:"
                            + Paths.get(workPath.toString(), jobName, "mapFiles");

            // Start the loader
            new Thread(processor, "map-file-watcher").start();

            // Wait up to 30 secs for the bulk loader to log the failure
            for (int i = 1; i <= 10; i++) {
                Thread.sleep(3000);
                if (log.contains(expectedMsg)) {
                    break;
                }
            }

            Assert.assertTrue("Unexpected log output", log.contains("Bringing Map Files online for " + METADATA_TABLE));
            Assert.assertTrue("Unexpected log output", log.contains(expectedMsg));
            Assert.assertTrue("Bad metadata rfile should have remained in the job dir: " + metaRfile, Files.exists(metaRfile));
            Assert.assertTrue("Missing 'job.failed' marker after failed import", Files.exists(failedMarker));

        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            BulkIngestMapFileLoaderTest.LOG.info("testLoadFailedWithMiniAccumuloCluster completed.");
            if (processor != null) {
                processor.shutdown();
            }
        }
    }

    @Test
    public void testMainWithAllOptionalArgsNoTablePriorites() throws IOException, InterruptedException {

        BulkIngestMapFileLoaderTest.LOG.info("testMainWithAllOptionalArgsNoTablePriorites called...");

        try {

            List<String> cmdList = ProcessUtils.buildApplicationCommandLine(BulkIngestMapFileLoader.class.getName(), systemProperties, false);

            for (int counter = 0; counter < 6; counter++) {

                cmdList.add(String.format("%d", counter));
            }

            cmdList.add(BulkIngestMapFileLoaderTest.class.getResource("/datawave/ingest/mapreduce/job/BulkIngestMapFileLoader-type.xml").toString());

            String[] cmdArray = ProcessUtils.convertCommandLine(cmdList);

            Map<String,String> newEnvironment = new HashMap<>();
            List<String> dropFromEnvironment = new ArrayList<>();
            File workingDirectory = new File(System.getProperty("user.dir"));

            Process proc = ProcessUtils.runInstance(cmdArray, newEnvironment, dropFromEnvironment, workingDirectory);

            int procResults = proc.waitFor();

            Assert.assertEquals("BulkIngestMapLoader#main failed to return the expected value.", ProcessUtils.SYSTEM_EXIT_MINUS_TWO, procResults);

        } finally {

            BulkIngestMapFileLoaderTest.LOG.info("testMainWithAllOptionalArgsNoTablePriorites completed.");

        }
    }

    @Test
    public void testMainWithBadResource() throws IOException, InterruptedException {

        BulkIngestMapFileLoaderTest.LOG.info("testMainWithBadResource called...");

        try {

            List<String> cmdList = ProcessUtils.buildApplicationCommandLine(BulkIngestMapFileLoader.class.getName(), systemProperties, false);

            for (int counter = 0; counter < 6; counter++) {

                cmdList.add(String.format("%d", counter));
            }

            cmdList.add("/datawave/ingest/mapreduce/job/BulkIngestMapFileLoader-type.xml");

            String[] cmdArray = ProcessUtils.convertCommandLine(cmdList);

            Map<String,String> newEnvironment = new HashMap<>();
            List<String> dropFromEnvironment = new ArrayList<>();
            File workingDirectory = new File(System.getProperty("user.dir"));

            Process proc = ProcessUtils.runInstance(cmdArray, newEnvironment, dropFromEnvironment, workingDirectory);

            int procResults = proc.waitFor();

            Assert.assertEquals("BulkIngestMapLoader#main failed to return the expected value.", ProcessUtils.SYSTEM_EXIT_MINUS_TWO, procResults);

        } finally {

            BulkIngestMapFileLoaderTest.LOG.info("testMainWithBadResource completed.");

        }
    }

    @Test
    public void testMainWithBadSleepTime() throws IOException, InterruptedException {

        BulkIngestMapFileLoaderTest.LOG.info("testMainWithBadSleepTime called...");

        try {

            List<String> cmdList = ProcessUtils.buildApplicationCommandLine(BulkIngestMapFileLoader.class.getName(), systemProperties, false);

            for (int counter = 0; counter < 6; counter++) {

                cmdList.add(String.format("%d", counter));
            }

            cmdList.add("-sleepTime");
            cmdList.add("hello, world");

            String[] cmdArray = ProcessUtils.convertCommandLine(cmdList);

            Map<String,String> newEnvironment = new HashMap<>();
            List<String> dropFromEnvironment = new ArrayList<>();
            File workingDirectory = new File(System.getProperty("user.dir"));

            Process proc = ProcessUtils.runInstance(cmdArray, newEnvironment, dropFromEnvironment, workingDirectory);

            int procResults = proc.waitFor();

            Assert.assertEquals("BulkIngestMapLoader#main failed to return the expected value.", ProcessUtils.SYSTEM_EXIT_MINUS_TWO, procResults);

            List<String> stdOut = ProcessUtils.getStandardOutDumps(proc);

            Assert.assertTrue("BulkIngestMapLoader#main failed to generate the expected SleepTime error message",
                            processOutputContains(stdOut, "-sleepTime must be followed by the number of ms to sleep between checks for map files."));

        } finally {

            BulkIngestMapFileLoaderTest.LOG.info("testMainWithBadSleepTime completed.");

        }
    }

    @Test
    public void testMainWithMissingSleepTime() throws IOException, InterruptedException {

        BulkIngestMapFileLoaderTest.LOG.info("testMainWithMissingSleepTime called...");

        try {

            List<String> cmdList = ProcessUtils.buildApplicationCommandLine(BulkIngestMapFileLoader.class.getName(), systemProperties, false);

            for (int counter = 0; counter < 6; counter++) {

                cmdList.add(String.format("%d", counter));
            }

            cmdList.add("-sleepTime");

            String[] cmdArray = ProcessUtils.convertCommandLine(cmdList);

            Map<String,String> newEnvironment = new HashMap<>();
            List<String> dropFromEnvironment = new ArrayList<>();
            File workingDirectory = new File(System.getProperty("user.dir"));

            Process proc = ProcessUtils.runInstance(cmdArray, newEnvironment, dropFromEnvironment, workingDirectory);

            int procResults = proc.waitFor();

            Assert.assertEquals("BulkIngestMapLoader#main failed to return the expected value.", ProcessUtils.SYSTEM_EXIT_MINUS_TWO, procResults);

            List<String> stdOut = ProcessUtils.getStandardOutDumps(proc);

            Assert.assertTrue("BulkIngestMapLoader#main failed to generate the expected SleepTime error message",
                            processOutputContains(stdOut, "-sleepTime must be followed by the number of ms to sleep between checks for map files."));

        } finally {

            BulkIngestMapFileLoaderTest.LOG.info("testMainWithMissingSleepTime completed.");

        }
    }

    @Test
    public void testMainWithBadMajCThreshold() throws IOException, InterruptedException {

        BulkIngestMapFileLoaderTest.LOG.info("testMainWithBadMajCThreshold called...");

        try {

            List<String> cmdList = ProcessUtils.buildApplicationCommandLine(BulkIngestMapFileLoader.class.getName(), systemProperties, false);

            for (int counter = 0; counter < 6; counter++) {

                cmdList.add(String.format("%d", counter));
            }

            cmdList.add("-majcThreshold");
            cmdList.add("hello, world");

            String[] cmdArray = ProcessUtils.convertCommandLine(cmdList);

            Map<String,String> newEnvironment = new HashMap<>();
            List<String> dropFromEnvironment = new ArrayList<>();
            File workingDirectory = new File(System.getProperty("user.dir"));

            Process proc = ProcessUtils.runInstance(cmdArray, newEnvironment, dropFromEnvironment, workingDirectory);

            int procResults = proc.waitFor();

            Assert.assertEquals("BulkIngestMapLoader#main failed to return the expected value.", ProcessUtils.SYSTEM_EXIT_MINUS_TWO, procResults);

            List<String> stdOut = ProcessUtils.getStandardOutDumps(proc);

            Assert.assertTrue("BulkIngestMapLoader#main failed to generate the expected error message",
                            processOutputContains(stdOut, "-majcThreshold must be followed by the maximum number of major compactions allowed before waiting"));

        } finally {

            BulkIngestMapFileLoaderTest.LOG.info("testMainWithBadMajCThreshold completed.");

        }
    }

    @Test
    public void testMainWithMissingMajCThreshold() throws IOException, InterruptedException {

        BulkIngestMapFileLoaderTest.LOG.info("testMainWithMissingMajCThreshold called...");

        try {

            List<String> cmdList = ProcessUtils.buildApplicationCommandLine(BulkIngestMapFileLoader.class.getName(), systemProperties, false);

            for (int counter = 0; counter < 6; counter++) {

                cmdList.add(String.format("%d", counter));
            }

            cmdList.add("-majcThreshold");

            String[] cmdArray = ProcessUtils.convertCommandLine(cmdList);

            Map<String,String> newEnvironment = new HashMap<>();
            List<String> dropFromEnvironment = new ArrayList<>();
            File workingDirectory = new File(System.getProperty("user.dir"));

            Process proc = ProcessUtils.runInstance(cmdArray, newEnvironment, dropFromEnvironment, workingDirectory);

            int procResults = proc.waitFor();

            Assert.assertEquals("BulkIngestMapLoader#main failed to return the expected value.", ProcessUtils.SYSTEM_EXIT_MINUS_TWO, procResults);

            List<String> stdOut = ProcessUtils.getStandardOutDumps(proc);

            Assert.assertTrue("BulkIngestMapLoader#main failed to generate the expected error message",
                            processOutputContains(stdOut, "-majcThreshold must be followed by the maximum number of major compactions allowed before waiting"));

        } finally {

            BulkIngestMapFileLoaderTest.LOG.info("testMainWithMissingMajCThreshold completed.");

        }
    }

    @Test
    public void testMainWithBadMajCDelay() throws IOException, InterruptedException {

        BulkIngestMapFileLoaderTest.LOG.info("testMainWithBadMajCDelay called...");

        try {

            List<String> cmdList = ProcessUtils.buildApplicationCommandLine(BulkIngestMapFileLoader.class.getName(), systemProperties, false);

            for (int counter = 0; counter < 6; counter++) {

                cmdList.add(String.format("%d", counter));
            }

            cmdList.add("-majcDelay");
            cmdList.add("hello, world");

            String[] cmdArray = ProcessUtils.convertCommandLine(cmdList);

            Map<String,String> newEnvironment = new HashMap<>();
            List<String> dropFromEnvironment = new ArrayList<>();
            File workingDirectory = new File(System.getProperty("user.dir"));

            Process proc = ProcessUtils.runInstance(cmdArray, newEnvironment, dropFromEnvironment, workingDirectory);

            int procResults = proc.waitFor();

            Assert.assertEquals("BulkIngestMapLoader#main failed to return the expected value.", ProcessUtils.SYSTEM_EXIT_MINUS_TWO, procResults);

            List<String> stdOut = ProcessUtils.getStandardOutDumps(proc);

            Assert.assertTrue("BulkIngestMapLoader#main failed to generate the expected error message", processOutputContains(stdOut,
                            "-majcDelay must be followed by the minimum number of ms to elapse between bringing map files online"));

        } finally {

            BulkIngestMapFileLoaderTest.LOG.info("testMainWithBadMajCDelay completed.");

        }
    }

    @Test
    public void testMainWithMissingMajCDelay() throws IOException, InterruptedException {

        BulkIngestMapFileLoaderTest.LOG.info("testMainWithMissingMajCDelay called...");

        try {

            List<String> cmdList = ProcessUtils.buildApplicationCommandLine(BulkIngestMapFileLoader.class.getName(), systemProperties, false);

            for (int counter = 0; counter < 6; counter++) {

                cmdList.add(String.format("%d", counter));
            }

            cmdList.add("-majcDelay");

            String[] cmdArray = ProcessUtils.convertCommandLine(cmdList);

            Map<String,String> newEnvironment = new HashMap<>();
            List<String> dropFromEnvironment = new ArrayList<>();
            File workingDirectory = new File(System.getProperty("user.dir"));

            Process proc = ProcessUtils.runInstance(cmdArray, newEnvironment, dropFromEnvironment, workingDirectory);

            int procResults = proc.waitFor();

            Assert.assertEquals("BulkIngestMapLoader#main failed to return the expected value.", ProcessUtils.SYSTEM_EXIT_MINUS_TWO, procResults);

            List<String> stdOut = ProcessUtils.getStandardOutDumps(proc);

            Assert.assertTrue("BulkIngestMapLoader#main failed to generate the expected error message", processOutputContains(stdOut,
                            "-majcDelay must be followed by the minimum number of ms to elapse between bringing map files online"));

        } finally {

            BulkIngestMapFileLoaderTest.LOG.info("testMainWithMissingMajCDelay completed.");

        }
    }

    @Test
    public void testMainWithBadMaxDirectories() throws IOException, InterruptedException {

        BulkIngestMapFileLoaderTest.LOG.info("testMainWithBadMaxDirectories called...");

        try {

            List<String> cmdList = ProcessUtils.buildApplicationCommandLine(BulkIngestMapFileLoader.class.getName(), systemProperties, false);

            for (int counter = 0; counter < 6; counter++) {

                cmdList.add(String.format("%d", counter));
            }

            cmdList.add("-maxDirectories");
            cmdList.add("hello, world");

            String[] cmdArray = ProcessUtils.convertCommandLine(cmdList);

            Map<String,String> newEnvironment = new HashMap<>();
            List<String> dropFromEnvironment = new ArrayList<>();
            File workingDirectory = new File(System.getProperty("user.dir"));

            Process proc = ProcessUtils.runInstance(cmdArray, newEnvironment, dropFromEnvironment, workingDirectory);

            int procResults = proc.waitFor();

            Assert.assertEquals("BulkIngestMapLoader#main failed to return the expected value.", ProcessUtils.SYSTEM_EXIT_MINUS_TWO, procResults);

            List<String> stdOut = ProcessUtils.getStandardOutDumps(proc);

            Assert.assertTrue("BulkIngestMapLoader#main failed to generate the expected error message",
                            processOutputContains(stdOut, "-maxDirectories must be followed a number of directories"));

        } finally {

            BulkIngestMapFileLoaderTest.LOG.info("testMainWithBadMaxDirectories completed.");

        }
    }

    @Test
    public void testMainWithMissingMaxDirectories() throws IOException, InterruptedException {

        BulkIngestMapFileLoaderTest.LOG.info("testMainWithMissingMaxDirectories called...");

        try {

            List<String> cmdList = ProcessUtils.buildApplicationCommandLine(BulkIngestMapFileLoader.class.getName(), systemProperties, false);

            for (int counter = 0; counter < 6; counter++) {

                cmdList.add(String.format("%d", counter));
            }

            cmdList.add("-maxDirectories");

            String[] cmdArray = ProcessUtils.convertCommandLine(cmdList);

            Map<String,String> newEnvironment = new HashMap<>();
            List<String> dropFromEnvironment = new ArrayList<>();
            File workingDirectory = new File(System.getProperty("user.dir"));

            Process proc = ProcessUtils.runInstance(cmdArray, newEnvironment, dropFromEnvironment, workingDirectory);

            int procResults = proc.waitFor();

            Assert.assertEquals("BulkIngestMapLoader#main failed to return the expected value.", ProcessUtils.SYSTEM_EXIT_MINUS_TWO, procResults);

            List<String> stdOut = ProcessUtils.getStandardOutDumps(proc);

            Assert.assertTrue("BulkIngestMapLoader#main failed to generate the expected error message",
                            processOutputContains(stdOut, "-maxDirectories must be followed a number of directories"));

        } finally {

            BulkIngestMapFileLoaderTest.LOG.info("testMainWithMissingMaxDirectories completed.");

        }
    }

    @Test
    public void testMainWithBadNumThreads() throws IOException, InterruptedException {

        BulkIngestMapFileLoaderTest.LOG.info("testMainWithBadNumThreads called...");

        try {

            List<String> cmdList = ProcessUtils.buildApplicationCommandLine(BulkIngestMapFileLoader.class.getName(), systemProperties, false);

            for (int counter = 0; counter < 6; counter++) {

                cmdList.add(String.format("%d", counter));
            }

            cmdList.add("-numThreads");
            cmdList.add("hello, world");

            String[] cmdArray = ProcessUtils.convertCommandLine(cmdList);

            Map<String,String> newEnvironment = new HashMap<>();
            List<String> dropFromEnvironment = new ArrayList<>();
            File workingDirectory = new File(System.getProperty("user.dir"));

            Process proc = ProcessUtils.runInstance(cmdArray, newEnvironment, dropFromEnvironment, workingDirectory);

            int procResults = proc.waitFor();

            Assert.assertEquals("BulkIngestMapLoader#main failed to return the expected value.", ProcessUtils.SYSTEM_EXIT_MINUS_TWO, procResults);

            List<String> stdOut = ProcessUtils.getStandardOutDumps(proc);

            Assert.assertTrue("BulkIngestMapLoader#main failed to generate the expected error message",
                            processOutputContains(stdOut, "-numThreads must be followed by the number of bulk import threads"));

        } finally {

            BulkIngestMapFileLoaderTest.LOG.info("testMainWithBadNumThreads completed.");

        }
    }

    @Test
    public void testMainWithMissingNumThreads() throws IOException, InterruptedException {

        BulkIngestMapFileLoaderTest.LOG.info("testMainWithMissingNumThreads called...");

        try {

            List<String> cmdList = ProcessUtils.buildApplicationCommandLine(BulkIngestMapFileLoader.class.getName(), systemProperties, false);

            for (int counter = 0; counter < 6; counter++) {

                cmdList.add(String.format("%d", counter));
            }

            cmdList.add("-numThreads");

            String[] cmdArray = ProcessUtils.convertCommandLine(cmdList);

            Map<String,String> newEnvironment = new HashMap<>();
            List<String> dropFromEnvironment = new ArrayList<>();
            File workingDirectory = new File(System.getProperty("user.dir"));

            Process proc = ProcessUtils.runInstance(cmdArray, newEnvironment, dropFromEnvironment, workingDirectory);

            int procResults = proc.waitFor();

            Assert.assertEquals("BulkIngestMapLoader#main failed to return the expected value.", ProcessUtils.SYSTEM_EXIT_MINUS_TWO, procResults);

            List<String> stdOut = ProcessUtils.getStandardOutDumps(proc);

            Assert.assertTrue("BulkIngestMapLoader#main failed to generate the expected error message",
                            processOutputContains(stdOut, "-numThreads must be followed by the number of bulk import threads"));

        } finally {

            BulkIngestMapFileLoaderTest.LOG.info("testMainWithMissingNumThreads completed.");

        }
    }

    @Test
    public void testMainWithBadNumAssignThreads() throws IOException, InterruptedException {

        BulkIngestMapFileLoaderTest.LOG.info("testMainWithBadNumAssignThreads called...");

        try {

            List<String> cmdList = ProcessUtils.buildApplicationCommandLine(BulkIngestMapFileLoader.class.getName(), systemProperties, false);

            for (int counter = 0; counter < 6; counter++) {

                cmdList.add(String.format("%d", counter));
            }

            cmdList.add("-numAssignThreads");
            cmdList.add("hello, world");

            String[] cmdArray = ProcessUtils.convertCommandLine(cmdList);

            Map<String,String> newEnvironment = new HashMap<>();
            List<String> dropFromEnvironment = new ArrayList<>();
            File workingDirectory = new File(System.getProperty("user.dir"));

            Process proc = ProcessUtils.runInstance(cmdArray, newEnvironment, dropFromEnvironment, workingDirectory);

            int procResults = proc.waitFor();

            Assert.assertEquals("BulkIngestMapLoader#main failed to return the expected value.", ProcessUtils.SYSTEM_EXIT_MINUS_TWO, procResults);

            List<String> stdOut = ProcessUtils.getStandardOutDumps(proc);

            Assert.assertTrue("BulkIngestMapLoader#main failed to generate the expected error message",
                            processOutputContains(stdOut, "-numAssignThreads must be followed by the number of bulk import assignment threads"));

        } finally {

            BulkIngestMapFileLoaderTest.LOG.info("testMainWithBadNumAssignThreads completed.");

        }
    }

    @Test
    public void testMainWithMissingNumAssignThreads() throws IOException, InterruptedException {

        BulkIngestMapFileLoaderTest.LOG.info("testMainWithMissingNumAssignThreads called...");

        try {

            List<String> cmdList = ProcessUtils.buildApplicationCommandLine(BulkIngestMapFileLoader.class.getName(), systemProperties, false);

            for (int counter = 0; counter < 6; counter++) {

                cmdList.add(String.format("%d", counter));
            }

            cmdList.add("-numAssignThreads");

            String[] cmdArray = ProcessUtils.convertCommandLine(cmdList);

            Map<String,String> newEnvironment = new HashMap<>();
            List<String> dropFromEnvironment = new ArrayList<>();
            File workingDirectory = new File(System.getProperty("user.dir"));

            Process proc = ProcessUtils.runInstance(cmdArray, newEnvironment, dropFromEnvironment, workingDirectory);

            int procResults = proc.waitFor();

            Assert.assertEquals("BulkIngestMapLoader#main failed to return the expected value.", ProcessUtils.SYSTEM_EXIT_MINUS_TWO, procResults);

            List<String> stdOut = ProcessUtils.getStandardOutDumps(proc);

            Assert.assertTrue("BulkIngestMapLoader#main failed to generate the expected error message",
                            processOutputContains(stdOut, "-numAssignThreads must be followed by the number of bulk import assignment threads"));

        } finally {

            BulkIngestMapFileLoaderTest.LOG.info("testMainWithMissingNumAssignThreads completed.");

        }
    }

    @Test
    public void testMainWithBadSeqFileHdfs() throws IOException, InterruptedException {

        BulkIngestMapFileLoaderTest.LOG.info("testMainWithBadSeqFileHdfs called...");

        try {

            List<String> cmdList = ProcessUtils.buildApplicationCommandLine(BulkIngestMapFileLoader.class.getName(), systemProperties, false);

            for (int counter = 0; counter < 6; counter++) {

                cmdList.add(String.format("%d", counter));
            }

            cmdList.add("-seqFileHdfs");
            cmdList.add("hello, world");

            String[] cmdArray = ProcessUtils.convertCommandLine(cmdList);

            Map<String,String> newEnvironment = new HashMap<>();
            List<String> dropFromEnvironment = new ArrayList<>();
            File workingDirectory = new File(System.getProperty("user.dir"));

            Process proc = ProcessUtils.runInstance(cmdArray, newEnvironment, dropFromEnvironment, workingDirectory);

            int procResults = proc.waitFor();

            Assert.assertEquals("BulkIngestMapLoader#main failed to return the expected value.", ProcessUtils.SYSTEM_EXIT_MINUS_TWO, procResults);

            List<String> stdOut = ProcessUtils.getStandardOutDumps(proc);

            Assert.assertTrue("BulkIngestMapLoader#main failed to generate the expected error message",
                            processOutputContains(stdOut, "-seqFileHdfs must be followed a file system URI (e.g. hdfs://hostname:54310)."));

        } finally {

            BulkIngestMapFileLoaderTest.LOG.info("testMainWithBadSeqFileHdfs completed.");

        }
    }

    @Test
    public void testMainWithMissingSeqFileHdfs() throws IOException, InterruptedException {

        BulkIngestMapFileLoaderTest.LOG.info("testMainWithMissingSeqFileHdfs called...");

        try {

            List<String> cmdList = ProcessUtils.buildApplicationCommandLine(BulkIngestMapFileLoader.class.getName(), systemProperties, false);

            for (int counter = 0; counter < 6; counter++) {

                cmdList.add(String.format("%d", counter));
            }

            cmdList.add("-seqFileHdfs");

            String[] cmdArray = ProcessUtils.convertCommandLine(cmdList);

            Map<String,String> newEnvironment = new HashMap<>();
            List<String> dropFromEnvironment = new ArrayList<>();
            File workingDirectory = new File(System.getProperty("user.dir"));

            Process proc = ProcessUtils.runInstance(cmdArray, newEnvironment, dropFromEnvironment, workingDirectory);

            int procResults = proc.waitFor();

            Assert.assertEquals("BulkIngestMapLoader#main failed to return the expected value.", ProcessUtils.SYSTEM_EXIT_MINUS_TWO, procResults);

            List<String> stdOut = ProcessUtils.getStandardOutDumps(proc);

            Assert.assertTrue("BulkIngestMapLoader#main failed to generate the expected error message",
                            processOutputContains(stdOut, "-seqFileHdfs must be followed a file system URI (e.g. hdfs://hostname:54310)."));

        } finally {

            BulkIngestMapFileLoaderTest.LOG.info("testMainWithMissingSeqFileHdfs completed.");

        }
    }

    @Test
    public void testMainWithBadSrcHdfs() throws IOException, InterruptedException {

        BulkIngestMapFileLoaderTest.LOG.info("testMainWithBadSrcHdfs called...");

        try {

            List<String> cmdList = ProcessUtils.buildApplicationCommandLine(BulkIngestMapFileLoader.class.getName(), systemProperties, false);

            for (int counter = 0; counter < 6; counter++) {

                cmdList.add(String.format("%d", counter));
            }

            cmdList.add("-srcHdfs");
            cmdList.add("hello, world");

            String[] cmdArray = ProcessUtils.convertCommandLine(cmdList);

            Map<String,String> newEnvironment = new HashMap<>();
            List<String> dropFromEnvironment = new ArrayList<>();
            File workingDirectory = new File(System.getProperty("user.dir"));

            Process proc = ProcessUtils.runInstance(cmdArray, newEnvironment, dropFromEnvironment, workingDirectory);

            int procResults = proc.waitFor();

            Assert.assertEquals("BulkIngestMapLoader#main failed to return the expected value.", ProcessUtils.SYSTEM_EXIT_MINUS_TWO, procResults);

            List<String> stdOut = ProcessUtils.getStandardOutDumps(proc);

            Assert.assertTrue("BulkIngestMapLoader#main failed to generate the expected error message",
                            processOutputContains(stdOut, "-srcHdfs must be followed a file system URI (e.g. hdfs://hostname:54310)."));

        } finally {

            BulkIngestMapFileLoaderTest.LOG.info("testMainWithBadSrcHdfs completed.");

        }
    }

    @Test
    public void testMainWithMissingSrcHdfs() throws IOException, InterruptedException {

        BulkIngestMapFileLoaderTest.LOG.info("testMainWithMissingSrcHdfs called...");

        try {

            List<String> cmdList = ProcessUtils.buildApplicationCommandLine(BulkIngestMapFileLoader.class.getName(), systemProperties, false);

            for (int counter = 0; counter < 6; counter++) {

                cmdList.add(String.format("%d", counter));
            }

            cmdList.add("-srcHdfs");

            String[] cmdArray = ProcessUtils.convertCommandLine(cmdList);

            Map<String,String> newEnvironment = new HashMap<>();
            List<String> dropFromEnvironment = new ArrayList<>();
            File workingDirectory = new File(System.getProperty("user.dir"));

            Process proc = ProcessUtils.runInstance(cmdArray, newEnvironment, dropFromEnvironment, workingDirectory);

            int procResults = proc.waitFor();

            Assert.assertEquals("BulkIngestMapLoader#main failed to return the expected value.", ProcessUtils.SYSTEM_EXIT_MINUS_TWO, procResults);

            List<String> stdOut = ProcessUtils.getStandardOutDumps(proc);

            Assert.assertTrue("BulkIngestMapLoader#main failed to generate the expected error message",
                            processOutputContains(stdOut, "-srcHdfs must be followed a file system URI (e.g. hdfs://hostname:54310)."));

        } finally {

            BulkIngestMapFileLoaderTest.LOG.info("testMainWithMissingSrcHdfs completed.");

        }
    }

    @Test
    public void testMainWithBadDestHDFS() throws IOException, InterruptedException {

        BulkIngestMapFileLoaderTest.LOG.info("testMainWithBadDestHDFS called...");

        try {

            List<String> cmdList = ProcessUtils.buildApplicationCommandLine(BulkIngestMapFileLoader.class.getName(), systemProperties, false);

            for (int counter = 0; counter < 6; counter++) {

                cmdList.add(String.format("%d", counter));
            }

            cmdList.add("-destHdfs");
            cmdList.add("hello, world");

            String[] cmdArray = ProcessUtils.convertCommandLine(cmdList);

            Map<String,String> newEnvironment = new HashMap<>();
            List<String> dropFromEnvironment = new ArrayList<>();
            File workingDirectory = new File(System.getProperty("user.dir"));

            Process proc = ProcessUtils.runInstance(cmdArray, newEnvironment, dropFromEnvironment, workingDirectory);

            int procResults = proc.waitFor();

            Assert.assertEquals("BulkIngestMapLoader#main failed to return the expected value.", ProcessUtils.SYSTEM_EXIT_MINUS_TWO, procResults);

            List<String> stdOut = ProcessUtils.getStandardOutDumps(proc);

            Assert.assertTrue("BulkIngestMapLoader#main failed to generate the expected error message",
                            processOutputContains(stdOut, "-destHdfs must be followed a file system URI (e.g. hdfs://hostname:54310)."));

        } finally {

            BulkIngestMapFileLoaderTest.LOG.info("testMainWithBadDestHDFS completed.");

        }
    }

    @Test
    public void testMainWithMissingDestHDFS() throws IOException, InterruptedException {

        BulkIngestMapFileLoaderTest.LOG.info("testMainWithMissingDestHDFS called...");

        try {

            List<String> cmdList = ProcessUtils.buildApplicationCommandLine(BulkIngestMapFileLoader.class.getName(), systemProperties, false);

            for (int counter = 0; counter < 6; counter++) {

                cmdList.add(String.format("%d", counter));
            }

            cmdList.add("-destHdfs");

            String[] cmdArray = ProcessUtils.convertCommandLine(cmdList);

            Map<String,String> newEnvironment = new HashMap<>();
            List<String> dropFromEnvironment = new ArrayList<>();
            File workingDirectory = new File(System.getProperty("user.dir"));

            Process proc = ProcessUtils.runInstance(cmdArray, newEnvironment, dropFromEnvironment, workingDirectory);

            int procResults = proc.waitFor();

            Assert.assertEquals("BulkIngestMapLoader#main failed to return the expected value.", ProcessUtils.SYSTEM_EXIT_MINUS_TWO, procResults);

            List<String> stdOut = ProcessUtils.getStandardOutDumps(proc);

            Assert.assertTrue("BulkIngestMapLoader#main failed to generate the expected error message",
                            processOutputContains(stdOut, "-destHdfs must be followed a file system URI (e.g. hdfs://hostname:54310)."));

        } finally {

            BulkIngestMapFileLoaderTest.LOG.info("testMainWithMissingDestHDFS completed.");

        }
    }

    @Test
    public void testMainWithBadJT() throws IOException, InterruptedException {

        BulkIngestMapFileLoaderTest.LOG.info("testMainWithBadJT called...");

        try {

            List<String> cmdList = ProcessUtils.buildApplicationCommandLine(BulkIngestMapFileLoader.class.getName(), systemProperties, false);

            for (int counter = 0; counter < 6; counter++) {

                cmdList.add(String.format("%d", counter));
            }

            cmdList.add("-jt");

            String[] cmdArray = ProcessUtils.convertCommandLine(cmdList);

            Map<String,String> newEnvironment = new HashMap<>();
            List<String> dropFromEnvironment = new ArrayList<>();
            File workingDirectory = new File(System.getProperty("user.dir"));

            Process proc = ProcessUtils.runInstance(cmdArray, newEnvironment, dropFromEnvironment, workingDirectory);

            int procResults = proc.waitFor();

            Assert.assertEquals("BulkIngestMapLoader#main failed to return the expected value.", ProcessUtils.SYSTEM_EXIT_MINUS_TWO, procResults);

            List<String> stdOut = ProcessUtils.getStandardOutDumps(proc);

            Assert.assertTrue("BulkIngestMapLoader#main failed to generate the expected error message",
                            processOutputContains(stdOut, "-jt must be followed a jobtracker (e.g. hostname:54311)."));

        } finally {

            BulkIngestMapFileLoaderTest.LOG.info("testMainWithBadJT completed.");

        }
    }

    @Test
    public void testMainWithBadShutdownPort() throws IOException, InterruptedException {

        BulkIngestMapFileLoaderTest.LOG.info("testMainWithBadShutdownPort called...");

        try {

            List<String> cmdList = ProcessUtils.buildApplicationCommandLine(BulkIngestMapFileLoader.class.getName(), systemProperties, false);

            for (int counter = 0; counter < 6; counter++) {

                cmdList.add(String.format("%d", counter));
            }

            cmdList.add("-shutdownPort");
            cmdList.add("hello, world");

            String[] cmdArray = ProcessUtils.convertCommandLine(cmdList);

            Map<String,String> newEnvironment = new HashMap<>();
            List<String> dropFromEnvironment = new ArrayList<>();
            File workingDirectory = new File(System.getProperty("user.dir"));

            Process proc = ProcessUtils.runInstance(cmdArray, newEnvironment, dropFromEnvironment, workingDirectory);

            int procResults = proc.waitFor();

            Assert.assertEquals("BulkIngestMapLoader#main failed to return the expected value.", ProcessUtils.SYSTEM_EXIT_MINUS_TWO, procResults);

            List<String> stdOut = ProcessUtils.getStandardOutDumps(proc);

            Assert.assertTrue("BulkIngestMapLoader#main failed to generate the expected error message",
                            processOutputContains(stdOut, "-shutdownPort must be followed a port number"));

        } finally {

            BulkIngestMapFileLoaderTest.LOG.info("testMainWithBadShutdownPort completed.");

        }
    }

    @Test
    public void testMainWithMissingShutdownPort() throws IOException, InterruptedException {

        BulkIngestMapFileLoaderTest.LOG.info("testMainWithMissingShutdownPort called...");

        try {

            List<String> cmdList = ProcessUtils.buildApplicationCommandLine(BulkIngestMapFileLoader.class.getName(), systemProperties, false);

            for (int counter = 0; counter < 6; counter++) {

                cmdList.add(String.format("%d", counter));
            }

            cmdList.add("-shutdownPort");

            String[] cmdArray = ProcessUtils.convertCommandLine(cmdList);

            Map<String,String> newEnvironment = new HashMap<>();
            List<String> dropFromEnvironment = new ArrayList<>();
            File workingDirectory = new File(System.getProperty("user.dir"));

            Process proc = ProcessUtils.runInstance(cmdArray, newEnvironment, dropFromEnvironment, workingDirectory);

            int procResults = proc.waitFor();

            Assert.assertEquals("BulkIngestMapLoader#main failed to return the expected value.", ProcessUtils.SYSTEM_EXIT_MINUS_TWO, procResults);

            List<String> stdOut = ProcessUtils.getStandardOutDumps(proc);

            Assert.assertTrue("BulkIngestMapLoader#main failed to generate the expected error message",
                            processOutputContains(stdOut, "-shutdownPort must be followed a port number"));

        } finally {

            BulkIngestMapFileLoaderTest.LOG.info("testMainWithMissingShutdownPort completed.");

        }
    }

    @Test
    public void testMainWithBadPropery() throws IOException, InterruptedException {

        BulkIngestMapFileLoaderTest.LOG.info("testMainWithBadPropery called...");

        try {

            List<String> cmdList = ProcessUtils.buildApplicationCommandLine(BulkIngestMapFileLoader.class.getName(), systemProperties, false);

            for (int counter = 0; counter < 6; counter++) {

                cmdList.add(String.format("%d", counter));
            }

            cmdList.add("-property1");

            String[] cmdArray = ProcessUtils.convertCommandLine(cmdList);

            Map<String,String> newEnvironment = new HashMap<>();
            List<String> dropFromEnvironment = new ArrayList<>();
            File workingDirectory = new File(System.getProperty("user.dir"));

            Process proc = ProcessUtils.runInstance(cmdArray, newEnvironment, dropFromEnvironment, workingDirectory);

            int procResults = proc.waitFor();

            Assert.assertEquals("BulkIngestMapLoader#main failed to return the expected value.", ProcessUtils.SYSTEM_EXIT_MINUS_TWO, procResults);

            List<String> stdOut = ProcessUtils.getStandardOutDumps(proc);

            Assert.assertTrue("BulkIngestMapLoader#main failed to generate the expected error message",
                            processOutputContains(stdOut, "WARN: skipping bad property configuration -property1"));

        } finally {

            BulkIngestMapFileLoaderTest.LOG.info("testMainWithBadPropery completed.");

        }
    }

    private BulkIngestMapFileLoader createBulkIngestFileMapLoader(URL url) {
        try {
            URI uri = url.toURI();

            String workDir = ".";
            String jobDirPattern = "jobs/";
            String instanceName = "localhost";
            String zooKeepers = "localhost";
            URI seqFileHdfs = uri;
            URI srcHdfs = uri;
            URI destHdfs = uri;
            String jobtracker = "localhost";
            Map<String,Integer> tablePriorities = new HashMap<>();

            BulkIngestMapFileLoader uut = new BulkIngestMapFileLoader(workDir, jobDirPattern, instanceName, zooKeepers, "user", new PasswordToken("pass"),
                            seqFileHdfs, srcHdfs, destHdfs, jobtracker, tablePriorities, conf, 0);

            Assert.assertNotNull("BulkIngestMapFileLoader constructor failed to create an instance.", uut);

            uut = new BulkIngestMapFileLoader(workDir, jobDirPattern, instanceName, zooKeepers, "user", new PasswordToken("pass"), seqFileHdfs, srcHdfs,
                            destHdfs, jobtracker, tablePriorities, conf, 0);

            return uut;
        } catch (URISyntaxException e) {
            return null;
        }
    }

    private Path createNewPath(URL url) {
        try {
            return new Path(url.toString());
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    @Test
    public void testCtors() {

        BulkIngestMapFileLoaderTest.LOG.info("testCtors called...");

        try {
            URL url = BulkIngestMapFileLoaderTest.class.getResource("/datawave/ingest/mapreduce/job/all-splits.txt");

            BulkIngestMapFileLoader uut = createBulkIngestFileMapLoader(url);

            Assert.assertNotNull("BulkIngestMapFileLoader constructor failed to create an instance.", uut);

            uut = createBulkIngestFileMapLoader(url);

            Assert.assertNotNull("BulkIngestMapFileLoader constructor failed to create an instance.", uut);

        } finally {

            BulkIngestMapFileLoaderTest.LOG.info("testCtors comleted.");

        }

    }

    @Test
    public void testCleanUpJobDirectoryHappyPath() throws Exception {

        BulkIngestMapFileLoaderTest.LOG.info("testCleanUpJobDirectoryUnableToMakeDirectory called...");

        try {

            URL url = BulkIngestMapFileLoaderTest.class.getResource("/datawave/ingest/mapreduce/job/all-splits.txt");

            BulkIngestMapFileLoader uut = createBulkIngestFileMapLoader(url);

            Assert.assertNotNull("BulkIngestMapFileLoader constructor failed to create an instance.", uut);

            BulkIngestMapFileLoaderTest.WrappedLocalFileSystem fs = new BulkIngestMapFileLoaderTest.WrappedLocalFileSystem(createMockInputStream(), null, true,
                            true, false, false, null, false, false);

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, conf, fs);

            Path mapFilesDir = createNewPath(url);

            uut.cleanUpJobDirectory(mapFilesDir);

            List<String> calls = fs.callsLogs();

            Assert.assertTrue("BulkIngestMapFileLoader#cleanUpJobDirectory failed to call FileSystem#mkdirs",
                            processOutputContains(calls, "FileSystem#mkdirs("));
            Assert.assertTrue("BulkIngestMapFileLoader#cleanUpJobDirectory failed to call FileSystem#rename",
                            processOutputContains(calls, "FileSystem#rename("));
            Assert.assertTrue("BulkIngestMapFileLoader#cleanUpJobDirectory failed to call FileSystem#delete",
                            processOutputContains(calls, "FileSystem#delete("));

        } catch (IOException ioe) {

            String msg = ioe.getMessage();

            Assert.assertTrue("BulkIngestMapFileLoader#markSourceFilesLoaded failed to throw the excepted IOException.", msg.startsWith("Unable to rename "));

        } catch (Throwable t) {

            Assert.fail(String.format("BulkIngestMapFileLoader unexpectedly threw an exception: %s with message of '%s'", t.getClass().getName(),
                            t.getMessage()));

        } finally {

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, null, null);

            BulkIngestMapFileLoaderTest.LOG.info("testCleanUpJobDirectoryUnableToMakeDirectory completed.");
        }

    }

    @Test
    public void testCleanUpJobDirectoryMakesDirectory() throws Exception {

        BulkIngestMapFileLoaderTest.LOG.info("testCleanUpJobDirectoryMakesDirectory called...");

        try {

            URL url = BulkIngestMapFileLoaderTest.class.getResource("/datawave/ingest/mapreduce/job/all-splits.txt");

            BulkIngestMapFileLoader uut = createBulkIngestFileMapLoader(url);

            Assert.assertNotNull("BulkIngestMapFileLoader constructor failed to create an instance.", uut);

            BulkIngestMapFileLoaderTest.WrappedLocalFileSystem fs = new BulkIngestMapFileLoaderTest.WrappedLocalFileSystem(createMockInputStream(), null, true,
                            false, false, false, new HashMap<>(), false, false);

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, conf, fs);

            Path mapFilesDir = createNewPath(url);

            uut.cleanUpJobDirectory(mapFilesDir);

            Assert.fail();

        } catch (IOException ioe) {

            String msg = ioe.getMessage();

            Assert.assertTrue("BulkIngestMapFileLoader#markSourceFilesLoaded failed to throw the excepted IOException.", msg.startsWith("Unable to rename "));

        } catch (Throwable t) {

            Assert.fail(String.format("BulkIngestMapFileLoader unexpectedly threw an exception: %s with message of '%s'", t.getClass().getName(),
                            t.getMessage()));

        } finally {

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, null, null);

            BulkIngestMapFileLoaderTest.LOG.info("testCleanUpJobDirectoryMakesDirectory completed.");
        }

    }

    @Test
    public void testCleanUpJobDirectoryUnableToMakeDirectory() throws Exception {

        BulkIngestMapFileLoaderTest.LOG.info("testCleanUpJobDirectoryUnableToMakeDirectory called...");

        try {

            URL url = BulkIngestMapFileLoaderTest.class.getResource("/datawave/ingest/mapreduce/job/all-splits.txt");

            BulkIngestMapFileLoader uut = createBulkIngestFileMapLoader(url);

            Assert.assertNotNull("BulkIngestMapFileLoader constructor failed to create an instance.", uut);

            BulkIngestMapFileLoaderTest.WrappedLocalFileSystem fs = new BulkIngestMapFileLoaderTest.WrappedLocalFileSystem(
                            createMockInputStream(new String[] {"/dummy/entry"}), null, false, false, false, false, null, false, false);

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, conf, fs);

            Path mapFilesDir = createNewPath(url);

            uut.cleanUpJobDirectory(mapFilesDir);

            Assert.fail();

        } catch (IOException ioe) {

            String msg = ioe.getMessage();

            Assert.assertTrue("BulkIngestMapFileLoader#markSourceFilesLoaded failed to throw the excepted IOException. actually received" + msg,
                            msg.startsWith("Unable to create parent dir "));

        } catch (Throwable t) {

            Assert.fail(String.format("BulkIngestMapFileLoader unexpectedly threw an exception: %s with message of '%s'", t.getClass().getName(),
                            t.getMessage()));

        } finally {

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, null, null);

            BulkIngestMapFileLoaderTest.LOG.info("testCleanUpJobDirectoryUnableToMakeDirectory completed.");
        }

    }

    @Test
    public void testCleanUpJobDirectoryJobSuccess() throws Exception {

        BulkIngestMapFileLoaderTest.LOG.info("testCleanUpJobDirectoryJobSuccess called...");

        try {

            URL url = BulkIngestMapFileLoaderTest.class.getResource("/datawave/ingest/mapreduce/job/all-splits.txt");

            BulkIngestMapFileLoader uut = createBulkIngestFileMapLoader(url);

            Assert.assertNotNull("BulkIngestMapFileLoader constructor failed to create an instance.", uut);

            BulkIngestMapFileLoaderTest.WrappedLocalFileSystem fs = new BulkIngestMapFileLoaderTest.WrappedLocalFileSystem(createMockInputStream(),
                            new FileStatus[0], false, false, false, false, null, false, false);

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, conf, fs);

            Path mapFilesDir = createNewPath(url);

            uut.cleanUpJobDirectory(mapFilesDir);

            Assert.fail();

        } catch (IOException ioe) {

            String msg = ioe.getMessage();

            Assert.assertTrue("BulkIngestMapFileLoader#markSourceFilesLoaded failed to throw the excepted IOException.",
                            msg.startsWith("Unable to create parent dir "));

        } catch (Throwable t) {

            Assert.fail(String.format("BulkIngestMapFileLoader unexpectedly threw an exception: %s with message of '%s'", t.getClass().getName(),
                            t.getMessage()));

        } finally {

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, null, null);

            BulkIngestMapFileLoaderTest.LOG.info("testCleanUpJobDirectoryJobSuccess completed.");
        }

    }

    @Test
    public void testCleanUpJobDirectoryWithFailedJobAndFailedCreateNewFile() throws Exception {

        BulkIngestMapFileLoaderTest.LOG.info("testCleanUpJobDirectoryWithFailedJobAndFailedCreateNewFile called...");

        try {

            URL url = BulkIngestMapFileLoaderTest.class.getResource("/datawave/ingest/mapreduce/job/");

            BulkIngestMapFileLoader uut = createBulkIngestFileMapLoader(url);

            Assert.assertNotNull("BulkIngestMapFileLoader constructor failed to create an instance.", uut);

            BulkIngestMapFileLoaderTest.WrappedLocalFileSystem fs = new BulkIngestMapFileLoaderTest.WrappedLocalFileSystem(createMockInputStream(),
                            new FileStatus[] {createMockFileStatus()}, false, false, false, false, null, false, false);

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, conf, fs);

            Path mapFilesDir = createNewPath(url);

            uut.cleanUpJobDirectory(mapFilesDir);

            List<String> uutLogEntries = retrieveUUTLogs();

            Assert.assertTrue("BulkIngestMapFileLoader#cleanUpJobDirectory failed to call FileSystem#mkdirs",
                            processOutputContains(uutLogEntries, "There were failures bringing map files online."));
            Assert.assertTrue("BulkIngestMapFileLoader#cleanUpJobDirectory failed to call FileSystem#rename",
                            processOutputContains(uutLogEntries, "Unable to rename map files directory "));
            Assert.assertTrue("BulkIngestMapFileLoader#cleanUpJobDirectory failed to call FileSystem#delete",
                            processOutputContains(uutLogEntries, "Unable to create job.failed file in "));

        } catch (IOException ioe) {

            String msg = ioe.getMessage();

            Assert.assertTrue("BulkIngestMapFileLoader#markSourceFilesLoaded failed to throw the excepted IOException.",
                            msg.startsWith("Unable to create parent dir "));

        } catch (Throwable t) {

            Assert.fail(String.format("BulkIngestMapFileLoader unexpectedly threw an exception: %s with message of '%s'", t.getClass().getName(),
                            t.getMessage()));

        } finally {

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, null, null);

            BulkIngestMapFileLoaderTest.LOG.info("testCleanUpJobDirectoryWithFailedJobAndFailedCreateNewFile completed.");
        }

    }

    @Test
    public void testCleanUpJobDirectoryWithFailedJobAndFailedRenames() throws Exception {

        BulkIngestMapFileLoaderTest.LOG.info("testCleanUpJobDirectoryWithFailedJobAndFailedRenames called...");

        try {

            URL url = BulkIngestMapFileLoaderTest.class.getResource("/datawave/ingest/mapreduce/job/");

            BulkIngestMapFileLoader uut = createBulkIngestFileMapLoader(url);

            Assert.assertNotNull("BulkIngestMapFileLoader constructor failed to create an instance.", uut);

            BulkIngestMapFileLoaderTest.WrappedLocalFileSystem fs = new BulkIngestMapFileLoaderTest.WrappedLocalFileSystem(createMockInputStream(),
                            new FileStatus[] {createMockFileStatus()}, false, false, false, true, null, false, false);

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, conf, fs);

            Path mapFilesDir = createNewPath(url);

            uut.cleanUpJobDirectory(mapFilesDir);

            List<String> uutLogEntries = retrieveUUTLogs();

            Assert.assertTrue("BulkIngestMapFileLoader#cleanUpJobDirectory failed to call FileSystem#mkdirs",
                            processOutputContains(uutLogEntries, "There were failures bringing map files online."));
            Assert.assertTrue("BulkIngestMapFileLoader#cleanUpJobDirectory failed to call FileSystem#rename",
                            processOutputContains(uutLogEntries, "Unable to rename map files directory "));

        } catch (IOException ioe) {

            String msg = ioe.getMessage();

            Assert.assertTrue("BulkIngestMapFileLoader#markSourceFilesLoaded failed to throw the excepted IOException.",
                            msg.startsWith("Unable to create parent dir "));

        } catch (Throwable t) {

            Assert.fail(String.format("BulkIngestMapFileLoader unexpectedly threw an exception: %s with message of '%s'", t.getClass().getName(),
                            t.getMessage()));

        } finally {

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, null, null);

            BulkIngestMapFileLoaderTest.LOG.info("testCleanUpJobDirectoryWithFailedJobAndFailedRenames completed.");
        }

    }

    @Test
    public void testCleanUpJobDirectoryWithFailedJob() throws Exception {

        BulkIngestMapFileLoaderTest.LOG.info("testCleanUpJobDirectoryWithFailedJob called...");

        try {

            URL url = BulkIngestMapFileLoaderTest.class.getResource("/datawave/ingest/mapreduce/job/");

            BulkIngestMapFileLoader uut = createBulkIngestFileMapLoader(url);

            Assert.assertNotNull("BulkIngestMapFileLoader constructor failed to create an instance.", uut);

            BulkIngestMapFileLoaderTest.WrappedLocalFileSystem fs = new BulkIngestMapFileLoaderTest.WrappedLocalFileSystem(createMockInputStream(),
                            new FileStatus[] {createMockFileStatus()}, false, true, false, true, null, false, false);

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, conf, fs);

            Path mapFilesDir = createNewPath(url);

            uut.cleanUpJobDirectory(mapFilesDir);

            List<String> uutLogEntries = retrieveUUTLogs();

            Assert.assertTrue("BulkIngestMapFileLoader#cleanUpJobDirectory failed to call FileSystem#mkdirs",
                            processOutputContains(uutLogEntries, "There were failures bringing map files online."));
        } catch (IOException ioe) {

            String msg = ioe.getMessage();

            Assert.assertTrue("BulkIngestMapFileLoader#markSourceFilesLoaded failed to throw the excepted IOException.",
                            msg.startsWith("Unable to create parent dir "));

        } catch (Throwable t) {

            Assert.fail(String.format("BulkIngestMapFileLoader unexpectedly threw an exception: %s with message of '%s'", t.getClass().getName(),
                            t.getMessage()));

        } finally {

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, null, null);

            BulkIngestMapFileLoaderTest.LOG.info("testCleanUpJobDirectoryWithFailedJob completed.");
        }

    }

    @Test
    public void testTakeOwnershipJobDirectoryHappyPath() throws Exception {

        BulkIngestMapFileLoaderTest.LOG.info("testTakeOwnershipJobDirectoryHappyPath called...");

        try {

            URL url = BulkIngestMapFileLoaderTest.class.getResource("/datawave/ingest/mapreduce/job/");

            BulkIngestMapFileLoader uut = createBulkIngestFileMapLoader(url);

            Assert.assertNotNull("BulkIngestMapFileLoader constructor failed to create an instance.", uut);

            Map<String,Boolean> exists = new HashMap<>();
            String filePath = String.format("%s%s", url.toString(), BulkIngestMapFileLoader.LOADING_FILE_MARKER);

            exists.put(filePath, Boolean.TRUE);
            filePath = String.format("%s%s", url.toString(), COMPLETE_FILE_MARKER);
            exists.put(filePath, Boolean.FALSE);

            BulkIngestMapFileLoaderTest.WrappedLocalFileSystem fs = new BulkIngestMapFileLoaderTest.WrappedLocalFileSystem(createMockInputStream(),
                            new FileStatus[] {createMockFileStatus()}, false, true, false, false, exists, false, false);

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, conf, fs);

            Path jobDirectory = createNewPath(url);

            boolean results = uut.takeOwnershipJobDirectory(jobDirectory);

            Assert.assertTrue("BulkIngestMapFileLoader#takeOwnershipJobDirectory failed to return true as expected.", results);

            List<String> uutLogEntries = retrieveUUTLogs();

            List<String> calls = fs.callsLogs();

            Assert.assertTrue("BulkIngestMapFileLoader#takeOwnershipJobDirectory failed generate a renamed message",
                            processOutputContains(uutLogEntries, "Renamed"));
            Assert.assertTrue("BulkIngestMapFileLoader#takeOwnershipJobDirectory failed to call FileSystem#rename",
                            processOutputContains(calls, "FileSystem#rename("));
            Assert.assertTrue("BulkIngestMapFileLoader#takeOwnershipJobDirectory failed to call FileSystem#exists",
                            processOutputContains(calls, "FileSystem#exists("));
        } finally {

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, null, null);

            BulkIngestMapFileLoaderTest.LOG.info("testTakeOwnershipJobDirectoryHappyPath completed.");
        }

    }

    @Test
    public void testTakeOwnershipJobDirectoryFailedOwnershipExchangeLoading() throws Exception {

        BulkIngestMapFileLoaderTest.LOG.info("testTakeOwnershipJobDirectoryFailedOwnershipExchangeLoading called...");

        try {
            URL url = BulkIngestMapFileLoaderTest.class.getResource("/datawave/ingest/mapreduce/job/");

            BulkIngestMapFileLoader uut = createBulkIngestFileMapLoader(url);

            Assert.assertNotNull("BulkIngestMapFileLoader constructor failed to create an instance.", uut);

            Map<String,Boolean> existsResults = new HashMap<>();
            String filePath = String.format("%s%s", url, BulkIngestMapFileLoader.LOADING_FILE_MARKER);
            existsResults.put(filePath, Boolean.FALSE);
            filePath = String.format("%s%s", url, COMPLETE_FILE_MARKER);
            existsResults.put(filePath, Boolean.TRUE);

            BulkIngestMapFileLoaderTest.WrappedLocalFileSystem fs = new BulkIngestMapFileLoaderTest.WrappedLocalFileSystem(createMockInputStream(),
                            new FileStatus[] {createMockFileStatus()}, false, true, false, false, existsResults, false, false);

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, conf, fs);

            Path jobDirectory = createNewPath(url);

            boolean results = uut.takeOwnershipJobDirectory(jobDirectory);

            Assert.assertFalse("BulkIngestMapFileLoader#takeOwnershipJobDirectory failed to return false as expected.", results);

            List<String> uutLogEntries = retrieveUUTLogs();

            List<String> calls = fs.callsLogs();

            Assert.assertTrue("BulkIngestMapFileLoader#takeOwnershipJobDirectory failed generate a renamed message",
                            processOutputContains(uutLogEntries, "Renamed"));
            Assert.assertTrue("BulkIngestMapFileLoader#takeOwnershipJobDirectory failed generate a failed to take ownership message",
                            processOutputContains(uutLogEntries, "Rename returned success but yet we did not take ownership of"));
            Assert.assertTrue("BulkIngestMapFileLoader#takeOwnershipJobDirectory failed to call FileSystem#rename",
                            processOutputContains(calls, "FileSystem#rename("));
            Assert.assertTrue("BulkIngestMapFileLoader#takeOwnershipJobDirectory failed to call FileSystem#exists",
                            processOutputContains(calls, "FileSystem#exists("));
        } finally {

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, null, null);

            BulkIngestMapFileLoaderTest.LOG.info("testTakeOwnershipJobDirectoryFailedOwnershipExchangeLoading completed.");
        }

    }

    @Test
    public void testTakeOwnershipJobDirectoryFailedOwnershipExchangeComplete() throws Exception {

        BulkIngestMapFileLoaderTest.LOG.info("testTakeOwnershipJobDirectoryFailedOwnershipExchangeComplete called...");

        try {

            URL url = BulkIngestMapFileLoaderTest.class.getResource("/datawave/ingest/mapreduce/job/");

            BulkIngestMapFileLoader uut = createBulkIngestFileMapLoader(url);

            Assert.assertNotNull("BulkIngestMapFileLoader constructor failed to create an instance.", uut);

            Map<String,Boolean> existsResults = new HashMap<>();
            String filePath = String.format("%s%s", url, BulkIngestMapFileLoader.LOADING_FILE_MARKER);
            existsResults.put(filePath, Boolean.TRUE);
            filePath = String.format("%s%s", url, COMPLETE_FILE_MARKER);
            existsResults.put(filePath, Boolean.TRUE);

            BulkIngestMapFileLoaderTest.WrappedLocalFileSystem fs = new BulkIngestMapFileLoaderTest.WrappedLocalFileSystem(createMockInputStream(),
                            new FileStatus[] {createMockFileStatus()}, false, true, false, false, existsResults, false, false);

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, conf, fs);

            Path jobDirectory = createNewPath(url);

            boolean results = uut.takeOwnershipJobDirectory(jobDirectory);

            Assert.assertFalse("BulkIngestMapFileLoader#takeOwnershipJobDirectory failed to return false as expected.", results);

            List<String> uutLogEntries = retrieveUUTLogs();

            List<String> calls = fs.callsLogs();

            Assert.assertTrue("BulkIngestMapFileLoader#takeOwnershipJobDirectory failed generate a renamed message",
                            processOutputContains(uutLogEntries, "Renamed"));
            Assert.assertTrue("BulkIngestMapFileLoader#takeOwnershipJobDirectory failed generate a failed to take ownership message",
                            processOutputContains(uutLogEntries, "Rename returned success but yet we did not"));
            Assert.assertTrue("BulkIngestMapFileLoader#takeOwnershipJobDirectory failed to call FileSystem#rename",
                            processOutputContains(calls, "FileSystem#rename("));
            Assert.assertTrue("BulkIngestMapFileLoader#takeOwnershipJobDirectory failed to call FileSystem#exists",
                            processOutputContains(calls, "FileSystem#exists("));
        } finally {

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, null, null);

            BulkIngestMapFileLoaderTest.LOG.info("testTakeOwnershipJobDirectoryFailedOwnershipExchangeComplete completed.");
        }

    }

    @Test
    public void testTakeOwnershipJobDirectoryFailedRenameLoadedExists() throws Exception {

        BulkIngestMapFileLoaderTest.LOG.info("testTakeOwnershipJobDirectoryFailedRenameLoadedExists called...");

        try {

            URL url = BulkIngestMapFileLoaderTest.class.getResource("/datawave/ingest/mapreduce/job/");

            BulkIngestMapFileLoader uut = createBulkIngestFileMapLoader(url);

            Assert.assertNotNull("BulkIngestMapFileLoader constructor failed to create an instance.", uut);

            Map<String,Boolean> existsResults = new HashMap<>();
            String filePath = String.format("%s%s", url, BulkIngestMapFileLoader.LOADING_FILE_MARKER);
            existsResults.put(filePath, Boolean.TRUE);

            BulkIngestMapFileLoaderTest.WrappedLocalFileSystem fs = new BulkIngestMapFileLoaderTest.WrappedLocalFileSystem(createMockInputStream(),
                            new FileStatus[] {createMockFileStatus()}, false, false, false, false, existsResults, false, false);

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, conf, fs);

            Path jobDirectory = createNewPath(url);

            boolean results = uut.takeOwnershipJobDirectory(jobDirectory);

            Assert.assertFalse("BulkIngestMapFileLoader#takeOwnershipJobDirectory failed to return false as expected.", results);

            List<String> uutLogEntries = retrieveUUTLogs();

            List<String> calls = fs.callsLogs();

            Assert.assertTrue("BulkIngestMapFileLoader#takeOwnershipJobDirectory failed generate a renamed message",
                            processOutputContains(uutLogEntries, "Renamed"));
            Assert.assertTrue("BulkIngestMapFileLoader#takeOwnershipJobDirectory failed generate another process took ownership message",
                            processOutputContains(uutLogEntries, "Another process already took ownership of "));
            Assert.assertTrue("BulkIngestMapFileLoader#takeOwnershipJobDirectory failed to call FileSystem#rename",
                            processOutputContains(calls, "FileSystem#rename("));
            Assert.assertTrue("BulkIngestMapFileLoader#takeOwnershipJobDirectory failed to call FileSystem#exists",
                            processOutputContains(calls, "FileSystem#exists("));
        } finally {

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, null, null);

            BulkIngestMapFileLoaderTest.LOG.info("testTakeOwnershipJobDirectoryFailedRenameLoadedExists completed.");
        }

    }

    @Test
    public void testTakeOwnershipJobDirectoryFailedRenameLoadedDoesNotExists() throws Exception {

        BulkIngestMapFileLoaderTest.LOG.info("testTakeOwnershipJobDirectoryFailedRenameLoadedDoesNotExists called...");

        try {

            URL url = BulkIngestMapFileLoaderTest.class.getResource("/datawave/ingest/mapreduce/job/");

            BulkIngestMapFileLoader uut = createBulkIngestFileMapLoader(url);

            Assert.assertNotNull("BulkIngestMapFileLoader constructor failed to create an instance.", uut);

            Map<String,Boolean> existsResults = new HashMap<>();

            String filePath = String.format("%s%s", url, BulkIngestMapFileLoader.LOADING_FILE_MARKER);
            existsResults.put(filePath, Boolean.FALSE);

            BulkIngestMapFileLoaderTest.WrappedLocalFileSystem fs = new BulkIngestMapFileLoaderTest.WrappedLocalFileSystem(createMockInputStream(),
                            new FileStatus[] {createMockFileStatus()}, false, false, false, false, existsResults, false, false);

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, conf, fs);

            Path jobDirectory = createNewPath(url);

            boolean results = uut.takeOwnershipJobDirectory(jobDirectory);

            Assert.assertFalse("BulkIngestMapFileLoader#takeOwnershipJobDirectory failed to return false as expected.", results);

            List<String> uutLogEntries = retrieveUUTLogs();

            List<String> calls = fs.callsLogs();

            Assert.assertTrue("BulkIngestMapFileLoader#takeOwnershipJobDirectory failed generate a renamed message",
                            processOutputContains(uutLogEntries, "Renamed"));
            Assert.assertTrue("BulkIngestMapFileLoader#takeOwnershipJobDirectory failed generate unable to take ownership message",
                            processOutputContains(uutLogEntries, "Unable to take ownership of "));
            Assert.assertTrue("BulkIngestMapFileLoader#takeOwnershipJobDirectory failed to call FileSystem#rename",
                            processOutputContains(calls, "FileSystem#rename("));
            Assert.assertTrue("BulkIngestMapFileLoader#takeOwnershipJobDirectory failed to call FileSystem#exists",
                            processOutputContains(calls, "FileSystem#exists("));
        } finally {

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, null, null);

            BulkIngestMapFileLoaderTest.LOG.info("testTakeOwnershipJobDirectoryFailedRenameLoadedDoesNotExists completed.");
        }

    }

    @Test
    public void testTakeOwnershipJobDirectoryRenameThrowsException() throws Exception {

        BulkIngestMapFileLoaderTest.LOG.info("testTakeOwnershipJobDirectoryRenameThrowsException called...");

        try {

            URL url = BulkIngestMapFileLoaderTest.class.getResource("/datawave/ingest/mapreduce/job/");

            BulkIngestMapFileLoader uut = createBulkIngestFileMapLoader(url);

            Assert.assertNotNull("BulkIngestMapFileLoader constructor failed to create an instance.", uut);

            Map<String,Boolean> existsResults = new HashMap<>();

            String filePath = String.format("%s%s", url, BulkIngestMapFileLoader.LOADING_FILE_MARKER);
            existsResults.put(filePath, Boolean.FALSE);

            BulkIngestMapFileLoaderTest.WrappedLocalFileSystem fs = new BulkIngestMapFileLoaderTest.WrappedLocalFileSystem(createMockInputStream(),
                            new FileStatus[] {createMockFileStatus()}, false, false, false, false, existsResults, true, false);

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, conf, fs);

            Path jobDirectory = createNewPath(url);

            boolean results = uut.takeOwnershipJobDirectory(jobDirectory);

            Assert.assertFalse("BulkIngestMapFileLoader#takeOwnershipJobDirectory failed to return false as expected.", results);

            List<String> uutLogEntries = retrieveUUTLogs();

            List<String> calls = fs.callsLogs();

            Assert.assertTrue("BulkIngestMapFileLoader#takeOwnershipJobDirectory failed generate unable to take ownership message",
                            processOutputContains(uutLogEntries, "Exception while marking "));
            Assert.assertTrue("BulkIngestMapFileLoader#takeOwnershipJobDirectory failed to call FileSystem#rename",
                            processOutputContains(calls, "FileSystem#rename("));
            Assert.assertTrue("BulkIngestMapFileLoader#takeOwnershipJobDirectory failed to call FileSystem#exists",
                            processOutputContains(calls, "FileSystem#exists("));
        } finally {

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, null, null);

            BulkIngestMapFileLoaderTest.LOG.info("testTakeOwnershipJobDirectoryRenameThrowsException completed.");
        }

    }

    @Test
    public void testTakeOwnershipJobDirectoryExistsThrowsException() throws Exception {

        BulkIngestMapFileLoaderTest.LOG.info("testTakeOwnershipJobDirectoryExistsThrowsException called...");

        try {

            URL url = BulkIngestMapFileLoaderTest.class.getResource("/datawave/ingest/mapreduce/job/");

            BulkIngestMapFileLoader uut = createBulkIngestFileMapLoader(url);

            Assert.assertNotNull("BulkIngestMapFileLoader constructor failed to create an instance.", uut);

            Map<String,Boolean> existsResults = new HashMap<>();

            String filePath = String.format("%s%s", url, BulkIngestMapFileLoader.LOADING_FILE_MARKER);
            existsResults.put(filePath, Boolean.FALSE);

            BulkIngestMapFileLoaderTest.WrappedLocalFileSystem fs = new BulkIngestMapFileLoaderTest.WrappedLocalFileSystem(createMockInputStream(),
                            new FileStatus[] {createMockFileStatus()}, false, false, false, false, existsResults, false, true);

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, conf, fs);

            Path jobDirectory = createNewPath(url);

            boolean results = uut.takeOwnershipJobDirectory(jobDirectory);

            Assert.assertFalse("BulkIngestMapFileLoader#takeOwnershipJobDirectory failed to return false as expected.", results);

            List<String> uutLogEntries = retrieveUUTLogs();

            List<String> calls = fs.callsLogs();

            Assert.assertTrue("BulkIngestMapFileLoader#takeOwnershipJobDirectory failed generate a renamed message",
                            processOutputContains(uutLogEntries, "Renamed"));
            Assert.assertTrue("BulkIngestMapFileLoader#takeOwnershipJobDirectory failed generate unable to take ownership message",
                            processOutputContains(uutLogEntries, "Exception while marking "));
            Assert.assertTrue("BulkIngestMapFileLoader#takeOwnershipJobDirectory failed to call FileSystem#rename",
                            processOutputContains(calls, "FileSystem#rename("));
            Assert.assertTrue("BulkIngestMapFileLoader#takeOwnershipJobDirectory failed to call FileSystem#exists",
                            processOutputContains(calls, "FileSystem#exists("));
        } finally {

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, null, null);

            BulkIngestMapFileLoaderTest.LOG.info("testTakeOwnershipJobDirectoryExistsThrowsException completed.");
        }

    }

    @Test
    public void testMarkJobDirectoryFailedFailedRenameAndCreate() throws Exception {

        BulkIngestMapFileLoaderTest.LOG.info("testMarkJobDirectoryFailedFailedRenameAndCreate called...");

        try {

            URL url = BulkIngestMapFileLoaderTest.class.getResource("/datawave/ingest/mapreduce/job/");

            BulkIngestMapFileLoader uut = createBulkIngestFileMapLoader(url);

            Assert.assertNotNull("BulkIngestMapFileLoader constructor failed to create an instance.", uut);

            BulkIngestMapFileLoaderTest.WrappedLocalFileSystem fs = new BulkIngestMapFileLoaderTest.WrappedLocalFileSystem(null, null, false, false, false,
                            false, null, false, false);

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, conf, fs);

            Path jobDirectory = createNewPath(url);

            boolean results = uut.markJobDirectoryFailed(url.toURI(), jobDirectory);

            Assert.assertFalse("BulkIngestMapFileLoader#markJobDirectoryFailed failed to return false as expected.", results);

            List<String> uutLogEntries = retrieveUUTLogs();

            List<String> calls = fs.callsLogs();

            Assert.assertTrue("BulkIngestMapFileLoader#markJobDirectoryFailed failed generate an unable to create message",
                            processOutputContains(uutLogEntries, "Unable to create "));
            Assert.assertTrue("BulkIngestMapFileLoader#markJobDirectoryFailed failed to call FileSystem#rename",
                            processOutputContains(calls, "FileSystem#rename("));
            Assert.assertTrue("BulkIngestMapFileLoader#markJobDirectoryFailed failed to call FileSystem#createNewFile",
                            processOutputContains(calls, "FileSystem#createNewFile("));
        } finally {

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, null, null);

            BulkIngestMapFileLoaderTest.LOG.info("testMarkJobDirectoryFailedFailedRenameAndCreate completed.");
        }

    }

    @Test
    public void testMarkJobDirectoryFailedFailedRename() throws Exception {

        BulkIngestMapFileLoaderTest.LOG.info("testMarkJobDirectoryFailedFailedRename called...");

        try {

            URL url = BulkIngestMapFileLoaderTest.class.getResource("/datawave/ingest/mapreduce/job/");

            BulkIngestMapFileLoader uut = createBulkIngestFileMapLoader(url);

            Assert.assertNotNull("BulkIngestMapFileLoader constructor failed to create an instance.", uut);

            BulkIngestMapFileLoaderTest.WrappedLocalFileSystem fs = new BulkIngestMapFileLoaderTest.WrappedLocalFileSystem(null, null, false, false, false,
                            true, null, false, false);

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, conf, fs);

            Path jobDirectory = createNewPath(url);

            boolean results = uut.markJobDirectoryFailed(url.toURI(), jobDirectory);

            List<String> calls = fs.callsLogs();

            Assert.assertTrue("BulkIngestMapFileLoader#markJobDirectoryFailed failed to return true as expected.", results);
            Assert.assertTrue("BulkIngestMapFileLoader#markJobDirectoryFailed failed to call FileSystem#rename",
                            processOutputContains(calls, "FileSystem#rename("));
            Assert.assertTrue("BulkIngestMapFileLoader#markJobDirectoryFailed failed to call FileSystem#createNewFile",
                            processOutputContains(calls, "FileSystem#createNewFile("));
        } finally {

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, null, null);

            BulkIngestMapFileLoaderTest.LOG.info("testMarkJobDirectoryFailedFailedRename completed.");
        }

    }

    @Test
    public void testMarkJobDirectoryFailedHappyPath() throws Exception {

        BulkIngestMapFileLoaderTest.LOG.info("testMarkJobDirectoryFailedHappyPath called...");

        try {

            URL url = BulkIngestMapFileLoaderTest.class.getResource("/datawave/ingest/mapreduce/job/");

            BulkIngestMapFileLoader uut = createBulkIngestFileMapLoader(url);

            Assert.assertNotNull("BulkIngestMapFileLoader constructor failed to create an instance.", uut);

            BulkIngestMapFileLoaderTest.WrappedLocalFileSystem fs = new BulkIngestMapFileLoaderTest.WrappedLocalFileSystem(null, null, false, true, false,
                            false, null, false, false);

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, conf, fs);

            Path jobDirectory = createNewPath(url);

            boolean results = uut.markJobDirectoryFailed(url.toURI(), jobDirectory);

            List<String> calls = fs.callsLogs();

            Assert.assertTrue("BulkIngestMapFileLoader#markJobDirectoryFailed failed to return true as expected.", results);
            Assert.assertTrue("BulkIngestMapFileLoader#markJobDirectoryFailed failed to call FileSystem#rename",
                            processOutputContains(calls, "FileSystem#rename("));
        } finally {

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, null, null);

            BulkIngestMapFileLoaderTest.LOG.info("testMarkJobDirectoryFailedHappyPath completed.");
        }

    }

    @Test
    public void testMarkJobDirectoryFailedHandlesThrownException() throws Exception {

        BulkIngestMapFileLoaderTest.LOG.info("testMarkJobDirectoryFailedHandlesThrownException called...");

        try {

            URL url = BulkIngestMapFileLoaderTest.class.getResource("/datawave/ingest/mapreduce/job/");

            BulkIngestMapFileLoader uut = createBulkIngestFileMapLoader(url);

            Assert.assertNotNull("BulkIngestMapFileLoader constructor failed to create an instance.", uut);

            BulkIngestMapFileLoaderTest.WrappedLocalFileSystem fs = new BulkIngestMapFileLoaderTest.WrappedLocalFileSystem(null, null, false, false, false,
                            false, null, true, false);

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, conf, fs);

            Path jobDirectory = createNewPath(url);

            boolean results = uut.markJobDirectoryFailed(url.toURI(), jobDirectory);

            Assert.assertFalse("BulkIngestMapFileLoader#markJobDirectoryFailed failed to return false as expected.", results);

            List<String> uutLogEntries = retrieveUUTLogs();

            Assert.assertTrue("BulkIngestMapFileLoader#markJobDirectoryFailed failed generate a Exception thrown message",
                            processOutputContains(uutLogEntries, "Exception while marking "));
            Assert.assertTrue("BulkIngestMapFileLoader#markJobDirectoryFailed failed to call FileSystem#rename",
                            processOutputContains(fs.callsLogs(), "FileSystem#rename("));
        } finally {

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, null, null);

            BulkIngestMapFileLoaderTest.LOG.info("testMarkJobDirectoryFailedHandlesThrownException completed.");
        }

    }

    @Test
    public void testMarkJobCleanup() throws Exception {
        BulkIngestMapFileLoaderTest.LOG.info("testMarkJobCleanup called...");

        try {

            URL url = BulkIngestMapFileLoaderTest.class.getResource("/datawave/ingest/mapreduce/job/");

            BulkIngestMapFileLoader uut = createBulkIngestFileMapLoader(url);

            Assert.assertNotNull("BulkIngestMapFileLoader constructor failed to create an instance.", uut);

            Map<String,Boolean> exists = new HashMap<>();
            String filePath = String.format("%s%s", url, BulkIngestMapFileLoader.CLEANUP_FILE_MARKER);

            exists.put(filePath, Boolean.TRUE);
            filePath = String.format("%s%s", url, BulkIngestMapFileLoader.LOADING_FILE_MARKER);
            exists.put(filePath, Boolean.FALSE);

            BulkIngestMapFileLoaderTest.WrappedLocalFileSystem fs = new BulkIngestMapFileLoaderTest.WrappedLocalFileSystem(createMockInputStream(),
                            new FileStatus[] {createMockFileStatus()}, false, true, false, false, exists, false, false);

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, conf, fs);

            Path jobDirectory = createNewPath(url);

            boolean results = uut.markDirectoryForCleanup(jobDirectory, url.toURI());

            Assert.assertTrue("BulkIngestMapFileLoader#markDirectoryForCleanup failed to return true as expected.", results);
        } finally {

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, null, null);

            BulkIngestMapFileLoaderTest.LOG.info("testMarkJobCleanup completed.");
        }
    }

    @Test
    public void testJobCleanupOnStartup() throws Exception {
        BulkIngestMapFileLoaderTest.LOG.info("testMarkJobCleanupOnStartup called...");
        try {

            URL url = BulkIngestMapFileLoaderTest.class.getResource("/datawave/ingest/mapreduce/job/");

            FileSystem mfs = FileSystem.get(conf);

            mfs.create(new Path(url.toString() + "/job.cleanup"));

            BulkIngestMapFileLoader uut = createBulkIngestFileMapLoader(url);

            Assert.assertNotNull("BulkIngestMapFileLoader constructor failed to create an instance.", uut);

            Map<String,Boolean> exists = new HashMap<>();
            String filePath = String.format("%s%s", url, BulkIngestMapFileLoader.CLEANUP_FILE_MARKER);

            exists.put(filePath, Boolean.TRUE);
            filePath = String.format("%s%s", url, BulkIngestMapFileLoader.LOADING_FILE_MARKER);
            exists.put(filePath, Boolean.FALSE);

            BulkIngestMapFileLoaderTest.WrappedLocalFileSystem fs = new BulkIngestMapFileLoaderTest.WrappedLocalFileSystem(createMockInputStream(),
                            new FileStatus[] {createMockFileStatus(new Path(url.toString() + "/job.cleaning"))}, true, true, true, false, exists, false, false);

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, conf, fs);

            List<String> calls = fs.callsLogs();

            uut.cleanJobDirectoriesOnStartup();

            Assert.assertTrue("BulkIngestMapFileLoader#cleanUpJobDirectory failed to call FileSystem#mkdirs",
                            processOutputContains(calls, "FileSystem#mkdirs("));
            Assert.assertTrue("BulkIngestMapFileLoader#cleanUpJobDirectory failed to call FileSystem#rename",
                            processOutputContains(calls, "FileSystem#rename("));
            Assert.assertTrue("BulkIngestMapFileLoader#cleanUpJobDirectory failed to call FileSystem#delete",
                            processOutputContains(calls, "FileSystem#delete("));

        } finally {

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, null, null);

            BulkIngestMapFileLoaderTest.LOG.info("testMarkJobCleanupOnStartup completed.");
        }
    }
}
