package datawave.ingest.mapreduce.job;

import static datawave.ingest.mapreduce.job.BulkIngestMapFileLoader.CLEANUP_FILE_MARKER;
import static datawave.ingest.mapreduce.job.BulkIngestMapFileLoader.COMPLETE_FILE_MARKER;
import static datawave.ingest.mapreduce.job.BulkIngestMapFileLoader.FAILED_FILE_MARKER;
import static datawave.ingest.mapreduce.job.BulkIngestMapFileLoader.INPUT_FILES_MARKER;
import static datawave.ingest.mapreduce.job.BulkIngestMapFileLoader.LOADING_FILE_MARKER;
import static datawave.ingest.mapreduce.job.BulkIngestMapFileLoader.LOADPLAN_FILE_GLOB;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.rfile.RFile;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.data.LoadPlan;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.spi.crypto.NoCryptoServiceFactory;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
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
import datawave.ingest.mapreduce.job.BulkIngestMapFileLoader.ImportMode;

@Category(IntegrationTest.class)
public class BulkIngestMapFileLoaderTest {

    @ClassRule
    public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    protected static final URI FILE_SYSTEM_URI = URI.create("file:///");

    protected static final Logger logger = Logger.getLogger(BulkIngestMapFileLoaderTest.class);

    private static final String PASSWORD = "secret";

    private static final String USER = "root";

    private static final Authorizations USER_AUTHS = new Authorizations("BAR", "FOO", "PRIVATE", "PUBLIC");

    private static final String METADATA_TABLE = "metadata";
    private static final String METADATA_RFILE_PATH = "/datawave/rfiles/metadata/I3abcdef01.rf";

    private static final String SHARD_TABLE = "shard";
    private static final String SHARD_RFILE_PATH = "/datawave/rfiles/shard/I2abcdef01.rf";

    private static final Map<String,Long> EXPECTED_TABLE_KEYCOUNT = Map.of(SHARD_TABLE, 16301L, METADATA_TABLE, 380L);

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
     * Stages a completed datawave ingest job (job.complete) in preparation for running the returned {@link BulkIngestMapFileLoader} instance against MAC
     *
     * @param jobName
     *            should uniquely identify the bulk load job to be run
     * @param loaderSleepTime
     *            desired sleep time (in ms) for the bulk loader
     * @param mode
     *            Desired bulk mode
     *
     * @return BulkIngestMapFileLoader instance for running the job
     * @throws IOException
     */
    private BulkIngestMapFileLoader newJobCompleteLoader(String jobName, int loaderSleepTime, ImportMode mode) throws Exception {

        Assert.assertFalse("jobName can't be null/empty", jobName == null || jobName.isEmpty());

        java.nio.file.Path metaSrc, metaDest, shardSrc, shardDest, inputFilesPath, inputFile, jobPathsFile;

        var parentDir = workPath.toString();
        var mapFilesDir = "mapFiles";

        Assert.assertFalse(jobName + " directory already exists", Files.exists(Paths.get(parentDir, jobName)));
        Assert.assertFalse(jobName + " flagged directory already exists", Files.exists(Paths.get(flaggedPath.toString(), jobName)));
        Assert.assertFalse(jobName + " loaded directory already exists", Files.exists(Paths.get(loadedPath.toString(), jobName)));

        // Copy metadata rfile into jobName/mapFiles/DW_METADATA_TABLE dir
        metaSrc = Paths.get(metadataRfile);
        var metaSrcString = metaSrc.toAbsolutePath().toString();
        metaDest = Files.createDirectories(Paths.get(parentDir, jobName, mapFilesDir, METADATA_TABLE));
        Files.copy(metaSrc, Paths.get(metaDest.toString(), metaSrc.getFileName().toString()));

        validateRfile(metaSrcString, conf);

        // Copy shard rfile into jobName/mapFiles/DW_SHARD_TABLE dir
        shardSrc = Paths.get(shardRfile);
        var shardSrcString = shardSrc.toAbsolutePath().toString();
        shardDest = Files.createDirectories(Paths.get(parentDir, jobName, mapFilesDir, SHARD_TABLE));
        Files.copy(shardSrc, Paths.get(shardDest.toString(), shardSrc.getFileName().toString()));

        validateRfile(shardSrcString, conf);

        // For V2, create load plans. And add some non-default splits to make things slightly more interesting
        if (mode == ImportMode.V2) {
            var shardSplits = toRowSet(toRfileScanner(shardSrcString, conf));
            setTableSplits(SHARD_TABLE, shardSplits);
            setLoadPlan(shardRfile, new Path(shardDest.toUri()), shardSplits, conf);

            var metaSplits = toRowSet(toRfileScanner(metaSrcString, conf));
            setTableSplits(METADATA_TABLE, metaSplits);
            setLoadPlan(metadataRfile, new Path(metaDest.toUri()), metaSplits, conf);
        }

        // Create 'job.paths' marker and associated dummy input file...
        inputFilesPath = Files.createDirectory(Paths.get(flaggedPath.toString(), jobName));
        inputFile = Files.createFile(Paths.get(inputFilesPath.toString(), "dummy"));
        jobPathsFile = Files.createFile(Paths.get(parentDir, jobName, INPUT_FILES_MARKER));
        Files.write(jobPathsFile, inputFile.toString().getBytes(StandardCharsets.UTF_8));

        // Create 'job.complete' marker
        Files.createFile(Paths.get(parentDir, jobName, COMPLETE_FILE_MARKER));

        return newMapFileLoader(loaderSleepTime, mode, conf);
    }

    private static BulkIngestMapFileLoader newMapFileLoader(int loaderSleepTime, ImportMode mode, Configuration config) {
        return new BulkIngestMapFileLoader(workPath.toString(), "*", cluster.getInstanceName(), cluster.getZooKeepers(), USER, new PasswordToken(PASSWORD),
                        tmpDir.toURI(), tmpDir.toURI(), tmpDir.toURI(), null, new HashMap<>(), config, 0, 1, new ArrayList<>(), loaderSleepTime,
                        loaderSleepTime, false, mode);
    }

    private static void setTableSplits(String table, SortedSet<Text> splits) throws Exception {
        try (AccumuloClient client = cluster.createAccumuloClient(USER, new PasswordToken(PASSWORD))) {
            client.tableOperations().addSplits(table, splits);
        }
    }

    private static Scanner toRfileScanner(String rfile, Configuration config) throws IOException {
        // @formatter:off
        return RFile.newScanner().from(rfile)
                .withFileSystem(FileSystem.getLocal(config))
                .withAuthorizations(USER_AUTHS).build();
        // @formatter:on
    }

    private static SortedSet<Text> toRowSet(Scanner scanner) {
        SortedSet<Text> rows = new TreeSet<>();
        if (scanner != null) {
            scanner.stream().forEach(e -> rows.add(e.getKey().getRow()));
        }
        return rows;
    }

    private static void setLoadPlan(URI rfileSrc, Path loadPlanDir, SortedSet<Text> tableSplits, Configuration config) throws IOException {
        var loadPlan = LoadPlan.compute(rfileSrc, LoadPlan.SplitResolver.from(tableSplits));
        var filename = getLoadPlanFilename(new Path(rfileSrc).getName());
        var path = new Path(String.format("%s/%s", loadPlanDir.toString(), filename));
        try (FSDataOutputStream out = FileSystem.getLocal(config).create(path)) {
            out.write(loadPlan.toJson().getBytes(StandardCharsets.UTF_8));
        }
    }

    private static String getLoadPlanFilename(String rfileName) {
        return LOADPLAN_FILE_GLOB.replaceAll("\\*", "-" + rfileName);
    }

    /**
     * Instantiates and runs {@link BulkIngestMapFileLoader} after staging a 'job.complete' directory structure, using canned rfiles for shard and metadata
     * tables. Expects/verifies that rfile imports are successful and that job directories are cleaned up as appropriate
     *
     * @param jobName
     *            Unique name for the M/R job, denoting the parent directory name for loader processing
     * @param bulkMode
     *            Desired bulk import mode
     * @throws Exception
     */
    private void runLoaderHappyTest(String jobName, ImportMode bulkMode) throws Exception {
        List<String> log = logCollector.getMessages();
        Assert.assertTrue("Unexpected log messages", log.isEmpty());

        BulkIngestMapFileLoader loader = null;

        var jobDir = Paths.get(workPath.toString(), jobName);
        var loadingFileMarker = Paths.get(jobDir.toString(), LOADING_FILE_MARKER).toAbsolutePath().toString();

        try {
            loader = newJobCompleteLoader(jobName, 1000, bulkMode);

            new Thread(loader, "map-file-watcher").start();

            // Wait up to 30 secs for the bulk loader to log completion
            for (int i = 1; i <= 15; i++) {
                Thread.sleep(2000);
                if (log.contains("Renamed file:" + loadingFileMarker + " to " + CLEANUP_FILE_MARKER)) {
                    break;
                }
            }
            Assert.assertTrue("Unexpected log output", log.contains("Bringing Map Files online for " + METADATA_TABLE));
            Assert.assertTrue("Unexpected log output", log.contains("Bringing Map Files online for " + SHARD_TABLE));
            Assert.assertTrue("Unexpected log output", log.contains("Completed bringing map files online for " + METADATA_TABLE));
            Assert.assertTrue("Unexpected log output", log.contains("Completed bringing map files online for " + SHARD_TABLE));
            Assert.assertTrue("Unexpected log output", log.contains("Marking 1 sequence files from flagged to loaded"));

            verifyImportedData(SHARD_TABLE, METADATA_TABLE);

            Assert.assertTrue(jobDir + " still exists, but should've been cleaned up by the loader", !Files.exists(jobDir));

        } finally {
            if (loader != null) {
                loader.shutdown();
            }
        }
    }

    /**
     * Instantiates and runs {@link BulkIngestMapFileLoader} using intentionally corrupted rfile(s)/loadplan(s) to verify that loader behaves as expected in the
     * face of import failure
     *
     * @param jobName
     *            Unique name for the M/R job, denoting the parent directory name for loader processing
     * @param bulkMode
     *            Desired bulk import mode
     * @throws Exception
     */
    private void runLoaderSadTest(String jobName, ImportMode bulkMode) throws Exception {
        List<String> log = logCollector.getMessages();
        Assert.assertTrue("Unexpected log messages", log.isEmpty());

        java.nio.file.Path metaRfilePath, shardRfilePath, expectedFailedMarker, mapFilesDir;

        expectedFailedMarker = Paths.get(workPath.toString(), jobName, FAILED_FILE_MARKER);
        mapFilesDir = Paths.get(workPath.toString(), jobName, "mapFiles");

        BulkIngestMapFileLoader loader = null;
        try {
            // Tee up a loader as usual
            loader = newJobCompleteLoader(jobName, 500, bulkMode);

            // rfile to corrupt...
            metaRfilePath = Paths.get(mapFilesDir.toString(), METADATA_TABLE, new Path(metadataRfile).getName());
            Assert.assertTrue(metaRfilePath + " missing after setup", Files.exists(metaRfilePath));

            // Overwrite the staged metadata rfile with bad content
            Files.delete(metaRfilePath);
            Files.createFile(metaRfilePath);
            Files.write(metaRfilePath, "Intentionally invalid rfile content here!!!".getBytes(StandardCharsets.UTF_8));

            try {
                validateRfile(metaRfilePath.toAbsolutePath().toString(), conf);
                Assert.fail();
            } catch (IOException e) {
                // Ignore
            }

            // rfile that should succeed...
            shardRfilePath = Paths.get(mapFilesDir.toString(), SHARD_TABLE, new Path(shardRfile).getName());
            Assert.assertTrue(shardRfilePath + " missing after setup", Files.exists(shardRfilePath));

            if (bulkMode == ImportMode.V2) {
                // TODO: Should client side Accumulo or DataWave code detect a corrupt rfile at this point in a load planning scenario?

                // For V2, corrupting the rfile alone isn't enough to trigger a client- or server-side import failure,
                // assuming the rfile has an associated LoadPlan already written for it. So, without additional action
                // here, the import would succeed with no way to know there's a problem until later, if/when a scan or
                // majc hits the affected tablet, resulting in the tserver throwing "IOException: Not a valid BCFile".
                // I noted that the tserver logs the full stacktrace as well as the path of the rfile culprit.

                // It's also worth noting that, in the case of actual M/R jobs, our reduce tasks (via MultiRFileOutputFormatter)
                // perform a final validating read from hdfs of all the rfiles that were written. That mitigates most, but
                // not all, of the risk associated with this particular edge case. We may want to consider similar
                // validation in BulkIngestMapFileLoader prior to the import attempt

                // For testing purposes, we'll delete the rfile's LoadPlan here to ensure job.failed for V2
                var loadPlanPath = Paths.get(mapFilesDir.toString(), METADATA_TABLE, getLoadPlanFilename(metaRfilePath.toFile().getName()));
                Files.delete(loadPlanPath);
            }

            // Start loader and wait up to 30 secs for it to mark the failure
            new Thread(loader, "map-file-watcher").start();

            for (int i = 1; i <= 10; i++) {
                Thread.sleep(3000);
                if (Files.exists(expectedFailedMarker)) {
                    break;
                }
            }

            Assert.assertTrue("Unexpected log output", log.contains("Bringing Map Files online for " + METADATA_TABLE));

            switch (bulkMode) {
                case V1:
                    // Verify failures dir has corrupted rfile, which was renamed by Accumulo
                    var failDir = Paths.get(workPath.toString(), jobName, "mapFiles", "failures", METADATA_TABLE);
                    Assert.assertTrue(Files.exists(failDir));

                    boolean found = false;
                    var failMatcher = FileSystems.getDefault().getPathMatcher("glob:I*.rf");
                    try (DirectoryStream<java.nio.file.Path> dirStream = Files.newDirectoryStream(failDir)) {
                        for (java.nio.file.Path p : dirStream) {
                            Assert.assertTrue(failMatcher.matches(p.getFileName()));
                            logger.info("Found " + p.getFileName() + " in " + failDir);
                            found = true;
                        }
                    }
                    Assert.assertTrue("We found " + failDir + "as expected, but rfile was missing", found);
                    break;
                case V2:
                    // Verify that metadata rfile remains in place
                    Assert.assertTrue("metadata rfile should've remained in place after failure", Files.exists(metaRfilePath));
                    break;
                default:
                    throw new RuntimeException("Unsupported import mode " + bulkMode);
            }

            Assert.assertTrue(shardRfilePath + " should've been imported but it got left behind", !Files.exists(shardRfilePath));

            verifyImportedData(SHARD_TABLE);

            Assert.assertTrue("Missing " + FAILED_FILE_MARKER + " marker after failed import", Files.exists(expectedFailedMarker));

        } finally {
            if (loader != null) {
                loader.shutdown();
            }
        }
    }

    /**
     * Throws an exception unless the given path is a valid rfile
     */
    private static void validateRfile(String rfilePath, Configuration conf) throws Exception {
        FileOperations fops = FileOperations.getInstance();
        try (FileSKVIterator reader = fops.newReaderBuilder().forFile(rfilePath, FileSystem.getLocal(conf), conf, NoCryptoServiceFactory.NONE)
                        .withTableConfiguration(DefaultConfiguration.getInstance()).build()) {}
    }

    private static void verifyImportedData(String... tables) throws TableNotFoundException {
        for (String table : tables) {
            Assert.assertTrue("Unexpected table name: " + table, EXPECTED_TABLE_KEYCOUNT.containsKey(table));
            long actualKeyCount = 0;
            Collection ranges = Collections.singleton(new Range());
            try (AccumuloClient client = cluster.createAccumuloClient(USER, new PasswordToken(PASSWORD))) {
                try (BatchScanner scanner = client.createBatchScanner(table, USER_AUTHS)) {
                    scanner.setRanges(ranges);
                    Iterator it = scanner.iterator();
                    while (it.hasNext()) {
                        it.next();
                        actualKeyCount++;
                    }
                }
            }
            Assert.assertEquals("Unexpected number of " + table + " entries", (long) EXPECTED_TABLE_KEYCOUNT.get(table), actualKeyCount);
        }
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

        testDriverLevel = BulkIngestMapFileLoaderTest.logger.getLevel();
        BulkIngestMapFileLoaderTest.logger.setLevel(Level.ALL);
    }

    @After
    public void teardown() {
        BulkIngestMapFileLoaderTest.logger.setLevel(testDriverLevel);
    }

    @Test
    public void testMainWithoutArgs() throws IOException, InterruptedException {

        BulkIngestMapFileLoaderTest.logger.info("testMainWitoutArgs called...");

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

            BulkIngestMapFileLoaderTest.logger.info("testMainWitoutArgs completed.");

        }
    }

    @Test
    public void testMainWithSixArgs() throws IOException, InterruptedException {

        BulkIngestMapFileLoaderTest.logger.info("testMainWithSixArgs called...");

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

            BulkIngestMapFileLoaderTest.logger.info("testMainWithSixArgs completed.");

        }
    }

    @Test
    public void testMainWithAllOptionalArgs() throws IOException, InterruptedException {

        BulkIngestMapFileLoaderTest.logger.info("testMainWithAllOptionalArgs called...");

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

            BulkIngestMapFileLoaderTest.logger.info("testMainWithAllOptionalArgs completed.");

        }
    }

    /**
     * Verify that loader in bulk V1 mode behaves as expected in the typical use case, utilizing MiniAccumuloCluster
     */
    @Test
    public void testLoaderV1SucceedsWithMAC() throws Exception {
        BulkIngestMapFileLoaderTest.logger.info("testLoaderV1SucceedsWithMAC called...");
        runLoaderHappyTest("job_shouldSucceed_V1", ImportMode.V1);
        BulkIngestMapFileLoaderTest.logger.info("testLoaderV1SucceedsWithMAC completed.");
    }

    /**
     * Verify that loader in bulk V2 mode behaves as expected in the typical use case, utilizing MiniAccumuloCluster
     */
    @Test
    public void testLoaderV2SucceedsWithMAC() throws Exception {
        BulkIngestMapFileLoaderTest.logger.info("testLoaderV2SucceedsWithMAC called...");
        runLoaderHappyTest("job_shouldSucceed_V2", ImportMode.V2);
        BulkIngestMapFileLoaderTest.logger.info("testLoaderV2SucceedsWithMAC completed.");
    }

    /**
     * Verify that loader in bulk V1 mode behaves as expected when an importDirectory failure occurs, utilizing MiniAccumuloCluster
     */
    @Test
    public void testLoaderV1FailsWithMAC() throws Exception {
        BulkIngestMapFileLoaderTest.logger.info("testLoaderV1FailsWithMAC called...");
        runLoaderSadTest("job_shouldFail_V1", ImportMode.V1);
        BulkIngestMapFileLoaderTest.logger.info("testLoaderV1FailsWithMAC completed.");
    }

    /**
     * Verify that loader in bulk V2 mode behaves as expected when an importDirectory failure occurs, utilizing MiniAccumuloCluster
     */
    @Test
    public void testLoaderV2FailsWithMAC() throws Exception {
        BulkIngestMapFileLoaderTest.logger.info("testLoaderV2FailsWithMAC called...");
        runLoaderSadTest("job_shouldFail_V2", ImportMode.V2);
        BulkIngestMapFileLoaderTest.logger.info("testLoaderV2FailsWithMAC completed.");
    }

    @Test
    public void testMainWithAllOptionalArgsNoTablePriorites() throws IOException, InterruptedException {

        BulkIngestMapFileLoaderTest.logger.info("testMainWithAllOptionalArgsNoTablePriorites called...");

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

            BulkIngestMapFileLoaderTest.logger.info("testMainWithAllOptionalArgsNoTablePriorites completed.");

        }
    }

    @Test
    public void testMainWithBadResource() throws IOException, InterruptedException {

        BulkIngestMapFileLoaderTest.logger.info("testMainWithBadResource called...");

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

            BulkIngestMapFileLoaderTest.logger.info("testMainWithBadResource completed.");

        }
    }

    @Test
    public void testMainWithBadSleepTime() throws IOException, InterruptedException {

        BulkIngestMapFileLoaderTest.logger.info("testMainWithBadSleepTime called...");

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

            BulkIngestMapFileLoaderTest.logger.info("testMainWithBadSleepTime completed.");

        }
    }

    @Test
    public void testMainWithMissingSleepTime() throws IOException, InterruptedException {

        BulkIngestMapFileLoaderTest.logger.info("testMainWithMissingSleepTime called...");

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

            BulkIngestMapFileLoaderTest.logger.info("testMainWithMissingSleepTime completed.");

        }
    }

    @Test
    public void testMainWithBadMajCThreshold() throws IOException, InterruptedException {

        BulkIngestMapFileLoaderTest.logger.info("testMainWithBadMajCThreshold called...");

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

            BulkIngestMapFileLoaderTest.logger.info("testMainWithBadMajCThreshold completed.");

        }
    }

    @Test
    public void testMainWithMissingMajCThreshold() throws IOException, InterruptedException {

        BulkIngestMapFileLoaderTest.logger.info("testMainWithMissingMajCThreshold called...");

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

            BulkIngestMapFileLoaderTest.logger.info("testMainWithMissingMajCThreshold completed.");

        }
    }

    @Test
    public void testMainWithBadMajCDelay() throws IOException, InterruptedException {

        BulkIngestMapFileLoaderTest.logger.info("testMainWithBadMajCDelay called...");

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

            BulkIngestMapFileLoaderTest.logger.info("testMainWithBadMajCDelay completed.");

        }
    }

    @Test
    public void testMainWithMissingMajCDelay() throws IOException, InterruptedException {

        BulkIngestMapFileLoaderTest.logger.info("testMainWithMissingMajCDelay called...");

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

            BulkIngestMapFileLoaderTest.logger.info("testMainWithMissingMajCDelay completed.");

        }
    }

    @Test
    public void testMainWithBadMaxDirectories() throws IOException, InterruptedException {

        BulkIngestMapFileLoaderTest.logger.info("testMainWithBadMaxDirectories called...");

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

            BulkIngestMapFileLoaderTest.logger.info("testMainWithBadMaxDirectories completed.");

        }
    }

    @Test
    public void testMainWithMissingMaxDirectories() throws IOException, InterruptedException {

        BulkIngestMapFileLoaderTest.logger.info("testMainWithMissingMaxDirectories called...");

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

            BulkIngestMapFileLoaderTest.logger.info("testMainWithMissingMaxDirectories completed.");

        }
    }

    @Test
    public void testMainWithBadNumThreads() throws IOException, InterruptedException {

        BulkIngestMapFileLoaderTest.logger.info("testMainWithBadNumThreads called...");

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

            BulkIngestMapFileLoaderTest.logger.info("testMainWithBadNumThreads completed.");

        }
    }

    @Test
    public void testMainWithMissingNumThreads() throws IOException, InterruptedException {

        BulkIngestMapFileLoaderTest.logger.info("testMainWithMissingNumThreads called...");

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

            BulkIngestMapFileLoaderTest.logger.info("testMainWithMissingNumThreads completed.");

        }
    }

    @Test
    public void testMainWithBadNumAssignThreads() throws IOException, InterruptedException {

        BulkIngestMapFileLoaderTest.logger.info("testMainWithBadNumAssignThreads called...");

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

            BulkIngestMapFileLoaderTest.logger.info("testMainWithBadNumAssignThreads completed.");

        }
    }

    @Test
    public void testMainWithMissingNumAssignThreads() throws IOException, InterruptedException {

        BulkIngestMapFileLoaderTest.logger.info("testMainWithMissingNumAssignThreads called...");

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

            BulkIngestMapFileLoaderTest.logger.info("testMainWithMissingNumAssignThreads completed.");

        }
    }

    @Test
    public void testMainWithBadSeqFileHdfs() throws IOException, InterruptedException {

        BulkIngestMapFileLoaderTest.logger.info("testMainWithBadSeqFileHdfs called...");

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

            BulkIngestMapFileLoaderTest.logger.info("testMainWithBadSeqFileHdfs completed.");

        }
    }

    @Test
    public void testMainWithMissingSeqFileHdfs() throws IOException, InterruptedException {

        BulkIngestMapFileLoaderTest.logger.info("testMainWithMissingSeqFileHdfs called...");

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

            BulkIngestMapFileLoaderTest.logger.info("testMainWithMissingSeqFileHdfs completed.");

        }
    }

    @Test
    public void testMainWithBadSrcHdfs() throws IOException, InterruptedException {

        BulkIngestMapFileLoaderTest.logger.info("testMainWithBadSrcHdfs called...");

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

            BulkIngestMapFileLoaderTest.logger.info("testMainWithBadSrcHdfs completed.");

        }
    }

    @Test
    public void testMainWithMissingSrcHdfs() throws IOException, InterruptedException {

        BulkIngestMapFileLoaderTest.logger.info("testMainWithMissingSrcHdfs called...");

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

            BulkIngestMapFileLoaderTest.logger.info("testMainWithMissingSrcHdfs completed.");

        }
    }

    @Test
    public void testMainWithBadDestHDFS() throws IOException, InterruptedException {

        BulkIngestMapFileLoaderTest.logger.info("testMainWithBadDestHDFS called...");

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

            BulkIngestMapFileLoaderTest.logger.info("testMainWithBadDestHDFS completed.");

        }
    }

    @Test
    public void testMainWithMissingDestHDFS() throws IOException, InterruptedException {

        BulkIngestMapFileLoaderTest.logger.info("testMainWithMissingDestHDFS called...");

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

            BulkIngestMapFileLoaderTest.logger.info("testMainWithMissingDestHDFS completed.");

        }
    }

    @Test
    public void testMainWithBadJT() throws IOException, InterruptedException {

        BulkIngestMapFileLoaderTest.logger.info("testMainWithBadJT called...");

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

            BulkIngestMapFileLoaderTest.logger.info("testMainWithBadJT completed.");

        }
    }

    @Test
    public void testMainWithBadShutdownPort() throws IOException, InterruptedException {

        BulkIngestMapFileLoaderTest.logger.info("testMainWithBadShutdownPort called...");

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

            BulkIngestMapFileLoaderTest.logger.info("testMainWithBadShutdownPort completed.");

        }
    }

    @Test
    public void testMainWithMissingShutdownPort() throws IOException, InterruptedException {

        BulkIngestMapFileLoaderTest.logger.info("testMainWithMissingShutdownPort called...");

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

            BulkIngestMapFileLoaderTest.logger.info("testMainWithMissingShutdownPort completed.");

        }
    }

    @Test
    public void testMainWithBadPropery() throws IOException, InterruptedException {

        BulkIngestMapFileLoaderTest.logger.info("testMainWithBadPropery called...");

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

            BulkIngestMapFileLoaderTest.logger.info("testMainWithBadPropery completed.");

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

        BulkIngestMapFileLoaderTest.logger.info("testCtors called...");

        try {
            URL url = BulkIngestMapFileLoaderTest.class.getResource("/datawave/ingest/mapreduce/job/all-splits.txt");

            BulkIngestMapFileLoader uut = createBulkIngestFileMapLoader(url);

            Assert.assertNotNull("BulkIngestMapFileLoader constructor failed to create an instance.", uut);

            uut = createBulkIngestFileMapLoader(url);

            Assert.assertNotNull("BulkIngestMapFileLoader constructor failed to create an instance.", uut);

        } finally {

            BulkIngestMapFileLoaderTest.logger.info("testCtors comleted.");

        }

    }

    @Test
    public void testCleanUpJobDirectoryHappyPath() throws Exception {

        BulkIngestMapFileLoaderTest.logger.info("testCleanUpJobDirectoryUnableToMakeDirectory called...");

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

            BulkIngestMapFileLoaderTest.logger.info("testCleanUpJobDirectoryUnableToMakeDirectory completed.");
        }

    }

    @Test
    public void testCleanUpJobDirectoryMakesDirectory() throws Exception {

        BulkIngestMapFileLoaderTest.logger.info("testCleanUpJobDirectoryMakesDirectory called...");

        try {

            URL url = BulkIngestMapFileLoaderTest.class.getResource("/datawave/ingest/mapreduce/job/all-splits.txt");

            BulkIngestMapFileLoader uut = createBulkIngestFileMapLoader(url);

            Assert.assertNotNull("BulkIngestMapFileLoader constructor failed to create an instance.", uut);

            BulkIngestMapFileLoaderTest.WrappedLocalFileSystem fs = new BulkIngestMapFileLoaderTest.WrappedLocalFileSystem(createMockInputStream(), null, true,
                            false, false, false, new HashMap<>(), false, false);

            try {
                Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, conf, fs);
            } catch (IOException ioException) {
                Assert.fail(ioException.getMessage());
            } catch (Exception e) {
                Assert.fail(e.getMessage());
            }
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

            BulkIngestMapFileLoaderTest.logger.info("testCleanUpJobDirectoryMakesDirectory completed.");
        }

    }

    @Test
    public void testCleanUpJobDirectoryUnableToMakeDirectory() throws Exception {

        BulkIngestMapFileLoaderTest.logger.info("testCleanUpJobDirectoryUnableToMakeDirectory called...");

        try {

            URL url = BulkIngestMapFileLoaderTest.class.getResource("/datawave/ingest/mapreduce/job/all-splits.txt");

            BulkIngestMapFileLoader uut = createBulkIngestFileMapLoader(url);

            Assert.assertNotNull("BulkIngestMapFileLoader constructor failed to create an instance.", uut);

            BulkIngestMapFileLoaderTest.WrappedLocalFileSystem fs = new BulkIngestMapFileLoaderTest.WrappedLocalFileSystem(
                            createMockInputStream(new String[] {"/dummy/entry"}), null, false, false, false, false, null, false, false);

            try {
                Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, conf, fs);
            } catch (Exception e) {
                Assert.fail(e.getMessage());
            }
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

            BulkIngestMapFileLoaderTest.logger.info("testCleanUpJobDirectoryUnableToMakeDirectory completed.");
        }

    }

    @Test
    public void testCleanUpJobDirectoryJobSuccess() throws Exception {

        BulkIngestMapFileLoaderTest.logger.info("testCleanUpJobDirectoryJobSuccess called...");

        try {

            URL url = BulkIngestMapFileLoaderTest.class.getResource("/datawave/ingest/mapreduce/job/all-splits.txt");

            BulkIngestMapFileLoader uut = createBulkIngestFileMapLoader(url);

            Assert.assertNotNull("BulkIngestMapFileLoader constructor failed to create an instance.", uut);

            BulkIngestMapFileLoaderTest.WrappedLocalFileSystem fs = new BulkIngestMapFileLoaderTest.WrappedLocalFileSystem(createMockInputStream(),
                            new FileStatus[0], false, false, false, false, null, false, false);
            try {
                Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, conf, fs);
            } catch (Exception e) {
                Assert.fail(e.getMessage());
            }
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

            BulkIngestMapFileLoaderTest.logger.info("testCleanUpJobDirectoryJobSuccess completed.");
        }

    }

    @Test
    public void testCleanUpJobDirectoryWithFailedJobAndFailedCreateNewFile() throws Exception {

        BulkIngestMapFileLoaderTest.logger.info("testCleanUpJobDirectoryWithFailedJobAndFailedCreateNewFile called...");

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

            BulkIngestMapFileLoaderTest.logger.info("testCleanUpJobDirectoryWithFailedJobAndFailedCreateNewFile completed.");
        }

    }

    @Test
    public void testCleanUpJobDirectoryWithFailedJobAndFailedRenames() throws Exception {

        BulkIngestMapFileLoaderTest.logger.info("testCleanUpJobDirectoryWithFailedJobAndFailedRenames called...");

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

            BulkIngestMapFileLoaderTest.logger.info("testCleanUpJobDirectoryWithFailedJobAndFailedRenames completed.");
        }

    }

    @Test
    public void testCleanUpJobDirectoryWithFailedJob() throws Exception {

        BulkIngestMapFileLoaderTest.logger.info("testCleanUpJobDirectoryWithFailedJob called...");

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

            BulkIngestMapFileLoaderTest.logger.info("testCleanUpJobDirectoryWithFailedJob completed.");
        }

    }

    @Test
    public void testTakeOwnershipJobDirectoryHappyPath() throws Exception {

        BulkIngestMapFileLoaderTest.logger.info("testTakeOwnershipJobDirectoryHappyPath called...");

        try {

            URL url = BulkIngestMapFileLoaderTest.class.getResource("/datawave/ingest/mapreduce/job/");

            BulkIngestMapFileLoader uut = createBulkIngestFileMapLoader(url);

            Assert.assertNotNull("BulkIngestMapFileLoader constructor failed to create an instance.", uut);

            Map<String,Boolean> exists = new HashMap<>();
            String filePath = String.format("%s%s", url.toString(), LOADING_FILE_MARKER);

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

            BulkIngestMapFileLoaderTest.logger.info("testTakeOwnershipJobDirectoryHappyPath completed.");
        }

    }

    @Test
    public void testTakeOwnershipJobDirectoryFailedOwnershipExchangeLoading() throws Exception {

        BulkIngestMapFileLoaderTest.logger.info("testTakeOwnershipJobDirectoryFailedOwnershipExchangeLoading called...");

        try {
            URL url = BulkIngestMapFileLoaderTest.class.getResource("/datawave/ingest/mapreduce/job/");

            BulkIngestMapFileLoader uut = createBulkIngestFileMapLoader(url);

            Assert.assertNotNull("BulkIngestMapFileLoader constructor failed to create an instance.", uut);

            Map<String,Boolean> existsResults = new HashMap<>();
            String filePath = String.format("%s%s", url, LOADING_FILE_MARKER);
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

            BulkIngestMapFileLoaderTest.logger.info("testTakeOwnershipJobDirectoryFailedOwnershipExchangeLoading completed.");
        }

    }

    @Test
    public void testTakeOwnershipJobDirectoryFailedOwnershipExchangeComplete() throws Exception {

        BulkIngestMapFileLoaderTest.logger.info("testTakeOwnershipJobDirectoryFailedOwnershipExchangeComplete called...");

        try {

            URL url = BulkIngestMapFileLoaderTest.class.getResource("/datawave/ingest/mapreduce/job/");

            BulkIngestMapFileLoader uut = createBulkIngestFileMapLoader(url);

            Assert.assertNotNull("BulkIngestMapFileLoader constructor failed to create an instance.", uut);

            Map<String,Boolean> existsResults = new HashMap<>();
            String filePath = String.format("%s%s", url, LOADING_FILE_MARKER);
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

            BulkIngestMapFileLoaderTest.logger.info("testTakeOwnershipJobDirectoryFailedOwnershipExchangeComplete completed.");
        }

    }

    @Test
    public void testTakeOwnershipJobDirectoryFailedRenameLoadedExists() throws Exception {

        BulkIngestMapFileLoaderTest.logger.info("testTakeOwnershipJobDirectoryFailedRenameLoadedExists called...");

        try {

            URL url = BulkIngestMapFileLoaderTest.class.getResource("/datawave/ingest/mapreduce/job/");

            BulkIngestMapFileLoader uut = createBulkIngestFileMapLoader(url);

            Assert.assertNotNull("BulkIngestMapFileLoader constructor failed to create an instance.", uut);

            Map<String,Boolean> existsResults = new HashMap<>();
            String filePath = String.format("%s%s", url, LOADING_FILE_MARKER);
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

            BulkIngestMapFileLoaderTest.logger.info("testTakeOwnershipJobDirectoryFailedRenameLoadedExists completed.");
        }

    }

    @Test
    public void testTakeOwnershipJobDirectoryFailedRenameLoadedDoesNotExists() throws Exception {

        BulkIngestMapFileLoaderTest.logger.info("testTakeOwnershipJobDirectoryFailedRenameLoadedDoesNotExists called...");

        try {

            URL url = BulkIngestMapFileLoaderTest.class.getResource("/datawave/ingest/mapreduce/job/");

            BulkIngestMapFileLoader uut = createBulkIngestFileMapLoader(url);

            Assert.assertNotNull("BulkIngestMapFileLoader constructor failed to create an instance.", uut);

            Map<String,Boolean> existsResults = new HashMap<>();

            String filePath = String.format("%s%s", url, LOADING_FILE_MARKER);
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

            BulkIngestMapFileLoaderTest.logger.info("testTakeOwnershipJobDirectoryFailedRenameLoadedDoesNotExists completed.");
        }

    }

    @Test
    public void testTakeOwnershipJobDirectoryRenameThrowsException() throws Exception {

        BulkIngestMapFileLoaderTest.logger.info("testTakeOwnershipJobDirectoryRenameThrowsException called...");

        try {

            URL url = BulkIngestMapFileLoaderTest.class.getResource("/datawave/ingest/mapreduce/job/");

            BulkIngestMapFileLoader uut = createBulkIngestFileMapLoader(url);

            Assert.assertNotNull("BulkIngestMapFileLoader constructor failed to create an instance.", uut);

            Map<String,Boolean> existsResults = new HashMap<>();

            String filePath = String.format("%s%s", url, LOADING_FILE_MARKER);
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

            BulkIngestMapFileLoaderTest.logger.info("testTakeOwnershipJobDirectoryRenameThrowsException completed.");
        }

    }

    @Test
    public void testTakeOwnershipJobDirectoryExistsThrowsException() throws Exception {

        BulkIngestMapFileLoaderTest.logger.info("testTakeOwnershipJobDirectoryExistsThrowsException called...");

        try {

            URL url = BulkIngestMapFileLoaderTest.class.getResource("/datawave/ingest/mapreduce/job/");

            BulkIngestMapFileLoader uut = createBulkIngestFileMapLoader(url);

            Assert.assertNotNull("BulkIngestMapFileLoader constructor failed to create an instance.", uut);

            Map<String,Boolean> existsResults = new HashMap<>();

            String filePath = String.format("%s%s", url, LOADING_FILE_MARKER);
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

            BulkIngestMapFileLoaderTest.logger.info("testTakeOwnershipJobDirectoryExistsThrowsException completed.");
        }

    }

    private BulkIngestMapFileLoader setupBulkIngestMapFileLoader(String delayPathPattern, int maxDirectories) {
        BulkIngestMapFileLoader loader = new BulkIngestMapFileLoader();
        BulkIngestMapFileLoader.DELAY_PATH_PATTERN = delayPathPattern;
        BulkIngestMapFileLoader.MAX_DIRECTORIES = maxDirectories;
        return loader;
    }

    private FileSystem setupMockFileSystem(Path pathPattern, FileStatus[] fileStatuses) throws IOException {
        FileSystem mockFileSystem = mock(FileSystem.class);
        when(mockFileSystem.globStatus(pathPattern)).thenReturn(fileStatuses);
        return mockFileSystem;
    }

    private FileStatus createFileStatusMock(Path filePath, long modificationTime) {
        FileStatus fileStatus = mock(FileStatus.class);
        when(fileStatus.getPath()).thenReturn(filePath);
        when(fileStatus.getModificationTime()).thenReturn(modificationTime);
        return fileStatus;
    }

    private void setupDelayPatternMock(FileSystem mockFileSystem, Path parentPath, String delayPattern, FileStatus[] delayStatuses) throws IOException {
        if (delayPattern != null) {
            when(mockFileSystem.globStatus(new Path(parentPath, delayPattern))).thenReturn(delayStatuses);
        }
    }

    private void assertJobDirectories(Path[] actualResult, Path... expectedPaths) {
        assertNotNull(actualResult, "The result should not be null");
        assertEquals(expectedPaths.length, actualResult.length, "Unexpected number of job directories returned");

        for (int i = 0; i < expectedPaths.length; i++) {
            assertEquals(expectedPaths[i], actualResult[i], "Unexpected directory at index " + i);
        }
    }

    @Test
    public void testGetJobDirectoriesNothingToDelay() throws IOException {
        final Path PATH_PATTERN = new Path("/path/*/job.complete");
        final String DELAY_PATH_PATTERN = "*delay_pattern*";

        // Create mocks
        Path mockParentDir1 = new Path("/parent1");
        Path mockParentDir2 = new Path("/parent2");
        FileStatus mockStatus1 = createFileStatusMock(mock(Path.class), 1000L);
        FileStatus mockStatus2 = createFileStatusMock(mock(Path.class), 2000L);

        when(mockStatus1.getPath().getParent()).thenReturn(mockParentDir1);
        when(mockStatus2.getPath().getParent()).thenReturn(mockParentDir2);

        FileSystem mockFileSystem = setupMockFileSystem(PATH_PATTERN, new FileStatus[] {mockStatus1, mockStatus2});
        setupDelayPatternMock(mockFileSystem, mockParentDir1, DELAY_PATH_PATTERN, new FileStatus[] {});
        setupDelayPatternMock(mockFileSystem, mockParentDir2, DELAY_PATH_PATTERN, new FileStatus[] {});

        // Configure loader and call method
        BulkIngestMapFileLoader testClass = setupBulkIngestMapFileLoader(DELAY_PATH_PATTERN, 999);
        Path[] result = testClass.getJobDirectories(mockFileSystem, PATH_PATTERN);

        // Verify
        assertJobDirectories(result, mockParentDir1, mockParentDir2);
    }

    @Test
    public void testGetJobDirectoriesNullDelayPattern() throws IOException {
        final Path PATH_PATTERN = new Path("/path/*/job.complete");
        final String DELAY_PATH_PATTERN = null;

        // Create mocks
        Path mockParentDir1 = new Path("/parent1");
        Path mockParentDir2 = new Path("/parent2");
        FileStatus mockStatus1 = createFileStatusMock(mock(Path.class), 1000L);
        FileStatus mockStatus2 = createFileStatusMock(mock(Path.class), 2000L);

        when(mockStatus1.getPath().getParent()).thenReturn(mockParentDir1);
        when(mockStatus2.getPath().getParent()).thenReturn(mockParentDir2);

        FileSystem mockFileSystem = setupMockFileSystem(PATH_PATTERN, new FileStatus[] {mockStatus1, mockStatus2});
        setupDelayPatternMock(mockFileSystem, mockParentDir1, DELAY_PATH_PATTERN, new FileStatus[] {});
        setupDelayPatternMock(mockFileSystem, mockParentDir2, DELAY_PATH_PATTERN, new FileStatus[] {});

        // Configure loader and call method
        BulkIngestMapFileLoader testClass = setupBulkIngestMapFileLoader(DELAY_PATH_PATTERN, 999);
        Path[] result = testClass.getJobDirectories(mockFileSystem, PATH_PATTERN);

        // Verify
        assertJobDirectories(result, mockParentDir1, mockParentDir2);
    }

    @Test
    public void testGetJobDirectoriesNoNonDelayed() throws IOException {
        final Path PATH_PATTERN = new Path("/path/*/job.complete");
        final String DELAY_PATH_PATTERN = "*delay_pattern*";

        // Create mocks
        Path mockParentDir1 = new Path("/parent1");
        Path mockParentDir2 = new Path("/parent2");
        FileStatus mockStatus1 = createFileStatusMock(mock(Path.class), 1000L);
        FileStatus mockStatus2 = createFileStatusMock(mock(Path.class), 2000L);
        FileStatus mockDelayStatus = mock(FileStatus.class);

        when(mockStatus1.getPath().getParent()).thenReturn(mockParentDir1);
        when(mockStatus2.getPath().getParent()).thenReturn(mockParentDir2);

        when(mockDelayStatus.isFile()).thenReturn(true);

        FileSystem mockFileSystem = setupMockFileSystem(PATH_PATTERN, new FileStatus[] {mockStatus1, mockStatus2});
        setupDelayPatternMock(mockFileSystem, mockParentDir1, DELAY_PATH_PATTERN, new FileStatus[] {mockDelayStatus});
        setupDelayPatternMock(mockFileSystem, mockParentDir2, DELAY_PATH_PATTERN, new FileStatus[] {mockDelayStatus});

        // Configure loader and call method
        BulkIngestMapFileLoader testClass = setupBulkIngestMapFileLoader(DELAY_PATH_PATTERN, 999);
        Path[] result = testClass.getJobDirectories(mockFileSystem, PATH_PATTERN);

        // Verify
        assertJobDirectories(result, mockParentDir1, mockParentDir2);
    }

    @Test
    public void testGetJobDirectoriesNoBacklog() throws IOException {
        final Path PATH_PATTERN = new Path("/path/*/job.complete");
        final String DELAY_PATH_PATTERN = "*delay_pattern*";

        // Create mocks
        Path mockParentDir1 = new Path("/parent1");
        Path mockParentDir2 = new Path("/parent2");
        FileStatus mockStatus1 = createFileStatusMock(mock(Path.class), 1000L);
        FileStatus mockStatus2 = createFileStatusMock(mock(Path.class), 2000L);
        FileStatus mockDelayStatus = mock(FileStatus.class);

        when(mockStatus1.getPath().getParent()).thenReturn(mockParentDir1);
        when(mockStatus2.getPath().getParent()).thenReturn(mockParentDir2);

        when(mockDelayStatus.isFile()).thenReturn(true);

        FileSystem mockFileSystem = setupMockFileSystem(PATH_PATTERN, new FileStatus[] {mockStatus1, mockStatus2});
        setupDelayPatternMock(mockFileSystem, mockParentDir1, DELAY_PATH_PATTERN, new FileStatus[] {mockDelayStatus});
        setupDelayPatternMock(mockFileSystem, mockParentDir2, DELAY_PATH_PATTERN, new FileStatus[] {});

        // Configure loader and call method
        BulkIngestMapFileLoader testClass = setupBulkIngestMapFileLoader(DELAY_PATH_PATTERN, 999);
        Path[] result = testClass.getJobDirectories(mockFileSystem, PATH_PATTERN);

        // Verify
        // mockParentDir2 should be the first in result, as dir1 was delayed.
        assertJobDirectories(result, mockParentDir2, mockParentDir1);
    }

    @Test
    public void testGetJobDirectoriesInBacklog() throws IOException {
        final Path PATH_PATTERN = new Path("/path/*/job.complete");
        final String DELAY_PATH_PATTERN = "*delay_pattern*";

        // Create mocks
        Path mockParentDir1 = new Path("/parent1");
        Path mockParentDir2 = new Path("/parent2");
        FileStatus mockStatus1 = createFileStatusMock(mock(Path.class), 1000L);
        FileStatus mockStatus2 = createFileStatusMock(mock(Path.class), 2000L);
        FileStatus mockDelayStatus = mock(FileStatus.class);

        when(mockStatus1.getPath().getParent()).thenReturn(mockParentDir1);
        when(mockStatus2.getPath().getParent()).thenReturn(mockParentDir2);

        when(mockDelayStatus.isFile()).thenReturn(true);

        FileSystem mockFileSystem = setupMockFileSystem(PATH_PATTERN, new FileStatus[] {mockStatus1, mockStatus2});
        setupDelayPatternMock(mockFileSystem, mockParentDir1, DELAY_PATH_PATTERN, new FileStatus[] {mockDelayStatus});
        setupDelayPatternMock(mockFileSystem, mockParentDir2, DELAY_PATH_PATTERN, new FileStatus[] {});

        // Configure loader and call method
        BulkIngestMapFileLoader testClass = setupBulkIngestMapFileLoader(DELAY_PATH_PATTERN, 5);
        BulkIngestMapFileLoader.NUM_CONSIDERED_BACKLOG = 1;
        Path[] result = testClass.getJobDirectories(mockFileSystem, PATH_PATTERN);

        // Verify
        assertJobDirectories(result, mockParentDir2);
    }

    @Test
    public void testMarkJobDirectoryFailedFailedRenameAndCreate() throws Exception {

        BulkIngestMapFileLoaderTest.logger.info("testMarkJobDirectoryFailedFailedRenameAndCreate called...");

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

            BulkIngestMapFileLoaderTest.logger.info("testMarkJobDirectoryFailedFailedRenameAndCreate completed.");
        }

    }

    @Test
    public void testMarkJobDirectoryFailedFailedRename() throws Exception {

        BulkIngestMapFileLoaderTest.logger.info("testMarkJobDirectoryFailedFailedRename called...");

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

            BulkIngestMapFileLoaderTest.logger.info("testMarkJobDirectoryFailedFailedRename completed.");
        }

    }

    @Test
    public void testMarkJobDirectoryFailedHappyPath() throws Exception {

        BulkIngestMapFileLoaderTest.logger.info("testMarkJobDirectoryFailedHappyPath called...");

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

            BulkIngestMapFileLoaderTest.logger.info("testMarkJobDirectoryFailedHappyPath completed.");
        }

    }

    @Test
    public void testMarkJobDirectoryFailedHandlesThrownException() throws Exception {

        BulkIngestMapFileLoaderTest.logger.info("testMarkJobDirectoryFailedHandlesThrownException called...");

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

            BulkIngestMapFileLoaderTest.logger.info("testMarkJobDirectoryFailedHandlesThrownException completed.");
        }

    }

    @Test
    public void testMarkJobCleanup() throws Exception {
        BulkIngestMapFileLoaderTest.logger.info("testMarkJobCleanup called...");

        try {

            URL url = BulkIngestMapFileLoaderTest.class.getResource("/datawave/ingest/mapreduce/job/");

            BulkIngestMapFileLoader uut = createBulkIngestFileMapLoader(url);

            Assert.assertNotNull("BulkIngestMapFileLoader constructor failed to create an instance.", uut);

            Map<String,Boolean> exists = new HashMap<>();
            String filePath = String.format("%s%s", url, CLEANUP_FILE_MARKER);

            exists.put(filePath, Boolean.TRUE);
            filePath = String.format("%s%s", url, LOADING_FILE_MARKER);
            exists.put(filePath, Boolean.FALSE);

            BulkIngestMapFileLoaderTest.WrappedLocalFileSystem fs = new BulkIngestMapFileLoaderTest.WrappedLocalFileSystem(createMockInputStream(),
                            new FileStatus[] {createMockFileStatus()}, false, true, false, false, exists, false, false);

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, conf, fs);

            Path jobDirectory = createNewPath(url);

            boolean results = uut.markDirectoryForCleanup(jobDirectory, url.toURI());

            Assert.assertTrue("BulkIngestMapFileLoader#markDirectoryForCleanup failed to return true as expected.", results);
        } finally {

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, null, null);

            BulkIngestMapFileLoaderTest.logger.info("testMarkJobCleanup completed.");
        }
    }

    @Test
    public void testJobCleanupOnStartup() throws Exception {
        BulkIngestMapFileLoaderTest.logger.info("testMarkJobCleanupOnStartup called...");
        try {

            URL url = BulkIngestMapFileLoaderTest.class.getResource("/datawave/ingest/mapreduce/job/");

            FileSystem mfs = FileSystem.get(conf);

            mfs.create(new Path(url.toString() + "/job.cleanup"));

            BulkIngestMapFileLoader uut = createBulkIngestFileMapLoader(url);

            Assert.assertNotNull("BulkIngestMapFileLoader constructor failed to create an instance.", uut);

            Map<String,Boolean> exists = new HashMap<>();
            String filePath = String.format("%s%s", url, CLEANUP_FILE_MARKER);

            exists.put(filePath, Boolean.TRUE);
            filePath = String.format("%s%s", url, LOADING_FILE_MARKER);
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

            BulkIngestMapFileLoaderTest.logger.info("testMarkJobCleanupOnStartup completed.");
        }
    }
}
