package datawave.ingest.mapreduce.job;

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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import datawave.common.test.integration.IntegrationTest;
import datawave.common.test.logging.TestLogCollector;
import datawave.common.test.utils.ProcessUtils;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.ingest.BaseIngestHelper;
import datawave.ingest.input.reader.EventRecordReader;
import datawave.ingest.input.reader.LongLineEventRecordReader;

import org.apache.accumulo.core.client.security.tokens.PasswordToken;
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;
import org.junit.experimental.categories.Category;
import org.powermock.api.easymock.PowerMock;
import org.powermock.reflect.Whitebox;

import com.google.common.collect.Multimap;

@Category(IntegrationTest.class)
public class BulkIngestMapFileLoaderTest {

    protected static final URI FILE_SYSTEM_URI = URI.create("file:///");

    protected static final Logger logger = Logger.getLogger(BulkIngestMapFileLoaderTest.class);
    protected Level testDriverLevel;

    private List<String> systemProperties;

    @Rule
    public final ExpectedSystemExit exit = ExpectedSystemExit.none();

    @Rule
    public TestLogCollector logCollector = new TestLogCollector.Builder().with(BulkIngestMapFileLoader.class, Level.ALL).build();

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

    protected ByteArrayInputStream createMockInputStream() throws IOException {

        return createMockInputStream(null);
    }

    protected ByteArrayInputStream createMockInputStream(String[] additionalEntries) throws IOException {

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(baos));

        for (int index = 0; index < 3; index++) {

            bw.write(String.format("/flagged/file%d\n", index));
        }

        if (null != additionalEntries) {

            for (String entries : additionalEntries) {

                bw.write(entries);
            }
        }

        bw.close();
        baos.close();

        return new ByteArrayInputStream(baos.toByteArray());
    }

    protected FileStatus createMockFileStatus() throws Exception {

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

    @Test
    public void testCtors() {

        BulkIngestMapFileLoaderTest.logger.info("testCtors called...");

        try {
            URL url = BulkIngestMapFileLoaderTest.class.getResource("/datawave/ingest/mapreduce/job/all-splits.txt");

            String workDir = ".";
            String jobDirPattern = "jobs/";
            String instanceName = "localhost";
            String zooKeepers = "localhost";
            URI seqFileHdfs = url.toURI();
            URI srcHdfs = url.toURI();
            URI destHdfs = url.toURI();
            String jobtracker = "localhost";
            Map<String,Integer> tablePriorities = new HashMap<>();
            Configuration conf = new Configuration();

            BulkIngestMapFileLoader uut = new BulkIngestMapFileLoader(workDir, jobDirPattern, instanceName, zooKeepers, "user", new PasswordToken("pass"),
                            seqFileHdfs, srcHdfs, destHdfs, jobtracker, tablePriorities, conf, 0);

            Assert.assertNotNull("BulkIngestMapFileLoader constructor failed to create an instance.", uut);

            uut = new BulkIngestMapFileLoader(workDir, jobDirPattern, instanceName, zooKeepers, "user", new PasswordToken("pass"), seqFileHdfs, srcHdfs,
                            destHdfs, jobtracker, tablePriorities, conf, 0);

            Assert.assertNotNull("BulkIngestMapFileLoader constructor failed to create an instance.", uut);

        } catch (URISyntaxException e) {

            Assert.fail("Class#getResource failed to return a valid URI");

        } finally {

            BulkIngestMapFileLoaderTest.logger.info("testCtors comleted.");

        }

    }

    @Test
    public void testCleanUpJobDirectoryHappyPath() throws Exception {

        BulkIngestMapFileLoaderTest.logger.info("testCleanUpJobDirectoryUnableToMakeDirectory called...");

        try {

            URL url = BulkIngestMapFileLoaderTest.class.getResource("/datawave/ingest/mapreduce/job/all-splits.txt");

            String workDir = ".";
            String jobDirPattern = "jobs/";
            String instanceName = "localhost";
            String zooKeepers = "localhost";
            URI seqFileHdfs = url.toURI();
            URI srcHdfs = url.toURI();
            URI destHdfs = url.toURI();
            String jobtracker = "localhost";
            Map<String,Integer> tablePriorities = new HashMap<>();
            Configuration conf = new Configuration();

            BulkIngestMapFileLoader uut = new BulkIngestMapFileLoader(workDir, jobDirPattern, instanceName, zooKeepers, "user", new PasswordToken("pass"),
                            seqFileHdfs, srcHdfs, destHdfs, jobtracker, tablePriorities, conf, 0);

            Assert.assertNotNull("BulkIngestMapFileLoader constructor failed to create an instance.", uut);

            BulkIngestMapFileLoaderTest.WrappedLocalFileSystem fs = new BulkIngestMapFileLoaderTest.WrappedLocalFileSystem(createMockInputStream(), null, true,
                            true, false, false, null, false, false);

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, conf, fs);

            Path mapFilesDir = new Path(url.toString());

            uut.cleanUpJobDirectory(mapFilesDir);

            List<String> calls = fs.callsLogs();

            Assert.assertTrue("BulkIngestMapFileLoader#cleanUpJobDirectory failed to call FileSystem#mkdirs",
                            processOutputContains(calls, "FileSystem#mkdirs("));
            Assert.assertTrue("BulkIngestMapFileLoader#cleanUpJobDirectory failed to call FileSystem#rename",
                            processOutputContains(calls, "FileSystem#rename("));
            Assert.assertTrue("BulkIngestMapFileLoader#cleanUpJobDirectory failed to call FileSystem#delete",
                            processOutputContains(calls, "FileSystem#delete("));

        } catch (AssertionError ae) {

            // Ignore any assertion failed...
        } catch (URISyntaxException e) {

            Assert.fail("Class#getResource failed to return a valid URI");

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

            String workDir = ".";
            String jobDirPattern = "jobs/";
            String instanceName = "localhost";
            String zooKeepers = "localhost";
            URI seqFileHdfs = url.toURI();
            URI srcHdfs = url.toURI();
            URI destHdfs = url.toURI();
            String jobtracker = "localhost";
            Map<String,Integer> tablePriorities = new HashMap<>();
            Configuration conf = new Configuration();

            BulkIngestMapFileLoader uut = new BulkIngestMapFileLoader(workDir, jobDirPattern, instanceName, zooKeepers, "user", new PasswordToken("pass"),
                            seqFileHdfs, srcHdfs, destHdfs, jobtracker, tablePriorities, conf, 0);

            Assert.assertNotNull("BulkIngestMapFileLoader constructor failed to create an instance.", uut);

            BulkIngestMapFileLoaderTest.WrappedLocalFileSystem fs = new BulkIngestMapFileLoaderTest.WrappedLocalFileSystem(createMockInputStream(), null, true,
                            false, false, false, new HashMap<>(), false, false);

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, conf, fs);

            Path mapFilesDir = new Path(url.toString());

            uut.cleanUpJobDirectory(mapFilesDir);

            Assert.fail();

        } catch (URISyntaxException e) {

            Assert.fail("Class#getResource failed to return a valid URI");

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

            String workDir = ".";
            String jobDirPattern = "jobs/";
            String instanceName = "localhost";
            String zooKeepers = "localhost";
            URI seqFileHdfs = url.toURI();
            URI srcHdfs = url.toURI();
            URI destHdfs = url.toURI();
            String jobtracker = "localhost";
            Map<String,Integer> tablePriorities = new HashMap<>();
            Configuration conf = new Configuration();

            BulkIngestMapFileLoader uut = new BulkIngestMapFileLoader(workDir, jobDirPattern, instanceName, zooKeepers, "user", new PasswordToken("pass"),
                            seqFileHdfs, srcHdfs, destHdfs, jobtracker, tablePriorities, conf, 0);

            Assert.assertNotNull("BulkIngestMapFileLoader constructor failed to create an instance.", uut);

            BulkIngestMapFileLoaderTest.WrappedLocalFileSystem fs = new BulkIngestMapFileLoaderTest.WrappedLocalFileSystem(
                            createMockInputStream(new String[] {"/dummy/entry"}), null, false, false, false, false, null, false, false);

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, conf, fs);

            Path mapFilesDir = new Path(url.toString());

            uut.cleanUpJobDirectory(mapFilesDir);

            Assert.fail();

        } catch (URISyntaxException e) {

            Assert.fail("Class#getResource failed to return a valid URI");

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

            String workDir = ".";
            String jobDirPattern = "jobs/";
            String instanceName = "localhost";
            String zooKeepers = "localhost";
            URI seqFileHdfs = url.toURI();
            URI srcHdfs = url.toURI();
            URI destHdfs = url.toURI();
            String jobtracker = "localhost";
            Map<String,Integer> tablePriorities = new HashMap<>();
            Configuration conf = new Configuration();

            BulkIngestMapFileLoader uut = new BulkIngestMapFileLoader(workDir, jobDirPattern, instanceName, zooKeepers, "user", new PasswordToken("pass"),
                            seqFileHdfs, srcHdfs, destHdfs, jobtracker, tablePriorities, conf, 0);

            Assert.assertNotNull("BulkIngestMapFileLoader constructor failed to create an instance.", uut);

            BulkIngestMapFileLoaderTest.WrappedLocalFileSystem fs = new BulkIngestMapFileLoaderTest.WrappedLocalFileSystem(createMockInputStream(),
                            new FileStatus[0], false, false, false, false, null, false, false);

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, conf, fs);

            Path mapFilesDir = new Path(url.toString());

            uut.cleanUpJobDirectory(mapFilesDir);

            Assert.fail();

        } catch (URISyntaxException e) {

            Assert.fail("Class#getResource failed to return a valid URI");

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

            String workDir = ".";
            String jobDirPattern = "jobs/";
            String instanceName = "localhost";
            String zooKeepers = "localhost";
            URI seqFileHdfs = url.toURI();
            URI srcHdfs = url.toURI();
            URI destHdfs = url.toURI();
            String jobtracker = "localhost";
            Map<String,Integer> tablePriorities = new HashMap<>();
            Configuration conf = new Configuration();

            BulkIngestMapFileLoader uut = new BulkIngestMapFileLoader(workDir, jobDirPattern, instanceName, zooKeepers, "user", new PasswordToken("pass"),
                            seqFileHdfs, srcHdfs, destHdfs, jobtracker, tablePriorities, conf, 0);

            Assert.assertNotNull("BulkIngestMapFileLoader constructor failed to create an instance.", uut);

            BulkIngestMapFileLoaderTest.WrappedLocalFileSystem fs = new BulkIngestMapFileLoaderTest.WrappedLocalFileSystem(createMockInputStream(),
                            new FileStatus[] {createMockFileStatus()}, false, false, false, false, null, false, false);

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, conf, fs);

            Path mapFilesDir = new Path(url.toString());

            uut.cleanUpJobDirectory(mapFilesDir);

            List<String> uutLogEntries = retrieveUUTLogs();

            Assert.assertTrue("BulkIngestMapFileLoader#cleanUpJobDirectory failed to call FileSystem#mkdirs",
                            processOutputContains(uutLogEntries, "There were failures bringing map files online."));
            Assert.assertTrue("BulkIngestMapFileLoader#cleanUpJobDirectory failed to call FileSystem#rename",
                            processOutputContains(uutLogEntries, "Unable to rename map files directory "));
            Assert.assertTrue("BulkIngestMapFileLoader#cleanUpJobDirectory failed to call FileSystem#delete",
                            processOutputContains(uutLogEntries, "Unable to create job.failed file in "));

        } catch (URISyntaxException e) {

            Assert.fail("Class#getResource failed to return a valid URI");

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

            String workDir = ".";
            String jobDirPattern = "jobs/";
            String instanceName = "localhost";
            String zooKeepers = "localhost";
            URI seqFileHdfs = url.toURI();
            URI srcHdfs = url.toURI();
            URI destHdfs = url.toURI();
            String jobtracker = "localhost";
            Map<String,Integer> tablePriorities = new HashMap<>();
            Configuration conf = new Configuration();

            BulkIngestMapFileLoader uut = new BulkIngestMapFileLoader(workDir, jobDirPattern, instanceName, zooKeepers, "user", new PasswordToken("pass"),
                            seqFileHdfs, srcHdfs, destHdfs, jobtracker, tablePriorities, conf, 0);

            Assert.assertNotNull("BulkIngestMapFileLoader constructor failed to create an instance.", uut);

            BulkIngestMapFileLoaderTest.WrappedLocalFileSystem fs = new BulkIngestMapFileLoaderTest.WrappedLocalFileSystem(createMockInputStream(),
                            new FileStatus[] {createMockFileStatus()}, false, false, false, true, null, false, false);

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, conf, fs);

            Path mapFilesDir = new Path(url.toString());

            uut.cleanUpJobDirectory(mapFilesDir);

            List<String> uutLogEntries = retrieveUUTLogs();

            Assert.assertTrue("BulkIngestMapFileLoader#cleanUpJobDirectory failed to call FileSystem#mkdirs",
                            processOutputContains(uutLogEntries, "There were failures bringing map files online."));
            Assert.assertTrue("BulkIngestMapFileLoader#cleanUpJobDirectory failed to call FileSystem#rename",
                            processOutputContains(uutLogEntries, "Unable to rename map files directory "));
        } catch (URISyntaxException e) {

            Assert.fail("Class#getResource failed to return a valid URI");

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

            String workDir = ".";
            String jobDirPattern = "jobs/";
            String instanceName = "localhost";
            String zooKeepers = "localhost";
            URI seqFileHdfs = url.toURI();
            URI srcHdfs = url.toURI();
            URI destHdfs = url.toURI();
            String jobtracker = "localhost";
            Map<String,Integer> tablePriorities = new HashMap<>();
            Configuration conf = new Configuration();

            BulkIngestMapFileLoader uut = new BulkIngestMapFileLoader(workDir, jobDirPattern, instanceName, zooKeepers, "user", new PasswordToken("pass"),
                            seqFileHdfs, srcHdfs, destHdfs, jobtracker, tablePriorities, conf, 0);

            Assert.assertNotNull("BulkIngestMapFileLoader constructor failed to create an instance.", uut);

            BulkIngestMapFileLoaderTest.WrappedLocalFileSystem fs = new BulkIngestMapFileLoaderTest.WrappedLocalFileSystem(createMockInputStream(),
                            new FileStatus[] {createMockFileStatus()}, false, true, false, true, null, false, false);

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, conf, fs);

            Path mapFilesDir = new Path(url.toString());

            uut.cleanUpJobDirectory(mapFilesDir);

            List<String> uutLogEntries = retrieveUUTLogs();

            Assert.assertTrue("BulkIngestMapFileLoader#cleanUpJobDirectory failed to call FileSystem#mkdirs",
                            processOutputContains(uutLogEntries, "There were failures bringing map files online."));
        } catch (URISyntaxException e) {

            Assert.fail("Class#getResource failed to return a valid URI");

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

            String workDir = ".";
            String jobDirPattern = "jobs/";
            String instanceName = "localhost";
            String zooKeepers = "localhost";
            URI seqFileHdfs = url.toURI();
            URI srcHdfs = url.toURI();
            URI destHdfs = url.toURI();
            String jobtracker = "localhost";
            Map<String,Integer> tablePriorities = new HashMap<>();
            Configuration conf = new Configuration();

            BulkIngestMapFileLoader uut = new BulkIngestMapFileLoader(workDir, jobDirPattern, instanceName, zooKeepers, "user", new PasswordToken("pass"),
                            seqFileHdfs, srcHdfs, destHdfs, jobtracker, tablePriorities, conf, 0);

            Assert.assertNotNull("BulkIngestMapFileLoader constructor failed to create an instance.", uut);

            Map<String,Boolean> exists = new HashMap<>();
            String filePath = String.format("%s%s", url.toString(), BulkIngestMapFileLoader.LOADING_FILE_MARKER);

            exists.put(filePath, Boolean.TRUE);
            filePath = String.format("%s%s", url.toString(), BulkIngestMapFileLoader.COMPLETE_FILE_MARKER);
            exists.put(filePath, Boolean.FALSE);

            BulkIngestMapFileLoaderTest.WrappedLocalFileSystem fs = new BulkIngestMapFileLoaderTest.WrappedLocalFileSystem(createMockInputStream(),
                            new FileStatus[] {createMockFileStatus()}, false, true, false, false, exists, false, false);

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, conf, fs);

            Path jobDirectory = new Path(url.toString());

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

            String workDir = ".";
            String jobDirPattern = "jobs/";
            String instanceName = "localhost";
            String zooKeepers = "localhost";
            URI seqFileHdfs = url.toURI();
            URI srcHdfs = url.toURI();
            URI destHdfs = url.toURI();
            String jobtracker = "localhost";
            Map<String,Integer> tablePriorities = new HashMap<>();
            Configuration conf = new Configuration();

            BulkIngestMapFileLoader uut = new BulkIngestMapFileLoader(workDir, jobDirPattern, instanceName, zooKeepers, "user", new PasswordToken("pass"),
                            seqFileHdfs, srcHdfs, destHdfs, jobtracker, tablePriorities, conf, 0);

            Assert.assertNotNull("BulkIngestMapFileLoader constructor failed to create an instance.", uut);

            Map<String,Boolean> existsResults = new HashMap<>();
            String filePath = String.format("%s%s", url.toString(), BulkIngestMapFileLoader.LOADING_FILE_MARKER);
            existsResults.put(filePath, Boolean.FALSE);
            filePath = String.format("%s%s", url.toString(), BulkIngestMapFileLoader.COMPLETE_FILE_MARKER);
            existsResults.put(filePath, Boolean.TRUE);

            BulkIngestMapFileLoaderTest.WrappedLocalFileSystem fs = new BulkIngestMapFileLoaderTest.WrappedLocalFileSystem(createMockInputStream(),
                            new FileStatus[] {createMockFileStatus()}, false, true, false, false, existsResults, false, false);

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, conf, fs);

            Path jobDirectory = new Path(url.toString());

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

            String workDir = ".";
            String jobDirPattern = "jobs/";
            String instanceName = "localhost";
            String zooKeepers = "localhost";
            URI seqFileHdfs = url.toURI();
            URI srcHdfs = url.toURI();
            URI destHdfs = url.toURI();
            String jobtracker = "localhost";
            Map<String,Integer> tablePriorities = new HashMap<>();
            Configuration conf = new Configuration();

            BulkIngestMapFileLoader uut = new BulkIngestMapFileLoader(workDir, jobDirPattern, instanceName, zooKeepers, "user", new PasswordToken("pass"),
                            seqFileHdfs, srcHdfs, destHdfs, jobtracker, tablePriorities, conf, 0);

            Assert.assertNotNull("BulkIngestMapFileLoader constructor failed to create an instance.", uut);

            Map<String,Boolean> existsResults = new HashMap<>();
            String filePath = String.format("%s%s", url.toString(), BulkIngestMapFileLoader.LOADING_FILE_MARKER);
            existsResults.put(filePath, Boolean.TRUE);
            filePath = String.format("%s%s", url.toString(), BulkIngestMapFileLoader.COMPLETE_FILE_MARKER);
            existsResults.put(filePath, Boolean.TRUE);

            BulkIngestMapFileLoaderTest.WrappedLocalFileSystem fs = new BulkIngestMapFileLoaderTest.WrappedLocalFileSystem(createMockInputStream(),
                            new FileStatus[] {createMockFileStatus()}, false, true, false, false, existsResults, false, false);

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, conf, fs);

            Path jobDirectory = new Path(url.toString());

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

            String workDir = ".";
            String jobDirPattern = "jobs/";
            String instanceName = "localhost";
            String zooKeepers = "localhost";
            URI seqFileHdfs = url.toURI();
            URI srcHdfs = url.toURI();
            URI destHdfs = url.toURI();
            String jobtracker = "localhost";
            Map<String,Integer> tablePriorities = new HashMap<>();
            Configuration conf = new Configuration();

            BulkIngestMapFileLoader uut = new BulkIngestMapFileLoader(workDir, jobDirPattern, instanceName, zooKeepers, "user", new PasswordToken("pass"),
                            seqFileHdfs, srcHdfs, destHdfs, jobtracker, tablePriorities, conf, 0);

            Assert.assertNotNull("BulkIngestMapFileLoader constructor failed to create an instance.", uut);

            Map<String,Boolean> existsResults = new HashMap<>();
            String filePath = String.format("%s%s", url.toString(), BulkIngestMapFileLoader.LOADING_FILE_MARKER);
            existsResults.put(filePath, Boolean.TRUE);

            BulkIngestMapFileLoaderTest.WrappedLocalFileSystem fs = new BulkIngestMapFileLoaderTest.WrappedLocalFileSystem(createMockInputStream(),
                            new FileStatus[] {createMockFileStatus()}, false, false, false, false, existsResults, false, false);

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, conf, fs);

            Path jobDirectory = new Path(url.toString());

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

            String workDir = ".";
            String jobDirPattern = "jobs/";
            String instanceName = "localhost";
            String zooKeepers = "localhost";
            URI seqFileHdfs = url.toURI();
            URI srcHdfs = url.toURI();
            URI destHdfs = url.toURI();
            String jobtracker = "localhost";
            Map<String,Integer> tablePriorities = new HashMap<>();
            Configuration conf = new Configuration();

            BulkIngestMapFileLoader uut = new BulkIngestMapFileLoader(workDir, jobDirPattern, instanceName, zooKeepers, "user", new PasswordToken("pass"),
                            seqFileHdfs, srcHdfs, destHdfs, jobtracker, tablePriorities, conf, 0);

            Assert.assertNotNull("BulkIngestMapFileLoader constructor failed to create an instance.", uut);

            Map<String,Boolean> existsResults = new HashMap<>();

            String filePath = String.format("%s%s", url.toString(), BulkIngestMapFileLoader.LOADING_FILE_MARKER);
            existsResults.put(filePath, Boolean.FALSE);

            BulkIngestMapFileLoaderTest.WrappedLocalFileSystem fs = new BulkIngestMapFileLoaderTest.WrappedLocalFileSystem(createMockInputStream(),
                            new FileStatus[] {createMockFileStatus()}, false, false, false, false, existsResults, false, false);

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, conf, fs);

            Path jobDirectory = new Path(url.toString());

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

            String workDir = ".";
            String jobDirPattern = "jobs/";
            String instanceName = "localhost";
            String zooKeepers = "localhost";
            URI seqFileHdfs = url.toURI();
            URI srcHdfs = url.toURI();
            URI destHdfs = url.toURI();
            String jobtracker = "localhost";
            Map<String,Integer> tablePriorities = new HashMap<>();
            Configuration conf = new Configuration();

            BulkIngestMapFileLoader uut = new BulkIngestMapFileLoader(workDir, jobDirPattern, instanceName, zooKeepers, "user", new PasswordToken("pass"),
                            seqFileHdfs, srcHdfs, destHdfs, jobtracker, tablePriorities, conf, 0);

            Assert.assertNotNull("BulkIngestMapFileLoader constructor failed to create an instance.", uut);

            Map<String,Boolean> existsResults = new HashMap<>();

            String filePath = String.format("%s%s", url.toString(), BulkIngestMapFileLoader.LOADING_FILE_MARKER);
            existsResults.put(filePath, Boolean.FALSE);

            BulkIngestMapFileLoaderTest.WrappedLocalFileSystem fs = new BulkIngestMapFileLoaderTest.WrappedLocalFileSystem(createMockInputStream(),
                            new FileStatus[] {createMockFileStatus()}, false, false, false, false, existsResults, true, false);

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, conf, fs);

            Path jobDirectory = new Path(url.toString());

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

            String workDir = ".";
            String jobDirPattern = "jobs/";
            String instanceName = "localhost";
            String zooKeepers = "localhost";
            URI seqFileHdfs = url.toURI();
            URI srcHdfs = url.toURI();
            URI destHdfs = url.toURI();
            String jobtracker = "localhost";
            Map<String,Integer> tablePriorities = new HashMap<>();
            Configuration conf = new Configuration();

            BulkIngestMapFileLoader uut = new BulkIngestMapFileLoader(workDir, jobDirPattern, instanceName, zooKeepers, "user", new PasswordToken("pass"),
                            seqFileHdfs, srcHdfs, destHdfs, jobtracker, tablePriorities, conf, 0);

            Assert.assertNotNull("BulkIngestMapFileLoader constructor failed to create an instance.", uut);

            Map<String,Boolean> existsResults = new HashMap<>();

            String filePath = String.format("%s%s", url.toString(), BulkIngestMapFileLoader.LOADING_FILE_MARKER);
            existsResults.put(filePath, Boolean.FALSE);

            BulkIngestMapFileLoaderTest.WrappedLocalFileSystem fs = new BulkIngestMapFileLoaderTest.WrappedLocalFileSystem(createMockInputStream(),
                            new FileStatus[] {createMockFileStatus()}, false, false, false, false, existsResults, false, true);

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, conf, fs);

            Path jobDirectory = new Path(url.toString());

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

    @Test
    public void testMarkJobDirectoryFailedFailedRenameAndCreate() throws Exception {

        BulkIngestMapFileLoaderTest.logger.info("testMarkJobDirectoryFailedFailedRenameAndCreate called...");

        try {

            URL url = BulkIngestMapFileLoaderTest.class.getResource("/datawave/ingest/mapreduce/job/");

            String workDir = ".";
            String jobDirPattern = "jobs/";
            String instanceName = "localhost";
            String zooKeepers = "localhost";
            URI seqFileHdfs = url.toURI();
            URI srcHdfs = url.toURI();
            URI destHdfs = url.toURI();
            String jobtracker = "localhost";
            Map<String,Integer> tablePriorities = new HashMap<>();
            Configuration conf = new Configuration();

            BulkIngestMapFileLoader uut = new BulkIngestMapFileLoader(workDir, jobDirPattern, instanceName, zooKeepers, "user", new PasswordToken("pass"),
                            seqFileHdfs, srcHdfs, destHdfs, jobtracker, tablePriorities, conf, 0);

            Assert.assertNotNull("BulkIngestMapFileLoader constructor failed to create an instance.", uut);

            BulkIngestMapFileLoaderTest.WrappedLocalFileSystem fs = new BulkIngestMapFileLoaderTest.WrappedLocalFileSystem(null, null, false, false, false,
                            false, null, false, false);

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, conf, fs);

            Path jobDirectory = new Path(url.toString());

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

            String workDir = ".";
            String jobDirPattern = "jobs/";
            String instanceName = "localhost";
            String zooKeepers = "localhost";
            URI seqFileHdfs = url.toURI();
            URI srcHdfs = url.toURI();
            URI destHdfs = url.toURI();
            String jobtracker = "localhost";
            Map<String,Integer> tablePriorities = new HashMap<>();
            Configuration conf = new Configuration();

            BulkIngestMapFileLoader uut = new BulkIngestMapFileLoader(workDir, jobDirPattern, instanceName, zooKeepers, "user", new PasswordToken("pass"),
                            seqFileHdfs, srcHdfs, destHdfs, jobtracker, tablePriorities, conf, 0);

            Assert.assertNotNull("BulkIngestMapFileLoader constructor failed to create an instance.", uut);

            BulkIngestMapFileLoaderTest.WrappedLocalFileSystem fs = new BulkIngestMapFileLoaderTest.WrappedLocalFileSystem(null, null, false, false, false,
                            true, null, false, false);

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, conf, fs);

            Path jobDirectory = new Path(url.toString());

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

            String workDir = ".";
            String jobDirPattern = "jobs/";
            String instanceName = "localhost";
            String zooKeepers = "localhost";
            URI seqFileHdfs = url.toURI();
            URI srcHdfs = url.toURI();
            URI destHdfs = url.toURI();
            String jobtracker = "localhost";
            Map<String,Integer> tablePriorities = new HashMap<>();
            Configuration conf = new Configuration();

            BulkIngestMapFileLoader uut = new BulkIngestMapFileLoader(workDir, jobDirPattern, instanceName, zooKeepers, "user", new PasswordToken("pass"),
                            seqFileHdfs, srcHdfs, destHdfs, jobtracker, tablePriorities, conf, 0);

            Assert.assertNotNull("BulkIngestMapFileLoader constructor failed to create an instance.", uut);

            BulkIngestMapFileLoaderTest.WrappedLocalFileSystem fs = new BulkIngestMapFileLoaderTest.WrappedLocalFileSystem(null, null, false, true, false,
                            false, null, false, false);

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, conf, fs);

            Path jobDirectory = new Path(url.toString());

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

            String workDir = ".";
            String jobDirPattern = "jobs/";
            String instanceName = "localhost";
            String zooKeepers = "localhost";
            URI seqFileHdfs = url.toURI();
            URI srcHdfs = url.toURI();
            URI destHdfs = url.toURI();
            String jobtracker = "localhost";
            Map<String,Integer> tablePriorities = new HashMap<>();
            Configuration conf = new Configuration();

            BulkIngestMapFileLoader uut = new BulkIngestMapFileLoader(workDir, jobDirPattern, instanceName, zooKeepers, "user", new PasswordToken("pass"),
                            seqFileHdfs, srcHdfs, destHdfs, jobtracker, tablePriorities, conf, 0);

            Assert.assertNotNull("BulkIngestMapFileLoader constructor failed to create an instance.", uut);

            BulkIngestMapFileLoaderTest.WrappedLocalFileSystem fs = new BulkIngestMapFileLoaderTest.WrappedLocalFileSystem(null, null, false, false, false,
                            false, null, true, false);

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, conf, fs);

            Path jobDirectory = new Path(url.toString());

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

            String workDir = ".";
            String jobDirPattern = "jobs/";
            String instanceName = "localhost";
            String zooKeepers = "localhost";
            URI seqFileHdfs = url.toURI();
            URI srcHdfs = url.toURI();
            URI destHdfs = url.toURI();
            String jobtracker = "localhost";
            Map<String,Integer> tablePriorities = new HashMap<>();
            Configuration conf = new Configuration();

            BulkIngestMapFileLoader uut = new BulkIngestMapFileLoader(workDir, jobDirPattern, instanceName, zooKeepers, "user", new PasswordToken("pass"),
                            seqFileHdfs, srcHdfs, destHdfs, jobtracker, tablePriorities, conf, 0);

            Assert.assertNotNull("BulkIngestMapFileLoader constructor failed to create an instance.", uut);

            Map<String,Boolean> exists = new HashMap<>();
            String filePath = String.format("%s%s", url.toString(), BulkIngestMapFileLoader.CLEANUP_FILE_MARKER);

            exists.put(filePath, Boolean.TRUE);
            filePath = String.format("%s%s", url.toString(), BulkIngestMapFileLoader.LOADING_FILE_MARKER);
            exists.put(filePath, Boolean.FALSE);

            BulkIngestMapFileLoaderTest.WrappedLocalFileSystem fs = new BulkIngestMapFileLoaderTest.WrappedLocalFileSystem(createMockInputStream(),
                            new FileStatus[] {createMockFileStatus()}, false, true, false, false, exists, false, false);

            Whitebox.invokeMethod(FileSystem.class, "addFileSystemForTesting", BulkIngestMapFileLoaderTest.FILE_SYSTEM_URI, conf, fs);

            Path jobDirectory = new Path(url.toString());

            boolean results = uut.markDirectoryForCleanup(jobDirectory, srcHdfs);

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

            String workDir = ".";
            String jobDirPattern = "jobs/";
            String instanceName = "localhost";
            String zooKeepers = "localhost";
            URI seqFileHdfs = url.toURI();
            URI srcHdfs = url.toURI();
            URI destHdfs = url.toURI();
            String jobtracker = "localhost";
            Map<String,Integer> tablePriorities = new HashMap<>();
            Configuration conf = new Configuration();

            FileSystem mfs = FileSystem.get(conf);

            mfs.create(new Path(url.toString() + "/job.cleanup"));

            BulkIngestMapFileLoader uut = new BulkIngestMapFileLoader(workDir, jobDirPattern, instanceName, zooKeepers, "user", new PasswordToken("pass"),
                            seqFileHdfs, srcHdfs, destHdfs, jobtracker, tablePriorities, conf, 0);

            Assert.assertNotNull("BulkIngestMapFileLoader constructor failed to create an instance.", uut);

            Map<String,Boolean> exists = new HashMap<>();
            String filePath = String.format("%s%s", url.toString(), BulkIngestMapFileLoader.CLEANUP_FILE_MARKER);

            exists.put(filePath, Boolean.TRUE);
            filePath = String.format("%s%s", url.toString(), BulkIngestMapFileLoader.LOADING_FILE_MARKER);
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
