package nsa.datawave.poller.manager;

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import nsa.datawave.poller.Poller;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.cli.MissingOptionException;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.*;

public class AccumuloBlobStorePollManagerTest {
    protected static final String TABLE_NAME = "ErrorFiles";
    protected static final String INDEX_TABLE_NAME = "ErrorFilesIndex";
    protected static final int POLL_INTERVAL_MS = 100;
    protected static final int MAX_CYCLE_COUNT = 500000000;
    protected static final String INSTANCE_NAME = "testInstance";
    protected static final String USER_NAME = "testUser";
    protected static final String PASSWORD = "letMeIn";
    
    protected static Logger log = Logger.getLogger(AccumuloBlobStorePollManagerTest.class);
    protected static ExecutorService executorService = Executors.newSingleThreadExecutor();
    
    protected Connector mockConnector;
    protected Poller poller = new Poller();
    protected List<String> args;
    protected File input, queue, error;
    
    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();
    
    @Rule
    public Timeout timeout = new Timeout(10000);
    
    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    
    @AfterClass
    public static void shutdownExecutor() {
        executorService.shutdownNow();
    }
    
    @Before
    public void setup() throws Exception {
        TestAccumuloBlobStorePollerManager.chunkError = false;
        
        input = tmpDir.newFolder("input");
        queue = tmpDir.newFolder("queue");
        error = tmpDir.newFolder("error");
        
        args = Lists.newArrayList("-m", TestAccumuloBlobStorePollerManager.class.getName(), "-d", input.getAbsolutePath(), "-q", queue.getAbsolutePath(), "-e",
                        error.getAbsolutePath(), "-i", Integer.toString(POLL_INTERVAL_MS), "-t", "falloutErrors", "-u", USER_NAME, "-p", PASSWORD, "-in",
                        INSTANCE_NAME, "-zk", "notUsed", "-tn", TABLE_NAME, "-itn", INDEX_TABLE_NAME, "-wt", "1", "-ml", "500", "-mm", "100");
        
        MockInstance mockInstance = new MockInstance(INSTANCE_NAME);
        mockConnector = mockInstance.getConnector(USER_NAME, new PasswordToken(PASSWORD));
        if (mockConnector.tableOperations().exists(TABLE_NAME))
            mockConnector.tableOperations().delete(TABLE_NAME);
        mockConnector.tableOperations().create(TABLE_NAME);
    }
    
    @Test
    public void smallFileTest() throws Exception {
        final String FILE_CONTENT = "Test file content";
        File inputFile = writeInputFile(FILE_CONTENT);
        long size = inputFile.length();
        long ts = inputFile.lastModified();
        String fileName = getFileName(inputFile);
        
        poll();
        
        assertEquals(FILE_CONTENT, getFileContents(inputFile.getName(), fileName, size, ts));
    }
    
    @Test
    public void multiDirTest() throws Exception {
        File input2 = tmpDir.newFolder("input2");
        File queue2 = tmpDir.newFolder("queue2");
        File error2 = tmpDir.newFolder("error2");
        
        args.add("-d");
        args.add(input2.getAbsolutePath());
        args.add("-q");
        args.add(queue2.getAbsolutePath());
        args.add("-e");
        args.add(error2.getAbsolutePath());
        
        final String FILE_CONTENT = "Test file content";
        File inputFile = writeInputFile(FILE_CONTENT);
        long size = inputFile.length();
        long ts = inputFile.lastModified();
        String fileName = getFileName(inputFile);
        
        final String FILE_CONTENT2 = "Test file content2";
        File inputFile2 = new File(input2, "testFile2.txt");
        FileOutputStream fos = new FileOutputStream(inputFile2);
        fos.write(FILE_CONTENT2.getBytes());
        fos.close();
        long size2 = inputFile2.length();
        long ts2 = inputFile2.lastModified();
        String fileName2 = getFileName(inputFile2);
        
        poll();
        
        assertEquals(FILE_CONTENT, getFileContents(inputFile.getName(), fileName, size, ts));
        assertEquals(FILE_CONTENT2, getFileContents(inputFile2.getName(), fileName2, size2, ts2));
    }
    
    @Test
    public void archiveFileTest() throws Exception {
        File archiveDir = new File(tmpDir.getRoot(), "archive");
        assertFalse("Archive directory shouldn't exist before test start", archiveDir.exists());
        
        final String FILE_CONTENT = "Test file content";
        File inputFile = writeInputFile(FILE_CONTENT);
        String inputFileName = inputFile.getName();
        long size = inputFile.length();
        long ts = inputFile.lastModified();
        String fileName = getFileName(inputFile);
        
        args.add("-kr");
        args.add(archiveDir.getAbsolutePath());
        poll();
        
        assertEquals(FILE_CONTENT, getFileContents(inputFile.getName(), fileName, size, ts));
        assertTrue("Archive dir doesn't exist after test end", archiveDir.isDirectory());
        
        File archiveFile = new File(archiveDir, inputFileName);
        assertTrue("Archive file " + archiveFile.getAbsolutePath() + " does not exist!", archiveFile.exists());
        assertTrue("Archive file " + archiveFile.getAbsolutePath() + " is not a file!", archiveFile.isFile());
        assertTrue("Archive file " + archiveFile.getAbsolutePath() + " is not readable!", archiveFile.canRead());
        
        String movedContents = Files.toString(archiveFile, Charset.defaultCharset());
        assertEquals(FILE_CONTENT, movedContents);
    }
    
    @Test
    public void fileInQueueDirTest() throws Exception {
        // Place a file in the queue dir to make sure the poller picks up the file from there at start
        final String FILE_CONTENT = "Test file content";
        File inputFile = new File(queue, "testFile.txt");
        FileOutputStream fos = new FileOutputStream(inputFile);
        fos.write(FILE_CONTENT.getBytes());
        fos.close();
        
        long size = inputFile.length();
        long ts = inputFile.lastModified();
        String fileName = getFileName(inputFile);
        
        poll();
        
        assertEquals(FILE_CONTENT, getFileContents(inputFile.getName(), fileName, size, ts));
    }
    
    @Test
    public void multiFileTest() throws Exception {
        final String CONTENT_BASE = "Test file content ";
        Map<String,Object[]> fileContents = new HashMap<>();
        for (int i = 1; i <= 10; ++i) {
            String content = CONTENT_BASE + i;
            File inputFile = writeInputFile(content);
            Object[] val = {content, inputFile.length(), inputFile.lastModified()};
            fileContents.put(inputFile.getName() + ":" + getFileName(inputFile), val);
        }
        
        poll();
        
        for (Map.Entry<String,Object[]> entry : fileContents.entrySet()) {
            String key = entry.getKey();
            int idx = key.indexOf(':');
            String fileName = key.substring(0, idx);
            String filePath = key.substring(idx + 1);
            assertEquals(entry.getValue()[0], getFileContents(fileName, filePath, (Long) entry.getValue()[1], (Long) entry.getValue()[2]));
        }
    }
    
    @Test
    public void chunkedFileTest() throws Exception {
        final String FILE_CONTENT = "Line 1\nLine 2\nLine 3\nLine 4\n";
        File inputFile = writeInputFile(FILE_CONTENT);
        long size = inputFile.length();
        long ts = inputFile.lastModified();
        String fileName = getFileName(inputFile);
        
        // Chunk size of 7 to split the file in 4 chunks
        args.add("-cs");
        args.add("7");
        
        poll();
        
        assertEquals(FILE_CONTENT, getChunkedFileContents(inputFile.getName(), fileName, size, ts, 4));
    }
    
    @Test
    public void chunkedLargeFileTest() throws Exception {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 20; ++i)
            sb.append("Line ").append(String.format("%02d", i)).append("\n");
        final String FILE_CONTENT = sb.toString();
        File inputFile = writeInputFile(FILE_CONTENT);
        long size = inputFile.length();
        long ts = inputFile.lastModified();
        String fileName = getFileName(inputFile);
        
        // Chunk size of 8 to split the file in 4 chunks
        args.add("-cs");
        args.add("8");
        
        poll();
        
        assertEquals(FILE_CONTENT, getChunkedFileContents(inputFile.getName(), fileName, size, ts, 20));
    }
    
    @Test
    public void columnVisibilityTest() throws Exception {
        final String VISIBILITY = "FOO&BAR";
        final String FILE_CONTENT = "Test file content";
        File inputFile = writeInputFile(FILE_CONTENT);
        long size = inputFile.length();
        long ts = inputFile.lastModified();
        String fileName = getFileName(inputFile);
        
        args.add("-cv");
        args.add(VISIBILITY);
        poll();
        
        assertEquals("User shouldn't have auths by default", new Authorizations(),
                        mockConnector.securityOperations().getUserAuthorizations(mockConnector.whoami()));
        Scanner s = mockConnector.createScanner(TABLE_NAME, mockConnector.securityOperations().getUserAuthorizations(mockConnector.whoami()));
        s.setRange(new Range(fileName));
        assertFalse("Scanner reported entries when we shouldn't have seen any!", s.iterator().hasNext());
        
        // Update user auths and try again and we should see the result
        mockConnector.securityOperations().changeUserAuthorizations(mockConnector.whoami(), new Authorizations("FOO", "BAR"));
        assertEquals(FILE_CONTENT, getFileContents(inputFile.getName(), fileName, size, ts, VISIBILITY));
    }
    
    @Test
    public void indexTest() throws Exception {
        final String FILE_CONTENT = "Test file content";
        File inputFile = writeInputFile(FILE_CONTENT);
        long size = inputFile.length();
        long ts = inputFile.lastModified();
        String fileName = getFileName(inputFile);
        
        poll();
        
        Scanner s = mockConnector.createScanner(INDEX_TABLE_NAME, mockConnector.securityOperations().getUserAuthorizations(mockConnector.whoami()));
        s.setRange(new Range(fileName));
        Iterator<Map.Entry<Key,Value>> indexIterator = s.iterator();
        assertTrue("No entries found in the index table", indexIterator.hasNext());
        Map.Entry<Key,Value> entry = indexIterator.next();
        assertEquals("Invalid row", fileName, entry.getKey().getRow().toString());
        assertEquals("Invalid column family", Integer.toHexString(fileName.hashCode()).toUpperCase(), entry.getKey().getColumnFamily().toString());
        assertEquals("Invalid column qualifier", Long.toString(size), entry.getKey().getColumnQualifier().toString());
        assertEquals("Invalid column visibility", "", entry.getKey().getColumnVisibility().toString());
        assertEquals("Invalid timestamp", ts, entry.getKey().getTimestamp());
        assertArrayEquals("Invalid value", new byte[0], entry.getValue().get());
        assertFalse("More than one index entry for single file!", indexIterator.hasNext());
    }
    
    @Test
    public void testErrorDir() throws Exception {
        TestAccumuloBlobStorePollerManager.chunkError = true;
        
        final String FILE_CONTENT = "Test file content";
        File inputFile = writeInputFile(FILE_CONTENT);
        
        poll(false);
        
        assertTrue("Poller input dir still has files: " + Arrays.toString(input.list()), dirEmpty(input));
        assertTrue("Poller queue dir still has files: " + Arrays.toString(queue.list()), dirEmpty(queue));
        File[] errorFiles = error.listFiles();
        assertEquals("Wrong number of error files", 1, errorFiles.length);
        assertEquals("Wrong error file name", inputFile.getName(), errorFiles[0].getName());
        assertEquals("Wrong error file contents", FILE_CONTENT, new String(Files.toByteArray(errorFiles[0])));
    }
    
    @Test
    public void directoryFormatTest() throws Exception {
        String format = "/files/{1,date,yyyy/MM/dd}/{0}";
        final String FILE_CONTENT = "Test file content";
        File inputFile = writeInputFile(FILE_CONTENT);
        long size = inputFile.length();
        long ts = inputFile.lastModified();
        String fileName = MessageFormat.format(format, inputFile.getName(), inputFile.lastModified());
        
        args.add("-df");
        args.add(format);
        poll();
        
        assertEquals(FILE_CONTENT, getFileContents(inputFile.getName(), fileName, size, ts));
    }
    
    @Test
    public void invalidDirectoryFormatTest() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Illegal pattern character 'O");
        
        String format = "/files/{1,date,OO/MM/dd}/{0}";
        final String FILE_CONTENT = "Test file content";
        File inputFile = writeInputFile(FILE_CONTENT);
        long size = inputFile.length();
        long ts = inputFile.lastModified();
        String fileName = MessageFormat.format(format, inputFile.getName(), inputFile.lastModified());
        
        args.add("-df");
        args.add(format);
        poll();
        
        assertEquals(FILE_CONTENT, getFileContents(inputFile.getName(), fileName, size, ts));
    }
    
    @Test
    public void missingErrorDirTest() throws Exception {
        boolean deleted = error.delete();
        assertTrue("Unable to remove error directory.", deleted);
        
        final String FILE_CONTENT = "Test file content";
        File inputFile = writeInputFile(FILE_CONTENT);
        long size = inputFile.length();
        long ts = inputFile.lastModified();
        String fileName = getFileName(inputFile);
        
        poll();
        
        assertEquals(FILE_CONTENT, getFileContents(inputFile.getName(), fileName, size, ts));
        assertTrue(error.exists());
        assertTrue(error.isDirectory());
    }
    
    @Test
    public void mismatchQueueDirTest() throws Exception {
        
        File input2 = tmpDir.newFolder("input2");
        File error2 = tmpDir.newFolder("error2");
        
        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage("Need one error directory per queue directory.");
        
        args.add("-d");
        args.add(input2.getAbsolutePath());
        args.add("-e");
        args.add(error2.getAbsolutePath());
        
        final String FILE_CONTENT = "Test file content";
        File inputFile = writeInputFile(FILE_CONTENT);
        long size = inputFile.length();
        long ts = inputFile.lastModified();
        String fileName = getFileName(inputFile);
        
        poll();
        
        assertEquals(FILE_CONTENT, getFileContents(inputFile.getName(), fileName, size, ts));
    }
    
    @Test
    public void mismatchErrorDirTest() throws Exception {
        
        File input2 = tmpDir.newFolder("input2");
        File queue2 = tmpDir.newFolder("queue2");
        
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Need one error directory per queue directory.");
        
        args.add("-d");
        args.add(input2.getAbsolutePath());
        args.add("-q");
        args.add(queue2.getAbsolutePath());
        
        final String FILE_CONTENT = "Test file content";
        File inputFile = writeInputFile(FILE_CONTENT);
        long size = inputFile.length();
        long ts = inputFile.lastModified();
        String fileName = getFileName(inputFile);
        
        poll();
        
        assertEquals(FILE_CONTENT, getFileContents(inputFile.getName(), fileName, size, ts));
    }
    
    @Test
    public void checkMissingDatatypeOption() throws Exception {
        testMissingRequiredArgWithOption("t");
    }
    
    @Test
    public void checkMissingErrorDirOption() throws Exception {
        testMissingRequiredArgWithOption("e");
    }
    
    @Test
    public void checkMissingUsernameOption() throws Exception {
        testMissingRequiredArgWithOption("u");
    }
    
    @Test
    public void checkMissingPasswordOption() throws Exception {
        testMissingRequiredArgWithOption("p");
    }
    
    @Test
    public void checkMissingInstanceOption() throws Exception {
        testMissingRequiredArgWithOption("in");
    }
    
    @Test
    public void checkMissingZookeeperOption() throws Exception {
        testMissingRequiredArgWithOption("zk");
    }
    
    @Test
    public void checkMissingTableNameOption() throws Exception {
        testMissingRequiredArgWithOption("tn");
    }
    
    private void testMissingRequiredArgWithOption(String option) throws Exception {
        // Remove the required option (with its option value)
        int idx = args.indexOf("-" + option);
        args.remove(idx);
        args.remove(idx);
        
        expectedException.expect(MissingOptionException.class);
        expectedException.expectMessage("Missing required option: " + option);
        
        poll();
    }
    
    private void poll() throws Exception {
        poll(true);
    }
    
    private void poll(boolean assertions) throws Exception {
        Callable<Object> pollerCallable = new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                try {
                    poller.setVerbose(true);
                    poller.poll(args.toArray(new String[args.size()]));
                } catch (Exception e) {
                    log.error(e);
                    poller.shutdown();
                    throw e;
                }
                return null;
            }
        };
        Future<?> pollerTask = executorService.submit(pollerCallable);
        
        // Sleep up to MAX_CYCLE_COUNT times to allow the poller to run. Check to make sure
        // the input and queue directories are empty.
        for (int cycleCount = 0; cycleCount < MAX_CYCLE_COUNT && !poller.isShuttingDown(); ++cycleCount) {
            Thread.sleep(POLL_INTERVAL_MS);
            if (dirEmpty(input) && dirEmpty(queue)) {
                break;
            }
        }
        
        // Shut the poller down and wait
        poller.shutdown();
        try {
            pollerTask.get(); // get will wait for the poller to shut down, and will throw any exception reported by the poller
        } catch (ExecutionException e) {
            log.error("Poller error", e.getCause());
            if (e.getCause() instanceof Exception)
                throw (Exception) e.getCause();
            else
                fail("Poller failed with throwable: " + e.getCause());
        }
        
        // Make sure everything really got cleared out
        if (assertions) {
            assertTrue("Poller input dir still has files: " + Arrays.toString(input.list()), dirEmpty(input));
            assertTrue("Poller queue dir still has files: " + Arrays.toString(queue.list()), dirEmpty(queue));
            assertTrue("Poller error dir has files: " + Arrays.toString(error.list()), dirEmpty(error));
        }
    }
    
    private File writeInputFile(String contents) throws Exception {
        File inputFile = new File(input, "testFile.txt");
        FileOutputStream fos = new FileOutputStream(inputFile);
        fos.write(contents.getBytes());
        fos.close();
        return inputFile;
    }
    
    private boolean dirEmpty(File dir) {
        String[] files = dir.list();
        return (files == null) || (files.length == 0);
    }
    
    private String getFileName(File file) {
        return MessageFormat.format("/{1,date,yyyy/MM/dd}/{0}", file.getName(), file.lastModified());
    }
    
    private String getFileContents(String fileName, String filePath, long size, long ts) throws Exception {
        return getFileContents(fileName, filePath, size, ts, "");
    }
    
    private String getFileContents(String fileName, String filePath, long size, long ts, String vis) throws Exception {
        String expectedKey = Integer.toHexString(filePath.hashCode()).toUpperCase() + ":" + fileName;
        Scanner s = mockConnector.createScanner(TABLE_NAME, mockConnector.securityOperations().getUserAuthorizations(mockConnector.whoami()));
        s.setRange(new Range(expectedKey));
        
        Iterator<Map.Entry<Key,Value>> it = s.iterator();
        assertTrue("No file " + fileName + " in mock accumulo.", it.hasNext());
        Map.Entry<Key,Value> entry = it.next();
        String contents = entry.getValue().toString();
        validateKey(entry.getKey(), expectedKey, size, 0, vis, ts);
        assertFalse("Too many entries for file " + fileName + " in mock accumulo", it.hasNext());
        return contents;
    }
    
    private String getChunkedFileContents(String fileName, String filePath, long size, long ts, int expectedChunks) throws Exception {
        String expectedKey = Integer.toHexString(filePath.hashCode()).toUpperCase() + ":" + fileName;
        Scanner s = mockConnector.createScanner(TABLE_NAME, mockConnector.securityOperations().getUserAuthorizations(mockConnector.whoami()));
        s.setRange(new Range(expectedKey));
        
        StringBuilder contents = new StringBuilder();
        int readChunks = 0;
        for (Map.Entry<Key,Value> entry : s) {
            contents.append(entry.getValue());
            ++readChunks;
            
            validateKey(entry.getKey(), expectedKey, size, readChunks, "", ts);
        }
        assertEquals("Incorrect number of file chunks", expectedChunks, readChunks);
        return contents.toString();
    }
    
    private void validateKey(Key key, String fileName, long size, int chunkNumber, String vis, long ts) throws Exception {
        assertEquals("file path is incorrect", fileName, key.getRow().toString());
        assertEquals("size is incorrect", Long.toString(size), key.getColumnFamily().toString());
        assertEquals("chunk number is incorrect", chunkNumber == 0 ? "" : String.format("%010d", chunkNumber), key.getColumnQualifier().toString());
        assertEquals("timestamp is incorrect", ts, key.getTimestamp());
        assertEquals("column visibility is incorrect", vis, key.getColumnVisibility().toString());
    }
    
    public static class TestAccumuloBlobStorePollerManager extends AccumuloBlobStorePollManager {
        public static boolean chunkError = false;
        
        @Override
        protected Connector getConnector(String user, String pass, String instance, String zookeepers) throws AccumuloSecurityException, AccumuloException {
            MockInstance mi = new MockInstance(instance);
            return mi.getConnector(user, new PasswordToken(pass));
        }
        
        @Override
        protected boolean readChunk(FileChannel channel, ByteBuffer buffer) throws IOException {
            if (chunkError)
                throw new IOException("Error reading chunk!");
            else
                return super.readChunk(channel, buffer);
        }
    }
}
