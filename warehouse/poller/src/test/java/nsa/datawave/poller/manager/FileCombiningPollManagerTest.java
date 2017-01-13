package nsa.datawave.poller.manager;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import nsa.datawave.poller.Poller;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class FileCombiningPollManagerTest {
    public static final String DEFAULT_CLASS_NAME_POLL_MANAGER = "nsa.datawave.poller.manager.FileCombiningPollManagerProxy";
    
    /*******************************************************************
     * NOTE: Possibly the FileCombiningPollManager & FileCombiningPollManagerProxy should have Unit Tests which directly access the methods. This direct access
     * eliminates timing issues associated with running thread.
     */
    
    Logger log = Logger.getLogger(FileCombiningPollManagerTest.class);
    
    private static final int POLL_TIME = 500;
    private static final int LAT_TIME = 60000;
    
    File input = null;
    File work = null;
    File queue = null;
    File error = null;
    
    String[] args = null;
    
    int maxFailures = 10;
    
    private static class PollerRunner implements Runnable {
        private final String[] args;
        Poller p = new Poller();
        
        public PollerRunner(String[] args) {
            this.args = args;
            p.setVerbose(true);
        }
        
        @Override
        public void run() {
            try {
                p.poll(args);
            } catch (Exception e) {
                System.out.println("Error in PollerRunner: ");
                e.printStackTrace();
            }
        }
        
        public boolean isAlive() {
            return !p.isShuttingDown();
        }
        
        public void close() {
            if (isAlive()) {
                p.shutdown();
            }
        }
    }
    
    private static File tmpDir;
    
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    
    @AfterClass
    public static void deleteTmpDir() {
        if (tmpDir.exists()) {
            tmpDir.delete();
        }
    }
    
    @Before
    public void testSetUp() throws Exception {
        
        // Make a local input, work, and error directory
        tmpDir = temporaryFolder.newFolder();
        
        input = new File(tmpDir, "input");
        input.mkdir();
        input.deleteOnExit();
        
        work = new File(tmpDir, "work");
        work.mkdir();
        work.deleteOnExit();
        queue = new File(work, "queue1");
        queue.mkdir();
        queue.deleteOnExit();
        
        error = new File(tmpDir, "error");
        error.mkdir();
        error.deleteOnExit();
        File queue = new File(tmpDir, "queue");
        queue.mkdir();
        queue.deleteOnExit();
        File completed = new File(tmpDir, "completed");
        completed.mkdir();
        completed.deleteOnExit();
        
        File dest = new File(tmpDir, "dest");
        dest.mkdir();
        dest.deleteOnExit();
        
        Path p = new Path(dest.toURI());
        
        List<String> arguments = new ArrayList<>();
        arguments.add("-d");
        arguments.add(input.getAbsolutePath());
        arguments.add("-e");
        arguments.add(error.getAbsolutePath());
        arguments.add("-w");
        arguments.add(work.getAbsolutePath());
        arguments.add("-m");
        arguments.add(DEFAULT_CLASS_NAME_POLL_MANAGER);
        arguments.add("-q");
        arguments.add(queue.getAbsolutePath());
        arguments.add("-t");
        arguments.add("foo");
        arguments.add("-i");
        arguments.add(String.valueOf(POLL_TIME));
        arguments.add("-l");
        arguments.add(String.valueOf(LAT_TIME));
        arguments.add("-h");
        arguments.add(p.toUri().toString());
        arguments.add("-r");
        arguments.add(completed.getAbsolutePath());
        arguments.add("-mfl");
        arguments.add("" + maxFailures);
        
        args = arguments.toArray(new String[arguments.size()]);
        
        // empty the directories
        clearFiles();
        
        // ensure the counters are reset
        FileCombiningPollManagerProxy.resetProxy();
    }
    
    @After
    public void tearDown() throws Exception {
        // ensure the counters are reset
        FileCombiningPollManagerProxy.resetProxy();
    }
    
    private void clearFiles() {
        String sep = System.getProperty("file.separator");
        clearFiles(new File(tmpDir, "input" + sep));
        clearFiles(new File(tmpDir, "queue" + sep));
        clearFiles(new File(tmpDir, "work" + sep));
        clearFiles(new File(tmpDir, "completed" + sep));
        clearFiles(new File(tmpDir, "dest" + sep));
    }
    
    private void clearFiles(File parent) {
        if (parent.list() != null) {
            for (String fileName : parent.list()) {
                File file = new File(parent, fileName);
                if (file.isDirectory()) {
                    clearFiles(file);
                } else {
                    file.delete();
                    if (file.exists()) {
                        // OK, so try moving it out of the way
                        File temp = new File(tmpDir, "temp");
                        temp.mkdir();
                        temp.deleteOnExit();
                        file.renameTo(new File(temp, file.getName()));
                    }
                    if (file.exists()) {
                        throw new RuntimeException("Could not delete " + file);
                    }
                }
            }
        }
    }
    
    private File createInputFile(String filename, int num) throws IOException {
        return createInputFile(filename, num, true);
    }
    
    private File createInputFile(String filename, int num, boolean isReadable) throws IOException {
        File f = File.createTempFile(String.valueOf(System.currentTimeMillis()), ".foo");
        FileWriter writer = new FileWriter(f);
        
        for (int i = 0; i < 60; i++)
            writer.write(UUID.randomUUID().toString() + "\n");
        
        writer.flush();
        writer.close();
        
        f.setReadable(isReadable);
        log.debug("createInputFile, lastModified: " + f.lastModified());
        f.setLastModified(f.lastModified() + (1000 * num));
        log.debug("createInputFile, lastModified: " + f.lastModified());
        
        final File createdFile = new File(input, (filename == null ? f.getName() : filename));
        return f.renameTo(createdFile) ? createdFile : null;
    }
    
    private int getNumDestFiles() {
        return FileCombiningPollManagerProxy.getFilesCompleted();
    }
    
    private int getNumQueuedFiles() {
        return FileCombiningPollManagerProxy.getFilesProcessed();
    }
    
    private int getNumWorkFiles() {
        return FileCombiningPollManagerProxy.getNumWorkFiles();
    }
    
    private Map<File,File> getWorkFiles() {
        return FileCombiningPollManagerProxy.getWorkFiles();
    }
    
    private void poll(String[] args, int count) throws Exception {
        poll(args, count, true);
    }
    
    private void poll(String[] args, int count, boolean areFilesReadable) throws Exception {
        final PollerRunner runner = new PollerRunner(args);
        final Thread t = new Thread(runner);
        t.start();
        
        for (int i = 0; i < count; i++)
            Assert.assertNotNull(createInputFile(null, i + 1, areFilesReadable));
        
        for (int j = 0; runner.isAlive() && getNumQueuedFiles() < count && j < 1000; j++)
            Thread.sleep(32l);
        
        runner.close();
        t.join();
    }
    
    private void poll(String[] args, int count, String filename) throws Exception {
        assert (filename != null);
        
        final PollerRunner runner = new PollerRunner(args);
        final Thread t = new Thread(runner);
        t.start();
        
        final File file = new File(input, filename);
        
        for (int i = 0; i <= count; i++) {
            if (i < count) {
                Assert.assertNotNull(createInputFile(filename, i + 1));
            }
            
            for (int j = 0; file.exists() && j < 1000; j++)
                Thread.sleep(32l);
        }
        
        runner.close();
        t.join();
    }
    
    /**
     * This test starts the poller, creates one file, then shuts down the poller.
     *
     * @throws Exception
     */
    @Test
    public void testSingleFile() throws Exception {
        poll(args, 1);
        
        // verify we have 1 output file
        Assert.assertEquals(1, getNumQueuedFiles());
        Assert.assertEquals(1, getNumWorkFiles());
        Assert.assertEquals(1, getNumDestFiles());
    }
    
    /**
     * This test starts the poller, creates one file, then shuts down the poller.
     *
     * @throws Exception
     */
    @Test
    public void testMultipleFiles() throws Exception {
        poll(args, 5);
        
        // verify we have 1 output file
        Assert.assertEquals(5, getNumQueuedFiles());
        Assert.assertEquals(5, getNumWorkFiles());
        Assert.assertEquals(1, getNumDestFiles());
    }
    
    /**
     * This test the poller handling multiple input files of the same name
     *
     * @throws Exception
     */
    @Test
    public void testDedupFile() throws Exception {
        String filename = String.valueOf(System.currentTimeMillis()) + "_testdedup.foo";
        poll(args, 5, filename);
        
        // verify we have 1 output file and we processed 5 unique work files
        Assert.assertEquals(5, getNumQueuedFiles());
        Assert.assertEquals(5, getNumWorkFiles());
        Assert.assertEquals(1, getNumDestFiles());
        
        Map<File,File> workFiles = getWorkFiles();
        for (File workFile : workFiles.keySet()) {
            Assert.assertEquals(filename, workFiles.get(workFile).getName());
            Assert.assertEquals(filename, workFile.getName());
        }
    }
    
    /**
     * This test starts the poller, creates several files, then shuts down the poller. In the process, it checks that only one file is merged at a time by
     * setting maxFilesToMerge.
     *
     * @throws Exception
     */
    @Test
    public void testMaxFilesMerge() throws Exception {
        final List<String> arguments = new ArrayList<>(Arrays.asList(args));
        arguments.add("-maxFilesToMerge");
        arguments.add("1");
        
        final String[] newArgs = arguments.toArray(new String[arguments.size()]);
        poll(newArgs, 5);
        
        // verify we have 5 output files
        Assert.assertEquals(5, getNumQueuedFiles());
        Assert.assertEquals(5, getNumWorkFiles());
        Assert.assertEquals(5, getNumDestFiles());
    }
    
    @Test(timeout = 1500)
    // junit timeout because previously this code exercised an infinite loop
    public void testFileGoesMissing() throws Exception {
        // use a different test class that will simulate dropping files
        for (int i = 0; i < args.length; i++) {
            if (DEFAULT_CLASS_NAME_POLL_MANAGER.equals(args[i])) {
                args[i] = FileCombiningPollManagerProxyDropsFile.class.getName();
            }
        }
        pollUntilTimeout(args, 1, 1000L);
        Assert.assertEquals(1, getNumQueuedFiles());
        Assert.assertEquals(getWorkFiles().toString(), 0, getNumWorkFiles());
        Assert.assertEquals(0, getNumDestFiles());
        
    }
    
    @Test(timeout = 3000)
    public void testThatWeDoNotDeadlockOnShutdownDueToFailures() throws Exception {
        // If we deadlock, we will fail due to timeout.
        // If we make it to the end of this test, then we are good.
        poll(args, maxFailures + 1, false);
    }
    
    private void pollUntilTimeout(String[] args, int count, long timeout) throws Exception {
        final PollerRunner runner = new PollerRunner(args);
        final Thread t = new Thread(runner);
        
        t.start();
        long startTime = System.currentTimeMillis();
        
        for (int i = 0; i < count; i++) {
            Assert.assertNotNull(createInputFile(null, i + 1));
        }
        
        while (System.currentTimeMillis() - startTime < timeout) {
            Thread.sleep(10L);
        }
        
        // once we've hit our timeout, close it up
        runner.close();
        t.interrupt();
    }
}
