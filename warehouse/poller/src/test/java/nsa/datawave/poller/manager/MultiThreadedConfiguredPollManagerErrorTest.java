package nsa.datawave.poller.manager;

import java.io.File;
import java.io.IOException;

import nsa.datawave.poller.manager.FileCombiningPollManagerErrorTest.OutErrPM;
import nsa.datawave.poller.manager.FileCombiningPollManagerErrorTest.ProcErrPM;
import nsa.datawave.poller.manager.FileCombiningPollManagerErrorTest.ZeroOutPM;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sadun.util.polling.DirectoryLookupStartEvent;
import org.sadun.util.polling.TestFileFound;
import org.sadun.util.polling.TestFileFound.TestDirectoryPoller;

/** Tests the MultiThreadedConfigurtedPollManager's ability to deal with certain errors. */
public class MultiThreadedConfiguredPollManagerErrorTest {
    
    Logger log = Logger.getLogger(MultiThreadedConfiguredPollManagerErrorTest.class);
    
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    
    /** Dirs: error, inbox, work, queue, archive, complete */
    private File errDir, inbDir, wrkDir, wrkDir1, wrqDir, arcDir, cptDir;
    
    /** ProcErrPM will allow this {@link File} to pass without an Exception. */
    private File goodFile;
    
    /** ProcErrPM will throw an Exception in {@code processFile()} when given this {@link File}. */
    private File brknFile;
    
    /** ProcErrPM will throw an Error in {@code processFile()} when given this {@link File}. */
    private File fatalFile;
    
    /** Preps the filesystem for each test. */
    @Before
    public void setup() throws IOException {
        // Create temp dirs
        final File tmpDir = temporaryFolder.newFolder();
        errDir = temporaryFolder.newFolder(tmpDir.getName(), "errors");
        inbDir = temporaryFolder.newFolder(tmpDir.getName(), "inbox");
        wrkDir = temporaryFolder.newFolder(tmpDir.getName(), "work");
        wrkDir1 = temporaryFolder.newFolder(wrkDir.getName(), "workDir1");
        wrqDir = temporaryFolder.newFolder(tmpDir.getName(), "work", "queue"); // subfolder of wrkDir
        arcDir = temporaryFolder.newFolder(tmpDir.getName(), "archive");
        cptDir = temporaryFolder.newFolder(tmpDir.getName(), "completed");
        
        goodFile = new File(inbDir.getAbsolutePath() + File.separator + "good.txt");
        brknFile = new File(inbDir.getAbsolutePath() + File.separator + "break.txt");
        fatalFile = new File(inbDir.getAbsolutePath() + File.separator + "fatal.txt");
        
        final File dataFile = new File(getClass().getResource("/test-data/simple.txt").getFile());
        FileUtils.copyFile(dataFile, goodFile);
        FileUtils.copyFile(dataFile, brknFile);
        FileUtils.copyFile(dataFile, fatalFile);
    }
    
    /** Cleans up after each test's mess. */
    @After
    public void teardown() throws IOException {
        temporaryFolder.delete();
    }
    
    /**
     * Ensures that processing errors are handled correctly.
     * 
     * @throws Exception
     */
    @Test
    public void processErrorTest() throws Exception {
        final MultiThreadedConfiguredPollManager pm = new MultiThreadedConfiguredPollManager(1, ProcErrPM.class.getName());
        CommandLineParser parser = new BasicParser();
        pm.configure(parser.parse(
                        pm.getConfigurationOptions(),
                        new String[] {"-e", errDir.getAbsolutePath(), "-w", wrkDir.getAbsolutePath(), "-a", arcDir.getAbsolutePath(), "-r",
                                cptDir.getAbsolutePath(), "-t", "text", "-h", "localhost", "-l", "1000",}));
        TestDirectoryPoller poller = new TestDirectoryPoller();
        pm.directoryLookupStarted(new DirectoryLookupStartEvent(poller, inbDir));
        
        pm.fileFound(new TestFileFound(poller, goodFile));
        pm.fileFound(new TestFileFound(poller, brknFile));
        while (pm.getActiveManagers() > 0 || pm.getCompletedTasks() < 2) {
            try {
                Thread.sleep(100);
            } catch (Exception e) {}
        }
        Assert.assertFalse(poller.wasShutdown);
        pm.close();
        
        Assert.assertTrue(goodFile.exists());
        Assert.assertFalse(brknFile.exists());
        Assert.assertTrue((new File(errDir, brknFile.getName())).exists());
        
        Assert.assertEquals(0, arcDir.list().length);
        Assert.assertEquals(0, cptDir.list().length);
        Assert.assertEquals(0, wrqDir.list().length);
        Assert.assertEquals(2, wrkDir.list().length);
        Assert.assertEquals(1, errDir.list().length);
        Assert.assertEquals(2, inbDir.list().length);
    }
    
    /**
     * Ensures that processing errors are handled correctly.
     * 
     * @throws Exception
     * @throws ParseException
     */
    @Test
    public void processErrorFatal() throws ParseException, Exception {
        final MultiThreadedConfiguredPollManager pm = new MultiThreadedConfiguredPollManager(1, ProcErrPM.class.getName());
        CommandLineParser parser = new BasicParser();
        pm.configure(parser.parse(
                        pm.getConfigurationOptions(),
                        new String[] {"-e", errDir.getAbsolutePath(), "-w", wrkDir.getAbsolutePath(), "-a", arcDir.getAbsolutePath(), "-r",
                                cptDir.getAbsolutePath(), "-t", "text", "-h", "localhost", "-l", "1000",}));
        TestDirectoryPoller poller = new TestDirectoryPoller();
        pm.directoryLookupStarted(new DirectoryLookupStartEvent(poller, inbDir));
        try {
            pm.fileFound(new TestFileFound(poller, fatalFile));
            // if we did not already fail (fileFound returned before task was run),
            // then wait for all tasks to complete and then test for error
            while (pm.getActiveManagers() > 0 || pm.getCompletedTasks() < 1) {
                try {
                    Thread.sleep(100);
                } catch (Exception e) {}
            }
            pm.failOnError();
            Assert.fail("Expected an error to be thrown");
        } catch (FileProcessingError e) {
            // expected
        }
        
        Assert.assertTrue(poller.wasShutdown);
    }
    
    /**
     * Ensures that output errors are handled correctly.
     * 
     * @throws Exception
     * @throws ParseException
     */
    @Test
    public void outputErrorTest() throws ParseException, Exception {
        final MultiThreadedConfiguredPollManager pm = new MultiThreadedConfiguredPollManager(1, OutErrPM.class.getName());
        CommandLineParser parser = new BasicParser();
        pm.configure(parser.parse(
                        pm.getConfigurationOptions(),
                        new String[] {"-e", errDir.getAbsolutePath(), "-w", wrkDir.getAbsolutePath(), "-a", arcDir.getAbsolutePath(), "-r",
                                cptDir.getAbsolutePath(), "-t", "text", "-h", "localhost", "-l", "1000",}));
        TestDirectoryPoller poller = new TestDirectoryPoller();
        pm.directoryLookupStarted(new DirectoryLookupStartEvent(poller, inbDir));
        pm.fileFound(new TestFileFound(poller, goodFile));
        while (pm.getActiveManagers() > 0 || pm.getCompletedTasks() < 1) {
            try {
                Thread.sleep(100);
            } catch (Exception e) {}
        }
        Assert.assertFalse(poller.wasShutdown);
        pm.close();
        
        Assert.assertTrue(goodFile + " has not been returned", goodFile.exists());
        Assert.assertTrue(brknFile + " disappeared...", brknFile.exists());
        
        Assert.assertEquals(0, arcDir.list().length);
        Assert.assertEquals(0, cptDir.list().length);
        Assert.assertEquals(0, wrqDir.list().length);
        Assert.assertEquals(2, wrkDir.list().length);
        Assert.assertEquals(0, errDir.list().length);
        Assert.assertEquals(3, inbDir.list().length);
    }
    
    /**
     * Tests that when files produce Zero Output, that they should go into the "error" directory.
     * 
     * @throws Exception
     * @throws ParseException
     */
    @Test
    public void zeroOutputTest() throws ParseException, Exception {
        final MultiThreadedConfiguredPollManager pm = new MultiThreadedConfiguredPollManager(1, ZeroOutPM.class.getName());
        CommandLineParser parser = new BasicParser();
        pm.configure(parser.parse(
                        pm.getConfigurationOptions(),
                        new String[] {"-e", errDir.getAbsolutePath(), "-w", wrkDir.getAbsolutePath(), "-a", arcDir.getAbsolutePath(), "-r",
                                cptDir.getAbsolutePath(), "-t", "text", "-h", "localhost", "-l", "1000",}));
        TestDirectoryPoller poller = new TestDirectoryPoller();
        pm.directoryLookupStarted(new DirectoryLookupStartEvent(poller, inbDir));
        pm.fileFound(new TestFileFound(poller, goodFile));
        pm.fileFound(new TestFileFound(poller, brknFile));
        while (pm.getActiveManagers() > 0 || pm.getCompletedTasks() < 2) {
            try {
                Thread.sleep(100);
            } catch (Exception e) {}
        }
        Assert.assertFalse(poller.wasShutdown);
        pm.close();
        
        Assert.assertFalse(goodFile.exists());
        Assert.assertFalse(brknFile.exists());
        Assert.assertFalse((new File(errDir, goodFile.getName())).exists());
        Assert.assertTrue((new File(errDir, brknFile.getName())).exists());
        
        for (File f : errDir.listFiles())
            log.debug("Found: " + f.getName());
        if (errDir.listFiles().length == 0)
            log.debug("NO FILES FOUND");
        
        Assert.assertTrue((new File(errDir, brknFile.getName())).exists());
        
        Assert.assertEquals(0, arcDir.list().length);
        Assert.assertEquals(1, cptDir.list().length); // since we closed the completed file should be here
        Assert.assertEquals(0, wrqDir.list().length);
        Assert.assertEquals(2, wrkDir.list().length);
        Assert.assertEquals(1, errDir.list().length);
        Assert.assertEquals(1, inbDir.list().length);
    }
    
    /**
     * Tests that the Poller self-terminates after reaching MaxFailures.
     * 
     * @throws Exception
     * @throws ParseException
     */
    @Test
    public void maxOutputFailsTest() throws ParseException, Exception {
        final MultiThreadedConfiguredPollManager pm = new MultiThreadedConfiguredPollManager(1, ZeroOutPM.class.getName());
        CommandLineParser parser = new BasicParser();
        pm.configure(parser.parse(
                        pm.getConfigurationOptions(),
                        new String[] {"-e", errDir.getAbsolutePath(), "-w", wrkDir.getAbsolutePath(), "-a", arcDir.getAbsolutePath(), "-r",
                                cptDir.getAbsolutePath(), "-t", "text", "-h", "localhost", "-l", "1000", "-mfl", "1",}));
        TestDirectoryPoller poller = new TestDirectoryPoller();
        pm.directoryLookupStarted(new DirectoryLookupStartEvent(poller, inbDir));
        pm.fileFound(new TestFileFound(poller, goodFile));
        pm.fileFound(new TestFileFound(poller, brknFile));
        
        while (pm.getActiveManagers() > 0 || pm.getCompletedTasks() < 2) {
            try {
                Thread.sleep(100);
            } catch (Exception e) {}
        }
        
        Assert.assertFalse(goodFile.exists());
        Assert.assertFalse(brknFile.exists());
        Assert.assertFalse((new File(errDir, goodFile.getName())).exists());
        Assert.assertTrue((new File(errDir, brknFile.getName())).exists());
        
        Assert.assertEquals(0, arcDir.list().length);
        Assert.assertEquals(0, cptDir.list().length);
        Assert.assertEquals(0, wrqDir.list().length);
        Assert.assertEquals(3, wrkDir.list().length);
        Assert.assertEquals(1, errDir.list().length);
        Assert.assertEquals(1, inbDir.list().length);
    }
    
}
