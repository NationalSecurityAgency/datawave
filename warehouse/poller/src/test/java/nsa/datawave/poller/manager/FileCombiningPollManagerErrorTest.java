package nsa.datawave.poller.manager;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

import nsa.datawave.poller.manager.io.SecurityMarkingLoadException;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
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

/** Tests the FileCombiningPollManager's ability to deal with certain errors. */
public class FileCombiningPollManagerErrorTest {
    
    Logger log = Logger.getLogger(FileCombiningPollManagerErrorTest.class);
    
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    
    // Changed temp file creation to use Junit rule since they were not being cleaned up properly.
    
    /** Dirs: error, inbox, work, queue, archive, complete */
    private File errDir, inbDir, wrkDir, wrkDir1, wrqDir, arcDir, cptDir;
    
    /** Base FileCombiningPollManager for tests; keeps configuration easy. */
    public static class ConfiguredFcpm extends FileCombiningPollManager {
        public ConfiguredFcpm() {
            super();
        }
        
        @Override
        protected void setupFilesystem(final CommandLine cl) throws IOException {
            fs = new RawLocalFileSystem();
        }
        
        public void setCompressedInput(final boolean ci) {
            this.compressedInput = ci;
        }
        
        /** What we're testing doesn't involve copying data to HDFS. */
        @Override
        void copyFileToHDFS(Path src, Path dst) throws IOException {}
    }
    
    /** Exception purposefully thrown by test code. */
    private static class TestEx extends IOException {
        public TestEx(String message) {
            super(message);
        }
    }
    
    /** Exception purposefully thrown by test code. */
    private static class TestEr extends Error {
        public TestEr(String message) {
            super(message);
        }
    }
    
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
    
    /** Throws a TestEx on {@code processFile()}. */
    public static class ProcErrPM extends ConfiguredFcpm {
        protected long processFile(File inputFile, OutputStream outStream) throws Exception {
            if (inputFile.getName().startsWith("break.txt"))
                throw new TestEx("THROWN FOR TESTING");
            if (inputFile.getName().startsWith("fatal.txt"))
                throw new TestEr("THROWN FOR TESTING");
            return super.processFile(inputFile, outStream);
        }
    }
    
    /**
     * Ensures that processing errors are handled correctly.
     * 
     * @throws Exception
     * @throws ParseException
     */
    @Test
    public void processErrorTest() throws ParseException, Exception {
        final ProcErrPM pm = new ProcErrPM();
        CommandLineParser parser = new BasicParser();
        pm.configure(parser.parse(
                        pm.getConfigurationOptions(),
                        new String[] {"-e", errDir.getAbsolutePath(), "-w", wrkDir.getAbsolutePath(), "-a", arcDir.getAbsolutePath(), "-r",
                                cptDir.getAbsolutePath(), "-t", "text", "-h", "localhost", "-l", "1000",}));
        TestDirectoryPoller poller = new TestDirectoryPoller();
        pm.directoryLookupStarted(new DirectoryLookupStartEvent(poller, inbDir));
        pm.fileFound(new TestFileFound(poller, goodFile));
        pm.fileFound(new TestFileFound(poller, brknFile));
        
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
        final ProcErrPM pm = new ProcErrPM();
        CommandLineParser parser = new BasicParser();
        pm.configure(parser.parse(
                        pm.getConfigurationOptions(),
                        new String[] {"-e", errDir.getAbsolutePath(), "-w", wrkDir.getAbsolutePath(), "-a", arcDir.getAbsolutePath(), "-r",
                                cptDir.getAbsolutePath(), "-t", "text", "-h", "localhost", "-l", "1000",}));
        TestDirectoryPoller poller = new TestDirectoryPoller();
        pm.directoryLookupStarted(new DirectoryLookupStartEvent(poller, inbDir));
        try {
            pm.fileFound(new TestFileFound(poller, fatalFile));
            Assert.fail("Expected an error to be thrown");
        } catch (Error e) {
            // expected
        }
        Assert.assertTrue(poller.wasShutdown);
    }
    
    /** Replaces the GZipOutputStream with a BrokenOutputStream. */
    public static class OutErrPM extends ConfiguredFcpm {
        /** Throws a TestEx on {@code flush()}. */
        private class BrokenOutputStream extends GZIPOutputStream {
            public BrokenOutputStream(OutputStream out) throws IOException {
                super(out);
            }
            
            @Override
            public void flush() throws IOException {
                throw new TestEx("THROWN FOR TESTING");
            }
        }
        
        @Override
        protected void createNewOutputFile() throws IOException {
            super.createNewOutputFile();
            out = new BrokenOutputStream(counting);
        }
    }
    
    /**
     * Ensures that output errors are handled correctly.
     * 
     * @throws Exception
     * @throws ParseException
     */
    @Test
    public void outputErrorTest() throws ParseException, Exception {
        final OutErrPM pm = new OutErrPM();
        CommandLineParser parser = new BasicParser();
        pm.configure(parser.parse(
                        pm.getConfigurationOptions(),
                        new String[] {"-e", errDir.getAbsolutePath(), "-w", wrkDir.getAbsolutePath(), "-a", arcDir.getAbsolutePath(), "-r",
                                cptDir.getAbsolutePath(), "-t", "text", "-h", "localhost", "-l", "1000",}));
        TestDirectoryPoller poller = new TestDirectoryPoller();
        pm.directoryLookupStarted(new DirectoryLookupStartEvent(poller, inbDir));
        pm.fileFound(new TestFileFound(poller, goodFile));
        try {
            pm.finishCurrentFile(false);
            Assert.fail("Expected finishCurrentFile to fail");
        } catch (IOException ioe) {
            // expected
        }
        
        Assert.assertTrue(goodFile + " has not been returned", goodFile.exists());
        Assert.assertTrue(brknFile + " disappeared...", brknFile.exists());
        
        Assert.assertEquals(0, arcDir.list().length);
        Assert.assertEquals(0, cptDir.list().length);
        Assert.assertEquals(0, wrqDir.list().length);
        Assert.assertEquals(2, wrkDir.list().length);
        Assert.assertEquals(0, errDir.list().length);
        Assert.assertEquals(3, inbDir.list().length);
    }
    
    /** Replaces the CountingOutputStream with a ZeroBytesOutputStream. */
    public static class ZeroOutPM extends ConfiguredFcpm {
        @Override
        protected long processFile(final File f, OutputStream os) throws Exception {
            if (f.getName().contains("break.txt"))
                return 0l;
            return super.processFile(f, os);
        }
    }
    
    /**
     * Tests that when files produce Zero Output, that they should go into the "error" directory.
     * 
     * @throws Exception
     * @throws ParseException
     */
    @Test
    public void zeroOutputTest() throws ParseException, Exception {
        final ZeroOutPM pm = new ZeroOutPM();
        CommandLineParser parser = new BasicParser();
        pm.configure(parser.parse(
                        pm.getConfigurationOptions(),
                        new String[] {"-e", errDir.getAbsolutePath(), "-w", wrkDir.getAbsolutePath(), "-a", arcDir.getAbsolutePath(), "-r",
                                cptDir.getAbsolutePath(), "-t", "text", "-h", "localhost", "-l", "1000",}));
        TestDirectoryPoller poller = new TestDirectoryPoller();
        pm.directoryLookupStarted(new DirectoryLookupStartEvent(poller, inbDir));
        pm.fileFound(new TestFileFound(poller, goodFile));
        pm.fileFound(new TestFileFound(poller, brknFile));
        pm.finishCurrentFile(false);
        
        Assert.assertFalse(poller.wasShutdown);
        
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
        Assert.assertEquals(0, cptDir.list().length);// since we have not closed, the complete file should still be in the work dir
        Assert.assertEquals(0, wrqDir.list().length);
        Assert.assertEquals(3, wrkDir.list().length);
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
        final ZeroOutPM pm = new ZeroOutPM();
        CommandLineParser parser = new BasicParser();
        pm.configure(parser.parse(
                        pm.getConfigurationOptions(),
                        new String[] {"-e", errDir.getAbsolutePath(), "-w", wrkDir.getAbsolutePath(), "-a", arcDir.getAbsolutePath(), "-r",
                                cptDir.getAbsolutePath(), "-t", "text", "-h", "localhost", "-l", "1000", "-mfl", "1",}));
        TestDirectoryPoller poller = new TestDirectoryPoller();
        pm.directoryLookupStarted(new DirectoryLookupStartEvent(poller, inbDir));
        pm.fileFound(new TestFileFound(poller, goodFile));
        pm.fileFound(new TestFileFound(poller, brknFile));
        pm.finishCurrentFile(false);
        
        Assert.assertTrue(poller.wasShutdown);
        
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
    
    /**
     * Tests the error handling for receiving a File with the wrong compression status.
     * 
     * @throws Exception
     * @throws ParseException
     */
    @Test
    public void consistentGzipTest() throws ParseException, Exception {
        final ConfiguredFcpm pm = new ConfiguredFcpm();
        CommandLineParser parser = new BasicParser();
        pm.configure(parser.parse(
                        pm.getConfigurationOptions(),
                        new String[] {"-e", errDir.getAbsolutePath(), "-w", wrkDir.getAbsolutePath(), "-a", arcDir.getAbsolutePath(), "-r",
                                cptDir.getAbsolutePath(), "-t", "text", "-h", "localhost", "-l", "1000",}));
        TestDirectoryPoller poller = new TestDirectoryPoller();
        pm.directoryLookupStarted(new DirectoryLookupStartEvent(poller, inbDir));
        pm.fileFound(new TestFileFound(poller, goodFile));
        pm.setCompressedInput(true);
        pm.fileFound(new TestFileFound(poller, brknFile));
        pm.finishCurrentFile(false);
        
        Assert.assertFalse(poller.wasShutdown);
        
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
    
    /**
     * The Poller self-terminates after reaching MaxFailures.
     * 
     * @throws Exception
     * @throws ParseException
     */
    @Test
    public void maxGzipFailTest() throws ParseException, Exception {
        final ConfiguredFcpm pm = new ConfiguredFcpm();
        CommandLineParser parser = new BasicParser();
        pm.configure(parser.parse(
                        pm.getConfigurationOptions(),
                        new String[] {"-e", errDir.getAbsolutePath(), "-w", wrkDir.getAbsolutePath(), "-a", arcDir.getAbsolutePath(), "-r",
                                cptDir.getAbsolutePath(), "-t", "text", "-h", "localhost", "-l", "1000", "-mfl", "1",}));
        TestDirectoryPoller poller = new TestDirectoryPoller();
        pm.directoryLookupStarted(new DirectoryLookupStartEvent(poller, inbDir));
        pm.fileFound(new TestFileFound(poller, goodFile));
        pm.setCompressedInput(true);
        pm.fileFound(new TestFileFound(poller, brknFile));
        pm.finishCurrentFile(false);
        
        Assert.assertTrue(poller.wasShutdown);
        
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
    
    /** Replaces the CountingOutputStream with a ZeroBytesOutputStream. */
    public static class AltSecurityMarkingErrPM extends ConfiguredFcpm {
        @Override
        protected long processFile(final File f, OutputStream os) throws Exception {
            throw new SecurityMarkingLoadException("THROWN FOR TESTING");
        }
    }
    
    /**
     * Test taht the Poller self-terminates after alt security markings fail to load.
     * 
     * @throws Exception
     * @throws ParseException
     */
    @Test
    public void camkeyFailsTest() throws ParseException, Exception {
        final AltSecurityMarkingErrPM pm = new AltSecurityMarkingErrPM();
        CommandLineParser parser = new BasicParser();
        pm.configure(parser.parse(
                        pm.getConfigurationOptions(),
                        new String[] {"-e", errDir.getAbsolutePath(), "-w", wrkDir.getAbsolutePath(), "-a", arcDir.getAbsolutePath(), "-r",
                                cptDir.getAbsolutePath(), "-t", "text", "-h", "localhost", "-l", "1000",}));
        TestDirectoryPoller poller = new TestDirectoryPoller();
        pm.directoryLookupStarted(new DirectoryLookupStartEvent(poller, inbDir));
        pm.fileFound(new TestFileFound(poller, goodFile));
        
        Assert.assertTrue(poller.wasShutdown);
        Assert.assertTrue(goodFile.exists());
        Assert.assertTrue(brknFile.exists());
        
        Assert.assertFalse((new File(errDir, goodFile.getName() + ".1")).exists());
        Assert.assertFalse((new File(errDir, brknFile.getName() + ".1")).exists());
        
        Assert.assertEquals(0, arcDir.list().length);
        Assert.assertEquals(0, cptDir.list().length);
        Assert.assertEquals(0, wrqDir.list().length);
        Assert.assertEquals(2, wrkDir.list().length);
        Assert.assertEquals(0, errDir.list().length);
        Assert.assertEquals(3, inbDir.list().length);
    }
}
