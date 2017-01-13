package nsa.datawave.poller.manager;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.zip.GZIPInputStream;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class MessageWrappingFileCombiningPollManagerTest {
    
    Logger log = Logger.getLogger(MessageWrappingFileCombiningPollManagerTest.class);
    
    File input = null;
    File work = null;
    File queue = null;
    File error = null;
    File completed = null;
    
    String[] args = null;
    String fileHeader = "ThisIsAFileHeader";
    String fileTrailer = "ThisIsAFileTrailer";
    String messageHeader = "ThisIsAMessageHeader";
    String messageTrailer = "ThisIsAMessageTrailer";
    String stripHeader = "ThisIsAStripHeader";
    String stripTrailer = "ThisIsAStripTrailer";
    
    MessageWrappingFileCombiningPollManager poller;
    
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    
    @Before
    public void setUp() throws Exception {
        
        ConsoleAppender appender = new ConsoleAppender();
        appender.setImmediateFlush(true);
        
        BasicConfigurator.configure(appender);
        
        poller = new MessageWrappingFileCombiningPollManager();
        
        // Make a local input, work, and error directory
        File tmpDir = temporaryFolder.newFolder("wrapping_poller_test");
        input = temporaryFolder.newFolder(tmpDir.getName(), "input");
        work = temporaryFolder.newFolder(tmpDir.getName(), "work");
        queue = temporaryFolder.newFolder(work.getName(), "queue");
        error = temporaryFolder.newFolder(tmpDir.getName(), "error");
        completed = temporaryFolder.newFolder(tmpDir.getName(), "completed");
        
        // Make a localhost HDFS test directory
        Configuration conf = new Configuration(true);
        FileSystem fs = FileSystem.get(conf);
        File dest = new File(tmpDir, "dest");
        dest.mkdir();
        dest.deleteOnExit();
        Path p = new Path(dest.toURI());
        fs.mkdirs(p);
        fs.deleteOnExit(p);
        
        List<String> arguments = new ArrayList<>();
        arguments.add("-e");
        arguments.add(error.getAbsolutePath());
        arguments.add("-w");
        arguments.add(work.getAbsolutePath());
        arguments.add("-t");
        arguments.add("foo");
        arguments.add("-l");
        arguments.add("60000");
        arguments.add("-h");
        arguments.add(p.toUri().toString());
        arguments.add("-r");
        arguments.add(completed.getAbsolutePath());
        arguments.add("-fh");
        arguments.add(fileHeader);
        arguments.add("-ft");
        arguments.add(fileTrailer);
        arguments.add("-mh");
        arguments.add(messageHeader);
        arguments.add("-mt");
        arguments.add(messageTrailer);
        arguments.add("-sh");
        arguments.add(stripHeader);
        arguments.add("-st");
        arguments.add(stripTrailer);
        
        args = arguments.toArray(new String[arguments.size()]);
        
        CommandLine cl = null;
        try {
            cl = new BasicParser().parse(poller.getConfigurationOptions(), args);
        } catch (ParseException pe) {
            new HelpFormatter().printHelp("Poller ", poller.getConfigurationOptions(), true);
            throw pe;
        }
        
        poller.configure(cl);
    }
    
    Random random = new Random(12349087);
    
    private File createInputFile(OutputStream expectedOutput) throws IOException {
        File f = File.createTempFile(String.valueOf(System.currentTimeMillis()), ".foo");
        f.deleteOnExit();
        FileWriter writer = new FileWriter(f);
        if (random.nextBoolean()) {
            writer.write(stripHeader);
        }
        expectedOutput.write(messageHeader.getBytes("UTF-8"));
        for (int i = 0; i < 60; i++) {
            String line = UUID.randomUUID().toString() + "\n";
            writer.write(line);
            expectedOutput.write(line.getBytes("UTF-8"));
        }
        if (random.nextBoolean()) {
            writer.write(stripTrailer);
        }
        expectedOutput.write(messageTrailer.getBytes("UTF-8"));
        writer.flush();
        writer.close();
        File newFile = new File(input, f.getName());
        newFile.deleteOnExit();
        if (f.renameTo(newFile)) {
            return newFile;
        } else {
            throw new IOException("Could not rename " + f + " to " + newFile);
        }
    }
    
    /**
     * This test starts the poller, creates multiple files, then shuts down the poller.
     * 
     * @throws Exception
     */
    @Test
    public void testMultipleFiles() throws Exception {
        ByteArrayOutputStream expectedOutput = new ByteArrayOutputStream();
        expectedOutput.write(fileHeader.getBytes("UTF-8"));
        poller.createNewOutputFile();
        File output = new File(poller.completeDir, poller.currentOutGzipFile.getName());
        output.deleteOnExit();
        for (int i = 0; i < 50; i++) {
            File file1 = createInputFile(expectedOutput);
            poller.processFile(file1, poller.out);
        }
        poller.finishCurrentFile(true);
        expectedOutput.write(fileTrailer.getBytes("UTF-8"));
        verifyOutputFiles(output, expectedOutput.toByteArray());
    }
    
    private void verifyOutputFiles(File output, byte[] expected) throws IOException {
        // verify the contents of the file
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        IOUtils.copy(new BufferedInputStream(new GZIPInputStream(new BufferedInputStream(new FileInputStream(output)))), stream);
        byte[] bytes = stream.toByteArray();
        Assert.assertEquals("Output length incorrect", bytes.length, expected.length);
        for (int i = 0; i < expected.length; i++) {
            Assert.assertEquals("Incorrect value", bytes[i], expected[i]);
        }
    }
}
