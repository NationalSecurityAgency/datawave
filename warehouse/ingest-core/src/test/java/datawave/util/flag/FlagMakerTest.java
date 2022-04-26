package datawave.util.flag;

import datawave.util.flag.config.ConfigUtil;
import datawave.util.flag.config.FlagDataTypeConfig;
import datawave.util.flag.config.FlagMakerConfig;
import datawave.util.flag.processor.DateUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.math.LongRange;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class FlagMakerTest extends AbstractFlagConfig {
    
    private static final Logger log = LoggerFactory.getLogger(FlagMaker.class);
    private static final String TEST_CONFIG = "target/test-classes/TestFlagMakerConfig.xml";
    private static final String FLAG_DIR = "target/test/flags";
    private static final int COUNTER_LIMIT = 102;
    private static final String CONFIG_BASE_HDFS_DIR = "target/test/BulkIngest";
    private static final String CONFIG_FLAG_FILE_DIR = "target/test/flags";
    private static final Object CONFIG_EXTRA_INGEST_ARGS = null;
    
    // private FlagMakerConfig fmc;
    
    @Before
    public void setUp() throws Exception {
        fmc = ConfigUtil.getXmlObject(FlagMakerConfig.class, TEST_CONFIG);
        File f = new File(fmc.getBaseHDFSDir());
        if (f.exists()) {
            // commons io has recursive delete.
            FileUtils.deleteDirectory(f);
        }
        f.mkdirs();
    }
    
    private File setUpFlagDir() throws Exception {
        File f = new File(FLAG_DIR);
        if (f.exists())
            FileUtils.deleteDirectory(f);
        f.mkdirs();
        return f;
    }
    
    /**
     * Test successful creation of local HDFS.
     */
    @Test
    public void testGetHadoopFS() throws Exception {
        log.info("-----  testGetHadoopFS  -----");
        File fdir = new File(FLAG_DIR);
        if (fdir.exists())
            fdir.delete();
        FlagMaker instance = new TestWrappedFlagMaker(fmc);
        FileSystem result = instance.getHadoopFS();
        result.mkdirs(new Path(FLAG_DIR));
        assertTrue(fdir.exists() && fdir.isDirectory());
        
    }
    
    @Test
    public void testFlagMakerConfigArg() throws JAXBException, IOException {
        log.info("-----  testFlagMakerConfigArg  -----");
        FlagMakerConfig flagMakerConfig = FlagMaker.getFlagMakerConfig(new String[] {"-flagConfig", TEST_CONFIG});
        assertEquals(CONFIG_FLAG_FILE_DIR, flagMakerConfig.getFlagFileDirectory());
        assertEquals(CONFIG_BASE_HDFS_DIR, flagMakerConfig.getBaseHDFSDir());
        assertEquals(CONFIG_EXTRA_INGEST_ARGS, flagMakerConfig.getDefaultCfg().getExtraIngestArgs());
        assertEquals(2, flagMakerConfig.getFilePatterns().size());
        assertTrue(flagMakerConfig.getFilePatterns().contains("2*/*/*/[0-9a-zA-Z]*[0-9a-zA-Z]"));
        assertTrue(flagMakerConfig.getFilePatterns().contains("2*/*/*/*/[0-9a-zA-Z]*[0-9a-zA-Z]"));
    }
    
    @Test
    public void testFlagMakerConfigWithFlagFileDirectoryOverride() throws JAXBException, IOException {
        log.info("-----  testFlagMakerConfigWithFlagFileDirectoryOverride  -----");
        final String overrideValue = "/srv/data/somewhere/else/";
        FlagMakerConfig flagMakerConfig = FlagMaker.getFlagMakerConfig(new String[] {"-flagConfig", TEST_CONFIG, "-flagFileDirectoryOverride", overrideValue});
        assertEquals(overrideValue, flagMakerConfig.getFlagFileDirectory());
        assertEquals(CONFIG_BASE_HDFS_DIR, flagMakerConfig.getBaseHDFSDir());
        assertEquals(CONFIG_EXTRA_INGEST_ARGS, flagMakerConfig.getDefaultCfg().getExtraIngestArgs());
    }
    
    @Test
    public void testFlagMakerConfigWithHdfsOverride() throws JAXBException, IOException {
        log.info("-----  testFlagMakerConfigWithHdfsOverride  -----");
        String overrideValue = "testDir/BulkIngest";
        FlagMakerConfig flagMakerConfig = FlagMaker.getFlagMakerConfig(new String[] {"-flagConfig", TEST_CONFIG, "-baseHDFSDirOverride", overrideValue});
        assertEquals(CONFIG_FLAG_FILE_DIR, flagMakerConfig.getFlagFileDirectory());
        assertEquals(overrideValue, flagMakerConfig.getBaseHDFSDir());
        assertEquals(CONFIG_EXTRA_INGEST_ARGS, flagMakerConfig.getDefaultCfg().getExtraIngestArgs());
    }
    
    @Test
    public void testFlagMakerConfigWithExtraArgsOverride() throws JAXBException, IOException {
        log.info("-----  testFlagMakerConfigWithExtraArgsOverride  -----");
        final String overrideValue = "-fastMode -topSpeed=MAX";
        FlagMakerConfig flagMakerConfig = FlagMaker.getFlagMakerConfig(new String[] {"-flagConfig", TEST_CONFIG, "-extraIngestArgsOverride", overrideValue});
        assertEquals(CONFIG_FLAG_FILE_DIR, flagMakerConfig.getFlagFileDirectory());
        assertEquals(CONFIG_BASE_HDFS_DIR, flagMakerConfig.getBaseHDFSDir());
        assertEquals(overrideValue, flagMakerConfig.getDefaultCfg().getExtraIngestArgs());
    }
    
    /**
     * Test of processFlags method, of class FlagMaker.
     */
    @Test
    public void testProcessFlags() throws Exception {
        log.info("-----  testProcessFlags  -----");
        File f = setUpFlagDir();
        // two days, 5 files each day, two folders in fmc = 20 flags
        LongRange range = createTestFiles(2, 5);
        FlagMaker instance = new TestWrappedFlagMaker(fmc);
        instance.processFlags();
        
        // ensure the flag files endings
        int flagCnt = 0;
        int cleanCnt = 0;
        for (File file : f.listFiles()) {
            if (file.getName().endsWith(".cleanup")) {
                cleanCnt++;
            } else if (file.getName().endsWith(".flag")) {
                flagCnt++;
            }
        }
        assertEquals(2, flagCnt);
        // no longer have cleanup files
        assertEquals(0, cleanCnt);
    }
    
    /**
     * Test of time stamps of the flag files
     */
    @Test
    public void testFlagFileTimeStamps() throws Exception {
        log.info("-----  testFlagFileTimeStamps  -----");
        File f = setUpFlagDir();
        // two days, 5 files each day, two folders in fmc = 20 flags
        LongRange range = createTestFiles(2, 5);
        fmc.setSetFlagFileTimestamp(true);
        FlagMaker instance = new TestWrappedFlagMaker(fmc);
        instance.processFlags();
        assertEquals("Incorrect files.  Expected 2 but got " + f.listFiles().length + ": " + Arrays.toString(f.listFiles()), 2, f.listFiles().length);
        // ensure the flag files have appropriate dates
        for (File file : f.listFiles()) {
            if (file.getName().endsWith(".flag")) {
                assertTrue(range.containsLong(file.lastModified()));
            }
        }
    }
    
    /**
     * Test not setting the time stamps of the flag files
     */
    @Test
    public void testUnsetFlagFileTimeStamps() throws Exception {
        log.info("-----  testUnsetFlagFileTimeStamps  -----");
        File f = setUpFlagDir();
        // two days, 5 files each day, two folders in fmc = 20 flags
        LongRange range = createTestFiles(2, 5);
        fmc.setSetFlagFileTimestamp(false);
        FlagMaker instance = new TestWrappedFlagMaker(fmc);
        instance.processFlags();
        assertEquals("Incorrect files.  Expected 2 but got " + f.listFiles().length + ": " + Arrays.toString(f.listFiles()), 2, f.listFiles().length);
        
        // ensure the flag files have appropriate dates
        for (File file : f.listFiles()) {
            if (file.getName().endsWith(".flag")) {
                assertFalse(range.containsLong(file.lastModified()));
            }
        }
    }
    
    /**
     * Test of time stamps of the flag files using folders instead of file timestamps
     */
    @Test
    public void testFolderTimeStamps() throws Exception {
        log.info("-----  testFolderTimeStamps  -----");
        File f = setUpFlagDir();
        // two days, 5 files each day, two folders in fmc = 20 flags
        LongRange range = createTestFiles(2, 5, true);
        fmc.setSetFlagFileTimestamp(true);
        fmc.setUseFolderTimestamp(true);
        FlagMaker instance = new TestWrappedFlagMaker(fmc);
        instance.processFlags();
        assertEquals("Incorrect files.  Expected 2 but got " + f.listFiles().length + ": " + Arrays.toString(f.listFiles()), 2, f.listFiles().length);
        
        // ensure the flag files have appropriate dates
        for (File file : f.listFiles()) {
            if (file.getName().endsWith(".flag")) {
                assertTrue(range.containsLong(file.lastModified()));
            }
        }
    }
    
    @Test
    public void testCounterLimitExceeded() throws Exception {
        log.info("-----  testCounterLimitExceeded  -----");
        int expectedMax = (COUNTER_LIMIT - 2) / 2;
        File f = setUpFlagDir();
        // two days, expectedMax + 20 files each day
        LongRange range = createTestFiles(2, expectedMax + 20, true);
        int total = 2 * 2 * (expectedMax + 20);
        fmc.setSetFlagFileTimestamp(true);
        fmc.setUseFolderTimestamp(true);
        // set the max flags to something larger than the expected limit based on the counters
        fmc.getDefaultCfg().setMaxFlags(expectedMax + 15);
        FlagMaker instance = new TestWrappedFlagMaker(fmc);
        instance.processFlags();
        // ensure the flag files have appropriate sizes
        int a = f.listFiles().length;
        int expectedFlagCount = 0;
        for (File file : f.listFiles()) {
            if (file.getName().endsWith("+" + expectedMax + ".flag")) {
                expectedFlagCount++;
            }
        }
        assertEquals("Unexpected number of flag files with size " + expectedMax, (total / expectedMax), expectedFlagCount);
    }
    
    /**
     * Test of the flag count exceeded mechanism
     */
    @Test
    public void testFlagCountExceeded() throws Exception {
        log.info("-----  testFlagCountExceeded  -----");
        File f = setUpFlagDir();
        
        fmc.setTimeoutMilliSecs(0);
        
        // two days, 5 files each day, two folders in fmc = 20 flags
        LongRange range = createTestFiles(2, 5);
        FlagMaker instance = new TestWrappedFlagMaker(fmc);
        instance.processFlags();
        
        // should have made 2 flag files
        assertEquals("Incorrect files.  Expected 2 but got " + f.listFiles().length + ": " + Arrays.toString(f.listFiles()), 2, f.listFiles().length);
        
        fmc.setFlagCountThreshold(2);
        for (FlagDataTypeConfig fc : fmc.getFlagConfigs()) {
            fc.setFlagCountThreshold(2);
        }
        range = createTestFiles(2, 2);
        
        for (int i = 0; i < 10; i++) {
            instance.processFlags();
            
            // should have not make anymore flag files despite timeout of 0 ms
            assertEquals("Incorrect files.  Expected 2 but got " + f.listFiles().length + ": " + Arrays.toString(f.listFiles()), 2, f.listFiles().length);
        }
        
        range = createTestFiles(2, 1);
        instance.processFlags();
        
        // should have made one more flag file
        assertEquals("Incorrect files.  Expected 3 but got " + f.listFiles().length + ": " + Arrays.toString(f.listFiles()), 3, f.listFiles().length);
    }
    
    @Test
    public void testMaxFileLength() throws Exception {
        log.info("-----  testMaxFileLength  -----");
        File f = setUpFlagDir();
        // two days, expectedMax + 20 files each day
        LongRange range = createTestFiles(2, 20, true);
        int total = 2 * 2 * 20;
        fmc.setSetFlagFileTimestamp(true);
        fmc.setUseFolderTimestamp(true);
        // baselen is all of the other stuff
        int baselen = "target/test/bin/ingest/one-hr-ingest.sh 10 -inputFormat datawave.ingest.input.reader.csvdata.WrappedCSVFileInputFormat".length();
        // filelen is the length of one file
        int filelen = "target/test/BulkIngest/flagged/bar/2013/01/01/5a2078da-1569-4cb9-bd50-f95fc53934e7".length();
        // set a maxfilelength to hold no more than 5 input files in the list
        int flaglen = baselen - 1 + filelen * 5;
        // add 20 characters just in case this is not exact
        fmc.setMaxFileLength(flaglen + 20);
        // set the max flags something larger
        fmc.getDefaultCfg().setMaxFlags(20);
        FlagMaker instance = new TestWrappedFlagMaker(fmc);
        instance.processFlags();
        // ensure the flag files have the appropriate size
        int a = f.listFiles().length;
        int expectedFlagCount = 0;
        for (File file : f.listFiles()) {
            if (file.getName().endsWith("+" + 5 + ".flag")) {
                expectedFlagCount++;
            }
        }
        // Expect up to total/5 files, however as soon as we have less than maxFlags (i.e. 20) files left we leave the loop.
        // Reason being that maxFlags dictates how many files must be available to create a flag file unless the timeout is reached.
        // In this case the timeout is not reached so we stop before processing all of the files.
        assertEquals("Unexpected number of flag files with size 5", (total / 5) - (19 / 5), expectedFlagCount);
    }
    
    /**
     * Test of processFlags method flag file order, of class FlagMaker.
     */
    @Test
    public void testProcessFlagsOrder() throws Exception {
        log.info("-----  testProcessFlagsOrder  -----");
        setUpFlagDir();
        // two days, 5 files each day, two folders in fmc = 20 flags
        createTestFiles(2, 5);
        final List<Collection<InputFile>> flagFileLists = new ArrayList<>();
        FlagMaker instance = new TestWrappedFlagMaker(fmc) {
            @Override
            void writeFlagFile(FlagDataTypeConfig fc, Collection<InputFile> inFiles) throws IOException {
                flagFileLists.add(inFiles);
            }
        };
        instance.processFlags();
        // should have made 2 flag files
        assertEquals(2, flagFileLists.size());
        long lastTime = 0;
        for (Collection<InputFile> flagFileList : flagFileLists) {
            long nextTime = 0;
            for (InputFile file : flagFileList) {
                assertTrue("Found file out of expected order", file.getTimestamp() > lastTime);
                nextTime = Math.max(nextTime, file.getTimestamp());
            }
            lastTime = nextTime;
        }
    }
    
    /**
     * Test of processFlags method flag file LIFO order, of class FlagMaker.
     */
    @Test
    public void testProcessFlagsOrderLifo() throws Exception {
        log.info("-----  testProcessFlagsOrderLifo  -----");
        setUpFlagDir();
        // two days, 5 files each day, two folders in fmc = 20 flags
        createTestFiles(2, 5);
        final List<Collection<InputFile>> flagFileLists = new ArrayList<>();
        fmc.getFlagConfigs().get(0).setLifo(true);
        FlagMaker instance = new TestWrappedFlagMaker(fmc) {
            @Override
            void writeFlagFile(FlagDataTypeConfig fc, Collection<InputFile> inFiles) throws IOException {
                flagFileLists.add(inFiles);
            }
        };
        instance.processFlags();
        // should have made 2 flag files
        assertEquals("Incorrect file lists: " + flagFileLists, 2, flagFileLists.size());
        long lastTime = Long.MAX_VALUE;
        for (Collection<InputFile> flagFileList : flagFileLists) {
            long nextTime = Long.MAX_VALUE;
            for (InputFile file : flagFileList) {
                assertTrue("Found file out of expected order", file.getTimestamp() < lastTime);
                nextTime = Math.min(nextTime, file.getTimestamp());
            }
            lastTime = nextTime;
        }
    }
    
    /**
     * Test with Slice distributor
     */
    @Test
    public void testDateFolderDistributor() throws Exception {
        log.info("-----  testDateFolderDistributor  -----");
        File f = setUpFlagDir();
        // 5 days, 5 files each day, two folders in fmc = 50 flags
        createTestFiles(5, 5);
        createBogusTestFiles(5, 5);
        createCopyingTestFiles(5, 5);
        fmc.setDistributorType("folderdate");
        FlagMaker instance = new TestWrappedFlagMaker(fmc);
        instance.processFlags();
        File[] flags = f.listFiles(pathname -> pathname.toString().endsWith("flag"));
        assertEquals("Incorrect number of flags: " + Arrays.toString(flags), 5, flags.length);
        HashSet<Long> buckets = new HashSet<>();
        DateUtils du = new DateUtils();
        for (File file : flags) {
            try (BufferedReader r = new BufferedReader(new FileReader(file))) {
                String[] files = r.readLine().split(" ")[1].split(",");
                
                // 10 inputs per flag file
                assertEquals("Incorrect number of files: " + Arrays.toString(files), 10, files.length);
                Map<String,Integer> folderCounts = new HashMap<>();
                Map<String,Integer> dateCounts = new HashMap<>();
                for (String string : files) {
                    assertTrue(string.startsWith("target/test/BulkIngest/flagged"));
                    // pull off the sub folder
                    String folder = string.substring(31, 34);
                    folderCounts.put(folder, (folderCounts.containsKey(folder) ? folderCounts.get(folder) : 0) + 1);
                    String date = string.substring(35, 45);
                    dateCounts.put(date, (dateCounts.containsKey(date) ? dateCounts.get(date) : 0) + 1);
                    buckets.add(DateUtils.getBucket("day", string));
                }
                assertEquals(2, folderCounts.size());
                for (String folder : folderCounts.keySet()) {
                    assertEquals(5, folderCounts.get(folder).intValue());
                }
                assertEquals(5, dateCounts.size());
                for (String date : dateCounts.keySet()) {
                    assertEquals(2, dateCounts.get(date).intValue());
                }
                // 2 from each folder for each day
                assertEquals("Incorrect number of buckets: " + buckets, 5, buckets.size());
                buckets.clear();
            }
        }
        
    }
    
    // ======================================
    // file list marker tests
    
    private static final String FLAG_MARKER = "XXXX--test-marker--XXXX";
    private static final String INVALID_MARKER = "xxxx  invalid-marker  xxxx";
    // this needs to be in sync
    private static final String FLAG_PRE = " ${JOB_FILE} 10 -inputFormat datawave.ingest.input.reader.event.EventSequenceFileInputFormat -inputFileLists -inputFileListMarker "
                    + FLAG_MARKER + " \n" + FLAG_MARKER;
    
    @Test
    public void testFlagFileMarkerError() throws Exception {
        log.info("-----  testFlagFileMarkerError  -----");
        File f = setUpFlagDir();
        // two days, 5 files each day, two folders in fmc = 20 flags
        LongRange range = createTestFiles(2, 5);
        fmc.setSetFlagFileTimestamp(true);
        List<FlagDataTypeConfig> cfgs = fmc.getFlagConfigs();
        FlagDataTypeConfig cfg = cfgs.get(0);
        cfg.setFileListMarker(INVALID_MARKER);
        try {
            FlagMaker instance = new TestWrappedFlagMaker(fmc);
            Assert.fail("invalid marker expected");
        } catch (IllegalArgumentException ie) {
            // expected
        }
    }
    
    @Test
    public void testFlagFileMarker() throws Exception {
        log.info("-----  testFlagFileMarker  -----");
        File f = setUpFlagDir();
        // two days, 5 files each day, two folders in fmc = 20 flags
        LongRange range = createTestFiles(2, 5);
        // fmc.setSetFlagFileTimestamp(true);
        List<FlagDataTypeConfig> cfgs = fmc.getFlagConfigs();
        FlagDataTypeConfig cfg = cfgs.get(0);
        cfg.setFileListMarker("XXXX--test-marker--XXXX");
        FlagMaker instance = new TestWrappedFlagMaker(fmc);
        instance.processFlags();
        // ensure the flag files have appropriate dates
        for (File file : f.listFiles()) {
            if (file.getName().endsWith(".flag")) {
                assertTrue(range.containsLong(file.lastModified()));
            }
        }
        assertEquals("Incorrect files.  Expected 2 but got " + f.listFiles().length + ": " + Arrays.toString(f.listFiles()), 2, f.listFiles().length);
    }
    
    /**
     * Tests the flag file output when the file list marker is set.
     * 
     * @throws Exception
     *             test error condition
     */
    @Test
    public void testFlagFileWriter() throws Exception {
        log.info("-----  testFlagFileWriter  -----");
        File f = setUpFlagDir();
        
        FlagMaker instance = new TestWrappedFlagMaker(fmc);
        FlagDataTypeConfig fc = fmc.getFlagConfigs().get(0);
        createTestFiles(1, 5);
        instance.fd.setup(fc);
        instance.loadFilesForDistributor(fc, instance.getHadoopFS());
        Collection<InputFile> inFiles = instance.fd.next(instance);
        List<FlagDataTypeConfig> cfgs = fmc.getFlagConfigs();
        FlagDataTypeConfig cfg = cfgs.get(0);
        cfg.setFileListMarker(FLAG_MARKER);
        
        assertTrue("Should be 10 InputFiles", inFiles != null && inFiles.size() == 10);
        FlagMetrics metrics = new FlagMetrics(instance.getHadoopFS(), fc.isCollectMetrics());
        File flag = instance.write(inFiles, fc, FLAG_DIR + "/testflagwriter", metrics);
        flag.deleteOnExit();
        String b;
        try (BufferedReader br = new BufferedReader(new FileReader(flag))) {
            b = br.lines().collect(Collectors.joining("\n"));
        }
        
        StringBuilder expected = new StringBuilder(fmc.getDatawaveHome());
        expected.append(File.separatorChar).append(fc.getScript()).append(FLAG_PRE);
        for (InputFile inFile : inFiles) {
            expected.append('\n').append(inFile.getFlagged().toUri());
        }
        
        Assert.assertEquals(expected.length(), b.length());
        for (int n = 0; n < b.length(); n++) {
            Assert.assertEquals("flag file char mismatch at char #" + n, expected.charAt(n), b.charAt(n));
        }
    }
    
    static class TestWrappedFlagMaker extends FlagMaker {
        TestWrappedFlagMaker(FlagMakerConfig fmc) {
            super(fmc);
            Configuration conf = new Configuration();
            conf.set("mapreduce.job.counters.max", Integer.toString(COUNTER_LIMIT));
            this.config = new JobConf(conf);
        }
    }
}
