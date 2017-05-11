package datawave.util.flag;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

import datawave.util.StringUtils;
import datawave.ingest.mapreduce.StandaloneStatusReporter;
import datawave.util.flag.config.ConfigUtil;
import datawave.util.flag.config.FlagDataTypeConfig;
import datawave.util.flag.config.FlagMakerConfig;
import datawave.util.flag.processor.DateUtils;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.math.LongRange;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.JobConf;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class FlagMakerTest {
    
    static final String TEST_CONFIG = "target/test-classes/TestFlagMakerConfig.xml";
    static final String FLAG_DIR = "target/test/flags";
    static final int COUNTER_LIMIT = 102;
    private FlagMakerConfig fmc;
    
    public FlagMakerTest() {}
    
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
    
    @After
    public void tearDown() {}
    
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
        File fdir = new File(FLAG_DIR);
        if (fdir.exists())
            fdir.delete();
        FlagMaker instance = new TestWrappedFlagMaker(fmc);
        FileSystem result = instance.getHadoopFS();
        result.mkdirs(new Path(FLAG_DIR));
        assertTrue(fdir.exists() && fdir.isDirectory());
        
    }
    
    /**
     * Test of processFlags method, of class FlagMaker.
     */
    @Test
    public void testProcessFlags() throws Exception {
        File f = setUpFlagDir();
        // two days, 5 files each day, two folders in fmc = 20 flags
        LongRange range = createTestFiles(2, 5);
        FlagMaker instance = new TestWrappedFlagMaker(fmc);
        instance.processFlags();
        // should have made 2 flag files per
        assertEquals("Incorrect files.  Expected 2 but got " + f.listFiles().length + ": " + Arrays.toString(f.listFiles()), 2, f.listFiles().length);
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
        File f = setUpFlagDir();
        // two days, 5 files each day, two folders in fmc = 20 flags
        LongRange range = createTestFiles(2, 5);
        fmc.setSetFlagFileTimestamp(true);
        FlagMaker instance = new TestWrappedFlagMaker(fmc);
        instance.processFlags();
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
        File f = setUpFlagDir();
        // two days, 5 files each day, two folders in fmc = 20 flags
        LongRange range = createTestFiles(2, 5);
        fmc.setSetFlagFileTimestamp(false);
        FlagMaker instance = new TestWrappedFlagMaker(fmc);
        instance.processFlags();
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
        File f = setUpFlagDir();
        // two days, 5 files each day, two folders in fmc = 20 flags
        LongRange range = createTestFiles(2, 5, true);
        fmc.setSetFlagFileTimestamp(true);
        fmc.setUseFolderTimestamp(true);
        FlagMaker instance = new TestWrappedFlagMaker(fmc);
        instance.processFlags();
        // ensure the flag files have appropriate dates
        for (File file : f.listFiles()) {
            if (file.getName().endsWith(".flag")) {
                assertTrue(range.containsLong(file.lastModified()));
            }
        }
    }
    
    @Test
    public void testCounterLimitExceeded() throws Exception {
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
        assertTrue(flagFileLists.size() == 2);
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
        File f = setUpFlagDir();
        // 5 days, 5 files each day, two folders in fmc = 50 flags
        createTestFiles(5, 5);
        createBogusTestFiles(5, 5);
        createCopyingTestFiles(5, 5);
        fmc.setDistributorType("folderdate");
        FlagMaker instance = new TestWrappedFlagMaker(fmc);
        instance.processFlags();
        File[] flags = f.listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return pathname.toString().endsWith("flag");
            }
        });
        assertEquals("Incorrect number of flags: " + Arrays.toString(flags), 5, flags.length);
        HashSet<Long> buckets = new HashSet<>();
        DateUtils du = new DateUtils();
        for (File file : flags) {
            BufferedReader r = new BufferedReader(new FileReader(file));
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
                buckets.add(du.getBucket("day", string));
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
            r.close();
        }
        
    }
    
    private static final String data = "data";
    
    private LongRange createTestFiles(int days, int filesPerDay) throws Exception {
        return createTestFiles(days, filesPerDay, false);
    }
    
    private LongRange createTestFiles(int days, int filesPerDay, boolean folderRange) throws Exception {
        return createTestFiles(days, filesPerDay, "2013/01", folderRange, "");
    }
    
    private LongRange createBogusTestFiles(int days, int filesPerDay) throws Exception {
        return createTestFiles(days, filesPerDay, "20xx/dd", false, "");
    }
    
    private LongRange createCopyingTestFiles(int days, int filesPerDay) throws Exception {
        return createTestFiles(days, filesPerDay, "2013/01", false, "._COPYING_");
    }
    
    private LongRange createTestFiles(int days, int filesPerDay, String datepath, boolean folderRange, String postfix) throws Exception {
        Calendar c = Calendar.getInstance();
        c.set(Calendar.HOUR_OF_DAY, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);
        c.set(Calendar.YEAR, 2013);
        c.set(Calendar.MONTH, Calendar.JANUARY);
        if (days < 1 || days > 9)
            throw new IllegalArgumentException("days argument must be [1-9]. Incorrect value was: " + days);
        // there should only be relative paths for testing!
        ArrayList<File> inputdirs = new ArrayList<>(10);
        for (FlagDataTypeConfig fc : fmc.getFlagConfigs()) {
            for (String s : fc.getFolder()) {
                for (String folder : StringUtils.split(s, ',')) {
                    folder = folder.trim();
                    if (!folder.startsWith(fmc.getBaseHDFSDir())) {
                        // we do this conditionally because once the FileMaker is created and the setup call is made, this
                        // is already done.
                        folder = fmc.getBaseHDFSDir() + File.separator + folder;
                    }
                    inputdirs.add(new File(folder));
                }
            }
        }
        LongRange range = null;
        for (File file : inputdirs) {
            for (int i = 0; i < days;) {
                File one = new File(file.getAbsolutePath() + File.separator + datepath + File.separator + "0" + ++i);
                // set a day that is 10 days past the folder date
                c.set(Calendar.DAY_OF_MONTH, i + 10);
                range = merge(range, writeTestFiles(one, filesPerDay, c.getTimeInMillis(), folderRange, postfix));
            }
        }
        return range;
    }
    
    private LongRange merge(LongRange range1, LongRange range2) {
        if (range1 == null) {
            return range2;
        } else if (range2 == null) {
            return range1;
        } else {
            long min = Math.min(range1.getMinimumLong(), range2.getMinimumLong());
            long max = Math.max(range1.getMaximumLong(), range2.getMaximumLong());
            return new LongRange(min, max);
        }
    }
    
    private LongRange writeTestFiles(File f, int count, long time, boolean folderRange, String postfix) throws Exception {
        if (!f.exists()) {
            f.mkdirs();
        }
        for (int i = 0; i < count; i++) {
            File testFile = new File(f.getAbsolutePath() + File.separator + UUID.randomUUID() + postfix);
            if (testFile.exists()) {
                testFile.delete();
            }
            FileOutputStream fos = new FileOutputStream(testFile);
            fos.write(data.getBytes());
            fos.close();
            testFile.setLastModified(time + (i * 1000));
        }
        if (folderRange) {
            Calendar c = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
            c.set(Calendar.HOUR_OF_DAY, 0);
            c.set(Calendar.MINUTE, 0);
            c.set(Calendar.SECOND, 0);
            c.set(Calendar.MILLISECOND, 0);
            String[] dir = StringUtils.split(f.getAbsolutePath(), File.separatorChar);
            c.set(Calendar.YEAR, Integer.parseInt(dir[dir.length - 3]));
            c.set(Calendar.MONTH, Integer.parseInt(dir[dir.length - 2]) - 1);
            c.set(Calendar.DAY_OF_MONTH, Integer.parseInt(dir[dir.length - 1]));
            return new LongRange(c.getTimeInMillis(), c.getTimeInMillis());
        } else {
            return new LongRange(time, time + ((count - 1) * 1000));
        }
    }
    
    public static class TestWrappedFlagMaker extends FlagMaker {
        public TestWrappedFlagMaker(FlagMakerConfig fmc) {
            super(fmc);
            Configuration conf = new Configuration();
            conf.set("mapreduce.job.counters.max", Integer.toString(COUNTER_LIMIT));
            this.config = new JobConf(conf);
        }
        
        @Override
        protected void writeMetrics(final StandaloneStatusReporter reporter, final String metricsDirectory, final String dataTypeName,
                        final CompressionType ct, final CompressionCodec cc) throws IOException {
            // Do Nothing
        }
    }
}
