package datawave.util.flag;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.UUID;

import javax.xml.bind.JAXBException;

import datawave.util.StringUtils;
import datawave.util.flag.config.ConfigUtil;
import datawave.util.flag.config.FlagDataTypeConfig;
import datawave.util.flag.config.FlagMakerConfig;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.math.LongRange;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

public class AbstractFlagConfig {
    
    protected static final String TEST_CONFIG = "target/test-classes/TestFlagMakerConfig.xml";
    
    protected FlagMakerConfig fmc;
    
    protected void cleanTestDirs() throws IOException {
        File f = new File(this.fmc.getBaseHDFSDir());
        if (f.exists()) {
            // commons io has recursive delete.
            FileUtils.deleteDirectory(f);
        }
        if (!f.mkdirs()) {
            throw new IOException("unable to create base HDFS directory (" + f.getAbsolutePath() + ")");
        }
    }
    
    protected void createTrackedDirs(final FileSystem fs, final InputFile file) throws IOException {
        final Path[] dirs = {file.getFlagged(), file.getFlagging(), file.getLoaded()};
        for (final Path dir : dirs) {
            final Path p = dir.getParent();
            if (!fs.mkdirs(p)) {
                throw new IllegalStateException("unable to create tracked directory (" + dir.getParent() + ")");
            }
        }
    }
    
    protected FlagMakerConfig getDefaultFMC() throws JAXBException, IOException {
        return ConfigUtil.getXmlObject(FlagMakerConfig.class, TEST_CONFIG);
    }
    
    protected LongRange createTestFiles(int days, int filesPerDay) throws IOException {
        return createTestFiles(days, filesPerDay, false);
    }
    
    protected LongRange createTestFiles(int days, int filesPerDay, boolean folderRange) throws IOException {
        return createTestFiles(days, filesPerDay, "2013/01", folderRange, "");
    }
    
    protected LongRange createBogusTestFiles(int days, int filesPerDay) throws IOException {
        return createTestFiles(days, filesPerDay, "20xx/dd", false, "");
    }
    
    protected LongRange createCopyingTestFiles(int days, int filesPerDay) throws IOException {
        return createTestFiles(days, filesPerDay, "2013/01", false, "._COPYING_");
    }
    
    protected LongRange createTestFiles(int days, int filesPerDay, String datepath, boolean folderRange, String postfix) throws IOException {
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
        for (FlagDataTypeConfig fc : this.fmc.getFlagConfigs()) {
            for (String s : fc.getFolder()) {
                for (String folder : StringUtils.split(s, ',')) {
                    folder = folder.trim();
                    if (!folder.startsWith(this.fmc.getBaseHDFSDir())) {
                        // we do this conditionally because once the FileMaker is created and the setup call is made, this
                        // is already done.
                        folder = this.fmc.getBaseHDFSDir() + File.separator + folder;
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
                final LongRange endRange = writeTestFiles(one, filesPerDay, c.getTimeInMillis(), folderRange, postfix);
                range = merge(range, endRange);
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
    
    private LongRange writeTestFiles(File f, int count, long time, boolean folderRange, String postfix) throws IOException {
        if (!f.exists()) {
            f.mkdirs();
        }
        for (int i = 0; i < count; i++) {
            File testFile = new File(f.getAbsolutePath() + File.separator + UUID.randomUUID() + postfix);
            if (testFile.exists()) {
                testFile.delete();
            }
            try (FileOutputStream fos = new FileOutputStream(testFile)) {
                fos.write(("" + System.currentTimeMillis()).getBytes());
            }
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
    
    protected Path getTestFile(FileSystem fs) throws IOException {
        createTestFiles(1, 1);
        Path file = null;
        for (RemoteIterator<LocatedFileStatus> it = fs.listFiles(new Path(this.fmc.getBaseHDFSDir()), true); it.hasNext();) {
            LocatedFileStatus status = it.next();
            if (status.isFile()) {
                file = status.getPath();
                break;
            }
        }
        return file;
    }
}
