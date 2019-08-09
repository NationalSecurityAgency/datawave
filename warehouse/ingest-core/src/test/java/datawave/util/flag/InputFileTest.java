package datawave.util.flag;

import datawave.util.flag.InputFile.TrackedDir;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class InputFileTest {
    private static final Logger log = LoggerFactory.getLogger(InputFileTest.class);
    
    private static final Random rVal = new Random(System.currentTimeMillis());
    private static final String BASE_DIR = "/base";
    private InputFile inFile;
    
    @Before
    public void setup() {
        Path p = new Path("path");
        this.inFile = new InputFile("foo", p, 10, 10, 0, BASE_DIR);
    }
    
    @Test
    public void testUpdateDir() {
        log.info("-----  testUpdateDir  -----");
        TrackedDir update = TrackedDir.FLAGGING_DIR;
        for (int n = 0; n < 10; n++) {
            TrackedDir dir = getRandomDir(update);
            inFile.updateCurrentDir(dir);
            Path cur = inFile.getCurrentDir();
            Path expect = inFile.getTrackedDir(dir);
            Assert.assertEquals(expect, cur);
            Assert.assertTrue(inFile.isMoved());
            inFile.setMoved(false);
        }
    }
    
    @Test
    public void testEqualsHashCode() {
        InputFile test = new InputFile(inFile.getFolder(), inFile.getPath(), inFile.getBlocksize(), inFile.getFilesize(), inFile.getTimestamp(), "/xx");
        Assert.assertEquals(test, this.inFile);
        Assert.assertEquals(test.hashCode(), this.inFile.hashCode());
        test.setFilesize(test.getFilesize() + 1);
        Assert.assertNotEquals(test, this.inFile);
        Assert.assertNotEquals(test.hashCode(), this.inFile.hashCode());
    }
    
    private TrackedDir getRandomDir(TrackedDir exclude) {
        List<TrackedDir> valid = new ArrayList<>();
        for (TrackedDir dir : TrackedDir.values()) {
            if (!(dir.equals(TrackedDir.PATH_DIR) || dir.equals(exclude))) {
                valid.add(dir);
            }
        }
        
        int idx = rVal.nextInt(valid.size());
        return valid.get(idx);
    }
}
