package datawave.query.util.sortedset;

import com.google.common.io.Files;
import datawave.query.iterator.ivarator.IvaratorCacheDir;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class HdfsBackedSortedSetTest {
    
    @Test
    public void persistReloadTest() throws Exception {
        File tempDir = Files.createTempDir();
        tempDir.deleteOnExit();
        
        File smallDir = new File(tempDir, "small");
        Assert.assertTrue(smallDir.mkdirs());
        
        File largeDir = new File(tempDir, "large");
        Assert.assertTrue(largeDir.mkdirs());
        
        LocalFileSystem fs = new LocalFileSystem();
        fs.initialize(tempDir.toURI(), new Configuration());
        
        FsStatus fsStatus = fs.getStatus();
        
        // set the min remaining MB to something which will cause the 'small' directiory to be skipped
        long minRemainingMB = (fsStatus.getRemaining() / 0x100000L) + 4096l;
        
        List<IvaratorCacheDir> ivaratorCacheDirs = new ArrayList<>();
        ivaratorCacheDirs
                        .add(new IvaratorCacheDir(new IvaratorCacheDirConfig(smallDir.toURI().toString(), 0, minRemainingMB), fs, smallDir.toURI().toString()));
        ivaratorCacheDirs.add(new IvaratorCacheDir(new IvaratorCacheDirConfig(largeDir.toURI().toString()), fs, largeDir.toURI().toString()));
        
        String uniquePath = "blah";
        
        HdfsBackedSortedSet<String> sortedSet = new HdfsBackedSortedSet<>(ivaratorCacheDirs, uniquePath, 9999, 2);
        
        // Add an entry to the sorted set
        String someTestString = "some test string";
        sortedSet.add(someTestString);
        
        // persist the sorted set
        sortedSet.persist();
        
        Path smallPath = new Path(smallDir.toURI().toString());
        Path smallSubPath = new Path(smallPath, uniquePath);
        Path largePath = new Path(largeDir.toURI().toString());
        Path largeSubPath = new Path(largePath, uniquePath);
        
        // ensure that data was written to the large folder, not the small folder
        Assert.assertFalse(fs.exists(smallSubPath));
        Assert.assertEquals(0, fs.listStatus(smallPath).length);
        Assert.assertTrue(fs.exists(largeSubPath));
        
        FileStatus[] fileStatuses = fs.listStatus(largeSubPath);
        Assert.assertEquals(1, fileStatuses.length);
        Assert.assertTrue(fileStatuses[0].getPath().getName().startsWith("SortedSet"));
        
        // Now make sure reloading an ivarator cache dir works
        HdfsBackedSortedSet<String> reloadedSortedSet = new HdfsBackedSortedSet<>(ivaratorCacheDirs, uniquePath, 9999, 2);
        
        Assert.assertEquals(1, reloadedSortedSet.size());
        Assert.assertEquals(someTestString, reloadedSortedSet.first());
    }
    
    @Test
    public void persistCompactReloadTest() throws Exception {
        File tempDir = Files.createTempDir();
        tempDir.deleteOnExit();
        
        File[] dirs = new File[] {new File(tempDir, "first"), new File(tempDir, "second"), new File(tempDir, "third")};
        
        for (File dir : dirs)
            Assert.assertTrue(dir.mkdirs());
        
        String uniquePath = "blah";
        
        Path[] paths = Arrays.stream(dirs).map(dir -> new Path(dir.toURI().toString())).toArray(Path[]::new);
        Path[] subPaths = Arrays.stream(paths).map(path -> new Path(path, uniquePath)).toArray(Path[]::new);
        
        LocalFileSystem fs = new LocalFileSystem();
        fs.initialize(tempDir.toURI(), new Configuration());
        
        // set the min remaining percent to something which will cause the second directory to be skipped
        double minRemainingPercent = 1.0;
        
        List<IvaratorCacheDir> ivaratorCacheDirs = new ArrayList<>();
        for (File dir : dirs) {
            if (dir.getName().equalsIgnoreCase("second"))
                ivaratorCacheDirs.add(new IvaratorCacheDir(new IvaratorCacheDirConfig(dir.toURI().toString(), 0, minRemainingPercent), fs, dir.toURI()
                                .toString()));
            else
                ivaratorCacheDirs.add(new IvaratorCacheDir(new IvaratorCacheDirConfig(dir.toURI().toString(), 1), fs, dir.toURI().toString()));
        }
        
        HdfsBackedSortedSet<String> firstSortedSet = new HdfsBackedSortedSet<>(Collections.singletonList(ivaratorCacheDirs.get(0)), uniquePath, 9999, 2);
        
        // Add an entry to the first sorted set
        String someTestString = "some test string";
        firstSortedSet.add(someTestString);
        
        // persist the sorted set
        firstSortedSet.persist();
        
        HdfsBackedSortedSet<String> thirdSortedSet = new HdfsBackedSortedSet<>(Collections.singletonList(ivaratorCacheDirs.get(2)), uniquePath, 9999, 2);
        
        // Add an entry to the third sorted set
        String anotherTestString = "another test string";
        thirdSortedSet.add(anotherTestString);
        
        // persist the sorted set
        thirdSortedSet.persist();
        
        // ensure that data was written to the first and third folders
        Assert.assertTrue(fs.exists(subPaths[0]));
        Assert.assertTrue(fs.exists(subPaths[2]));
        
        // ensure that data was not written to the second folder
        Assert.assertFalse(fs.exists(subPaths[1]));
        Assert.assertEquals(0, fs.listStatus(paths[1]).length);
        
        // ensure that 1 file was written to the first folder
        FileStatus[] fileStatuses = fs.listStatus(subPaths[0]);
        Assert.assertEquals(1, fileStatuses.length);
        Assert.assertTrue(fileStatuses[0].getPath().getName().startsWith("SortedSet"));
        
        // ensure that 1 file was written to the third folder
        fileStatuses = fs.listStatus(subPaths[2]);
        Assert.assertEquals(1, fileStatuses.length);
        Assert.assertTrue(fileStatuses[0].getPath().getName().startsWith("SortedSet"));
        
        // Now make sure reloading an ivarator cache dir works, and set maxOpenFiles to 1 so that we compact during the next persist
        HdfsBackedSortedSet<String> reloadedSortedSet = new HdfsBackedSortedSet<>(ivaratorCacheDirs, uniquePath, 1, 2);
        
        // Ensure that we have 2 entries total
        Assert.assertEquals(2, reloadedSortedSet.size());
        
        // This is what we expect to be loaded by the set
        List<String> results = new ArrayList<>();
        results.add(someTestString);
        results.add(anotherTestString);
        
        // for each result we find, remove it from the results list and ensure that the list is empty when we're done
        reloadedSortedSet.iterator().forEachRemaining(results::remove);
        Assert.assertTrue(results.isEmpty());
        
        // Finally, add an entry to the reloaded sorted set
        String lastTestString = "last test string";
        reloadedSortedSet.add(lastTestString);
        
        // persist the sorted set (this should cause a compaction down to 1 file)
        reloadedSortedSet.persist();
        
        // ensure that data was not written to the second folder
        Assert.assertFalse(fs.exists(subPaths[1]));
        Assert.assertEquals(0, fs.listStatus(paths[1]).length);
        
        // ensure that while the folder still exists, data no longer exists for the third folder
        Assert.assertTrue(fs.exists(subPaths[2]));
        Assert.assertEquals(0, fs.listStatus(subPaths[2]).length);
        
        // ensure that all data exists in the first folder
        fileStatuses = fs.listStatus(subPaths[0]);
        Assert.assertEquals(1, fileStatuses.length);
        Assert.assertTrue(fileStatuses[0].getPath().getName().startsWith("SortedSet"));
        
        // Finally, make sure that the compacted data can be reloaded
        HdfsBackedSortedSet<String> compactedSortedSet = new HdfsBackedSortedSet<>(ivaratorCacheDirs, uniquePath, 9999, 2);
        
        // Ensure that we have 3 entries total
        Assert.assertEquals(3, compactedSortedSet.size());
        
        // This is what we expect to be loaded by the set
        results.clear();
        results.add(someTestString);
        results.add(anotherTestString);
        results.add(lastTestString);
        
        // for each result we find, remove it from the results list and ensure that the list is empty when we're done
        compactedSortedSet.iterator().forEachRemaining(results::remove);
        Assert.assertTrue(results.isEmpty());
    }
}
