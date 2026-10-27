package datawave.query.util.sortedmap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import datawave.query.iterator.ivarator.IvaratorCacheDir;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import datawave.query.util.sortedset.FileSortedSet.PersistOptions;

public class HdfsBackedSortedMapTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void persistReloadTest() throws Exception {
        File tempDir = temporaryFolder.newFolder();

        File smallDir = new File(tempDir, "small");
        assertTrue(smallDir.mkdirs());

        File largeDir = new File(tempDir, "large");
        assertTrue(largeDir.mkdirs());

        LocalFileSystem fs = new LocalFileSystem();
        fs.initialize(tempDir.toURI(), new Configuration());

        FsStatus fsStatus = fs.getStatus();

        // set the min remaining MB to something which will cause the 'small' directory to be skipped
        long minRemainingMB = (fsStatus.getRemaining() / 0x100000L) + 4096L;

        List<IvaratorCacheDir> ivaratorCacheDirs = new ArrayList<>();
        ivaratorCacheDirs
                        .add(new IvaratorCacheDir(new IvaratorCacheDirConfig(smallDir.toURI().toString(), 0, minRemainingMB), fs, smallDir.toURI().toString()));
        ivaratorCacheDirs.add(new IvaratorCacheDir(new IvaratorCacheDirConfig(largeDir.toURI().toString()), fs, largeDir.toURI().toString()));

        String uniquePath = "blah";

        // @formatter:off
        @SuppressWarnings("unchecked")
        HdfsBackedSortedMap<String, String> sortedMap = (HdfsBackedSortedMap<String, String>) HdfsBackedSortedMap.builder()
                .withIvaratorCacheDirs(ivaratorCacheDirs)
                .withUniqueSubPath(uniquePath)
                .withMaxOpenFiles(9999)
                .withNumRetries(2)
                .withPersistOptions(new PersistOptions())
                .build();
        // @formatter:on

        // Add an entry to the sorted set
        String someTestString = "some test string";
        sortedMap.put("key", someTestString);

        // persist the sorted set
        sortedMap.persist();

        Path smallPath = new Path(smallDir.toURI().toString());
        Path smallSubPath = new Path(smallPath, uniquePath);
        Path largePath = new Path(largeDir.toURI().toString());
        Path largeSubPath = new Path(largePath, uniquePath);

        // ensure that data was written to the large folder, not the small folder
        assertFalse(fs.exists(smallSubPath));
        assertEquals(0, fs.listStatus(smallPath).length);
        assertTrue(fs.exists(largeSubPath));

        FileStatus[] fileStatuses = fs.listStatus(largeSubPath);
        assertEquals(1, fileStatuses.length);
        assertTrue(fileStatuses[0].getPath().getName().startsWith("SortedMap"));

        // Now make sure reloading an ivarator cache dir works
        // @formatter:off
        @SuppressWarnings("unchecked")
        HdfsBackedSortedMap<String, String> reloadedSortedMap = (HdfsBackedSortedMap<String, String>) HdfsBackedSortedMap.builder()
                .withIvaratorCacheDirs(ivaratorCacheDirs)
                .withUniqueSubPath(uniquePath)
                .withMaxOpenFiles(9999)
                .withNumRetries(2)
                .withPersistOptions(new PersistOptions())
                .build();
        // @formatter:on

        assertEquals(1, reloadedSortedMap.size());
        assertEquals(someTestString, reloadedSortedMap.get("key"));
    }

    @Test
    public void persistCompactReloadTest() throws Exception {
        File tempDir = temporaryFolder.newFolder();

        File[] dirs = new File[] {new File(tempDir, "first"), new File(tempDir, "second"), new File(tempDir, "third")};

        for (File dir : dirs)
            assertTrue(dir.mkdirs());

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
                ivaratorCacheDirs.add(
                                new IvaratorCacheDir(new IvaratorCacheDirConfig(dir.toURI().toString(), 0, minRemainingPercent), fs, dir.toURI().toString()));
            else
                ivaratorCacheDirs.add(new IvaratorCacheDir(new IvaratorCacheDirConfig(dir.toURI().toString(), 1), fs, dir.toURI().toString()));
        }

        // @formatter:off
        @SuppressWarnings("unchecked")
        HdfsBackedSortedMap<String, String> firstSortedMap = (HdfsBackedSortedMap<String, String>) HdfsBackedSortedMap.builder()
                .withIvaratorCacheDirs(Collections.singletonList(ivaratorCacheDirs.get(0)))
                .withUniqueSubPath(uniquePath)
                .withMaxOpenFiles(9999)
                .withNumRetries(2)
                .withPersistOptions(new PersistOptions())
                .build();
        // @formatter:on

        // Add an entry to the first sorted set
        String someTestString = "some test string";
        firstSortedMap.put("key1", someTestString);

        // persist the sorted set
        firstSortedMap.persist();

        // @formatter:off
        @SuppressWarnings("unchecked")
        HdfsBackedSortedMap<String, String> thirdSortedMap = (HdfsBackedSortedMap<String, String>) HdfsBackedSortedMap.builder()
                .withIvaratorCacheDirs(Collections.singletonList(ivaratorCacheDirs.get(2)))
                .withUniqueSubPath(uniquePath)
                .withMaxOpenFiles(9999)
                .withNumRetries(2)
                .withPersistOptions(new PersistOptions())
                .build();
        // @formatter:on

        // Add an entry to the third sorted set
        String anotherTestString = "another test string";
        thirdSortedMap.put("key2", anotherTestString);

        // persist the sorted set
        thirdSortedMap.persist();

        // ensure that data was written to the first and third folders
        assertTrue(fs.exists(subPaths[0]));
        assertTrue(fs.exists(subPaths[2]));

        // ensure that data was not written to the second folder
        assertFalse(fs.exists(subPaths[1]));
        assertEquals(0, fs.listStatus(paths[1]).length);

        // ensure that 1 file was written to the first folder
        FileStatus[] fileStatuses = fs.listStatus(subPaths[0]);
        assertEquals(1, fileStatuses.length);
        assertTrue(fileStatuses[0].getPath().getName().startsWith("SortedMap"));

        // ensure that 1 file was written to the third folder
        fileStatuses = fs.listStatus(subPaths[2]);
        assertEquals(1, fileStatuses.length);
        assertTrue(fileStatuses[0].getPath().getName().startsWith("SortedMap"));

        // Now make sure reloading an ivarator cache dir works, and set maxOpenFiles to 1 so that we compact during the next persist
        // @formatter:off
        @SuppressWarnings("unchecked")
        HdfsBackedSortedMap<String, String> reloadedSortedMap = (HdfsBackedSortedMap<String, String>) HdfsBackedSortedMap.builder()
                .withIvaratorCacheDirs(ivaratorCacheDirs)
                .withUniqueSubPath(uniquePath)
                .withMaxOpenFiles(1)
                .withNumRetries(2)
                .withPersistOptions(new PersistOptions())
                .build();
        // @formatter:on

        // Ensure that we have 2 entries total
        assertEquals(2, reloadedSortedMap.size());

        // This is what we expect to be loaded by the set
        List<Map.Entry<String,String>> results = new ArrayList<>();
        results.add(Map.entry("key1", someTestString));
        results.add(Map.entry("key2", anotherTestString));

        // for each result we find, remove it from the results list and ensure that the list is empty when we're done
        reloadedSortedMap.entrySet().forEach(results::remove);
        assertTrue(results.isEmpty());

        // Finally, add an entry to the reloaded sorted set
        String lastTestString = "last test string";
        reloadedSortedMap.put("key3", lastTestString);

        // persist the sorted set (this should cause a compaction down to 1 file)
        reloadedSortedMap.persist();

        // ensure that data was not written to the second folder
        assertFalse(fs.exists(subPaths[1]));
        assertEquals(0, fs.listStatus(paths[1]).length);

        // ensure that while the folder still exists, data no longer exists for the third folder
        assertTrue(fs.exists(subPaths[2]));
        assertEquals(0, fs.listStatus(subPaths[2]).length);

        // ensure that all data exists in the first folder
        fileStatuses = fs.listStatus(subPaths[0]);
        assertEquals(1, fileStatuses.length);
        assertTrue(fileStatuses[0].getPath().getName().startsWith("SortedMap"));

        // Finally, make sure that the compacted data can be reloaded
        // @formatter:off
        @SuppressWarnings("unchecked")
        HdfsBackedSortedMap<String, String> compactedSortedMap = (HdfsBackedSortedMap<String, String>) HdfsBackedSortedMap.builder()
                .withIvaratorCacheDirs(ivaratorCacheDirs)
                .withUniqueSubPath(uniquePath)
                .withMaxOpenFiles(9999)
                .withNumRetries(2)
                .withPersistOptions(new PersistOptions())
                .build();
        // @formatter:on

        // Ensure that we have 3 entries total
        assertEquals(3, compactedSortedMap.size());

        // This is what we expect to be loaded by the set
        results.clear();
        results.add(Map.entry("key1", someTestString));
        results.add(Map.entry("key2", anotherTestString));
        results.add(Map.entry("key3", lastTestString));

        // for each result we find, remove it from the results list and ensure that the list is empty when we're done
        compactedSortedMap.entrySet().forEach(results::remove);
        assertTrue(results.isEmpty());
    }
}
