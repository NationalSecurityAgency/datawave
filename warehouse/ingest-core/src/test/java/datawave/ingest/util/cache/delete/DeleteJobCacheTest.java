package datawave.ingest.util.cache.delete;

import com.google.common.io.Files;
import datawave.common.test.utils.FileUtils;
import datawave.ingest.util.cache.JobCacheFactory;
import datawave.ingest.util.cache.lease.JobCacheNoOpLockFactory;
import datawave.ingest.util.cache.path.FileSystemPath;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collection;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static datawave.common.test.utils.FileUtils.createTemporaryFile;

public class DeleteJobCacheTest {
    private static final String TEMP_DIR = Files.createTempDir().getAbsolutePath();
    
    public static final String JOB_CACHE_TIMESTAMP_FORMAT = "yyyyMMddHHmmssSSS";
    public static final DateTimeFormatter JOB_CACHE_FORMATER = DateTimeFormatter.ofPattern(JOB_CACHE_TIMESTAMP_FORMAT);
    public static final String JOB_CACHE_PREFIX = "jobCache_";
    public static final Pattern JOB_CACHE_PATTERN = Pattern.compile(JOB_CACHE_PREFIX + "\\d{" + JOB_CACHE_TIMESTAMP_FORMAT.length() + "}");
    
    private static String CACHE_TIMESTAMP_DIR_A;
    private static String CACHE_TIMESTAMP_DIR_B;
    private static String CACHE_TIMESTAMP_DIR_C;
    private static String CACHE_TIMESTAMP_DIR_D;
    
    @BeforeClass
    public static void setup() throws IOException, InterruptedException {
        CACHE_TIMESTAMP_DIR_A = getTimeStampDir();
        CACHE_TIMESTAMP_DIR_B = getTimeStampDir();
        CACHE_TIMESTAMP_DIR_C = getTimeStampDir();
        CACHE_TIMESTAMP_DIR_D = getTimeStampDir();
    }
    
    @AfterClass
    public static void tearDown() throws IOException {
        FileUtils.deletePath(TEMP_DIR);
    }
    
    private static String getTimeStampDir() throws IOException, InterruptedException {
        // @formatter:off
        String jarFile =  new StringJoiner(File.separator)
                .add(TEMP_DIR)
                .add(JOB_CACHE_PREFIX+JOB_CACHE_FORMATER.format(LocalDateTime.now()))
                .add("File.jar")
                .toString();
        // @formatter:on
        
        TimeUnit.MILLISECONDS.sleep(10);
        return new File(createTemporaryFile(jarFile)).getParent();
    }
    
    @Test
    public void shouldGivePriorityToKeepList() {
        Collection<Path> deletePaths = convertToPaths(CACHE_TIMESTAMP_DIR_C, CACHE_TIMESTAMP_DIR_D);
        Collection<Path> keepPaths = convertToPaths(CACHE_TIMESTAMP_DIR_D);
        Collection<Path> expectedCandidates = convertToPaths(CACHE_TIMESTAMP_DIR_C);
        
        Collection<Path> deletionCandidates = DeleteJobCache.getDeletionCandidates(deletePaths, keepPaths);
        Assert.assertEquals(expectedCandidates, deletionCandidates);
    }
    
    @Test
    public void shouldDeleteCacheCandidates() throws IOException {
        Collection<FileSystemPath> expectedDeletedCaches = convertToFileSystemPaths(CACHE_TIMESTAMP_DIR_B, CACHE_TIMESTAMP_DIR_A);
        Collection<FileSystemPath> expectedRemainingCaches = convertToFileSystemPaths(CACHE_TIMESTAMP_DIR_D, CACHE_TIMESTAMP_DIR_C);
        FileSystemPath jobCachePath = new FileSystemPath(FileSystem.getLocal(new Configuration()), new Path(TEMP_DIR));
        
        Collection<FileSystemPath> deletionCandidates = JobCacheFactory.getCacheCandidates(jobCachePath, JOB_CACHE_PATTERN, 2);
        Assert.assertEquals(deletionCandidates, expectedDeletedCaches);
        
        DeleteJobCache.deleteCacheIfNotActive(deletionCandidates, new JobCacheNoOpLockFactory());
        Assert.assertTrue(expectedDeletedCaches.stream().allMatch(this::cacheDoesNotExist));
        
        Collection<FileSystemPath> remainingCaches = JobCacheFactory.getCacheCandidates(jobCachePath, JOB_CACHE_PATTERN, 0);
        Assert.assertEquals(expectedRemainingCaches, remainingCaches);
    }
    
    private boolean cacheDoesNotExist(FileSystemPath fileSystemPath) {
        FileSystem fileSystem = fileSystemPath.getFileSystem();
        Path cachePath = fileSystemPath.getOutputPath();
        
        try {
            return !fileSystem.exists(cachePath);
        } catch (IOException e) {
            return false;
        }
    }
    
    private Collection<FileSystemPath> convertToFileSystemPaths(String... paths) throws IOException {
        FileSystem fileSystem = FileSystem.getLocal(new Configuration());
        // @formatter:off
        return Arrays.stream(paths)
                .map(Path::new)
                .map(path -> new FileSystemPath(fileSystem, path))
                .collect(Collectors.toList());
        // @formatter:on
    }
    
    private Collection<Path> convertToPaths(String... paths) {
        // @formatter:off
        return Arrays.stream(paths)
                .map(Path::new)
                .collect(Collectors.toList());
        // @formatter:on
    }
}
