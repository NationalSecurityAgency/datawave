package datawave.ingest.util.cache;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import datawave.common.io.FilesFinder;
import java.io.File;
import java.io.IOException;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static datawave.common.test.utils.FileUtils.createTemporaryFile;
import static datawave.common.test.utils.FileUtils.deletePath;
import static datawave.ingest.util.cache.LoadJobCache.getDeleteCacheConsumer;
import static datawave.ingest.util.cache.LoadJobCache.getLoadCacheConsumer;
import static datawave.ingest.util.cache.LoadJobCacheLauncher.JOB_CACHE_FORMATER;

public class LoadJobCacheTest {
    private static final File TEMP_DIR = Files.createTempDir();
    private static final Map<org.apache.hadoop.fs.Path,FileSystem> PATH_TO_FS = Maps.newHashMap();
    private static final String JOB_CACHE_DIR = "jobCache_" + JOB_CACHE_FORMATER.format(LocalDateTime.now());
    private static final String FILE_PREFIX = "File";
    private static Collection<String> FILES_TO_LOAD = Lists.newArrayList();
    
    @BeforeClass
    public static void setupClass() throws IOException {
        // @formatter: off
        FILES_TO_LOAD = Stream
                        .of(createTemporaryFile(TEMP_DIR.getAbsolutePath(), 0, FILE_PREFIX, "properties"),
                                        createTemporaryFile(TEMP_DIR.getAbsolutePath(), 1, FILE_PREFIX, "jar"),
                                        createTemporaryFile(TEMP_DIR.getAbsolutePath(), 1, FILE_PREFIX, "xml"),
                                        createTemporaryFile(TEMP_DIR.getAbsolutePath(), 2, FILE_PREFIX, "xml")).map(File::getAbsolutePath)
                        .collect(Collectors.toList());
        // @formatter: on
        
        PATH_TO_FS.put(new org.apache.hadoop.fs.Path(TEMP_DIR.getCanonicalPath() + File.separator + "output1"), FileSystem.get(new Configuration()));
        PATH_TO_FS.put(new org.apache.hadoop.fs.Path(TEMP_DIR.getCanonicalPath() + File.separator + "output2"), FileSystem.get(new Configuration()));
    }
    
    @AfterClass
    public static void tearDownClass() throws IOException {
        deletePath(TEMP_DIR.getAbsolutePath());
    }
    
    @After
    public void tearDown() throws IOException {
        for (org.apache.hadoop.fs.Path delPath : PATH_TO_FS.keySet()) {
            deletePath(delPath.toString());
        }
    }
    
    @Test
    public void testLoadJobCache() throws IOException {
        LoadJobCache.load(PATH_TO_FS, FILES_TO_LOAD, true, (short) 3, 2, JOB_CACHE_DIR, "");
        verifyResults(getOutputPaths(""));
    }
    
    @Test
    public void testLoadJobCacheWithSubDir() throws IOException {
        LoadJobCache.load(PATH_TO_FS, FILES_TO_LOAD, true, (short) 3, 2, JOB_CACHE_DIR, "subDir");
        verifyResults(getOutputPaths("subDir"));
    }
    
    @Test
    public void testCacheCleanupOnError() {
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        try {
            LoadJobCache.load(PATH_TO_FS, true, getLoadCacheConsumer(FILES_TO_LOAD, executorService, JOB_CACHE_DIR, "", (short) 3),
                            getFinalizerThatThrowsError(), getDeleteCacheConsumer(JOB_CACHE_DIR));
        } finally {
            executorService.shutdown();
        }
        Assert.assertTrue(getOutputPaths("").stream().map(File::new).noneMatch(File::exists));
    }
    
    private static Consumer<Map.Entry<org.apache.hadoop.fs.Path,FileSystem>> getFinalizerThatThrowsError() {
        return (Map.Entry<org.apache.hadoop.fs.Path,FileSystem> entrySet) -> {
            throw new RuntimeException("Test exception check");
        };
    }
    
    private void verifyResults(Collection<String> outputPaths) throws IOException {
        for (String outputPath : outputPaths) {
            Collection<String> filesFound = FilesFinder.getFilesFromPattern(outputPath, "**", Integer.MAX_VALUE);
            Collection<String> filesExpected = getExpectedOutput(outputPath);
            Assert.assertEquals(filesExpected, filesFound);
        }
    }
    
    private Collection<String> getExpectedOutput(String outputPath) {
        return FILES_TO_LOAD.stream().map(path -> path.substring(path.lastIndexOf("/"))).map(fileName -> new File(outputPath, fileName))
                        .map(File::getAbsolutePath).collect(Collectors.toList());
    }
    
    private Collection<String> getOutputPaths(String optionalSubDir) {
        return PATH_TO_FS.keySet().stream().map(outputPath -> outputPath + "/" + JOB_CACHE_DIR + "/" + optionalSubDir).collect(Collectors.toList());
    }
}
