package datawave.ingest.util.cache.load;

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import datawave.common.io.FilesFinder;
import java.io.File;
import java.io.IOException;

import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import datawave.ingest.util.cache.path.FileSystemPath;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static datawave.common.test.utils.FileUtils.createTemporaryFile;
import static datawave.common.test.utils.FileUtils.deletePath;
import static datawave.ingest.util.cache.load.LoadJobCache.getDeleteCacheConsumer;
import static datawave.ingest.util.cache.load.LoadJobCache.getLoadCacheConsumer;

public class LoadJobCacheTest {
    private static final File TEMP_DIR = Files.createTempDir();
    private static final String JOB_CACHE_DIR = LoadJobCache.getJobCacheTimestampDir("jobCache_");
    private static final String FILE_PREFIX = "File";
    
    private static Collection<FileSystemPath> CACHE_PATHS = Lists.newArrayList();
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
        
        Collection<Configuration> confs = Stream.of(new Configuration()).collect(Collectors.toList());
        
        CACHE_PATHS = Stream
                        .of(new org.apache.hadoop.fs.Path(TEMP_DIR.getAbsolutePath() + File.separator + "output1"),
                                        new org.apache.hadoop.fs.Path(TEMP_DIR.getAbsolutePath() + File.separator + "output2"))
                        .map(path -> new FileSystemPath(path, confs)).collect(Collectors.toList());
        // @formatter: on
    }
    
    @AfterClass
    public static void tearDownClass() throws IOException {
        deletePath(TEMP_DIR.getAbsolutePath());
    }
    
    @After
    public void tearDown() throws IOException {
        for (FileSystemPath delPath : CACHE_PATHS) {
            deletePath(delPath.getOutputPath().toString());
        }
    }
    
    @Test
    public void testLoadJobCache() throws IOException {
        LoadJobCache.load(CACHE_PATHS, FILES_TO_LOAD, true, (short) 3, 2, JOB_CACHE_DIR, "");
        verifyResults(getOutputPaths(""));
    }
    
    @Test
    public void testLoadJobCacheWithSubDir() throws IOException {
        LoadJobCache.load(CACHE_PATHS, FILES_TO_LOAD, true, (short) 3, 2, JOB_CACHE_DIR, "subDir");
        verifyResults(getOutputPaths("subDir"));
    }
    
    @Test
    public void testCacheCleanupOnError() {
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        try {
            LoadJobCache.load(CACHE_PATHS, true, getLoadCacheConsumer(FILES_TO_LOAD, executorService, JOB_CACHE_DIR, "", (short) 3),
                            getFinalizerThatThrowsError(), getDeleteCacheConsumer(JOB_CACHE_DIR));
        } finally {
            executorService.shutdown();
        }
        Assert.assertTrue(getOutputPaths("").stream().map(File::new).noneMatch(File::exists));
    }
    
    private static Consumer<FileSystemPath> getFinalizerThatThrowsError() {
        return (FileSystemPath fsPath) -> {
            throw new RuntimeException("Test exception check");
        };
    }
    
    private void verifyResults(Collection<String> outputPaths) throws IOException {
        for (String outputPath : outputPaths) {
            // @formatter:off
            Collection<String> filesFound =
                    FilesFinder.getFilesFromPattern(outputPath, "**", Integer.MAX_VALUE)
                    .stream()
                    .sorted()
                    .collect(Collectors.toList());
            // @formatter:on
            Collection<String> filesExpected = getExpectedOutput(outputPath);
            Assert.assertEquals(filesExpected, filesFound);
        }
    }
    
    private Collection<String> getExpectedOutput(String outputPath) {
        // @formatter:off
        return FILES_TO_LOAD
                .stream()
                .map(path -> path.substring(path.lastIndexOf("/")))
                .map(fileName -> new File(outputPath, fileName))
                .map(File::getAbsolutePath)
                .collect(Collectors.toList());
        // @formatter:on
    }
    
    private Collection<String> getOutputPaths(String optionalSubDir) {
        // @formatter:off
        return CACHE_PATHS
                .stream()
                .map(FileSystemPath::getOutputPath)
                .map(outputPath -> outputPath + "/" + JOB_CACHE_DIR + "/" + optionalSubDir)
                .collect(Collectors.toList());
        // @formatter:on
    }
}
