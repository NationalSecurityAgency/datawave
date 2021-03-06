package datawave.common.io;

import com.google.common.io.Files;

import datawave.common.test.utils.FileUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Comparator;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static datawave.common.test.utils.FileUtils.createTemporaryFile;

public class FilesFinderTest {
    private static final File TEMP_DIR = Files.createTempDir();
    private static final String COLON_DELIMITER = ":";
    private static final String TEST_PATH_ENV_VAR = "TEST_PATH_ENV_VAR";
    private static final String FILE_PREFIX = "File";
    
    private static File TEST_LEVEL0_PROP_FILE;
    private static File TEST_LEVEL1_JAR_FILE;
    private static File TEST_LEVEL1_XML_FILE;
    private static File TEST_LEVEL2_XML_FILE;
    
    @BeforeClass
    public static void setup() throws IOException {
        TEST_LEVEL0_PROP_FILE = createTemporaryFile(TEMP_DIR.getAbsolutePath(), 0, FILE_PREFIX, "properties");
        TEST_LEVEL1_JAR_FILE = createTemporaryFile(TEMP_DIR.getAbsolutePath(), 1, FILE_PREFIX, "jar");
        TEST_LEVEL1_XML_FILE = createTemporaryFile(TEMP_DIR.getAbsolutePath(), 1, FILE_PREFIX, "xml");
        TEST_LEVEL2_XML_FILE = createTemporaryFile(TEMP_DIR.getAbsolutePath(), 2, FILE_PREFIX, "xml");
    }
    
    @AfterClass
    public static void tearDown() throws IOException {
        FileUtils.deletePath(TEMP_DIR.getAbsolutePath());
    }
    
    @Rule
    public final EnvironmentVariables environmentVariables = new EnvironmentVariables();
    
    @Test
    public void testGetFilesFromEnvironment() {
        Collection<String> expectedFiles = Stream.of(TEST_LEVEL1_XML_FILE.getAbsolutePath(), TEST_LEVEL2_XML_FILE.getAbsolutePath()).collect(
                        Collectors.toList());
        StringJoiner joiner = new StringJoiner(COLON_DELIMITER);
        
        expectedFiles.forEach(joiner::add);
        environmentVariables.set(TEST_PATH_ENV_VAR, joiner.toString());
        
        Collection<String> filesFound = FilesFinder.getFilesFromEnvironment(TEST_PATH_ENV_VAR, "", COLON_DELIMITER);
        Assert.assertEquals(expectedFiles, filesFound);
    }
    
    @Test
    public void testGetFilesFromEnvironmentWithRelativePaths() {
        Collection<String> expectedFiles = Stream.of(TEST_LEVEL0_PROP_FILE.getAbsolutePath(), TEST_LEVEL1_JAR_FILE.getAbsolutePath()).collect(
                        Collectors.toList());
        String baseDir = TEMP_DIR.getAbsolutePath() + "/1/2";
        String testPaths = "../../" + FILE_PREFIX + "0.properties:../../1/" + FILE_PREFIX + "1.jar";
        
        environmentVariables.set(TEST_PATH_ENV_VAR, testPaths);
        
        Collection<String> filesFound = FilesFinder.getFilesFromEnvironment(TEST_PATH_ENV_VAR, baseDir, COLON_DELIMITER);
        Assert.assertEquals(expectedFiles, filesFound);
    }
    
    @Test
    public void testGetFilesFromPatternByPattern() throws IOException {
        Collection<String> expectedFiles = Stream.of(TEST_LEVEL1_XML_FILE.getAbsolutePath(), TEST_LEVEL2_XML_FILE.getAbsolutePath()).collect(
                        Collectors.toList());
        Collection<String> filesFound = FilesFinder.getFilesFromPattern(TEMP_DIR.getAbsolutePath(), "**.xml", Integer.MAX_VALUE);
        Assert.assertEquals(expectedFiles, filesFound);
    }
    
    @Test
    public void testGetFilesFromPatternByDepth() throws IOException {
        Collection<String> expectedFiles = Stream.of(TEST_LEVEL1_XML_FILE.getAbsolutePath()).collect(Collectors.toList());
        Collection<String> filesFound = FilesFinder.getFilesFromPattern(TEMP_DIR.getAbsolutePath(), "**.xml", 2);
        Assert.assertEquals(expectedFiles, filesFound);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testGetFilesFromPatternWithNonDirectory() throws IOException {
        FilesFinder.getFilesFromPattern("File.out", "**", Integer.MAX_VALUE);
    }
    
}
