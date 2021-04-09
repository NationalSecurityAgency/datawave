package datawave.common.io;

import com.google.common.io.Files;

import datawave.common.test.utils.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.StringJoiner;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import java.util.stream.Stream;

import static datawave.common.test.utils.FileUtils.createTemporaryFile;

public class FilesFinderTest {
    private static final String TEMP_DIR = Files.createTempDir().getAbsolutePath();
    private static final String CLASSPATH_DELIMITER = ":";
    private static final String FILE_PREFIX = "File";
    
    private static String TEST_LEVEL0_PROP_FILE;
    private static String TEST_LEVEL1_JAR_FILE;
    private static String TEST_LEVEL1_XML_FILE;
    private static String TEST_LEVEL2_XML_FILE;
    
    @BeforeClass
    public static void setup() throws IOException {
        TEST_LEVEL0_PROP_FILE = createTemporaryFile(TEMP_DIR, 0, FILE_PREFIX, "properties");
        TEST_LEVEL1_JAR_FILE = createTemporaryFile(TEMP_DIR, 1, FILE_PREFIX, "jar");
        TEST_LEVEL1_XML_FILE = createTemporaryFile(TEMP_DIR, 1, FILE_PREFIX, "xml");
        TEST_LEVEL2_XML_FILE = createTemporaryFile(TEMP_DIR, 2, FILE_PREFIX, "xml");
    }
    
    @AfterClass
    public static void tearDown() throws IOException {
        FileUtils.deletePath(TEMP_DIR);
    }
    
    @Test
    public void testGetFilesFromClasspath() {
        Collection<String> expectedFiles = Stream.of(TEST_LEVEL1_XML_FILE, TEST_LEVEL2_XML_FILE).collect(Collectors.toList());
        StringJoiner joiner = new StringJoiner(CLASSPATH_DELIMITER);
        expectedFiles.forEach(joiner::add);
        
        Collection<String> filesFound = FilesFinder.getFilesFromClasspath(joiner.toString(), CLASSPATH_DELIMITER);
        Assert.assertEquals(expectedFiles, filesFound);
    }
    
    @Test
    public void testGetFilesFromClasspathWithRelativePaths() {
        Collection<String> expectedFiles = Stream.of(TEST_LEVEL0_PROP_FILE, TEST_LEVEL1_JAR_FILE).collect(Collectors.toList());
        String baseDir = TEMP_DIR + "/1/2/";
        String testPaths = baseDir + "../../" + FILE_PREFIX + "0.properties:" + baseDir + "../../1/" + FILE_PREFIX + "1.jar";
        
        Collection<String> filesFound = FilesFinder.getFilesFromClasspath(testPaths, CLASSPATH_DELIMITER);
        Assert.assertEquals(expectedFiles, filesFound);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testGetFilesFromClasspathWithRelativePathFailures() {
        Collection<String> expectedFiles = Stream.of(TEST_LEVEL0_PROP_FILE, TEST_LEVEL1_JAR_FILE).collect(Collectors.toList());
        String testPaths = "../../" + FILE_PREFIX + "0.properties:../../1/" + FILE_PREFIX + "1.jar";
        
        Collection<String> filesFound = FilesFinder.getFilesFromClasspath(testPaths, CLASSPATH_DELIMITER);
        Assert.assertEquals(expectedFiles, filesFound);
    }
    
    @Test
    public void testGetFilesFromPattern() throws IOException {
        Collection<String> expectedFiles = Stream.of(TEST_LEVEL1_XML_FILE, TEST_LEVEL2_XML_FILE).collect(Collectors.toList());
        Collection<String> filesFound = FilesFinder.getFilesFromPattern(TEMP_DIR, "**.xml", Integer.MAX_VALUE);
        Assert.assertEquals(expectedFiles, filesFound);
    }
    
    @Test
    public void testGetFilesFromPatternByDepth() throws IOException {
        Collection<String> expectedFiles = Stream.of(TEST_LEVEL1_XML_FILE).collect(Collectors.toList());
        Collection<String> filesFound = FilesFinder.getFilesFromPattern(TEMP_DIR, "**.xml", 2);
        Assert.assertEquals(expectedFiles, filesFound);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testGetFilesFromPatternWithNonDirectory() throws IOException {
        FilesFinder.getFilesFromPattern("File.out", "**", Integer.MAX_VALUE);
    }
    
    @Test
    public void testArtifactsInDirectoryThatMatchPattern() throws IOException {
        Collection<String> expectedFiles = Stream.of("1").collect(Collectors.toList());
        
        FileSystem fileSystem = FileSystem.getLocal(new Configuration());
        Pattern numDirPattern = Pattern.compile("\\d{1}");
        FileStatus[] fileStatuses = FilesFinder.listPath(fileSystem, new Path(TEMP_DIR), numDirPattern);
        
        // @formatter:off
        Collection<String> filesFound = Arrays
                .stream(fileStatuses)
                .map(FileStatus::getPath)
                .map(Path::getName)
                .collect(Collectors.toList());
        // @formatter: on

        Assert.assertEquals(expectedFiles, filesFound);
    }
}
