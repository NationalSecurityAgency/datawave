package datawave.common.io;

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HadoopFileSystemUtilsTest {
    private static final Configuration TEST_CONF_A = getConfiguration("A");
    private static final Configuration TEST_CONF_B = getConfiguration("B");
    private static final Path TEST_PATH_A = new HadoopTestPath("A/file/path");
    private static final Path TEST_PATH_B = new HadoopTestPath("B/file/path");
    private static final String HADOOP_TEST_SCHEME_KEY = "SCHEME";
    
    private static final Map<Configuration,FileSystem> CONF_TO_FS = Maps.newHashMap();
    
    @BeforeClass
    public static void setup() throws IOException {
        CONF_TO_FS.put(TEST_CONF_A, FileSystem.get(TEST_CONF_A));
        CONF_TO_FS.put(TEST_CONF_B, FileSystem.get(TEST_CONF_B));
    }
    
    @AfterClass
    public static void tearDown() throws IOException {
        for (FileSystem fs : CONF_TO_FS.values()) {
            fs.close();
        }
    }
    
    /**
     * Create a test configuration with a scheme key/value pair
     *
     * @param testScheme
     *            A scheme value to simulate a different hdfs file system
     * @return A configuration
     */
    private static Configuration getConfiguration(String testScheme) {
        Configuration conf = new Configuration();
        conf.set(HADOOP_TEST_SCHEME_KEY, testScheme);
        return conf;
    }
    
    @Test
    public void testWhenPathsFindAMatchingFileSystem() {
        Collection<Configuration> testConfs = Stream.of(TEST_CONF_A, TEST_CONF_B).collect(Collectors.toList());
        
        // @formatter:off
        Assert.assertTrue(Stream.of(TEST_PATH_B, TEST_PATH_A)
                .map(path -> HadoopFileSystemUtils.getFileSystem(testConfs, path))
                .allMatch(Optional::isPresent));
        // @formatter:on
        
    }
    
    @Test
    public void testNoFileSystemFoundForPath() {
        Collection<Configuration> testConfs = Stream.of(TEST_CONF_A).collect(Collectors.toList());
        
        // @formatter:off
        Assert.assertTrue(Stream.of(TEST_PATH_B)
                .map(path -> HadoopFileSystemUtils.getFileSystem(testConfs, path))
                .noneMatch(Optional::isPresent));
        // @formatter:on
    }
    
    /** A test helper class to simulate assigning filesystem objects by scheme of output path */
    private static class HadoopTestPath extends Path {
        private static final String UNKNOWN_SCHEME = "UNKNOWN";
        private final String scheme;
        
        public HadoopTestPath(String pathString) throws IllegalArgumentException {
            super(pathString);
            this.scheme = pathString.substring(0, pathString.indexOf("/"));
        }
        
        @Override
        public FileSystem getFileSystem(Configuration conf) throws IOException {
            String confType = conf.get(HADOOP_TEST_SCHEME_KEY, UNKNOWN_SCHEME);
            if (!confType.equals(scheme)) {
                throw new IOException("No fileSystem found with scheme");
            }
            return CONF_TO_FS.get(conf);
        }
    }
}
