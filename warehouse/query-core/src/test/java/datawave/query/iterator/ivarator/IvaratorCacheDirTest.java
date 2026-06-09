package datawave.query.iterator.ivarator;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Path;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class IvaratorCacheDirTest {

    @TempDir
    public Path tempDir;

    @Test
    public void testDirectoryValidation() throws Exception {

        File dir = new File(tempDir.toUri());
        IvaratorCacheDirConfig config = new IvaratorCacheDirConfig(dir.toURI().toString());

        FileSystem fs = new LocalFileSystem();
        fs.initialize(dir.toURI(), new Configuration());

        String pathURI = dir.getPath();
        IvaratorCacheDir cacheDir = new IvaratorCacheDir(config, fs, pathURI);

        boolean valid = cacheDir.validateFS();
        assertTrue(valid);
    }
}
