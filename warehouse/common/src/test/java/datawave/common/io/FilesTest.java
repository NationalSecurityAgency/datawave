package datawave.common.io;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.IOException;

import org.junit.jupiter.api.Test;

public class FilesTest {

    @Test
    public void testEnsureDirException() throws Exception {
        File file = mock(File.class, "test-file");
        doReturn(false).when(file).exists();
        doReturn(false).when(file).mkdirs();
        assertThrows(IllegalStateException.class, () -> Files.ensureDir(file));
    }

    @Test
    public void testEnsureDirString() throws Exception {
        Files.ensureDir("file");
    }

    @Test
    public void testCheckDirString() throws Exception {
        String ret = Files.checkDir("file");
        assertNull(ret);
    }

    @Test
    public void testCheckDirNoDir() throws Exception {
        File file = mock(File.class, "test-file");
        doReturn(false).when(file).exists();
        doReturn(false).when(file).mkdirs();
        String err = Files.checkDir(file);
        assertEquals(err, "Directory, 'test-file' does not exist; and cannot be created");
    }

    @Test
    public void testCheckDirIsDirectory() throws Exception {
        File file = mock(File.class, "test-file");
        doReturn(true).when(file).exists();
        doReturn(false).when(file).isDirectory();
        String err = Files.checkDir(file);
        assertEquals(err, "File, 'test-file' is not a directory.");
    }

    @Test
    public void testCheckDirCanRead() throws Exception {
        File file = mock(File.class, "test-file");
        doReturn(true).when(file).exists();
        doReturn(true).when(file).isDirectory();
        doReturn(false).when(file).canRead();
        String err = Files.checkDir(file);
        assertEquals(err, "Directory, 'test-file' cannot be read.");
    }

    @Test
    public void testCheckDirCanExecute() throws Exception {
        File file = mock(File.class, "test-file");
        doReturn(true).when(file).exists();
        doReturn(true).when(file).isDirectory();
        doReturn(true).when(file).canRead();
        doReturn(false).when(file).canExecute();
        String err = Files.checkDir(file);
        assertEquals(err, "Directory contents for 'test-file' cannot be listed.");
    }

    @Test
    public void testCheckDirCheckWrite() throws Exception {
        File file = mock(File.class, "test-file");
        doReturn(true).when(file).exists();
        doReturn(true).when(file).isDirectory();
        doReturn(true).when(file).canRead();
        doReturn(true).when(file).canExecute();
        doReturn(false).when(file).canWrite();
        String err = Files.checkDir(file, true);
        assertEquals(err, "Directory, 'test-file' cannot be written to.");
    }

    @Test
    public void testCheckDirNull() throws Exception {
        File file = null;
        assertThrows(IllegalArgumentException.class, () -> Files.checkDir(file));
    }

    @Test
    public void testCheckFileExists() throws Exception {
        File file = mock(File.class, "test-file");
        doReturn(false).when(file).exists();
        String err = Files.checkFile(file, true);
        assertEquals(err, "File, 'test-file' does not exist.");
    }

    @Test
    public void testCheckFileIsNotDirectory() throws Exception {
        File file = mock(File.class, "test-file");
        doReturn(true).when(file).exists();
        doReturn(true).when(file).isDirectory();
        String err = Files.checkFile(file, true);
        assertEquals(err, "Directory, 'test-file' is not a file.");
    }

    @Test
    public void testCheckFileCanRead() throws Exception {
        File file = mock(File.class, "test-file");
        doReturn(true).when(file).exists();
        doReturn(false).when(file).isDirectory();
        doReturn(false).when(file).canRead();
        String err = Files.checkFile(file, false);
        assertEquals(err, "File, 'test-file' cannot be read.");
    }

    @Test
    public void testCheckFileWritable() throws Exception {
        File file = mock(File.class, "test-file");
        doReturn(true).when(file).exists();
        doReturn(false).when(file).isDirectory();
        doReturn(true).when(file).canRead();
        doReturn(false).when(file).canWrite();
        String err = Files.checkFile(file, true);
        assertEquals(err, "File, 'test-file' cannot be written to.");
    }

    @Test
    public void testEnsureMvIOException() throws Exception {
        File src = mock(File.class, "test-file");
        File dest = mock(File.class, "test-file");
        File parent = mock(File.class, "test-file");
        lenient().doReturn(true).when(src).exists();
        lenient().doReturn(false).when(src).isDirectory();
        doReturn(true).when(src).canRead();
        doReturn(true).when(src).renameTo(dest);

        doReturn(true).when(parent).exists();
        doReturn(true).when(parent).isDirectory();
        doReturn(true).when(parent).canRead();
        doReturn(true).when(parent).canExecute();
        doReturn(true).when(parent).canWrite();

        lenient().doReturn(true).when(dest).exists();
        doReturn(false).when(dest).isDirectory();
        doReturn(parent).when(dest).getParentFile();
        lenient().doReturn("").when(dest).getPath();

        assertThrows(IOException.class, () -> Files.ensureMv(src, dest));
    }

    @Test
    public void testEnsureMvString() throws Exception {
        String src = "src";
        String dest = "dest";

        String ret = Files.mv(src, dest);
        assertEquals(ret, "Directory, 'src' is not a file.");
    }

    @Test
    public void testEnsureMvStringNull() throws Exception {
        String src = null;
        String dest = null;

        assertThrows(IllegalArgumentException.class, () -> Files.mv(src, dest));
    }

    @Test
    public void testMvNoOverwrite() throws Exception {
        File src = mock(File.class, "test-file");
        File dest = mock(File.class, "test-file");
        doReturn(true).when(src).exists();
        doReturn(false).when(src).isDirectory();
        doReturn(true).when(src).canRead();

        doReturn(true).when(dest).exists();
        doReturn(false).when(dest).isDirectory();
        doReturn(true).when(dest).canRead();

        String ret = Files.mv(src, dest);
        assertEquals(ret, "File, 'test-file' already exists.");
    }

    @Test
    public void testMvNotDestExists() throws Exception {
        File src = mock(File.class, "test-file");
        File dest = mock(File.class, "test-file");
        File parent = mock(File.class, "test-file");
        lenient().doReturn(true).when(src).exists();
        lenient().doReturn(false).when(src).isDirectory();
        doReturn(true).when(src).canRead();
        doReturn(true).when(src).isFile();
        doReturn(true).when(src).renameTo(dest);

        doReturn(true).when(parent).exists();
        doReturn(true).when(parent).isDirectory();
        doReturn(true).when(parent).canRead();
        doReturn(true).when(parent).canExecute();
        doReturn(true).when(parent).canWrite();

        lenient().doReturn(false).when(dest).exists();
        doReturn(false).when(dest).isDirectory();
        doReturn(parent).when(dest).getParentFile();
        lenient().doReturn("").when(dest).getPath();

        String ret = Files.mv(src, dest, true);
        assertNull(ret);
    }

    @Test
    public void testMvDestExists() throws Exception {
        File src = mock(File.class, "test-file");
        File dest = mock(File.class, "test-file");
        File parent = mock(File.class, "test-file");
        lenient().doReturn(true).when(src).exists();
        lenient().doReturn(false).when(src).isDirectory();
        doReturn(true).when(src).canRead();
        doReturn(true).when(src).renameTo(dest);

        doReturn(true).when(parent).exists();
        doReturn(true).when(parent).isDirectory();
        doReturn(true).when(parent).canRead();
        doReturn(true).when(parent).canExecute();
        doReturn(true).when(parent).canWrite();

        lenient().doReturn(true).when(dest).exists();
        doReturn(false).when(dest).isDirectory();
        doReturn(parent).when(dest).getParentFile();
        lenient().doReturn("").when(dest).getPath();

        String ret = Files.mv(src, dest);
        assertEquals(ret, "File, 'test-file' already exists.");
    }

    @Test
    public void testMvDestExistsOverwrite() throws Exception {
        File src = mock(File.class, "test-file");
        File dest = mock(File.class, "test-file");
        File parent = mock(File.class, "test-file");
        lenient().doReturn(true).when(src).exists();
        lenient().doReturn(false).when(src).isDirectory();
        doReturn(true).when(src).canRead();
        doReturn(true).when(src).isFile();
        doReturn(true).when(src).renameTo(dest);

        doReturn(true).when(parent).exists();
        doReturn(true).when(parent).isDirectory();
        doReturn(true).when(parent).canRead();
        doReturn(true).when(parent).canExecute();
        doReturn(true).when(parent).canWrite();

        doReturn(true, true, false).when(dest).exists();

        lenient().doReturn(false).when(dest).isDirectory();
        doReturn(true).when(dest).canRead();
        doReturn(true).when(dest).canWrite();
        doReturn(true).when(dest).delete();
        doReturn(parent).when(dest).getParentFile();
        lenient().doReturn("").when(dest).getPath();

        String ret = Files.mv(src, dest, true);
        assertNull(ret);
    }

    @Test
    public void testMvString() throws Exception {
        String ret = Files.mv("src", "dest", true);
        assertEquals(ret, "Directory, 'src' is not a file.");
    }

    @Test
    public void testMvStringNull() throws Exception {
        String src = null;
        String dest = null;
        assertThrows(IllegalArgumentException.class, () -> Files.mv(src, dest, true));
    }

    @Test
    public void testTmpDir() throws Exception {
        File tmp = Files.tmpDir();
        assertNotNull(tmp);
    }
}
