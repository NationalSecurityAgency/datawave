package datawave.common.io;

import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertEquals;

public class FilesTest {
    
    @Test
    public void testConstructor() {
        Files files = new Files();
        assertNotNull(files);
    }
    
    @Test(expected = IllegalStateException.class)
    public void testEnsureDirException() throws Exception {
        File file = mock(File.class);
        expect(file.exists()).andReturn(false);
        expect(file.mkdirs()).andReturn(false);
        replay(file);
        Files.ensureDir(file);
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
        File file = mock(File.class);
        expect(file.exists()).andReturn(false);
        expect(file.mkdirs()).andReturn(false);
        replay(file);
        String err = Files.checkDir(file);
        assertEquals(err, "Directory, 'EasyMock for class java.io.File' does not exist; and cannot be created");
    }
    
    @Test
    public void testCheckDirIsDirectory() throws Exception {
        File file = mock(File.class);
        expect(file.exists()).andReturn(true);
        expect(file.isDirectory()).andReturn(false);
        replay(file);
        String err = Files.checkDir(file);
        assertEquals(err, "File, 'EasyMock for class java.io.File' is not a directory.");
    }
    
    @Test
    public void testCheckDirCanRead() throws Exception {
        File file = mock(File.class);
        expect(file.exists()).andReturn(true);
        expect(file.isDirectory()).andReturn(true);
        expect(file.canRead()).andReturn(false);
        replay(file);
        String err = Files.checkDir(file);
        assertEquals(err, "Directory, 'EasyMock for class java.io.File' cannot be read.");
    }
    
    @Test
    public void testCheckDirCanExecute() throws Exception {
        File file = mock(File.class);
        expect(file.exists()).andReturn(true);
        expect(file.isDirectory()).andReturn(true);
        expect(file.canRead()).andReturn(true);
        expect(file.canExecute()).andReturn(false);
        replay(file);
        String err = Files.checkDir(file);
        assertEquals(err, "Directory contents for 'EasyMock for class java.io.File' cannot be listed.");
    }
    
    @Test
    public void testCheckDirCheckWrite() throws Exception {
        File file = mock(File.class);
        expect(file.exists()).andReturn(true);
        expect(file.isDirectory()).andReturn(true);
        expect(file.canRead()).andReturn(true);
        expect(file.canExecute()).andReturn(true);
        expect(file.canWrite()).andReturn(false);
        replay(file);
        String err = Files.checkDir(file, true);
        assertEquals(err, "Directory, 'EasyMock for class java.io.File' cannot be written to.");
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testCheckDirNull() throws Exception {
        File file = null;
        Files.checkDir(file);
    }
    
    @Test
    public void testCheckFileExists() throws Exception {
        File file = mock(File.class);
        expect(file.exists()).andReturn(false);
        replay(file);
        String err = Files.checkFile(file, true);
        assertEquals(err, "File, 'EasyMock for class java.io.File' does not exist.");
    }
    
    @Test
    public void testCheckFileIsNotDirectory() throws Exception {
        File file = mock(File.class);
        expect(file.exists()).andReturn(true);
        expect(file.isDirectory()).andReturn(true);
        replay(file);
        String err = Files.checkFile(file, true);
        assertEquals(err, "Directory, 'EasyMock for class java.io.File' is not a file.");
    }
    
    @Test
    public void testCheckFileCanRead() throws Exception {
        File file = mock(File.class);
        expect(file.exists()).andReturn(true);
        expect(file.isDirectory()).andReturn(false);
        expect(file.canRead()).andReturn(false);
        replay(file);
        String err = Files.checkFile(file, false);
        assertEquals(err, "File, 'EasyMock for class java.io.File' cannot be read.");
    }
    
    @Test
    public void testCheckFileWritable() throws Exception {
        File file = mock(File.class);
        expect(file.exists()).andReturn(true);
        expect(file.isDirectory()).andReturn(false);
        expect(file.canRead()).andReturn(true);
        expect(file.canWrite()).andReturn(false);
        replay(file);
        String err = Files.checkFile(file, true);
        assertEquals(err, "File, 'EasyMock for class java.io.File' cannot be written to.");
    }
    
    @Test(expected = IOException.class)
    public void testEnsureMvIOException() throws Exception {
        File src = mock(File.class);
        File dest = mock(File.class);
        File parent = mock(File.class);
        expect(src.exists()).andReturn(true).anyTimes();
        expect(src.isDirectory()).andReturn(false).anyTimes();
        expect(src.canRead()).andReturn(true);
        expect(src.renameTo(dest)).andReturn(true);
        replay(src);
        
        expect(parent.exists()).andReturn(true);
        expect(parent.isDirectory()).andReturn(true);
        expect(parent.canRead()).andReturn(true);
        expect(parent.canExecute()).andReturn(true);
        expect(parent.canWrite()).andReturn(true);
        replay(parent);
        
        expect(dest.exists()).andReturn(true).anyTimes();
        expect(dest.isDirectory()).andReturn(false);
        expect(dest.getParentFile()).andReturn(parent);
        expect(dest.getPath()).andReturn("").anyTimes();
        replay(dest);
        
        Files.ensureMv(src, dest);
    }
    
    @Test
    public void testEnsureMvString() throws Exception {
        String src = "src";
        String dest = "dest";
        
        String ret = Files.mv(src, dest);
        assertEquals(ret, "Directory, 'src' is not a file.");
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testEnsureMvStringNull() throws Exception {
        String src = null;
        String dest = null;
        
        String ret = Files.mv(src, dest);
    }
    
    @Test
    public void testMvNoOverwrite() throws Exception {
        File src = mock(File.class);
        File dest = mock(File.class);
        expect(src.exists()).andReturn(true);
        expect(src.isDirectory()).andReturn(false);
        expect(src.canRead()).andReturn(true);
        replay(src);
        
        expect(dest.exists()).andReturn(true);
        expect(dest.isDirectory()).andReturn(false);
        expect(dest.canRead()).andReturn(true);
        replay(dest);
        
        String ret = Files.mv(src, dest);
        assertEquals(ret, "File, 'EasyMock for class java.io.File' already exists.");
    }
    
    @Test
    public void testMvNotDestExists() throws Exception {
        File src = mock(File.class);
        File dest = mock(File.class);
        File parent = mock(File.class);
        expect(src.exists()).andReturn(true).anyTimes();
        expect(src.isDirectory()).andReturn(false).anyTimes();
        expect(src.canRead()).andReturn(true);
        expect(src.renameTo(dest)).andReturn(true);
        replay(src);
        
        expect(parent.exists()).andReturn(true);
        expect(parent.isDirectory()).andReturn(true);
        expect(parent.canRead()).andReturn(true);
        expect(parent.canExecute()).andReturn(true);
        expect(parent.canWrite()).andReturn(true);
        replay(parent);
        
        expect(dest.exists()).andReturn(false).anyTimes();
        expect(dest.isDirectory()).andReturn(false);
        expect(dest.getParentFile()).andReturn(parent);
        expect(dest.getPath()).andReturn("").anyTimes();
        replay(dest);
        
        String ret = Files.mv(src, dest, true);
        assertNull(ret);
    }
    
    @Test
    public void testMvDestExists() throws Exception {
        File src = mock(File.class);
        File dest = mock(File.class);
        File parent = mock(File.class);
        expect(src.exists()).andReturn(true).anyTimes();
        expect(src.isDirectory()).andReturn(false).anyTimes();
        expect(src.canRead()).andReturn(true);
        expect(src.renameTo(dest)).andReturn(true);
        replay(src);
        
        expect(parent.exists()).andReturn(true);
        expect(parent.isDirectory()).andReturn(true);
        expect(parent.canRead()).andReturn(true);
        expect(parent.canExecute()).andReturn(true);
        expect(parent.canWrite()).andReturn(true);
        replay(parent);
        
        expect(dest.exists()).andReturn(true).anyTimes();
        expect(dest.isDirectory()).andReturn(false);
        expect(dest.getParentFile()).andReturn(parent);
        expect(dest.getPath()).andReturn("").anyTimes();
        replay(dest);
        
        String ret = Files.mv(src, dest);
        assertEquals(ret, "File, 'EasyMock for class java.io.File' already exists.");
    }
    
    @Test
    public void testMvDestExistsOverwrite() throws Exception {
        File src = mock(File.class);
        File dest = mock(File.class);
        File parent = mock(File.class);
        expect(src.exists()).andReturn(true).anyTimes();
        expect(src.isDirectory()).andReturn(false).anyTimes();
        expect(src.canRead()).andReturn(true);
        expect(src.renameTo(dest)).andReturn(true);
        replay(src);
        
        expect(parent.exists()).andReturn(true);
        expect(parent.isDirectory()).andReturn(true);
        expect(parent.canRead()).andReturn(true);
        expect(parent.canExecute()).andReturn(true);
        expect(parent.canWrite()).andReturn(true);
        replay(parent);
        
        expect(dest.exists()).andReturn(true).once();
        expect(dest.exists()).andReturn(true).once();
        expect(dest.exists()).andReturn(false).once();
        expect(dest.isDirectory()).andReturn(false).anyTimes();
        expect(dest.canRead()).andReturn(true);
        expect(dest.canWrite()).andReturn(true);
        expect(dest.delete()).andReturn(true);
        expect(dest.getParentFile()).andReturn(parent);
        expect(dest.getPath()).andReturn("").anyTimes();
        replay(dest);
        
        String ret = Files.mv(src, dest, true);
        assertNull(ret);
    }
    
    @Test
    public void testMvString() throws Exception {
        String ret = Files.mv("src", "dest", true);
        assertEquals(ret, "Directory, 'src' is not a file.");
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testMvStringNull() throws Exception {
        String src = null;
        String dest = null;
        String ret = Files.mv(src, dest, true);
    }
    
    @Test
    public void testTmpDir() throws Exception {
        File tmp = Files.tmpDir();
        assertNotNull(tmp);
    }
}
