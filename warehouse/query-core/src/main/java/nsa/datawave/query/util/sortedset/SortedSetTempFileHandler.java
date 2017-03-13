package nsa.datawave.query.util.sortedset;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import nsa.datawave.query.util.sortedset.FileSortedSet.SortedSetFileHandler;

/**
 * A sorted set file handler factory that uses temporary local based files.
 * 
 * 
 * 
 */
public class SortedSetTempFileHandler implements SortedSetFileHandler {
    private File file;
    
    public SortedSetTempFileHandler() throws IOException {
        this.file = File.createTempFile("SortedSet", "bin");
        this.file.deleteOnExit();
    }
    
    public File getFile() {
        return file;
    }
    
    @Override
    public InputStream getInputStream() throws FileNotFoundException {
        return new FileInputStream(file);
    }
    
    @Override
    public OutputStream getOutputStream() throws FileNotFoundException {
        return new FileOutputStream(file);
    }
    
    @Override
    public long getSize() {
        return (file.exists() ? file.length() : -1);
    }
    
    @Override
    public void deleteFile() {
        this.file.delete();
    }
    
}
