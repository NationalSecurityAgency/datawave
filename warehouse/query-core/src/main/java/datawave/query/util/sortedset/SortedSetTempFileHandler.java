package datawave.query.util.sortedset;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import datawave.query.util.sortedset.FileSortedSet.SortedSetFileHandler;

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
    public SortedSetInputStream getSortedSetInputStream() throws IOException {
        return new SortedSetInputStream(getInputStream(), true);
    }
    
    @Override
    public InputStream getInputStream() throws IOException {
        return new BufferedInputStream(new FileInputStream(file));
    }
    
    @Override
    public SortedSetOutputStream getSortedSetOutputStream() throws IOException {
        return new SortedSetOutputStream(getOutputStream(), true);
    }
    
    public OutputStream getOutputStream() throws IOException {
        return new BufferedOutputStream(new FileOutputStream(file));
    }
    
    @Override
    public long getSize() {
        return (file.exists() ? file.length() : -1);
    }
    
    @Override
    public void deleteFile() {
        this.file.delete();
    }
    
    @Override
    public String toString() {
        return file.toString();
    }
    
}
