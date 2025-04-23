package datawave.query.util.sortedmap;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import datawave.query.util.sortedset.FileSortedSet;

/**
 * A sorted set file handler factory that uses temporary local based files.
 */
public class SortedMapTempFileHandler implements FileSortedMap.SortedMapFileHandler {
    private final FileSystem fs;
    private final File file;
    private final Path path;

    public SortedMapTempFileHandler() throws IOException {
        this.file = File.createTempFile("SortedSet", ".bin");
        this.file.deleteOnExit();
        this.path = new Path(file.toURI());
        Configuration conf = new Configuration();
        this.fs = path.getFileSystem(conf);
    }

    public File getFile() {
        return file;
    }

    @Override
    public InputStream getInputStream() throws IOException {
        return fs.open(path);
    }

    public OutputStream getOutputStream() throws IOException {
        return fs.create(path, true);
    }

    @Override
    public FileSortedSet.PersistOptions getPersistOptions() {
        return new FileSortedSet.PersistOptions(true, false);
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
