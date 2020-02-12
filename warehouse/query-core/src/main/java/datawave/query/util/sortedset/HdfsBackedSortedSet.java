package datawave.query.util.sortedset;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Comparator;
import java.util.List;
import java.util.SortedSet;

import datawave.query.util.sortedset.FileSortedSet.SortedSetFileHandler;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class HdfsBackedSortedSet<E> extends BufferedFileBackedSortedSet<E> implements SortedSet<E> {
    private static final Logger log = Logger.getLogger(HdfsBackedSortedSet.class);
    private static final String FILENAME_PREFIX = "SortedSetFile.";
    
    public HdfsBackedSortedSet(HdfsBackedSortedSet<E> other) throws IOException {
        super(other);
    }
    
    public HdfsBackedSortedSet(FileSystem fs, Path uniqueDir, int maxOpenFiles, FileSortedSet.FileSortedSetFactory<E> setFactory) throws IOException {
        this(null, fs, uniqueDir, maxOpenFiles, setFactory);
    }
    
    public HdfsBackedSortedSet(Comparator<? super E> comparator, FileSystem fs, Path uniqueDir, int maxOpenFiles,
                    FileSortedSet.FileSortedSetFactory<E> setFactory) throws IOException {
        this(comparator, 10000, fs, uniqueDir, maxOpenFiles, setFactory);
    }
    
    public HdfsBackedSortedSet(Comparator<? super E> comparator, int bufferPersistThreshold, FileSystem fs, Path uniqueDir, int maxOpenFiles,
                    FileSortedSet.FileSortedSetFactory<E> setFactory) throws IOException {
        super(comparator, bufferPersistThreshold, maxOpenFiles, new SortedSetHdfsFileHandlerFactory(fs, uniqueDir), setFactory);
        
        // now load up this sorted set with any existing files
        FileStatus[] files = fs.listStatus(uniqueDir);
        int count = 0;
        if (files != null) {
            for (FileStatus file : files) {
                if (!file.isDir() && file.getPath().getName().startsWith(FILENAME_PREFIX)) {
                    count++;
                    addSet(setFactory.newInstance(comparator, new SortedSetHdfsFileHandler(fs, file.getPath()), true));
                }
            }
        }
        
        ((SortedSetHdfsFileHandlerFactory) (this.handlerFactory)).setFileCount(count);
    }
    
    @Override
    public void clear() {
        // This will be a new ArrayList<>() containing the same FileSortedSets
        List<FileSortedSet<E>> sortedSets = super.getSets();
        // Clear will call clear on each of the FileSortedSets, clear the container, and null the buffer
        super.clear();
        // We should still be able to access the FileSortedSet objects to get their handler because we
        // have a copy of the object in 'sortedSets'
        for (FileSortedSet<E> fss : sortedSets) {
            if (fss.isPersisted() && fss.handler instanceof SortedSetHdfsFileHandler) {
                ((SortedSetHdfsFileHandler) fss.handler).deleteFile();
            }
        }
    }
    
    public static class SortedSetHdfsFileHandlerFactory implements SortedSetFileHandlerFactory {
        private FileSystem fs;
        private Path uniqueDir;
        private int fileCount = 0;
        
        public SortedSetHdfsFileHandlerFactory(FileSystem fs, Path uniqueDir) {
            this.fs = fs;
            this.uniqueDir = uniqueDir;
        }
        
        void setFileCount(int count) {
            this.fileCount = count;
        }
        
        @Override
        public SortedSetFileHandler createHandler() throws IOException {
            // generate a unique file name
            fileCount++;
            Path file = new Path(uniqueDir, FILENAME_PREFIX + fileCount + '.' + System.currentTimeMillis());
            return new SortedSetHdfsFileHandler(fs, file);
        }
        
        @Override
        public String toString() {
            return uniqueDir + " (fileCount=" + fileCount + ')';
        }
        
    }
    
    public static class SortedSetHdfsFileHandler implements SortedSetFileHandler {
        private FileSystem fs;
        private Path file;
        
        public SortedSetHdfsFileHandler(FileSystem fs, Path file) {
            this.fs = fs;
            this.file = file;
        }
        
        @Override
        public InputStream getInputStream() throws IOException {
            if (log.isDebugEnabled()) {
                log.debug("Creating " + file);
            }
            return fs.open(file);
        }
        
        @Override
        public OutputStream getOutputStream() throws IOException {
            if (log.isDebugEnabled()) {
                log.debug("Reading " + file);
            }
            return fs.create(file);
        }
        
        @Override
        public long getSize() {
            try {
                FileStatus status = fs.getFileStatus(file);
                return status.getLen();
            } catch (Exception e) {
                log.warn("Failed to verify file " + file, e);
                return -1;
            }
        }
        
        @Override
        public void deleteFile() {
            try {
                if (log.isDebugEnabled()) {
                    log.debug("Deleting " + file);
                }
                if (!fs.delete(file, true)) {
                    log.error("Failed to delete file " + file + ": delete returned false");
                }
            } catch (IOException e) {
                log.error("Failed to delete file " + file, e);
            }
        }
        
        @Override
        public String toString() {
            return file.toString();
        }
        
    }
}
