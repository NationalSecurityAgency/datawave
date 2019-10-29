package datawave.query.util.sortedset;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.SortedSet;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import datawave.query.iterator.ivarator.IvaratorCacheDir;
import datawave.query.util.sortedset.FileSortedSet.SortedSetFileHandler;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class HdfsBackedSortedSet<E extends Serializable> extends BufferedFileBackedSortedSet<E> implements SortedSet<E> {
    private static final Logger log = Logger.getLogger(HdfsBackedSortedSet.class);
    private static final String FILENAME_PREFIX = "SortedSetFile.";
    
    public HdfsBackedSortedSet(HdfsBackedSortedSet<E> other) throws IOException {
        super(other);
    }
    
    public HdfsBackedSortedSet(List<IvaratorCacheDir> ivaratorCacheDirs, String uniqueSubPath, int maxOpenFiles, int numRetries) throws IOException {
        this(null, ivaratorCacheDirs, uniqueSubPath, maxOpenFiles, numRetries);
    }
    
    public HdfsBackedSortedSet(Comparator<? super E> comparator, List<IvaratorCacheDir> ivaratorCacheDirs, String uniqueSubPath, int maxOpenFiles,
                    int numRetries) throws IOException {
        this(comparator, 10000, ivaratorCacheDirs, uniqueSubPath, maxOpenFiles, numRetries);
    }
    
    private static List<SortedSetFileHandlerFactory> createFileHandlerFactories(List<IvaratorCacheDir> ivaratorCacheDirs, String uniqueSubPath) {
        List<SortedSetFileHandlerFactory> fileHandlerFactories = new ArrayList<>();
        for (IvaratorCacheDir ivaratorCacheDir : ivaratorCacheDirs) {
            fileHandlerFactories.add(new SortedSetHdfsFileHandlerFactory(ivaratorCacheDir, uniqueSubPath));
        }
        return fileHandlerFactories;
    }
    
    public HdfsBackedSortedSet(Comparator<? super E> comparator, int bufferPersistThreshold, List<IvaratorCacheDir> ivaratorCacheDirs, String uniqueSubPath,
                    int maxOpenFiles, int numRetries) throws IOException {
        super(comparator, bufferPersistThreshold, maxOpenFiles, numRetries, createFileHandlerFactories(ivaratorCacheDirs, uniqueSubPath));
        
        // for each of the handler factories, check to see if there are any existing files we should load
        for (SortedSetFileHandlerFactory handlerFactory : handlerFactories) {
            // Note: All of the file handler factories created by 'createFileHandlerFactories' are SortedSetHdfsFileHandlerFactories
            if (handlerFactory instanceof SortedSetHdfsFileHandlerFactory) {
                SortedSetHdfsFileHandlerFactory hdfsHandlerFactory = (SortedSetHdfsFileHandlerFactory) handlerFactory;
                FileSystem fs = hdfsHandlerFactory.getFs();
                int count = 0;
                
                // if the directory already exists, load up this sorted set with any existing files
                if (fs.exists(hdfsHandlerFactory.getUniqueDir())) {
                    FileStatus[] files = fs.listStatus(hdfsHandlerFactory.getUniqueDir());
                    if (files != null) {
                        for (FileStatus file : files) {
                            if (!file.isDir() && file.getPath().getName().startsWith(FILENAME_PREFIX)) {
                                count++;
                                addSet(new FileSortedSet<>(comparator, new SortedSetHdfsFileHandler(fs, file.getPath()), true));
                            }
                        }
                    }
                    
                    hdfsHandlerFactory.setFileCount(count);
                }
            }
        }
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
        final private IvaratorCacheDir ivaratorCacheDir;
        private String uniqueSubPath;
        private int fileCount = 0;
        
        public SortedSetHdfsFileHandlerFactory(IvaratorCacheDir ivaratorCacheDir, String uniqueSubPath) {
            this.ivaratorCacheDir = ivaratorCacheDir;
            this.uniqueSubPath = uniqueSubPath;
        }
        
        public IvaratorCacheDir getIvaratorCacheDir() {
            return ivaratorCacheDir;
        }
        
        public FileSystem getFs() {
            return ivaratorCacheDir.getFs();
        }
        
        public Path getUniqueDir() {
            return new Path(ivaratorCacheDir.getPathURI(), uniqueSubPath);
        }
        
        public int getFileCount() {
            return fileCount;
        }
        
        void setFileCount(int count) {
            this.fileCount = count;
        }
        
        @Override
        public SortedSetFileHandler createHandler() throws IOException {
            FileSystem fs = getFs();
            Path uniqueDir = getUniqueDir();
            
            // attempt to create the folder if it doesn't exist
            if (!fs.exists(uniqueDir) && !fs.mkdirs(uniqueDir))
                throw new IOException("Unable to create directory [" + uniqueDir + "] in filesystem [" + fs + "]");
            
            // generate a unique file name
            fileCount++;
            Path file = new Path(uniqueDir, FILENAME_PREFIX + fileCount + '.' + System.currentTimeMillis());
            return new SortedSetHdfsFileHandler(fs, file);
        }
        
        @Override
        public String toString() {
            return getUniqueDir() + " (fileCount=" + fileCount + ')';
        }
        
    }
    
    public static class SortedSetHdfsFileHandler implements SortedSetFileHandler {
        private FileSystem fs;
        private Path file;
        
        public SortedSetHdfsFileHandler(FileSystem fs, Path file) {
            this.fs = fs;
            this.file = file;
        }
        
        private String getScheme() {
            String scheme = file.toUri().getScheme();
            if (scheme == null) {
                scheme = fs.getScheme();
            }
            return scheme;
        }
        
        @Override
        public SortedSetInputStream getSortedSetInputStream() throws IOException {
            // only need to compress if we are using a local file system
            return new SortedSetInputStream(getInputStream(), "file".equals(getScheme()));
        }
        
        @Override
        public InputStream getInputStream() throws IOException {
            if (log.isDebugEnabled()) {
                log.debug("Reading " + file);
            }
            return new BufferedInputStream(fs.open(file));
        }
        
        @Override
        public SortedSetOutputStream getSortedSetOutputStream() throws IOException {
            // only need to compress if we are using a local file system
            return new SortedSetOutputStream(getOutputStream(), "file".equals(getScheme()));
        }
        
        private OutputStream getOutputStream() throws IOException {
            if (log.isDebugEnabled()) {
                log.debug("Creating " + file);
            }
            return new BufferedOutputStream(fs.create(file));
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
