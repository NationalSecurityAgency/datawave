package datawave.query.util.sortedset;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.SortedSet;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import datawave.query.iterator.ivarator.IvaratorCacheDir;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;

public class HdfsBackedSortedSet<E> extends BufferedFileBackedSortedSet<E> implements RewritableSortedSet<E> {
    private static final Logger log = Logger.getLogger(HdfsBackedSortedSet.class);
    private static final String FILENAME_PREFIX = "SortedSetFile.";

    public HdfsBackedSortedSet(HdfsBackedSortedSet<E> other) throws IOException {
        super(other);
    }

    public HdfsBackedSortedSet(List<IvaratorCacheDir> ivaratorCacheDirs, String uniqueSubPath, int maxOpenFiles, int numRetries,
                    FileSortedSet.PersistOptions persistOptions) throws IOException {
        this(ivaratorCacheDirs, uniqueSubPath, maxOpenFiles, numRetries, persistOptions, new FileSerializableSortedSet.Factory());
    }

    public HdfsBackedSortedSet(List<IvaratorCacheDir> ivaratorCacheDirs, String uniqueSubPath, int maxOpenFiles, int numRetries,
                    FileSortedSet.PersistOptions persistOptions, FileSortedSet.FileSortedSetFactory<E> setFactory) throws IOException {
        this(null, ivaratorCacheDirs, uniqueSubPath, maxOpenFiles, numRetries, persistOptions, setFactory);
    }

    public HdfsBackedSortedSet(Comparator<E> comparator, List<IvaratorCacheDir> ivaratorCacheDirs, String uniqueSubPath, int maxOpenFiles, int numRetries,
                    FileSortedSet.PersistOptions persistOptions) throws IOException {
        this(comparator, ivaratorCacheDirs, uniqueSubPath, maxOpenFiles, numRetries, persistOptions, new FileSerializableSortedSet.Factory());
    }

    public HdfsBackedSortedSet(Comparator<E> comparator, List<IvaratorCacheDir> ivaratorCacheDirs, String uniqueSubPath, int maxOpenFiles, int numRetries,
                    FileSortedSet.PersistOptions persistOptions, FileSortedSet.FileSortedSetFactory<E> setFactory) throws IOException {
        this(comparator, null, 10000, ivaratorCacheDirs, uniqueSubPath, maxOpenFiles, numRetries, persistOptions, setFactory);
    }

    private static List<SortedSetFileHandlerFactory> createFileHandlerFactories(List<IvaratorCacheDir> ivaratorCacheDirs, String uniqueSubPath,
                    FileSortedSet.PersistOptions persistOptions) {
        List<SortedSetFileHandlerFactory> fileHandlerFactories = new ArrayList<>();
        for (IvaratorCacheDir ivaratorCacheDir : ivaratorCacheDirs) {
            fileHandlerFactories.add(new SortedSetHdfsFileHandlerFactory(ivaratorCacheDir, uniqueSubPath, persistOptions));
        }
        return fileHandlerFactories;
    }

    public HdfsBackedSortedSet(Comparator<E> comparator, int bufferPersistThreshold, List<IvaratorCacheDir> ivaratorCacheDirs, String uniqueSubPath,
                    int maxOpenFiles, int numRetries, FileSortedSet.PersistOptions persistOptions) throws IOException {
        this(comparator, null, bufferPersistThreshold, ivaratorCacheDirs, uniqueSubPath, maxOpenFiles, numRetries, persistOptions,
                        new FileSerializableSortedSet.Factory());
    }

    public HdfsBackedSortedSet(Comparator<E> comparator, RewriteStrategy rewriteStrategy, int bufferPersistThreshold, List<IvaratorCacheDir> ivaratorCacheDirs,
                    String uniqueSubPath, int maxOpenFiles, int numRetries, FileSortedSet.PersistOptions persistOptions,
                    FileSortedSet.FileSortedSetFactory<E> setFactory) throws IOException {
        super(comparator, rewriteStrategy, bufferPersistThreshold, maxOpenFiles, numRetries,
                        createFileHandlerFactories(ivaratorCacheDirs, uniqueSubPath, persistOptions), setFactory);

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
                                addSet(setFactory.newInstance(comparator, new SortedSetHdfsFileHandler(fs, file.getPath(), persistOptions), true));
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
        private FileSortedSet.PersistOptions persistOptions;

        public SortedSetHdfsFileHandlerFactory(IvaratorCacheDir ivaratorCacheDir, String uniqueSubPath, FileSortedSet.PersistOptions persistOptions) {
            this.ivaratorCacheDir = ivaratorCacheDir;
            this.uniqueSubPath = uniqueSubPath;
            this.persistOptions = persistOptions;
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

        public boolean isValid() {
            FsStatus fsStatus = null;
            try {
                fsStatus = ivaratorCacheDir.getFs().getStatus();
            } catch (IOException e) {
                log.warn("Unable to determine status of the filesystem: " + ivaratorCacheDir.getFs());
            }

            // determine whether this fs is a good candidate
            if (fsStatus != null) {
                long availableStorageMiB = fsStatus.getRemaining() / 0x100000L;
                double availableStoragePercent = (double) fsStatus.getRemaining() / fsStatus.getCapacity();

                // if we are using less than our storage limit, the cache dir is valid
                return availableStorageMiB >= ivaratorCacheDir.getConfig().getMinAvailableStorageMiB()
                                && availableStoragePercent >= ivaratorCacheDir.getConfig().getMinAvailableStoragePercent();
            }

            return false;
        }

        @Override
        public FileSortedSet.SortedSetFileHandler createHandler() throws IOException {
            FileSystem fs = getFs();
            Path uniqueDir = getUniqueDir();

            // Lazily create the required ivarator cache dirs.
            ensureDirsCreated();

            // generate a unique file name
            fileCount++;
            Path file = new Path(uniqueDir, FILENAME_PREFIX + fileCount + '.' + System.currentTimeMillis());
            return new SortedSetHdfsFileHandler(fs, file, persistOptions);
        }

        private void ensureDirsCreated() throws IOException {
            IvaratorCacheDirConfig config = ivaratorCacheDir.getConfig();
            if (config.isValid()) {
                ensureCreation(new Path(ivaratorCacheDir.getPathURI()));
                ensureCreation(getUniqueDir());
            } else {
                throw new IOException("Unable to create Ivarator Cache Dir for invalid config: " + config);
            }
        }

        private void ensureCreation(Path path) throws IOException {
            try {
                FileSystem fs = getFs();
                if (!fs.exists(path)) {
                    // Attempt to create the required directory if it does not exist.
                    if (!fs.mkdirs(path)) {
                        throw new IOException("Unable to mkdirs: fs.mkdir(" + path + ")->false");
                    }
                }
            } catch (MalformedURLException e) {
                throw new IOException("Unable to load hadoop configuration", e);
            } catch (Exception e) {
                log.warn("Unable to create directory [" + path + "] in file system [" + getFs() + "]", e);
                throw new IOException("Unable to create directory [" + path + "] in file system [" + getFs() + "]", e);
            }
        }

        @Override
        public String toString() {
            return getUniqueDir() + " (fileCount=" + fileCount + ')';
        }

    }

    public static class SortedSetHdfsFileHandler implements FileSortedSet.SortedSetFileHandler {
        private FileSystem fs;
        private Path file;
        private FileSortedSet.PersistOptions persistOptions;

        public SortedSetHdfsFileHandler(FileSystem fs, Path file, FileSortedSet.PersistOptions persistOptions) {
            this.fs = fs;
            this.file = file;
            this.persistOptions = persistOptions;
        }

        private String getScheme() {
            String scheme = file.toUri().getScheme();
            if (scheme == null) {
                scheme = fs.getScheme();
            }
            return scheme;
        }

        @Override
        public InputStream getInputStream() throws IOException {
            if (log.isDebugEnabled()) {
                log.debug("Reading " + file);
            }
            return fs.open(file);
        }

        @Override
        public OutputStream getOutputStream() throws IOException {
            if (log.isDebugEnabled()) {
                log.debug("Creating " + file);
            }
            return fs.create(file);
        }

        @Override
        public FileSortedSet.PersistOptions getPersistOptions() {
            return persistOptions;
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
