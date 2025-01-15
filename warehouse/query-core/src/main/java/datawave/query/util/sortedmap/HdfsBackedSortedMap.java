package datawave.query.util.sortedmap;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import datawave.query.iterator.ivarator.IvaratorCacheDir;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import datawave.query.util.sortedset.FileSortedSet;

/**
 * Sorted map backed by HDFS
 *
 * @param <K>
 *            key of the map
 * @param <V>
 *            value of the map
 */
public class HdfsBackedSortedMap<K,V> extends BufferedFileBackedSortedMap<K,V> {
    private static final Logger log = Logger.getLogger(HdfsBackedSortedMap.class);
    private static final String FILENAME_PREFIX = "SortedMapFile.";

    public static class Builder<B extends Builder<B,K,V>,K,V> extends BufferedFileBackedSortedMap.Builder<B,K,V> {
        private List<IvaratorCacheDir> ivaratorCacheDirs;
        private String uniqueSubPath;
        private FileSortedSet.PersistOptions persistOptions;

        public Builder() {
            // change the default buffer persist threshold
            withBufferPersistThreshold(10_000);
        }

        @Override
        @SuppressWarnings("unchecked")
        protected B self() {
            return (B) this;
        }

        public B withIvaratorCacheDirs(List<IvaratorCacheDir> ivaratorCacheDirs) {
            this.ivaratorCacheDirs = ivaratorCacheDirs;
            return self();
        }

        public B withUniqueSubPath(String uniqueSubPath) {
            this.uniqueSubPath = uniqueSubPath;
            return self();
        }

        public B withPersistOptions(FileSortedSet.PersistOptions persistOptions) {
            this.persistOptions = persistOptions;
            return self();
        }

        public HdfsBackedSortedMap<?,?> build() throws IOException {
            return new HdfsBackedSortedMap<>(this);
        }
    }

    public static HdfsBackedSortedMap.Builder<?,?,?> builder() {
        return new HdfsBackedSortedMap.Builder<>();
    }

    protected HdfsBackedSortedMap(HdfsBackedSortedMap<K,V> other) {
        super(other);
    }

    protected HdfsBackedSortedMap(Builder builder) throws IOException {
        super(builder);
        List<SortedMapHdfsFileHandlerFactory> factories = createFileHandlerFactories(builder.ivaratorCacheDirs, builder.uniqueSubPath, builder.persistOptions);
        // update the parent handler factories (list of SortedMapFileHandlerFactory)
        this.handlerFactories = (List) factories;
        // for each of the handler factories, check to see if there are any existing files we should load
        for (SortedMapHdfsFileHandlerFactory handlerFactory : factories) {
            SortedMapHdfsFileHandlerFactory hdfsHandlerFactory = (SortedMapHdfsFileHandlerFactory) handlerFactory;
            FileSystem fs = hdfsHandlerFactory.getFs();
            int count = 0;

            // if the directory already exists, load up this sorted map with any existing files
            if (fs.exists(hdfsHandlerFactory.getUniqueDir())) {
                FileStatus[] files = fs.listStatus(hdfsHandlerFactory.getUniqueDir());
                if (files != null) {
                    for (FileStatus file : files) {
                        if (!file.isDir() && file.getPath().getName().startsWith(FILENAME_PREFIX)) {
                            count++;
                            addMap(mapFactory.newInstance(comparator, getRewriteStrategy(),
                                            new SortedMapHdfsFileHandler(fs, file.getPath(), builder.persistOptions), true));
                        }
                    }
                }

                hdfsHandlerFactory.mapFileCount(count);
            }
        }
    }

    private static List<SortedMapHdfsFileHandlerFactory> createFileHandlerFactories(List<IvaratorCacheDir> ivaratorCacheDirs, String uniqueSubPath,
                    FileSortedSet.PersistOptions persistOptions) {
        List<SortedMapHdfsFileHandlerFactory> fileHandlerFactories = new ArrayList<>();
        for (IvaratorCacheDir ivaratorCacheDir : ivaratorCacheDirs) {
            fileHandlerFactories.add(new SortedMapHdfsFileHandlerFactory(ivaratorCacheDir, uniqueSubPath, persistOptions));
        }
        return fileHandlerFactories;
    }

    @Override
    public void clear() {
        // This will be a new ArrayList<>() containing the same FileSortedMaps
        List<FileSortedMap<K,V>> SortedMaps = super.getMaps();
        // Clear will call clear on each of the FileSortedMaps, clear the container, and null the buffer
        super.clear();
        // We should still be able to access the FileSortedMap objects to get their handler because we
        // have a copy of the object in 'SortedMaps'
        for (FileSortedMap<K,V> fss : SortedMaps) {
            if (fss.isPersisted() && fss.handler instanceof SortedMapHdfsFileHandler) {
                ((SortedMapHdfsFileHandler) fss.handler).deleteFile();
            }
        }
    }

    public static class SortedMapHdfsFileHandlerFactory implements SortedMapFileHandlerFactory {
        final private IvaratorCacheDir ivaratorCacheDir;
        private String uniqueSubPath;
        private int fileCount = 0;
        private FileSortedSet.PersistOptions persistOptions;

        public SortedMapHdfsFileHandlerFactory(IvaratorCacheDir ivaratorCacheDir, String uniqueSubPath, FileSortedSet.PersistOptions persistOptions) {
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

        void mapFileCount(int count) {
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
        public FileSortedMap.SortedMapFileHandler createHandler() throws IOException {
            FileSystem fs = getFs();
            Path uniqueDir = getUniqueDir();

            // Lazily create the required ivarator cache dirs.
            ensureDirsCreated();

            // generate a unique file name
            fileCount++;
            Path file = new Path(uniqueDir, FILENAME_PREFIX + fileCount + '.' + System.currentTimeMillis());
            return new SortedMapHdfsFileHandler(fs, file, persistOptions);
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

    public static class SortedMapHdfsFileHandler implements FileSortedMap.SortedMapFileHandler {
        private FileSystem fs;
        private Path file;
        private FileSortedSet.PersistOptions persistOptions;

        public SortedMapHdfsFileHandler(FileSystem fs, Path file, FileSortedSet.PersistOptions persistOptions) {
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
