package datawave.query.util.sortedmap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import datawave.query.util.sortedmap.FileSortedMap.SortedMapFileHandler;

/**
 * This is a sorted map that will hold up to a specified number of entries before flushing the data to disk. Files will be created as needed. An additional
 * "persist" call is supplied to force flushing to disk. The iterator.remove and the submap operations will work up until any buffer has been flushed to disk.
 * After that, those operations will not work as specified by the underlying FileSortedMap.
 *
 * @param <K>
 *            key of the map
 * @param <V>
 *            value of the map
 **/
public class BufferedFileBackedSortedMap<K,V> implements SortedMap<K,V>, RewritableSortedMap<K,V> {
    private static final Logger log = Logger.getLogger(BufferedFileBackedSortedMap.class);
    protected static final int DEFAULT_BUFFER_PERSIST_THRESHOLD = 1000;
    protected static final int DEFAULT_MAX_OPEN_FILES = 100;
    protected static final int DEFAULT_NUM_RETRIES = 2;

    protected MultiMapBackedSortedMap<K,V> map = new MultiMapBackedSortedMap<>();
    protected int maxOpenFiles = DEFAULT_MAX_OPEN_FILES;
    protected FileSortedMap<K,V> buffer = null;
    protected FileSortedMap.FileSortedMapFactory<K,V> mapFactory = null;
    protected final Comparator<K> comparator;
    protected FileSortedMap.RewriteStrategy<K,V> rewriteStrategy;
    protected boolean sizeModified = false;
    protected int size = 0;
    protected int numRetries = DEFAULT_NUM_RETRIES;

    protected List<SortedMapFileHandlerFactory> handlerFactories;
    protected int bufferPersistThreshold = DEFAULT_BUFFER_PERSIST_THRESHOLD;

    /**
     * A factory for SortedMapFileHandlers
     *
     *
     *
     */
    public interface SortedMapFileHandlerFactory {
        SortedMapFileHandler createHandler() throws IOException;

        boolean isValid();
    }

    public static class Builder<B extends Builder<B,K,V>,K,V> {
        private int maxOpenFiles = DEFAULT_MAX_OPEN_FILES;
        private FileSortedMap.FileSortedMapFactory<K,V> mapFactory = new FileSerializableSortedMap.Factory();
        private Comparator<K> comparator;
        private FileSortedMap.RewriteStrategy<K,V> rewriteStrategy;
        private int numRetries = DEFAULT_NUM_RETRIES;
        private List<SortedMapFileHandlerFactory> handlerFactories = new ArrayList<>();
        private int bufferPersistThreshold = DEFAULT_BUFFER_PERSIST_THRESHOLD;

        public Builder() {}

        @SuppressWarnings("unchecked")
        protected B self() {
            return (B) this;
        }

        public B withMaxOpenFiles(int maxOpenFiles) {
            this.maxOpenFiles = maxOpenFiles;
            return self();
        }

        @SuppressWarnings("unchecked")
        public B withMapFactory(FileSortedMap.FileSortedMapFactory<?,?> mapFactory) {
            this.mapFactory = (FileSortedMap.FileSortedMapFactory<K,V>) mapFactory;
            return self();
        }

        @SuppressWarnings("unchecked")
        public B withComparator(Comparator<?> comparator) {
            this.comparator = (Comparator<K>) comparator;
            return self();
        }

        @SuppressWarnings("unchecked")
        public B withRewriteStrategy(FileSortedMap.RewriteStrategy<?,?> rewriteStrategy) {
            this.rewriteStrategy = (FileSortedMap.RewriteStrategy<K,V>) rewriteStrategy;
            return self();
        }

        public B withNumRetries(int numRetries) {
            this.numRetries = numRetries;
            return self();
        }

        public B withHandlerFactories(List<SortedMapFileHandlerFactory> handlerFactories) {
            this.handlerFactories = handlerFactories;
            return self();
        }

        public B withBufferPersistThreshold(int bufferPersistThreshold) {
            this.bufferPersistThreshold = bufferPersistThreshold;
            return self();
        }

        public BufferedFileBackedSortedMap<?,?> build() throws Exception {
            return new BufferedFileBackedSortedMap<>(this);
        }
    }

    public static Builder<?,?,?> builder() {
        return new Builder<>();
    }

    protected BufferedFileBackedSortedMap(BufferedFileBackedSortedMap<K,V> other) {
        this.comparator = other.comparator;
        this.rewriteStrategy = other.rewriteStrategy;
        this.handlerFactories = new ArrayList<>(other.handlerFactories);
        this.mapFactory = other.mapFactory;
        this.bufferPersistThreshold = other.bufferPersistThreshold;
        this.numRetries = other.numRetries;
        this.maxOpenFiles = other.maxOpenFiles;
        for (SortedMap<K,V> submap : other.map.getMaps()) {
            FileSortedMap<K,V> clone = ((FileSortedMap<K,V>) submap).clone();
            this.map.addMap(clone);
            if (!clone.isPersisted()) {
                this.buffer = clone;
            }
        }
        this.sizeModified = other.sizeModified;
        this.size = other.size;
    }

    protected BufferedFileBackedSortedMap(Builder builder) {
        this.comparator = builder.comparator;
        this.rewriteStrategy = builder.rewriteStrategy;
        this.handlerFactories = new ArrayList<>(builder.handlerFactories);
        this.mapFactory = builder.mapFactory;
        this.bufferPersistThreshold = builder.bufferPersistThreshold;
        this.numRetries = builder.numRetries;
        this.maxOpenFiles = builder.maxOpenFiles;
    }

    private SortedMapFileHandler createFileHandler(SortedMapFileHandlerFactory handlerFactory) throws IOException {
        if (handlerFactory.isValid()) {
            try {
                return handlerFactory.createHandler();
            } catch (IOException e) {
                log.warn("Unable to create file handler using handler factory: " + handlerFactory, e);
            }
        }

        return null;
    }

    public void persist() throws IOException {
        if (buffer != null) {
            // go through the handler factories and try to persist the sorted map
            for (int i = 0; i < handlerFactories.size() && !buffer.isPersisted(); i++) {
                SortedMapFileHandlerFactory handlerFactory = handlerFactories.get(i);
                SortedMapFileHandler handler = createFileHandler(handlerFactory);

                // if we have a valid handler, try to persist
                if (handler != null) {
                    Exception cause = null;
                    for (int attempts = 0; attempts <= numRetries && !buffer.isPersisted(); attempts++) {
                        try {
                            buffer.persist(handler);
                        } catch (IOException e) {
                            if (attempts == numRetries)
                                cause = e;
                        }
                    }

                    if (!buffer.isPersisted()) {
                        log.warn("Unable to persist the sorted map using the file handler: " + handler, cause);

                        // if this was an hdfs file handler, decrement the count
                        if (handlerFactory instanceof HdfsBackedSortedMap.SortedMapHdfsFileHandlerFactory) {
                            HdfsBackedSortedMap.SortedMapHdfsFileHandlerFactory hdfsHandlerFactory = ((HdfsBackedSortedMap.SortedMapHdfsFileHandlerFactory) handlerFactory);
                            hdfsHandlerFactory.mapFileCount(hdfsHandlerFactory.getFileCount() - 1);
                        }
                    }
                } else {
                    log.warn("Unable to create a file handler using the handler factory: " + handlerFactory);
                }
            }

            // if the buffer was not persisted, throw an exception
            if (!buffer.isPersisted())
                throw new IOException("Unable to persist the sorted map using the configured handler factories.");

            buffer = null;
            compact(maxOpenFiles);
        }
    }

    protected List<FileSortedMap<K,V>> getMaps() {
        List<FileSortedMap<K,V>> maps = new ArrayList<>();
        for (SortedMap<K,V> submap : map.getMaps()) {
            maps.add((FileSortedMap<K,V>) submap);
        }
        return maps;
    }

    protected void addMap(FileSortedMap<K,V> submap) {
        map.addMap(submap);
        size += submap.size();
    }

    public boolean hasPersistedData() {
        for (SortedMap<K,V> submap : map.getMaps()) {
            if (((FileSortedMap<K,V>) submap).isPersisted()) {
                return true;
            }
        }
        return false;
    }

    public boolean isPersisted() {
        // we are (completely) persisted iff the buffer is persisted
        return (buffer == null || buffer.isPersisted());
    }

    @Override
    public int size() {
        if (sizeModified) {
            this.size = map.size();
            sizeModified = false;
        }
        return this.size;
    }

    public int getBufferPersistThreshold() {
        return this.bufferPersistThreshold;
    }

    public int getBufferSize() {
        return (this.buffer == null ? 0 : this.buffer.size());
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean containsKey(Object o) {
        // try the cheap operation first
        if (buffer != null && buffer.containsKey(o)) {
            return true;
        } else {
            return map.containsKey(o);
        }
    }

    @Override
    public boolean containsValue(Object value) {
        return false;
    }

    private String printHandlerFactories() {
        return String.join(", ", handlerFactories.stream().map(SortedMapFileHandlerFactory::toString).collect(Collectors.toList()));
    }

    /**
     * If the number of maps is over maxFiles, then start compacting those files down. The goal is to get the number of files down around 50% of maxFiles.
     *
     * @param maxFiles
     *            the max number of files
     * @throws IOException
     *             for IO Exceptions
     */
    public void compact(int maxFiles) throws IOException {
        // if we have more maps than we are allowed, then we need to compact this down
        if (maxFiles > 0 && map.getMaps().size() > maxFiles) {
            if (log.isDebugEnabled()) {
                log.debug("Compacting [" + printHandlerFactories() + "]");
            }
            // create a copy of the map list (sorting below)
            List<SortedMap<K,V>> maps = new ArrayList<>(map.getMaps());

            // calculate the number of maps to compact
            int nummaps = maps.size();
            int excessmaps = nummaps - (maxFiles / 2); // those over 50% of maxFiles
            int mapsPerCompaction = Math.min(excessmaps + 1, nummaps); // Add in 1 to account for the compacted map being added back in

            // sort the maps by size (compact up smaller maps first)
            maps.sort(Comparator.comparing(SortedMap<K,V>::size).reversed());

            // newmap will be the final multimap
            MultiMapBackedSortedMap<K,V> newmap = new MultiMapBackedSortedMap<>();

            // create a map for those maps to be compacted into one file
            MultiMapBackedSortedMap<K,V> mapToCompact = new MultiMapBackedSortedMap<>();
            for (int i = 0; i < mapsPerCompaction; i++) {
                mapToCompact.addMap(maps.remove(maps.size() - 1));
            }

            // compact it
            if (log.isDebugEnabled()) {
                log.debug("Starting compaction for " + mapToCompact);
            }
            long start = System.currentTimeMillis();
            FileSortedMap<K,V> compaction = compact(mapToCompact);
            if (log.isDebugEnabled()) {
                long delta = System.currentTimeMillis() - start;
                log.debug("Compacted " + mapToCompact + " -> " + compaction + " in " + delta + "ms");
            }

            // add the compacted map to our final multimap
            newmap.addMap(compaction);

            // clear the compactions map to remove the files that were compacted
            mapToCompact.clear();

            // now add in the maps we did not compact
            for (int i = 0; i < maps.size(); i++) {
                newmap.addMap(maps.get(i));
            }

            // and replace our map
            this.map = newmap;
        }
    }

    private FileSortedMap<K,V> compact(MultiMapBackedSortedMap<K,V> mapToCompact) throws IOException {
        FileSortedMap<K,V> compactedmap = null;

        // go through the handler factories and try to persist the sorted map
        for (int i = 0; i < handlerFactories.size() && compactedmap == null; i++) {
            SortedMapFileHandlerFactory handlerFactory = handlerFactories.get(i);
            SortedMapFileHandler handler = createFileHandler(handlerFactory);

            // if we have a valid handler, try to persist
            if (handler != null) {
                Exception cause = null;
                for (int attempts = 0; attempts <= numRetries && compactedmap == null; attempts++) {
                    try {
                        compactedmap = mapFactory.newInstance(mapToCompact, handlerFactory.createHandler(), true);
                    } catch (IOException e) {
                        if (attempts == numRetries)
                            cause = e;
                    }
                }

                if (compactedmap == null) {
                    log.warn("Unable to compact the sorted map using the file handler: " + handler, cause);

                    // if this was an hdfs file handler, decrement the count
                    if (handlerFactory instanceof HdfsBackedSortedMap.SortedMapHdfsFileHandlerFactory) {
                        HdfsBackedSortedMap.SortedMapHdfsFileHandlerFactory hdfsHandlerFactory = ((HdfsBackedSortedMap.SortedMapHdfsFileHandlerFactory) handlerFactory);
                        hdfsHandlerFactory.mapFileCount(hdfsHandlerFactory.getFileCount() - 1);
                    }
                }
            } else {
                log.warn("Unable to create a file handler using the handler factory: " + handlerFactory);
            }
        }

        // if the sorted maps were not compacted, throw an exception
        if (compactedmap == null)
            throw new IOException("Unable to persist the sorted map using the configured handler factories.");

        return compactedmap;
    }

    @Override
    public V put(K key, V value) {
        if (buffer == null) {
            try {
                buffer = mapFactory.newInstance(comparator, rewriteStrategy, null, false);
            } catch (Exception ex) {
                throw new IllegalStateException("Unable to create an underlying FileSortedMap", ex);
            }

            map.addMap(buffer);
        }
        V previous = buffer.put(key, value);
        sizeModified = true;
        if (previous != null) {
            if (buffer.size() >= bufferPersistThreshold) {
                try {
                    persist();
                } catch (Exception ex) {
                    throw new IllegalStateException("Unable to persist or compact FileSortedMap", ex);
                }
            }
            return previous;
        }
        return null;
    }

    @Override
    public void putAll(Map<? extends K,? extends V> c) {
        if (buffer == null) {
            try {
                buffer = mapFactory.newInstance(comparator, rewriteStrategy, null, false);
            } catch (Exception ex) {
                throw new IllegalStateException("Unable to create an underlying FileSortedMap", ex);
            }
            map.addMap(buffer);
        }
        buffer.putAll(c);
        sizeModified = true;
        if (buffer.size() >= bufferPersistThreshold) {
            try {
                persist();
            } catch (Exception ex) {
                throw new IllegalStateException("Unable to persist or compact FileSortedMap", ex);
            }
        }
    }

    @Override
    public V remove(Object o) {
        V value = null;
        for (SortedMap<K,V> map : map.getMaps()) {
            FileSortedMap<K,V> filemap = (FileSortedMap<K,V>) map;
            boolean persist = false;
            if (filemap.isPersisted()) {
                try {
                    filemap.load();
                    persist = true;
                } catch (Exception e) {
                    throw new IllegalStateException("Unable to remove item from underlying files", e);
                }
            }

            V testValue = map.remove(o);
            if (testValue != null) {
                if (value != null) {
                    if (rewriteStrategy == null || rewriteStrategy.rewrite((K) o, value, testValue)) {
                        value = testValue;
                    }
                } else {
                    value = testValue;
                }
            }

            if (persist) {
                try {
                    filemap.persist();
                } catch (Exception e) {
                    throw new IllegalStateException("Unable to remove item from underlying files", e);
                }
            }
        }
        if (value != null) {
            this.sizeModified = true;
        }
        return value;
    }

    @Override
    public void clear() {
        // This will cause the MultimapBackedSortedMap to call clear on each map in its map of maps, including the buffer
        // It will also call clear on its map of maps, emptying the contents
        map.clear();
        // Null the buffer so that it will start new on the next add
        buffer = null;
        this.size = 0;
        this.sizeModified = false;
    }

    @Override
    public Comparator<K> comparator() {
        return comparator;
    }

    @Override
    public SortedMap<K,V> subMap(K fromKey, K toKey) {
        return map.subMap(fromKey, toKey);
    }

    @Override
    public SortedMap<K,V> headMap(K toKey) {
        return map.headMap(toKey);
    }

    @Override
    public SortedMap<K,V> tailMap(K fromKey) {
        return map.tailMap(fromKey);
    }

    @Override
    public K firstKey() {
        return map.firstKey();
    }

    @Override
    public K lastKey() {
        return map.lastKey();
    }

    @Override
    public Set<K> keySet() {
        return map.keySet();
    }

    @Override
    public Collection<V> values() {
        return map.values();
    }

    @Override
    public Set<Entry<K,V>> entrySet() {
        return map.entrySet();
    }

    @Override
    public void setRewriteStrategy(FileSortedMap.RewriteStrategy<K,V> rewriteStrategy) {
        this.rewriteStrategy = rewriteStrategy;
    }

    @Override
    public FileSortedMap.RewriteStrategy getRewriteStrategy() {
        return rewriteStrategy;
    }

    @Override
    public V get(Object o) {
        V value = null;
        for (SortedMap<K,V> map : map.getMaps()) {
            V testValue = map.get(o);
            if (testValue != null) {
                if (value != null) {
                    if (rewriteStrategy == null || rewriteStrategy.rewrite((K) o, value, testValue)) {
                        value = testValue;
                    }
                } else {
                    value = testValue;
                }
            }
        }
        return value;
    }

}
