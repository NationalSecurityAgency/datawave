package datawave.query.util.sortedset;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import datawave.query.util.sortedset.FileSortedSet.SortedSetFileHandler;

/**
 * This is a sorted set that will hold up to a specified number of entries before flushing the data to disk. Files will be created as needed. An additional
 * "persist" call is supplied to force flushing to disk. The iterator.remove and the subset operations will work up until any buffer has been flushed to disk.
 * After that, those operations will not work as specified by the underlying FileSortedSet.
 *
 * @param <E>
 *            type of the set
 */
public class BufferedFileBackedSortedSet<E> implements RewritableSortedSet<E> {
    private static final Logger log = Logger.getLogger(BufferedFileBackedSortedSet.class);
    protected static final int DEFAULT_BUFFER_PERSIST_THRESHOLD = 1000;
    protected static final int DEFAULT_MAX_OPEN_FILES = 100;
    protected static final int DEFAULT_NUM_RETRIES = 2;

    protected MultiSetBackedSortedSet<E> set = new MultiSetBackedSortedSet<>();
    protected int maxOpenFiles = DEFAULT_MAX_OPEN_FILES;
    protected FileSortedSet<E> buffer = null;
    protected FileSortedSet.FileSortedSetFactory<E> setFactory = null;
    protected final Comparator<E> comparator;
    protected final RewriteStrategy<E> rewriteStrategy;
    protected boolean sizeModified = false;
    protected int size = 0;
    protected int numRetries = DEFAULT_NUM_RETRIES;

    protected List<SortedSetFileHandlerFactory> handlerFactories;
    protected int bufferPersistThreshold = DEFAULT_BUFFER_PERSIST_THRESHOLD;

    /**
     * A factory for SortedSetFileHandlers
     *
     *
     *
     */
    public interface SortedSetFileHandlerFactory {
        SortedSetFileHandler createHandler() throws IOException;

        boolean isValid();
    }

    public static class Builder<B extends Builder<B,E>,E> {
        private int maxOpenFiles = DEFAULT_MAX_OPEN_FILES;
        private FileSortedSet.FileSortedSetFactory<E> setFactory = new FileSerializableSortedSet.Factory();
        private Comparator<E> comparator;
        private RewriteStrategy<E> rewriteStrategy;
        private int numRetries = DEFAULT_NUM_RETRIES;
        private List<SortedSetFileHandlerFactory> handlerFactories = new ArrayList<>();
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

        public B withSetFactory(FileSortedSet.FileSortedSetFactory<E> setFactory) {
            this.setFactory = setFactory;
            return self();
        }

        public B withComparator(Comparator<E> comparator) {
            this.comparator = comparator;
            return self();
        }

        public B withRewriteStrategy(RewriteStrategy<E> rewriteStrategy) {
            this.rewriteStrategy = rewriteStrategy;
            return self();
        }

        public B withNumRetries(int numRetries) {
            this.numRetries = numRetries;
            return self();
        }

        public B withHandlerFactories(List<SortedSetFileHandlerFactory> handlerFactories) {
            this.handlerFactories = handlerFactories;
            return self();
        }

        public B withBufferPersistThreshold(int bufferPersistThreshold) {
            this.bufferPersistThreshold = bufferPersistThreshold;
            return self();
        }

        public BufferedFileBackedSortedSet<E> build() throws Exception {
            return new BufferedFileBackedSortedSet<>(this);
        }
    }

    public BufferedFileBackedSortedSet(BufferedFileBackedSortedSet<E> other) {
        this.comparator = other.comparator;
        this.rewriteStrategy = other.rewriteStrategy;
        this.handlerFactories = new ArrayList<>(other.handlerFactories);
        this.setFactory = other.setFactory;
        this.bufferPersistThreshold = other.bufferPersistThreshold;
        this.numRetries = other.numRetries;
        this.maxOpenFiles = other.maxOpenFiles;
        for (SortedSet<E> subSet : other.set.getSets()) {
            FileSortedSet<E> clone = ((FileSortedSet<E>) subSet).clone();
            this.set.addSet(clone);
            if (!clone.isPersisted()) {
                this.buffer = clone;
            }
        }
        this.sizeModified = other.sizeModified;
        this.size = other.size;
    }

    protected BufferedFileBackedSortedSet(Builder builder) {
        this.comparator = builder.comparator;
        this.rewriteStrategy = builder.rewriteStrategy;
        this.handlerFactories = new ArrayList<>(builder.handlerFactories);
        this.setFactory = builder.setFactory;
        this.bufferPersistThreshold = builder.bufferPersistThreshold;
        this.numRetries = builder.numRetries;
        this.maxOpenFiles = builder.maxOpenFiles;
    }

    private SortedSetFileHandler createFileHandler(SortedSetFileHandlerFactory handlerFactory) throws IOException {
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
            // go through the handler factories and try to persist the sorted set
            for (int i = 0; i < handlerFactories.size() && !buffer.isPersisted(); i++) {
                SortedSetFileHandlerFactory handlerFactory = handlerFactories.get(i);
                SortedSetFileHandler handler = createFileHandler(handlerFactory);

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
                        log.warn("Unable to persist the sorted set using the file handler: " + handler, cause);

                        // if this was an hdfs file handler, decrement the count
                        if (handlerFactory instanceof HdfsBackedSortedSet.SortedSetHdfsFileHandlerFactory) {
                            HdfsBackedSortedSet.SortedSetHdfsFileHandlerFactory hdfsHandlerFactory = ((HdfsBackedSortedSet.SortedSetHdfsFileHandlerFactory) handlerFactory);
                            hdfsHandlerFactory.setFileCount(hdfsHandlerFactory.getFileCount() - 1);
                        }
                    }
                } else {
                    log.warn("Unable to create a file handler using the handler factory: " + handlerFactory);
                }
            }

            // if the buffer was not persisted, throw an exception
            if (!buffer.isPersisted())
                throw new IOException("Unable to persist the sorted set using the configured handler factories.");

            buffer = null;
            compact(maxOpenFiles);
        }
    }

    protected List<FileSortedSet<E>> getSets() {
        List<FileSortedSet<E>> sets = new ArrayList<>();
        for (SortedSet<E> subSet : set.getSets()) {
            sets.add((FileSortedSet<E>) subSet);
        }
        return sets;
    }

    protected void addSet(FileSortedSet<E> subSet) {
        set.addSet(subSet);
        size += subSet.size();
    }

    public boolean hasPersistedData() {
        for (SortedSet<E> subSet : set.getSets()) {
            if (((FileSortedSet<E>) subSet).isPersisted()) {
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
            this.size = set.size();
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
    public boolean contains(Object o) {
        // try the cheap operation first
        if (buffer != null && buffer.contains(o)) {
            return true;
        } else {
            return set.contains(o);
        }
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        // try the cheap operation first
        if (buffer != null && buffer.containsAll(c)) {
            return true;
        } else {
            return set.containsAll(c);
        }
    }

    @Override
    public Iterator<E> iterator() {
        // first lets compact down the sets if needed
        try {
            // if we have any persisted sets, then ensure we are persisted
            if (set.getSets().size() > 1) {
                persist();
            }
        } catch (IOException ioe) {
            throw new RuntimeException("Unable to persist or compact file backed sorted set", ioe);
        }
        return set.iterator();
    }

    private String printHandlerFactories() {
        return String.join(", ", handlerFactories.stream().map(SortedSetFileHandlerFactory::toString).collect(Collectors.toList()));
    }

    /**
     * If the number of sets is over maxFiles, then start compacting those files down. The goal is to get the number of files down around 50% of maxFiles.
     *
     * @param maxFiles
     *            the max number of files
     * @throws IOException
     *             for IO Exceptions
     */
    public void compact(int maxFiles) throws IOException {
        // if we have more sets than we are allowed, then we need to compact this down
        if (maxFiles > 0 && set.getSets().size() > maxFiles) {
            if (log.isDebugEnabled()) {
                log.debug("Compacting [" + printHandlerFactories() + "]");
            }
            // create a copy of the set list (sorting below)
            List<SortedSet<E>> sets = new ArrayList<>(set.getSets());

            // calculate the number of sets to compact
            int numSets = sets.size();
            int excessSets = numSets - (maxFiles / 2); // those over 50% of maxFiles
            int setsPerCompaction = Math.min(excessSets + 1, numSets); // Add in 1 to account for the compacted set being added back in

            // sort the sets by size (compact up smaller sets first)
            sets.sort(Comparator.comparing(SortedSet<E>::size).reversed());

            // newSet will be the final multiset
            MultiSetBackedSortedSet<E> newSet = new MultiSetBackedSortedSet<>();

            // create a set for those sets to be compacted into one file
            MultiSetBackedSortedSet<E> setToCompact = new MultiSetBackedSortedSet<>();
            for (int i = 0; i < setsPerCompaction; i++) {
                setToCompact.addSet(sets.remove(sets.size() - 1));
            }

            // compact it
            if (log.isDebugEnabled()) {
                log.debug("Starting compaction for " + setToCompact);
            }
            long start = System.currentTimeMillis();
            FileSortedSet<E> compaction = compact(setToCompact);
            if (log.isDebugEnabled()) {
                long delta = System.currentTimeMillis() - start;
                log.debug("Compacted " + setToCompact + " -> " + compaction + " in " + delta + "ms");
            }

            // add the compacted set to our final multiset
            newSet.addSet(compaction);

            // clear the compactions set to remove the files that were compacted
            setToCompact.clear();

            // now add in the sets we did not compact
            for (int i = 0; i < sets.size(); i++) {
                newSet.addSet(sets.get(i));
            }

            // and replace our set
            this.set = newSet;
        }
    }

    private FileSortedSet<E> compact(MultiSetBackedSortedSet<E> setToCompact) throws IOException {
        FileSortedSet<E> compactedSet = null;

        // go through the handler factories and try to persist the sorted set
        for (int i = 0; i < handlerFactories.size() && compactedSet == null; i++) {
            SortedSetFileHandlerFactory handlerFactory = handlerFactories.get(i);
            SortedSetFileHandler handler = createFileHandler(handlerFactory);

            // if we have a valid handler, try to persist
            if (handler != null) {
                Exception cause = null;
                for (int attempts = 0; attempts <= numRetries && compactedSet == null; attempts++) {
                    try {
                        compactedSet = setFactory.newInstance(setToCompact, handlerFactory.createHandler(), true);
                    } catch (IOException e) {
                        if (attempts == numRetries)
                            cause = e;
                    }
                }

                if (compactedSet == null) {
                    log.warn("Unable to compact the sorted set using the file handler: " + handler, cause);

                    // if this was an hdfs file handler, decrement the count
                    if (handlerFactory instanceof HdfsBackedSortedSet.SortedSetHdfsFileHandlerFactory) {
                        HdfsBackedSortedSet.SortedSetHdfsFileHandlerFactory hdfsHandlerFactory = ((HdfsBackedSortedSet.SortedSetHdfsFileHandlerFactory) handlerFactory);
                        hdfsHandlerFactory.setFileCount(hdfsHandlerFactory.getFileCount() - 1);
                    }
                }
            } else {
                log.warn("Unable to create a file handler using the handler factory: " + handlerFactory);
            }
        }

        // if the sorted sets were not compacted, throw an exception
        if (compactedSet == null)
            throw new IOException("Unable to persist the sorted set using the configured handler factories.");

        return compactedSet;
    }

    @Override
    public Object[] toArray() {
        return set.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return set.toArray(a);
    }

    @Override
    public boolean add(E e) {
        if (buffer == null) {
            try {
                buffer = setFactory.newInstance(comparator, rewriteStrategy, null, false);
            } catch (Exception ex) {
                throw new IllegalStateException("Unable to create an underlying FileSortedSet", ex);
            }

            set.addSet(buffer);
        }
        if (buffer.add(e)) {
            sizeModified = true;
            if (buffer.size() >= bufferPersistThreshold) {
                try {
                    persist();
                } catch (Exception ex) {
                    throw new IllegalStateException("Unable to persist or compact FileSortedSet", ex);
                }
            }
            return true;
        }
        return false;
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        if (buffer == null) {
            try {
                buffer = setFactory.newInstance(comparator, rewriteStrategy, null, false);
            } catch (Exception ex) {
                throw new IllegalStateException("Unable to create an underlying FileSortedSet", ex);
            }
            set.addSet(buffer);
        }
        if (buffer.addAll(c)) {
            sizeModified = true;
            if (buffer.size() >= bufferPersistThreshold) {
                try {
                    persist();
                } catch (Exception ex) {
                    throw new IllegalStateException("Unable to persist or compact FileSortedSet", ex);
                }
            }
            return true;
        }
        return false;
    }

    @Override
    public boolean remove(Object o) {
        boolean removed = false;
        for (SortedSet<E> subSet : set.getSets()) {
            FileSortedSet<E> fileSet = (FileSortedSet<E>) subSet;
            if (fileSet.contains(o)) {
                if (fileSet.isPersisted()) {
                    try {
                        fileSet.load();
                        if (fileSet.remove(o)) {
                            removed = true;
                            fileSet.persist();
                        } else {
                            fileSet.unload();
                            // since we checked for containership first, remove should have returned true
                            throw new IllegalStateException("FileSet contains object but failed to remove it from persisted set");
                        }
                    } catch (Exception e) {
                        throw new IllegalStateException("Unable to remove item from underlying files", e);
                    }
                } else {
                    if (fileSet.remove(o)) {
                        removed = true;
                    } else {
                        // since we checked for containership first, remove should have returned true
                        throw new IllegalStateException("FileSet contains object but failed to remove it");
                    }
                }
            }
        }
        if (removed) {
            this.sizeModified = true;
        }
        return removed;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        boolean modified = false;
        for (SortedSet<E> subSet : set.getSets()) {
            FileSortedSet<E> fileSet = (FileSortedSet<E>) subSet;
            if (fileSet.isPersisted()) {
                try {
                    fileSet.load();
                    if (fileSet.retainAll(c)) {
                        modified = true;
                        fileSet.persist();
                    } else {
                        fileSet.unload();
                    }
                } catch (Exception e) {
                    throw new IllegalStateException("Unable to remove item from underlying files", e);
                }
            } else {
                if (fileSet.retainAll(c)) {
                    modified = true;
                }
            }
        }
        if (modified) {
            this.sizeModified = true;
        }
        return modified;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        boolean modified = false;
        for (SortedSet<E> subSet : set.getSets()) {
            FileSortedSet<E> fileSet = (FileSortedSet<E>) subSet;
            if (fileSet.isPersisted()) {
                try {
                    fileSet.load();
                    if (fileSet.removeAll(c)) {
                        modified = true;
                        fileSet.persist();
                    } else {
                        fileSet.unload();
                    }
                } catch (Exception e) {
                    throw new IllegalStateException("Unable to remove item from underlying files", e);
                }
            } else {
                if (fileSet.removeAll(c)) {
                    modified = true;
                }
            }
        }
        if (modified) {
            this.sizeModified = true;
        }
        return modified;
    }

    @Override
    public boolean removeIf(Predicate<? super E> filter) {
        boolean removed = false;
        for (SortedSet<E> subSet : set.getSets()) {
            FileSortedSet<E> fileSet = (FileSortedSet<E>) subSet;
            if (fileSet.isPersisted()) {
                try {
                    fileSet.load();
                    if (fileSet.removeIf(filter)) {
                        removed = true;
                        fileSet.persist();
                    } else {
                        fileSet.unload();
                    }
                } catch (Exception e) {
                    throw new IllegalStateException("Unable to remove item from underlying files", e);
                }
            } else {
                if (fileSet.removeIf(filter)) {
                    removed = true;
                }
            }
        }
        if (removed) {
            this.sizeModified = true;
        }
        return removed;
    }

    @Override
    public void clear() {
        // This will cause the MultiSetBackedSortedSet to call clear on each Set in its Set of Sets, including the buffer
        // It will also call clear on its Set of Sets, emptying the contents
        set.clear();
        // Null the buffer so that it will start new on the next add
        buffer = null;
        this.size = 0;
        this.sizeModified = false;
    }

    @Override
    public Comparator<? super E> comparator() {
        return comparator;
    }

    @Override
    public RewriteStrategy getRewriteStrategy() {
        return rewriteStrategy;
    }

    @Override
    public E get(E e) {
        return null;
    }

    @Override
    public RewritableSortedSet<E> subSet(E fromElement, E toElement) {
        return set.subSet(fromElement, toElement);
    }

    @Override
    public RewritableSortedSet<E> headSet(E toElement) {
        return set.headSet(toElement);
    }

    @Override
    public RewritableSortedSet<E> tailSet(E fromElement) {
        return set.tailSet(fromElement);
    }

    @Override
    public E first() {
        return set.first();
    }

    @Override
    public E last() {
        return set.last();
    }
}
