package datawave.query.util.sortedset;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.log4j.Logger;

import datawave.query.util.sortedset.rfile.RFileKeyValueInputStream;
import datawave.query.util.sortedset.rfile.RFileKeyValueOutputStream;

/**
 * A sorted set that can be persisted into a file and still be read in its persisted state. The set can always be re-loaded and then all operations will work as
 * expected. This will support null contained in the underlying sets iff a comparator is supplied that can handle null values.
 *
 * The persisted file will contain the serialized entries, followed by the actual size.
 *
 */
public class FileKeyValueSortedSet extends FileSortedSet<Map.Entry<Key,Value>> {
    private static Logger log = Logger.getLogger(FileKeyValueSortedSet.class);

    public static class DefaultKeyValueComparator implements Comparator<Map.Entry<Key,Value>> {

        @Override
        public int compare(Map.Entry<Key,Value> o1, Map.Entry<Key,Value> o2) {
            return o1.getKey().compareTo(o2.getKey());
        }
    }

    /**
     * Create a file sorted set from another one
     *
     * @param other
     *            the other sorted set
     */
    public FileKeyValueSortedSet(FileKeyValueSortedSet other) {
        super(other);
    }

    /**
     * Create a file sorted subset from another one
     *
     * @param other
     *            the other sorted set
     * @param from
     *            the from key
     * @param to
     *            the to key
     */
    public FileKeyValueSortedSet(FileKeyValueSortedSet other, Map.Entry<Key,Value> from, Map.Entry<Key,Value> to) {
        super(other, from, to);
    }

    /**
     * Create a persisted sorted set
     *
     * @param handler
     *            the sorted set file handler
     * @param persisted
     *            a persisted boolean flag
     */
    public FileKeyValueSortedSet(SortedSetFileHandler handler, boolean persisted) {
        this(new DefaultKeyValueComparator(), handler, persisted);
    }

    /**
     * Create a persisted sorted set
     *
     * @param comparator
     *            the key comparator
     * @param handler
     *            the sorted set file handler
     * @param persisted
     *            a persisted boolean flag
     */
    public FileKeyValueSortedSet(Comparator<Map.Entry<Key,Value>> comparator, SortedSetFileHandler handler, boolean persisted) {
        super((comparator == null ? new DefaultKeyValueComparator() : comparator), new KeyValueFileHandler(handler), new FileKeyValueSortedSet.Factory(),
                        persisted);
    }

    /**
     * Create a persisted sorted set
     *
     * @param comparator
     *            the key comparator
     * @param rewriteStrategy
     *            rewrite strategy
     * @param handler
     *            the sorted set file handler
     * @param persisted
     *            a persisted boolean flag
     */
    public FileKeyValueSortedSet(Comparator<Map.Entry<Key,Value>> comparator, RewriteStrategy<Map.Entry<Key,Value>> rewriteStrategy,
                    SortedSetFileHandler handler, boolean persisted) {
        super((comparator == null ? new DefaultKeyValueComparator() : comparator), rewriteStrategy, new KeyValueFileHandler(handler),
                        new FileKeyValueSortedSet.Factory(), persisted);
    }

    /**
     * Create an unpersisted sorted set (still in memory)
     *
     * @param set
     *            the sorted set
     * @param handler
     *            the sorted set file handler
     */
    public FileKeyValueSortedSet(RewritableSortedSet<Map.Entry<Key,Value>> set, SortedSetFileHandler handler) {
        super(set, new KeyValueFileHandler(handler), new FileKeyValueSortedSet.Factory());
    }

    /**
     * Create an sorted set out of another sorted set. If persist is true, then the set will be directly persisted using the set's iterator which avoid pulling
     * all of its entries into memory at once.
     *
     * @param set
     *            the sorted set
     * @param handler
     *            the sorted set file handler
     * @param persist
     *            boolean flag for persist
     * @throws IOException
     *             for issues with read/write
     */
    public FileKeyValueSortedSet(RewritableSortedSet<Map.Entry<Key,Value>> set, SortedSetFileHandler handler, boolean persist) throws IOException {
        super(set, new KeyValueFileHandler(handler), new FileKeyValueSortedSet.Factory(), persist);
    }

    /**
     * This will dump the set to the file, making the set "persisted"
     *
     * @param handler
     *            the sorted set file handler
     * @throws IOException
     *             for issues with read/write
     */
    public void persist(SortedSetFileHandler handler) throws IOException {
        // ensure this handler is wrapped with our handler
        super.persist(new KeyValueFileHandler(handler));
    }

    /**
     * Clone this set
     */
    @Override
    public FileKeyValueSortedSet clone() {
        return (FileKeyValueSortedSet) super.clone();
    }

    /**
     * A sortedsetfilehandler that can bound the input stream
     */
    public static class KeyValueFileHandler implements BoundedTypedSortedSetFileHandler<Map.Entry<Key,Value>> {
        SortedSetFileHandler delegate;

        public KeyValueFileHandler(SortedSetFileHandler handler) {
            this.delegate = handler;
        }

        @Override
        public SortedSetInputStream<Map.Entry<Key,Value>> getInputStream() throws IOException {
            return new RFileKeyValueInputStream(delegate.getInputStream(), delegate.getSize());
        }

        @Override
        public SortedSetInputStream<Map.Entry<Key,Value>> getInputStream(Map.Entry<Key,Value> start, Map.Entry<Key,Value> end) throws IOException {
            return new RFileKeyValueInputStream(delegate.getInputStream(), delegate.getSize(), start, end);
        }

        @Override
        public SortedSetOutputStream getOutputStream() throws IOException {
            return new RFileKeyValueOutputStream(delegate.getOutputStream());
        }

        @Override
        public PersistOptions getPersistOptions() {
            return delegate.getPersistOptions();
        }

        @Override
        public long getSize() {
            return delegate.getSize();
        }

        @Override
        public void deleteFile() {
            delegate.deleteFile();
        }
    }

    /**
     * A factory for these file sorted sets
     */
    public static class Factory implements FileSortedSetFactory<Map.Entry<Key,Value>> {

        @Override
        public FileKeyValueSortedSet newInstance(FileSortedSet<Map.Entry<Key,Value>> other) {
            return new FileKeyValueSortedSet((FileKeyValueSortedSet) other);
        }

        @Override
        public FileKeyValueSortedSet newInstance(FileSortedSet<Map.Entry<Key,Value>> other, Map.Entry<Key,Value> from, Map.Entry<Key,Value> to) {
            return new FileKeyValueSortedSet((FileKeyValueSortedSet) other, from, to);
        }

        @Override
        public FileKeyValueSortedSet newInstance(SortedSetFileHandler handler, boolean persisted) {
            return new FileKeyValueSortedSet(handler, persisted);
        }

        @Override
        public FileKeyValueSortedSet newInstance(Comparator<Map.Entry<Key,Value>> comparator, SortedSetFileHandler handler, boolean persisted) {
            return new FileKeyValueSortedSet(comparator, handler, persisted);
        }

        @Override
        public FileKeyValueSortedSet newInstance(Comparator<Map.Entry<Key,Value>> comparator, RewriteStrategy<Map.Entry<Key,Value>> rewriteStategy,
                        SortedSetFileHandler handler, boolean persisted) {
            return new FileKeyValueSortedSet(comparator, rewriteStategy, handler, persisted);
        }

        @Override
        public FileKeyValueSortedSet newInstance(RewritableSortedSet<Map.Entry<Key,Value>> set, SortedSetFileHandler handler) {
            return new FileKeyValueSortedSet(set, handler);
        }

        @Override
        public FileKeyValueSortedSet newInstance(RewritableSortedSet<Map.Entry<Key,Value>> set, SortedSetFileHandler handler, boolean persist)
                        throws IOException {
            return new FileKeyValueSortedSet(set, handler, persist);
        }
    }
}
