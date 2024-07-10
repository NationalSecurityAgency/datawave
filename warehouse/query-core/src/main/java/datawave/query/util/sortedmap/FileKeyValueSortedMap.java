package datawave.query.util.sortedmap;

import datawave.query.util.sortedmap.rfile.RFileKeyValueInputStream;
import datawave.query.util.sortedmap.rfile.RFileKeyValueOutputStream;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;

/**
 * A sorted set that can be persisted into a file and still be read in its persisted state. The set can always be re-loaded and then all operations will work as
 * expected. This will support null contained in the underlying sets iff a comparator is supplied that can handle null values.
 *
 * The persisted file will contain the serialized entries, followed by the actual size.
 *
 */
public class FileKeyValueSortedMap extends FileSortedMap<Map.Entry<Key,Value>> {
    private static Logger log = Logger.getLogger(FileKeyValueSortedMap.class);

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
    public FileKeyValueSortedMap(FileKeyValueSortedMap other) {
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
    public FileKeyValueSortedMap(FileKeyValueSortedMap other, Map.Entry<Key,Value> from, Map.Entry<Key,Value> to) {
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
    public FileKeyValueSortedMap(SortedSetFileHandler handler, boolean persisted) {
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
    public FileKeyValueSortedMap(Comparator<Map.Entry<Key,Value>> comparator, SortedSetFileHandler handler, boolean persisted) {
        super((comparator == null ? new DefaultKeyValueComparator() : comparator), new KeyValueFileHandler(handler), new Factory(),
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
    public FileKeyValueSortedMap(Comparator<Map.Entry<Key,Value>> comparator, RewriteStrategy<Map.Entry<Key,Value>> rewriteStrategy,
                                 SortedSetFileHandler handler, boolean persisted) {
        super((comparator == null ? new DefaultKeyValueComparator() : comparator), rewriteStrategy, new KeyValueFileHandler(handler),
                        new Factory(), persisted);
    }

    /**
     * Create an unpersisted sorted set (still in memory)
     *
     * @param set
     *            the sorted set
     * @param handler
     *            the sorted set file handler
     */
    public FileKeyValueSortedMap(RewritableSortedSet<Map.Entry<Key,Value>> set, SortedSetFileHandler handler) {
        super(set, new KeyValueFileHandler(handler), new Factory());
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
    public FileKeyValueSortedMap(RewritableSortedSet<Map.Entry<Key,Value>> set, SortedSetFileHandler handler, boolean persist) throws IOException {
        super(set, new KeyValueFileHandler(handler), new Factory(), persist);
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
    public FileKeyValueSortedMap clone() {
        return (FileKeyValueSortedMap) super.clone();
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
        public FileKeyValueSortedMap newInstance(FileSortedMap<Map.Entry<Key,Value>> other) {
            return new FileKeyValueSortedMap((FileKeyValueSortedMap) other);
        }

        @Override
        public FileKeyValueSortedMap newInstance(FileSortedMap<Map.Entry<Key,Value>> other, Map.Entry<Key,Value> from, Map.Entry<Key,Value> to) {
            return new FileKeyValueSortedMap((FileKeyValueSortedMap) other, from, to);
        }

        @Override
        public FileKeyValueSortedMap newInstance(SortedSetFileHandler handler, boolean persisted) {
            return new FileKeyValueSortedMap(handler, persisted);
        }

        @Override
        public FileKeyValueSortedMap newInstance(Comparator<Map.Entry<Key,Value>> comparator, SortedSetFileHandler handler, boolean persisted) {
            return new FileKeyValueSortedMap(comparator, handler, persisted);
        }

        @Override
        public FileKeyValueSortedMap newInstance(Comparator<Map.Entry<Key,Value>> comparator, RewriteStrategy<Map.Entry<Key,Value>> rewriteStategy,
                                                 SortedSetFileHandler handler, boolean persisted) {
            return new FileKeyValueSortedMap(comparator, rewriteStategy, handler, persisted);
        }

        @Override
        public FileKeyValueSortedMap newInstance(RewritableSortedSet<Map.Entry<Key,Value>> set, SortedSetFileHandler handler) {
            return new FileKeyValueSortedMap(set, handler);
        }

        @Override
        public FileKeyValueSortedMap newInstance(RewritableSortedSet<Map.Entry<Key,Value>> set, SortedSetFileHandler handler, boolean persist)
                        throws IOException {
            return new FileKeyValueSortedMap(set, handler, persist);
        }
    }
}
