package datawave.query.util.sortedmap;

import datawave.query.util.sortedmap.rfile.RFileKeyInputStream;
import datawave.query.util.sortedmap.rfile.RFileKeyOutputStream;
import org.apache.accumulo.core.data.Key;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Comparator;

/**
 * A sorted set that can be persisted into a file and still be read in its persisted state. The set can always be re-loaded and then all operations will work as
 * expected. This will support null contained in the underlying sets iff a comparator is supplied that can handle null values.
 *
 * The persisted file will contain the serialized entries, followed by the actual size.
 *
 */
public class FileKeySortedMap extends FileSortedMap<Key> {
    private static Logger log = Logger.getLogger(FileKeySortedMap.class);

    /**
     * Create a file sorted set from another one
     *
     * @param other
     *            the other sorted set
     */
    public FileKeySortedMap(FileKeySortedMap other) {
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
    public FileKeySortedMap(FileKeySortedMap other, Key from, Key to) {
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
    public FileKeySortedMap(SortedSetFileHandler handler, boolean persisted) {
        super(new KeyFileHandler(handler), new Factory(), persisted);
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
    public FileKeySortedMap(Comparator<Key> comparator, SortedSetFileHandler handler, boolean persisted) {
        super(comparator, new KeyFileHandler(handler), new Factory(), persisted);
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
    public FileKeySortedMap(Comparator<Key> comparator, RewriteStrategy<Key> rewriteStrategy, SortedSetFileHandler handler, boolean persisted) {
        super(comparator, rewriteStrategy, new KeyFileHandler(handler), new Factory(), persisted);
    }

    /**
     * Create an unpersisted sorted set (still in memory)
     *
     * @param set
     *            the sorted set
     * @param handler
     *            the sorted set file handler
     */
    public FileKeySortedMap(RewritableSortedSet<Key> set, SortedSetFileHandler handler) {
        super(set, new KeyFileHandler(handler), new Factory());
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
    public FileKeySortedMap(RewritableSortedSet<Key> set, SortedSetFileHandler handler, boolean persist) throws IOException {
        super(set, new KeyFileHandler(handler), new Factory(), persist);
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
        super.persist(new KeyFileHandler(handler));
    }

    /**
     * Clone this set
     */
    @Override
    public FileKeySortedMap clone() {
        return (FileKeySortedMap) super.clone();
    }

    /**
     * A sortedsetfilehandler that can bound the input stream
     */
    public static class KeyFileHandler implements BoundedTypedSortedSetFileHandler<Key> {
        SortedSetFileHandler delegate;

        public KeyFileHandler(SortedSetFileHandler handler) {
            this.delegate = handler;
        }

        @Override
        public SortedSetInputStream<Key> getInputStream() throws IOException {
            return new RFileKeyInputStream(delegate.getInputStream(), delegate.getSize());
        }

        @Override
        public SortedSetInputStream<Key> getInputStream(Key start, Key end) throws IOException {
            return new RFileKeyInputStream(delegate.getInputStream(), delegate.getSize(), start, end);
        }

        @Override
        public SortedSetOutputStream getOutputStream() throws IOException {
            return new RFileKeyOutputStream(delegate.getOutputStream());
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
    public static class Factory implements FileSortedSetFactory<Key> {

        @Override
        public FileKeySortedMap newInstance(FileSortedMap<Key> other) {
            return new FileKeySortedMap((FileKeySortedMap) other);
        }

        @Override
        public FileKeySortedMap newInstance(FileSortedMap<Key> other, Key from, Key to) {
            return new FileKeySortedMap((FileKeySortedMap) other, from, to);
        }

        @Override
        public FileKeySortedMap newInstance(SortedSetFileHandler handler, boolean persisted) {
            return new FileKeySortedMap(handler, persisted);
        }

        @Override
        public FileKeySortedMap newInstance(Comparator<Key> comparator, SortedSetFileHandler handler, boolean persisted) {
            return new FileKeySortedMap(comparator, handler, persisted);
        }

        @Override
        public FileKeySortedMap newInstance(Comparator<Key> comparator, RewriteStrategy<Key> rewriteStrategy, SortedSetFileHandler handler, boolean persisted) {
            return new FileKeySortedMap(comparator, rewriteStrategy, handler, persisted);
        }

        @Override
        public FileKeySortedMap newInstance(RewritableSortedSet<Key> set, SortedSetFileHandler handler) {
            return new FileKeySortedMap(set, handler);
        }

        @Override
        public FileKeySortedMap newInstance(RewritableSortedSet<Key> set, SortedSetFileHandler handler, boolean persist) throws IOException {
            return new FileKeySortedMap(set, handler, persist);
        }
    }
}
