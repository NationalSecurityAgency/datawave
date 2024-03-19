package datawave.query.util.sortedset;

import java.io.IOException;
import java.util.Comparator;

import org.apache.accumulo.core.data.Key;
import org.apache.log4j.Logger;

import datawave.query.util.sortedset.rfile.RFileKeyInputStream;
import datawave.query.util.sortedset.rfile.RFileKeyOutputStream;

/**
 * A sorted set that can be persisted into a file and still be read in its persisted state. The set can always be re-loaded and then all operations will work as
 * expected. This will support null contained in the underlying sets iff a comparator is supplied that can handle null values.
 *
 * The persisted file will contain the serialized entries, followed by the actual size.
 *
 */
public class FileKeySortedSet extends FileSortedSet<Key> {
    private static Logger log = Logger.getLogger(FileKeySortedSet.class);

    /**
     * Create a file sorted set from another one
     *
     * @param other
     *            the other sorted set
     */
    public FileKeySortedSet(FileKeySortedSet other) {
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
    public FileKeySortedSet(FileKeySortedSet other, Key from, Key to) {
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
    public FileKeySortedSet(SortedSetFileHandler handler, boolean persisted) {
        super(new KeyFileHandler(handler), new FileKeySortedSet.Factory(), persisted);
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
    public FileKeySortedSet(Comparator<Key> comparator, SortedSetFileHandler handler, boolean persisted) {
        super(comparator, new KeyFileHandler(handler), new FileKeySortedSet.Factory(), persisted);
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
    public FileKeySortedSet(Comparator<Key> comparator, RewriteStrategy<Key> rewriteStrategy, SortedSetFileHandler handler, boolean persisted) {
        super(comparator, rewriteStrategy, new KeyFileHandler(handler), new FileKeySortedSet.Factory(), persisted);
    }

    /**
     * Create an unpersisted sorted set (still in memory)
     *
     * @param set
     *            the sorted set
     * @param handler
     *            the sorted set file handler
     */
    public FileKeySortedSet(RewritableSortedSet<Key> set, SortedSetFileHandler handler) {
        super(set, new KeyFileHandler(handler), new FileKeySortedSet.Factory());
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
    public FileKeySortedSet(RewritableSortedSet<Key> set, SortedSetFileHandler handler, boolean persist) throws IOException {
        super(set, new KeyFileHandler(handler), new FileKeySortedSet.Factory(), persist);
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
    public FileKeySortedSet clone() {
        return (FileKeySortedSet) super.clone();
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
        public FileKeySortedSet newInstance(FileSortedSet<Key> other) {
            return new FileKeySortedSet((FileKeySortedSet) other);
        }

        @Override
        public FileKeySortedSet newInstance(FileSortedSet<Key> other, Key from, Key to) {
            return new FileKeySortedSet((FileKeySortedSet) other, from, to);
        }

        @Override
        public FileKeySortedSet newInstance(SortedSetFileHandler handler, boolean persisted) {
            return new FileKeySortedSet(handler, persisted);
        }

        @Override
        public FileKeySortedSet newInstance(Comparator<Key> comparator, SortedSetFileHandler handler, boolean persisted) {
            return new FileKeySortedSet(comparator, handler, persisted);
        }

        @Override
        public FileKeySortedSet newInstance(Comparator<Key> comparator, RewriteStrategy<Key> rewriteStrategy, SortedSetFileHandler handler, boolean persisted) {
            return new FileKeySortedSet(comparator, rewriteStrategy, handler, persisted);
        }

        @Override
        public FileKeySortedSet newInstance(RewritableSortedSet<Key> set, SortedSetFileHandler handler) {
            return new FileKeySortedSet(set, handler);
        }

        @Override
        public FileKeySortedSet newInstance(RewritableSortedSet<Key> set, SortedSetFileHandler handler, boolean persist) throws IOException {
            return new FileKeySortedSet(set, handler, persist);
        }
    }
}
