package datawave.query.util.sortedmap;

import java.io.IOException;
import java.util.Comparator;
import java.util.SortedMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.log4j.Logger;

import datawave.query.util.sortedmap.rfile.RFileKeyValueInputStream;
import datawave.query.util.sortedmap.rfile.RFileKeyValueOutputStream;
import datawave.query.util.sortedset.FileSortedSet;

/**
 * A sorted map that can be persisted into a file and still be read in its persisted state. The map can always be re-loaded and then all operations will work as
 * expected. This will support null contained in the underlying maps iff a comparator is supplied that can handle null values.
 *
 * The persisted file will contain the serialized entries, followed by the actual size.
 *
 */
public class FileKeyValueSortedMap extends FileSortedMap<Key,Value> {
    private static Logger log = Logger.getLogger(FileKeyValueSortedMap.class);

    public static class DefaultKeyComparator implements Comparator<Key> {
        @Override
        public int compare(Key o1, Key o2) {
            return o1.compareTo(o2);
        }
    }

    /**
     * Create a file sorted map from another one
     *
     * @param other
     *            the other sorted map
     */
    public FileKeyValueSortedMap(FileKeyValueSortedMap other) {
        super(other);
    }

    /**
     * Create a file sorted submap from another one
     *
     * @param other
     *            the other sorted map
     * @param from
     *            the from key
     * @param to
     *            the to key
     */
    public FileKeyValueSortedMap(FileKeyValueSortedMap other, Key from, Key to) {
        super(other, from, to);
    }

    /**
     * Create a persisted sorted map
     *
     * @param handler
     *            the sorted map file handler
     * @param persisted
     *            a persisted boolean flag
     */
    public FileKeyValueSortedMap(SortedMapFileHandler handler, boolean persisted) {
        this(new DefaultKeyComparator(), handler, persisted);
    }

    /**
     * Create a persisted sorted map
     *
     * @param comparator
     *            the key comparator
     * @param handler
     *            the sorted map file handler
     * @param persisted
     *            a persisted boolean flag
     */
    public FileKeyValueSortedMap(Comparator<Key> comparator, SortedMapFileHandler handler, boolean persisted) {
        super(((comparator == null) ? new DefaultKeyComparator() : comparator), new KeyValueFileHandler(handler), new Factory(), persisted);
    }

    /**
     * Create an unpersisted sorted map (still in memory)
     *
     * @param map
     *            the sorted map
     * @param handler
     *            the sorted map file handler
     */
    public FileKeyValueSortedMap(SortedMap<Key,Value> map, SortedMapFileHandler handler) {
        super(map, new KeyValueFileHandler(handler), new Factory());
    }

    /**
     * Create a sorted map out of another sorted map. If persist is true, then the map will be directly persisted using the map's iterator which avoid pulling
     * all of its entries into memory at once.
     *
     * @param map
     *            the sorted map
     * @param handler
     *            the sorted map file handler
     * @param persist
     *            boolean flag for persist
     * @throws IOException
     *             for issues with read/write
     */
    public FileKeyValueSortedMap(SortedMap<Key,Value> map, SortedMapFileHandler handler, boolean persist) throws IOException {
        super(map, new KeyValueFileHandler(handler), new Factory(), persist);
    }

    /**
     * This will dump the map to the file, making the map "persisted"
     *
     * @param handler
     *            the sorted map file handler
     * @throws IOException
     *             for issues with read/write
     */
    public void persist(SortedMapFileHandler handler) throws IOException {
        // ensure this handler is wrapped with our handler
        super.persist(new KeyValueFileHandler(handler));
    }

    /**
     * Clone this map
     */
    @Override
    public FileKeyValueSortedMap clone() {
        return (FileKeyValueSortedMap) super.clone();
    }

    /**
     * A SortedMapfilehandler that can bound the input stream
     */
    public static class KeyValueFileHandler implements BoundedTypedSortedMapFileHandler<Key,Value> {
        SortedMapFileHandler delegate;

        public KeyValueFileHandler(SortedMapFileHandler handler) {
            this.delegate = handler;
        }

        @Override
        public SortedMapInputStream<Key,Value> getInputStream() throws IOException {
            return new RFileKeyValueInputStream(delegate.getInputStream(), delegate.getSize());
        }

        @Override
        public SortedMapInputStream<Key,Value> getInputStream(Key start, Key end) throws IOException {
            return new RFileKeyValueInputStream(delegate.getInputStream(), delegate.getSize(), start, end);
        }

        @Override
        public SortedMapOutputStream getOutputStream() throws IOException {
            return new RFileKeyValueOutputStream(delegate.getOutputStream());
        }

        @Override
        public FileSortedSet.PersistOptions getPersistOptions() {
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
     * A factory for these file sorted maps
     */
    public static class Factory implements FileSortedMapFactory<Key,Value> {

        @Override
        public FileKeyValueSortedMap newInstance(FileSortedMap<Key,Value> other) {
            return new FileKeyValueSortedMap((FileKeyValueSortedMap) other);
        }

        @Override
        public FileKeyValueSortedMap newInstance(FileSortedMap<Key,Value> other, Key from, Key to) {
            return new FileKeyValueSortedMap((FileKeyValueSortedMap) other, from, to);
        }

        @Override
        public FileKeyValueSortedMap newInstance(Comparator<Key> comparator, RewriteStrategy<Key,Value> rewriteStategy, SortedMapFileHandler handler,
                        boolean persisted) {
            FileKeyValueSortedMap map = new FileKeyValueSortedMap(comparator, handler, persisted);
            map.setRewriteStrategy(rewriteStategy);
            return map;
        }

        @Override
        public FileKeyValueSortedMap newInstance(SortedMap<Key,Value> map, SortedMapFileHandler handler) {
            return new FileKeyValueSortedMap(map, handler);
        }

        @Override
        public FileKeyValueSortedMap newInstance(SortedMap<Key,Value> map, SortedMapFileHandler handler, boolean persist) throws IOException {
            return new FileKeyValueSortedMap(map, handler, persist);
        }
    }
}
