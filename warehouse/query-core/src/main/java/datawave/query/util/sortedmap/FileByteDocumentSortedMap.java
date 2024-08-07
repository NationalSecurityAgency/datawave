package datawave.query.util.sortedmap;

import java.io.IOException;
import java.util.Comparator;
import java.util.SortedMap;

import org.apache.log4j.Logger;

import datawave.query.attributes.Document;
import datawave.query.util.sortedmap.rfile.RFileByteDocumentInputStream;
import datawave.query.util.sortedmap.rfile.RFileByteDocumentOutputStream;
import datawave.query.util.sortedset.ByteArrayComparator;
import datawave.query.util.sortedset.FileSortedSet;

/**
 * A sorted map that can be persisted into a file and still be read in its persisted state. The map can always be re-loaded and then all operations will work as
 * expected. This will support null contained in the underlying maps iff a comparator is supplied that can handle null values.
 *
 * The persisted file will contain the serialized entries, followed by the actual size.
 *
 */
public class FileByteDocumentSortedMap extends FileSortedMap<byte[],Document> {
    private static Logger log = Logger.getLogger(FileByteDocumentSortedMap.class);

    public final static class DefaultByteComparator implements Comparator<byte[]> {

        @Override
        public int compare(byte[] o1, byte[] o2) {
            return new ByteArrayComparator().compare(o1, o2);
        }
    }

    /**
     * Create a file sorted map from another one
     *
     * @param other
     *            the other sorted map
     */
    public FileByteDocumentSortedMap(FileByteDocumentSortedMap other) {
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
    public FileByteDocumentSortedMap(FileByteDocumentSortedMap other, byte[] from, byte[] to) {
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
    public FileByteDocumentSortedMap(SortedMapFileHandler handler, boolean persisted) {
        this(new DefaultByteComparator(), handler, persisted);
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
    public FileByteDocumentSortedMap(Comparator<byte[]> comparator, SortedMapFileHandler handler, boolean persisted) {
        super((comparator == null ? new DefaultByteComparator() : comparator), new ByteDocumentFileHandler(handler), new Factory(), persisted);
    }

    /**
     * Create an unpersisted sorted map (still in memory)
     *
     * @param map
     *            the sorted map
     * @param handler
     *            the sorted map file handler
     */
    public FileByteDocumentSortedMap(SortedMap<byte[],Document> map, SortedMapFileHandler handler) {
        super(map, new ByteDocumentFileHandler(handler), new Factory());
    }

    /**
     * Create a sorted map out of another sorted map. If persist is true, then the map will be directly persisted using the map's iterator which avoids pulling
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
    public FileByteDocumentSortedMap(SortedMap<byte[],Document> map, SortedMapFileHandler handler, boolean persist) throws IOException {
        super(map, new ByteDocumentFileHandler(handler), new Factory(), persist);
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
        super.persist(new ByteDocumentFileHandler(handler));
    }

    /**
     * Clone this map
     */
    @Override
    public FileByteDocumentSortedMap clone() {
        return (FileByteDocumentSortedMap) super.clone();
    }

    /**
     * A SortedMapfilehandler that can bound the input stream
     */
    public static class ByteDocumentFileHandler implements BoundedTypedSortedMapFileHandler<byte[],Document> {
        SortedMapFileHandler delegate;

        public ByteDocumentFileHandler(SortedMapFileHandler handler) {
            this.delegate = handler;
        }

        @Override
        public SortedMapInputStream<byte[],Document> getInputStream() throws IOException {
            return new RFileByteDocumentInputStream(delegate.getInputStream(), delegate.getSize());
        }

        @Override
        public SortedMapInputStream<byte[],Document> getInputStream(byte[] start, byte[] end) throws IOException {
            return new RFileByteDocumentInputStream(delegate.getInputStream(), delegate.getSize(), start, end);
        }

        @Override
        public SortedMapOutputStream getOutputStream() throws IOException {
            return new RFileByteDocumentOutputStream(delegate.getOutputStream());
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
    public static class Factory implements FileSortedMapFactory<byte[],Document> {

        @Override
        public FileByteDocumentSortedMap newInstance(FileSortedMap<byte[],Document> other) {
            return new FileByteDocumentSortedMap((FileByteDocumentSortedMap) other);
        }

        @Override
        public FileByteDocumentSortedMap newInstance(FileSortedMap<byte[],Document> other, byte[] from, byte[] to) {
            return new FileByteDocumentSortedMap((FileByteDocumentSortedMap) other, from, to);
        }

        @Override
        public FileSortedMap<byte[],Document> newInstance(Comparator<byte[]> comparator, RewriteStrategy<byte[],Document> rewriteStrategy,
                        SortedMapFileHandler handler, boolean persisted) {
            FileByteDocumentSortedMap map = new FileByteDocumentSortedMap(comparator, handler, persisted);
            map.setRewriteStrategy(rewriteStrategy);
            return map;
        }

        @Override
        public FileByteDocumentSortedMap newInstance(SortedMap<byte[],Document> map, SortedMapFileHandler handler) {
            return new FileByteDocumentSortedMap(map, handler);
        }

        @Override
        public FileByteDocumentSortedMap newInstance(SortedMap<byte[],Document> map, SortedMapFileHandler handler, boolean persist) throws IOException {
            return new FileByteDocumentSortedMap(map, handler, persist);
        }
    }
}
