package datawave.query.util.sortedmap;

import datawave.query.attributes.Document;
import datawave.query.util.sortedmap.rfile.RFileByteDocumentInputStream;
import datawave.query.util.sortedmap.rfile.RFileByteDocumentOutputStream;
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
public class FileByteDocumentSortedMap extends FileSortedMap<Map.Entry<byte[],Document>> {
    private static Logger log = Logger.getLogger(FileByteDocumentSortedMap.class);

    public final static class DefaultByteDocumentComparator implements Comparator<Map.Entry<byte[],Document>> {

        @Override
        public int compare(Map.Entry<byte[],Document> o1, Map.Entry<byte[],Document> o2) {
            return new ByteArrayComparator().compare(o1.getKey(), o2.getKey());
        }
    }

    /**
     * Create a file sorted set from another one
     *
     * @param other
     *            the other sorted set
     */
    public FileByteDocumentSortedMap(FileByteDocumentSortedMap other) {
        super(other);
    }

    /**
     * Create a file sorted subset from another one
     *
     * @param other
     *            the other sorted set
     * @param from
     *            the from Document
     * @param to
     *            the to Document
     */
    public FileByteDocumentSortedMap(FileByteDocumentSortedMap other, Map.Entry<byte[],Document> from, Map.Entry<byte[],Document> to) {
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
    public FileByteDocumentSortedMap(SortedSetFileHandler handler, boolean persisted) {
        this(new DefaultByteDocumentComparator(), handler, persisted);
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
    public FileByteDocumentSortedMap(Comparator<Map.Entry<byte[],Document>> comparator, SortedSetFileHandler handler, boolean persisted) {
        super((comparator == null ? new DefaultByteDocumentComparator() : comparator), new ByteDocumentFileHandler(handler),
                        new Factory(), persisted);
    }

    /**
     * Create a persisted sorted set
     *
     * @param comparator
     *            the key comparator
     * @param rewriteStrategy
     *            the rewrite strategy
     * @param handler
     *            the sorted set file handler
     * @param persisted
     *            a persisted boolean flag
     */
    public FileByteDocumentSortedMap(Comparator<Map.Entry<byte[],Document>> comparator, RewriteStrategy<Map.Entry<byte[],Document>> rewriteStrategy,
                                     SortedSetFileHandler handler, boolean persisted) {
        super((comparator == null ? new DefaultByteDocumentComparator() : comparator), rewriteStrategy, new ByteDocumentFileHandler(handler),
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
    public FileByteDocumentSortedMap(RewritableSortedSet<Map.Entry<byte[],Document>> set, SortedSetFileHandler handler) {
        super(set, new ByteDocumentFileHandler(handler), new Factory());
    }

    /**
     * Create a sorted set out of another sorted set. If persist is true, then the set will be directly persisted using the set's iterator which avoids pulling
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
    public FileByteDocumentSortedMap(RewritableSortedSet<Map.Entry<byte[],Document>> set, SortedSetFileHandler handler, boolean persist) throws IOException {
        super(set, new ByteDocumentFileHandler(handler), new Factory(), persist);
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
        super.persist(new ByteDocumentFileHandler(handler));
    }

    /**
     * Clone this set
     */
    @Override
    public FileByteDocumentSortedMap clone() {
        return (FileByteDocumentSortedMap) super.clone();
    }

    /**
     * A sortedsetfilehandler that can bound the input stream
     */
    public static class ByteDocumentFileHandler implements BoundedTypedSortedSetFileHandler<Map.Entry<byte[],Document>> {
        SortedSetFileHandler delegate;

        public ByteDocumentFileHandler(SortedSetFileHandler handler) {
            this.delegate = handler;
        }

        @Override
        public SortedSetInputStream<Map.Entry<byte[],Document>> getInputStream() throws IOException {
            return new RFileByteDocumentInputStream(delegate.getInputStream(), delegate.getSize());
        }

        @Override
        public SortedSetInputStream<Map.Entry<byte[],Document>> getInputStream(Map.Entry<byte[],Document> start, Map.Entry<byte[],Document> end)
                        throws IOException {
            return new RFileByteDocumentInputStream(delegate.getInputStream(), delegate.getSize(), start, end);
        }

        @Override
        public SortedSetOutputStream getOutputStream() throws IOException {
            return new RFileByteDocumentOutputStream(delegate.getOutputStream());
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
    public static class Factory implements FileSortedSetFactory<Map.Entry<byte[],Document>> {

        @Override
        public FileByteDocumentSortedMap newInstance(FileSortedMap<Map.Entry<byte[],Document>> other) {
            return new FileByteDocumentSortedMap((FileByteDocumentSortedMap) other);
        }

        @Override
        public FileByteDocumentSortedMap newInstance(FileSortedMap<Map.Entry<byte[],Document>> other, Map.Entry<byte[],Document> from,
                                                     Map.Entry<byte[],Document> to) {
            return new FileByteDocumentSortedMap((FileByteDocumentSortedMap) other, from, to);
        }

        @Override
        public FileByteDocumentSortedMap newInstance(SortedSetFileHandler handler, boolean persisted) {
            return new FileByteDocumentSortedMap(handler, persisted);
        }

        @Override
        public FileByteDocumentSortedMap newInstance(Comparator<Map.Entry<byte[],Document>> comparator, SortedSetFileHandler handler, boolean persisted) {
            return new FileByteDocumentSortedMap(comparator, handler, persisted);
        }

        @Override
        public FileSortedMap<Map.Entry<byte[],Document>> newInstance(Comparator<Map.Entry<byte[],Document>> comparator,
                                                                     RewriteStrategy<Map.Entry<byte[],Document>> rewriteStrategy, SortedSetFileHandler handler, boolean persisted) {
            return new FileByteDocumentSortedMap(comparator, rewriteStrategy, handler, persisted);
        }

        @Override
        public FileByteDocumentSortedMap newInstance(RewritableSortedSet<Map.Entry<byte[],Document>> set, SortedSetFileHandler handler) {
            return new FileByteDocumentSortedMap(set, handler);
        }

        @Override
        public FileByteDocumentSortedMap newInstance(RewritableSortedSet<Map.Entry<byte[],Document>> set, SortedSetFileHandler handler, boolean persist)
                        throws IOException {
            return new FileByteDocumentSortedMap(set, handler, persist);
        }
    }
}
