package datawave.query.util.sortedset;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.rfile.RFile;
import org.apache.accumulo.core.client.rfile.RFileSource;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;

public class RFileKeyInputStream implements FileSortedSet.SortedSetInputStream<Key> {
    private final InputStream inputStream;
    private final long length;
    private Key start;
    private Key end;
    private Scanner reader;
    private Iterator<Map.Entry<Key,Value>> iterator;
    private int size = -1;
    private static final Range ALL = new Range();

    public RFileKeyInputStream(InputStream inputStream, long length) throws IOException {
        this.inputStream = inputStream;
        this.length = length;
    }

    public RFileKeyInputStream(InputStream inputStream, long length, Key start, Key end) throws IOException {
        this(inputStream, length);
        this.start = start;
        this.end = end;
    }

    private Iterator<Map.Entry<Key,Value>> iterator() {
        if (iterator == null) {
            Range r = ALL;
            if (start != null || end != null) {
                r = new Range(start, true, end, false);
            }
            reader = RFile.newScanner().from(new RFileSource(inputStream, length)).withBounds(r).withoutSystemIterators().build();
            iterator = reader.iterator();
        }
        return iterator;
    }

    @Override
    public Key readObject() throws IOException {
        if (iterator().hasNext()) {
            Key next = iterator().next().getKey();
            if (RFileKeyOutputStream.SizeKeyUtil.isSizeKey(next)) {
                size = RFileKeyOutputStream.SizeKeyUtil.getSize(next);
                next = null;
            }
            return next;
        }
        return null;
    }

    @Override
    public int readSize() throws IOException {
        if (size < 0) {
            if (iterator != null) {
                throw new IllegalStateException("Cannot read size from undetermined location in stream");
            }
            reader = RFile.newScanner().from(new RFileSource(inputStream, length)).withBounds(new Range(RFileKeyOutputStream.SizeKeyUtil.SIZE_ROW)).build();
            iterator = reader.iterator();
            size = RFileKeyOutputStream.SizeKeyUtil.getSize(iterator.next().getKey());
        }
        return size;
    }

    @Override
    public void close() {
        if (reader != null) {
            reader.close();
            reader = null;
        }
    }
}
