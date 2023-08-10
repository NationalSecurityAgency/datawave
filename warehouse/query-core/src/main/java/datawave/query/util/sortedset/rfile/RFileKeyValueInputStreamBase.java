package datawave.query.util.sortedset.rfile;

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

import datawave.query.util.sortedset.FileSortedSet;

public abstract class RFileKeyValueInputStreamBase<E> implements FileSortedSet.SortedSetInputStream<E> {
    private final InputStream inputStream;
    private final long length;
    private Key start;
    private Key end;
    private Scanner reader;
    private Iterator<Map.Entry<Key,Value>> iterator;
    private int size = -1;
    private static final Range ALL = new Range();

    public RFileKeyValueInputStreamBase(InputStream inputStream, long length) throws IOException {
        this.inputStream = inputStream;
        this.length = length;
    }

    public RFileKeyValueInputStreamBase(InputStream inputStream, long length, Key start, Key end) throws IOException {
        this(inputStream, length);
        this.start = start;
        this.end = end;
    }

    public RFileKeyValueInputStreamBase(InputStream inputStream, long length, Map.Entry<Key,Value> start, Map.Entry<Key,Value> end) throws IOException {
        this(inputStream, length);
        this.start = (start == null ? null : start.getKey());
        this.end = (end == null ? null : end.getKey());
    }

    private Iterator<Map.Entry<Key,Value>> keyValueIterator() {
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

    public Map.Entry<Key,Value> readKeyValue() throws IOException {
        if (keyValueIterator().hasNext()) {
            Map.Entry<Key,Value> next = keyValueIterator().next();
            if (RFileKeyOutputStream.SizeKeyUtil.isSizeKey(next.getKey())) {
                size = RFileKeyOutputStream.SizeKeyUtil.getSize(next.getKey());
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
