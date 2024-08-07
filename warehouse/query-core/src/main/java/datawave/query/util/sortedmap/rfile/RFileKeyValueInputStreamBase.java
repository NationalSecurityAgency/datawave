package datawave.query.util.sortedmap.rfile;

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

public abstract class RFileKeyValueInputStreamBase {
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
            if (RFileKeyValueOutputStreamBase.SizeKeyUtil.isSizeKey(next.getKey())) {
                size = RFileKeyValueOutputStreamBase.SizeKeyUtil.getSize(next.getKey());
                next = null;
            }
            return next;
        }
        return null;
    }

    public int readSize() throws IOException {
        if (size < 0) {
            if (iterator != null) {
                throw new IllegalStateException("Cannot read size from undetermined location in stream");
            }
            reader = RFile.newScanner().from(new RFileSource(inputStream, length)).withBounds(new Range(RFileKeyValueOutputStreamBase.SizeKeyUtil.SIZE_ROW))
                            .build();
            iterator = reader.iterator();
            size = RFileKeyValueOutputStreamBase.SizeKeyUtil.getSize(iterator.next().getKey());
        }
        return size;
    }

    public void close() {
        if (reader != null) {
            reader.close();
            reader = null;
        }
    }
}
