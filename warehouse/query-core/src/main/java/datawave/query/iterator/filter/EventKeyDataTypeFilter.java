package datawave.query.iterator.filter;

import java.nio.ByteBuffer;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;

public class EventKeyDataTypeFilter extends FieldIndexKeyDataTypeFilter {

    public EventKeyDataTypeFilter(@SuppressWarnings("rawtypes") Iterable datatypes) {
        super(datatypes);
    }

    @Override
    public ByteBuffer extractPattern(byte[] bytes, int offset, int length) {
        // We expect at least one null byte in the array
        if (bytes.length <= 1) {
            return ByteBuffer.wrap(bytes);
        }
        int pos = offset;
        int start = pos;
        for (; pos < length && bytes[pos] != 0; ++pos)
            ;
        int stop = pos;
        return ByteBuffer.wrap(bytes, start, stop - start);
    }

    @Override
    public boolean apply(Key input) {
        return apply(input.getColumnFamily(textBuffer.get()));
    }

    @Override
    public Range getSeekRange(Key current, Key endKey, boolean endKeyInclusive) {
        // not implemented
        return null;
    }

    @Override
    public int getMaxNextCount() {
        // not implemented
        return -1;
    }
}
