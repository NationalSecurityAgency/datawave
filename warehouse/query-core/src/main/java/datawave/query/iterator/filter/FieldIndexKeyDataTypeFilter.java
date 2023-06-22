package datawave.query.iterator.filter;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import datawave.query.data.parsers.DatawaveKey;
import datawave.query.Constants;
import datawave.query.predicate.SeekingFilter;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.base.Predicate;
import com.google.common.collect.Sets;

public class FieldIndexKeyDataTypeFilter implements Predicate<Key>, SeekingFilter {
    public static final Logger log = Logger.getLogger(FieldIndexKeyDataTypeFilter.class);

    protected ThreadLocal<Text> textBuffer = ThreadLocal.withInitial(Text::new);
    protected HashSet<ByteBuffer> patterns;

    /**
     * Maintain a sorted list of the dataTypes this filter is using. Assumes dataTypes will all be UTF-8
     */
    protected TreeSet<String> sortedDataTypes;

    /**
     * The threshold for nextCount before getSeekRange will return a non-null range. Disable feature by setting to -1
     */
    protected int maxNextBeforeSeek = -1;

    /**
     * The number of failed keys since a key was accepted
     */
    private int nextCount = 0;

    public FieldIndexKeyDataTypeFilter(@SuppressWarnings("rawtypes") Iterable datatypes) {
        this(datatypes, -1);
    }

    public FieldIndexKeyDataTypeFilter(@SuppressWarnings("rawtypes") Iterable datatypes, int maxNextBeforeSeek) {
        this.maxNextBeforeSeek = maxNextBeforeSeek;
        patterns = Sets.newHashSet();
        sortedDataTypes = Sets.newTreeSet();
        for (Object obj : datatypes) {
            if (obj instanceof Text) {
                Text t = (Text) obj;
                ByteBuffer bb = ByteBuffer.wrap(t.getBytes(), 0, t.getLength());
                patterns.add(bb);
                sortedDataTypes.add(t.toString());
            } else if (obj instanceof ByteBuffer) {
                ByteBuffer bb = (ByteBuffer) obj;
                bb.rewind();
                patterns.add(bb);
                byte[] buff = new byte[bb.remaining()];
                bb.get(buff);
                sortedDataTypes.add(new String(buff));
            } else if (obj instanceof byte[]) {
                byte[] array = (byte[]) obj;
                ByteBuffer bb = ByteBuffer.wrap(array);
                patterns.add(bb);
                sortedDataTypes.add(new String(array));
            } else {
                throw new IllegalArgumentException("Cannot store data of type " + obj.getClass().getName());
            }
        }
    }

    @Override
    public boolean apply(Key input) {
        return apply(input.getColumnQualifier(textBuffer.get()));
    }

    protected boolean apply(Text t) {
        ByteBuffer bb = extractPattern(t);
        boolean applied = apply(bb);
        if (log.isTraceEnabled()) {
            log.trace("Applying datatype filter to " + t + " -> " + applied);
        }

        // manage tracking the nextCount if necessary
        if (maxNextBeforeSeek != -1) {
            if (applied) {
                // reset the count on a match
                nextCount = 0;
            } else {
                nextCount++;
            }
        }
        return applied;
    }

    protected boolean apply(ByteBuffer bb) {
        return patterns.contains(bb);
    }

    public Set<ByteBuffer> patterns() {
        return Collections.unmodifiableSet(patterns);
    }

    protected ByteBuffer extractPattern(Text text) {
        return extractPattern(text.getBytes(), 0, text.getLength());
    }

    protected ByteBuffer extractPattern(byte[] bytes, int offset, int length) {
        // We expect at least two null bytes in the array
        if (bytes.length <= 2) {
            return ByteBuffer.wrap(bytes);
        }

        // parse from the end to enable handling nasty field values
        int pos = offset + length - 1;
        for (; pos >= 0 && bytes[pos] != 0; pos--)
            ;
        // no null bytes found, assume no datatype
        if (pos < 0) {
            return ByteBuffer.wrap(bytes, 0, 0);
        }
        int stop = pos;
        pos--;
        for (; pos >= 0 && bytes[pos] != 0; pos--)
            ;
        // only one null byte found, assume second half is datatype
        if (pos < 0) {
            return ByteBuffer.wrap(bytes, stop + 1, length - (stop + 1));
        }
        int start = pos + 1;
        // multiple null bytes, assume next to last part is the datatype
        return ByteBuffer.wrap(bytes, start, stop - start);
    }

    @Override
    public Range getSeekRange(Key current, Key endKey, boolean endKeyInclusive) {
        // early return if possible
        if (maxNextBeforeSeek == -1 || nextCount < maxNextBeforeSeek) {
            return null;
        }

        // parse the key to get the value and dataType
        DatawaveKey datawaveKey = new DatawaveKey(current);

        // test if this key should have been accepted
        if (sortedDataTypes.contains(datawaveKey.getDataType())) {
            return null;
        }

        // still here, find the next valid sorted data type and apply it for a new range
        String nextDataType = null;
        for (String dataType : sortedDataTypes) {
            if (dataType.compareTo(datawaveKey.getDataType()) > 0) {
                nextDataType = dataType;
                break;
            }
        }

        // ensure a dataType was selected
        Key startKey;
        boolean inclusiveStart;
        if (nextDataType == null) {
            // roll over the key
            // this will be somewhat blind since the next value is not known
            startKey = new Key(current.getRow(), current.getColumnFamily(),
                            new Text(datawaveKey.getFieldValue() + Constants.NULL_BYTE_STRING + Constants.MAX_UNICODE_STRING));
            inclusiveStart = false;
        } else {
            // generate a new range with the current value and new dataType
            startKey = new Key(current.getRow(), current.getColumnFamily(), new Text(datawaveKey.getFieldValue() + Constants.NULL_BYTE_STRING + nextDataType));
            inclusiveStart = true;
        }

        if (startKey.compareTo(endKey) > 0) {
            // generate an empty range
            return new Range(startKey, false, startKey.followingKey(PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME), false);
        }

        return new Range(startKey, inclusiveStart, endKey, endKeyInclusive);
    }

    @Override
    public int getMaxNextCount() {
        return maxNextBeforeSeek;
    }
}
