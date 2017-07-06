package datawave.query.iterator.filter;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.base.Predicate;
import com.google.common.collect.Sets;

public class FieldIndexKeyDataTypeFilter implements Predicate<Key> {
    public static final Logger log = Logger.getLogger(FieldIndexKeyDataTypeFilter.class);
    
    protected ThreadLocal<Text> textBuffer = new ThreadLocal<Text>() {
        @Override
        protected Text initialValue() {
            return new Text();
        }
    };
    protected HashSet<ByteBuffer> patterns;
    
    public FieldIndexKeyDataTypeFilter(@SuppressWarnings("rawtypes") Iterable datatypes) {
        patterns = Sets.newHashSet();
        for (Object obj : datatypes) {
            if (obj instanceof Text) {
                Text t = (Text) obj;
                ByteBuffer bb = ByteBuffer.wrap(t.getBytes(), 0, t.getLength());
                patterns.add(bb);
            } else if (obj instanceof ByteBuffer) {
                ByteBuffer bb = (ByteBuffer) obj;
                bb.rewind();
                patterns.add(bb);
            } else if (obj instanceof byte[]) {
                byte[] array = (byte[]) obj;
                ByteBuffer bb = ByteBuffer.wrap(array);
                patterns.add(bb);
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
    
}
