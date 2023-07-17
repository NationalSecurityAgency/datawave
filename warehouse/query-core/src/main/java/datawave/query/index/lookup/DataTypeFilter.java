package datawave.query.index.lookup;

import java.io.IOException;
import java.util.Map;
import java.util.TreeSet;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;

import com.google.common.collect.ImmutableMap;

// column qualifier filter
public class DataTypeFilter extends Filter {
    public static final String TYPES = "dtf.types";

    private TreeSet<ByteSequence> allowed;
    private ImmutableMap<String,String> opts;

    @Override
    public boolean accept(Key k, Value v) {
        return allowed.isEmpty() || allowed.contains(parseDataType(k));
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        allowed = new TreeSet<>();
        opts = ImmutableMap.copyOf(options);
        if (opts.containsKey(TYPES)) {
            for (String type : opts.get(TYPES).split(",")) {
                if (!type.isEmpty()) {
                    Text typeT = new Text(type);
                    allowed.add(new ArrayByteSequence(typeT.getBytes(), 0, typeT.getLength()));
                }
            }
        }
    }

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        DataTypeFilter dtf = new DataTypeFilter();
        try {
            dtf.init(getSource().deepCopy(env), opts, env);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return dtf;
    }

    public static ByteSequence parseDataType(Key k) {
        ByteSequence cq = k.getColumnQualifierData();
        int i = cq.length();
        while (cq.byteAt(--i) != 0x00)
            ;
        return cq.subSequence(++i, cq.length());
    }
}
