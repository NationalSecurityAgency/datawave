package datawave.reggieTests;

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;

public class PrefixFilterIterator extends Filter {

    private String prefix;

    @Override
    public boolean accept(Key k, Value v) {
        return k.getRow().toString().startsWith(prefix);
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        this.prefix = options.get("prefix");
        if (prefix == null) {
            throw new IllegalArgumentException("prefix must be set in options");
        }
    }

    public static void setPrefixOption(Map<String,String> options, String prefix) {
        options.put("prefix", prefix);
    }
}
