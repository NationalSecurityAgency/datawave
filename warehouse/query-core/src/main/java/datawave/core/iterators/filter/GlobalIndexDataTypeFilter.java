package datawave.core.iterators.filter;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.log4j.Logger;

/**
 * The iterator skips entries in the global index for entries not in the specified set of data types
 */
public class GlobalIndexDataTypeFilter extends Filter {

    protected static final Logger log = Logger.getLogger(GlobalIndexDataTypeFilter.class);
    public static final String DATA_TYPE = "data.type.";
    private Set<String> dataTypes = null;

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);

        int i = 1;
        if (options.containsKey(DATA_TYPE + i)) {
            dataTypes = new HashSet<String>();
        }
        while (options.containsKey(DATA_TYPE + i)) {
            dataTypes.add(options.get(DATA_TYPE + i));
            i++;
        }
        if (log.isDebugEnabled()) {
            if (dataTypes != null) {
                log.debug("Set the data type filter to " + dataTypes);
            } else {
                log.debug("No data type filter set");
            }
        }
    }

    @Override
    public boolean accept(Key k, Value v) {
        if (dataTypes == null) {
            return true;
        }

        if (dataTypes.isEmpty()) { // accept none of the things
            return false;
        }

        // The column qualifier contains the shard id and datatype separated by a null byte.
        String dataType = k.getColumnQualifier().toString();
        dataType = dataType.substring(dataType.indexOf('\0') + 1);
        return dataTypes.contains(dataType); // accept only specified datatypes
    }

}
