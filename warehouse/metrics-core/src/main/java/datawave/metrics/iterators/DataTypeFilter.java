package datawave.metrics.iterators;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import datawave.metrics.keys.IngestEntryKey;
import datawave.metrics.keys.InvalidKeyException;
import datawave.metrics.util.WritableUtil;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.log4j.Logger;

// column family filter
public class DataTypeFilter extends Filter {
    private static final Logger log = Logger.getLogger(DataTypeFilter.class);

    private HashSet<String> dataTypes = new HashSet<>();

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);

        String typeOpt = options.get("types");
        log.debug("Received type option: " + typeOpt);
        if (typeOpt != null && !typeOpt.isEmpty()) {
            // if we have a JSON array, strip off the "[<data>]"'
            if (typeOpt.charAt(0) == '[' && typeOpt.charAt(typeOpt.length() - 1) == ']') {
                typeOpt = typeOpt.substring(1, typeOpt.length() - 2);
                String[] dtypes = typeOpt.split(",");
                for (String dtype : dtypes) {
                    // remove quotes
                    dtype = dtype.substring(1, dtype.length() - 2);
                    dataTypes.add(dtype);
                }
            } else {
                String[] dtypes = typeOpt.split(",");
                Collections.addAll(dataTypes, dtypes);
            }
        }
    }

    @Override
    public boolean accept(Key k, Value v) {
        try {
            IngestEntryKey iek = new IngestEntryKey(k) {
                @Override
                public void parse(Key k) throws InvalidKeyException {
                    try {
                        this.type = new String(k.getColumnFamily().getBytes(), 0, WritableUtil.findNth(k.getColumnFamily(), 1, (byte) 0x0));
                    } catch (NullPointerException | StringIndexOutOfBoundsException npe) {
                        throw new InvalidKeyException(npe);
                    }

                }
            };
            if (dataTypes.isEmpty()) {
                return true;
            } else {
                return dataTypes.contains(iek.getType());
            }
        } catch (InvalidKeyException npe) {
            if (dataTypes.isEmpty()) {
                return true;
            } else
                return dataTypes.contains(k.getColumnFamily().toString());
        }
    }

    protected Set<String> getTypes() {
        return Collections.unmodifiableSet(this.dataTypes);
    }

}
