package datawave.ingest.mapreduce;

import java.util.Map;
import java.util.TreeMap;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;

/**
 * The Accumulo table properties represented as a Map.
 */
public class TablePropertiesMap extends TreeMap<String,String> {
    private static final long serialVersionUID = -5825311588298970146L;

    public TablePropertiesMap(TableOperations tops, String tableName) throws AccumuloException, TableNotFoundException {
        for (Map.Entry<String,String> entry : tops.getProperties(tableName)) {
            put(entry.getKey(), entry.getValue());
        }
    }
}
