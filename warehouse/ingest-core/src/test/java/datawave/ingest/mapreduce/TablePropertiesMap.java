package datawave.ingest.mapreduce;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;

import java.util.Map;
import java.util.TreeMap;

/**
 * The Accumulo table properties represented as a Map.
 */
public class TablePropertiesMap extends TreeMap<String,String> {
    public TablePropertiesMap(TableOperations tops, String tableName) throws AccumuloException, TableNotFoundException {
        for (Map.Entry<String,String> entry : tops.getProperties(tableName)) {
            put(entry.getKey(), entry.getValue());
        }
    }
}
