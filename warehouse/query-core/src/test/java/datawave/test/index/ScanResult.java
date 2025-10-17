package datawave.test.index;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class that provides additional stats beyond simple key value pairs returned
 */
public class ScanResult {

    private static final Logger log = LoggerFactory.getLogger(ScanResult.class);

    public final String tableName;
    public int numKeys = 0;
    public int keyBytes = 0;
    public int valueBytes = 0;
    private final List<Map.Entry<Key,Value>> results = new ArrayList<>();

    public ScanResult(String tableName) {
        this.tableName = tableName;
    }

    public void addResult(Map.Entry<Key,Value> entry) {
        numKeys++;
        keyBytes += entry.getKey().getSize();
        valueBytes += entry.getValue().getSize();
        results.add(entry);
    }

    public List<Map.Entry<Key,Value>> getResults() {
        return results;
    }

    public void printTable() {
        for (Map.Entry<Key,Value> result : results) {
            log.info("{} {}", result.getKey(), result.getValue().getSize());
        }
    }

    @Override
    public String toString() {
        return tableName + " keys:" + numKeys + ", k-size:" + keyBytes + ", v-size:" + valueBytes;
    }
}
