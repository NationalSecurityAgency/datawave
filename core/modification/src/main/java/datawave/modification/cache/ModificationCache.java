package datawave.modification.cache;

import static datawave.core.common.connection.AccumuloConnectionFactory.Priority;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.modification.configuration.ModificationConfiguration;
import datawave.security.util.ScannerHelper;

public class ModificationCache {
    private static Logger log = LoggerFactory.getLogger(ModificationCache.class);

    private static final Text MODIFICATION_COLUMN = new Text("m");

    private Map<String,Set<String>> cache = new HashMap<>();

    private final AccumuloConnectionFactory connectionFactory;

    private ModificationConfiguration modificationConfiguration;

    public ModificationCache(AccumuloConnectionFactory connectionFactory, ModificationConfiguration modificationConfiguration) {
        this.connectionFactory = connectionFactory;
        this.modificationConfiguration = modificationConfiguration;
        if (modificationConfiguration != null) {
            reloadMutableFieldCache();
        } else {
            log.error("modificationConfiguration was null");
        }
    }

    /**
     * Reload the cache
     */
    public void reloadMutableFieldCache() {
        Map<String,Set<String>> cache = new HashMap<>();
        AccumuloClient client = null;
        BatchScanner s = null;
        try {
            Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
            log.trace("getting mutable list from table {}", this.modificationConfiguration.getTableName());
            log.trace("modificationConfiguration.getPoolName() = {}", modificationConfiguration.getPoolName());
            client = connectionFactory.getClient(null, null, modificationConfiguration.getPoolName(), Priority.ADMIN, trackingMap);
            log.trace("got connection");
            s = ScannerHelper.createBatchScanner(client, this.modificationConfiguration.getTableName(),
                            Collections.singleton(client.securityOperations().getUserAuthorizations(client.whoami())), 8);
            s.setRanges(Collections.singleton(new Range()));
            s.fetchColumnFamily(MODIFICATION_COLUMN);
            for (Entry<Key,Value> e : s) {
                // Field name is in the row and datatype is in the colq.
                String datatype = e.getKey().getColumnQualifier().toString();
                log.trace("datatype = {}", datatype);
                String fieldName = e.getKey().getRow().toString();
                log.trace("fieldname = {}", fieldName);
                if (null == cache.get(datatype))
                    cache.put(datatype, new HashSet<>());
                cache.get(datatype).add(fieldName);
            }
            log.trace("cache size = {}", cache.size());
            for (Entry<String,Set<String>> e : cache.entrySet()) {
                log.trace("datatype = {}, fieldcount = {}", e.getKey(), e.getValue().size());
            }
            // now atomically replace the cache
            this.cache = cache;
        } catch (Exception e) {
            log.error("Error during initialization of ModificationCacheBean", e);
            throw new RuntimeException("Error during initialization of ModificationCacheBean", e);
        } finally {
            if (null != s)
                s.close();
            try {
                connectionFactory.returnClient(client);
            } catch (Exception e) {
                log.error("Error returning connection to pool", e);
            }
        }
    }

    /**
     * List the mutable fields in the cache
     */
    public String listMutableFields() {
        return cache.toString();
    }

    /**
     * Check to see if field for specified datatype is mutable
     *
     * @param datatype
     * @param field
     *            name of field
     * @return true if field is mutable for the given datatype
     */
    public boolean isFieldMutable(String datatype, String field) {
        log.trace("datatype = {}, field = {}", datatype, field);
        return cache.get(datatype).contains(field);
    }

    public Map<String,Set<String>> getCachedMutableFieldList() {
        log.trace("cache = {}", cache);
        return Collections.unmodifiableMap(cache);
    }

    public ModificationConfiguration getModificationConfiguration() {
        return modificationConfiguration;
    }
}
